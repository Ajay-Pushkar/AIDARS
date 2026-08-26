"""E2E Test Infrastructure and Fixtures for AIDAR Milestone 5 (Distributed Asset Layer v1).

This module provides mock/in-memory workers, coordinator instances, temporary CAS directories,
sample binary payloads of various sizes (100B, 1 KiB, 1 MiB, 5 MiB, 10 MiB, 50 MiB),
and simulated network latency / fault injectors.
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import math
import os
import random
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO, Callable, Dict, Generator, Iterable, Iterator, List, Optional, Set, Tuple, Union

import pytest
from pydantic import BaseModel, Field

# Check if production implementation is available
try:
    import aidars.distributed.models as real_models
    import aidars.distributed.registry as real_registry
    import aidars.distributed.prioritizer as real_prioritizer
    import aidars.distributed.cas_adapter as real_cas_adapter
    import aidars.distributed.transfer as real_transfer
    import aidars.distributed.coordinator as real_coordinator
    import aidars.distributed.worker as real_worker
    import aidars.distributed.metrics as real_metrics
    HAS_REAL_DISTRIBUTED = True
except ImportError:
    HAS_REAL_DISTRIBUTED = False


# ============================================================================
# 1. Pydantic Models & Interface Contracts
# ============================================================================

class WorkerCapabilities(BaseModel):
    can_serve_cas: bool = True
    max_concurrent_streams: int = 16
    bandwidth_limit_mbps: Optional[float] = None


class WorkerInfo(BaseModel):
    worker_id: str
    endpoint_url: str
    ip_address: str
    port: int
    capacity_bytes: int = 100 * 1024 * 1024 * 1024  # 100 GiB
    used_bytes: int = 0
    capabilities: WorkerCapabilities = Field(default_factory=WorkerCapabilities)
    inventory_hashes: Set[str] = Field(default_factory=set)
    last_heartbeat_utc: float = Field(default_factory=time.time)
    estimated_rtt_ms: float = 0.0
    status: str = "ACTIVE"  # ACTIVE, DEGRADED, UNHEALTHY, OFFLINE


class HeartbeatPayload(BaseModel):
    worker_id: str
    timestamp_utc: float = Field(default_factory=time.time)
    active_transfers: int = 0
    storage_available_bytes: int = 50 * 1024 * 1024 * 1024
    cpu_percent: float = 0.0
    ram_percent: float = 0.0


class CandidateSource(BaseModel):
    worker_id: str
    endpoint_url: str
    locality_tier: str  # "loopback", "subnet", "lan", "wan"
    estimated_rtt_ms: float
    load_factor: float
    score: float = 0.0


class LocateAssetsRequest(BaseModel):
    requester_worker_id: str
    missing_hashes: List[str]


class LocateAssetsResponse(BaseModel):
    locations: Dict[str, List[CandidateSource]]  # hash -> ordered candidates
    unresolved_hashes: List[str] = Field(default_factory=list)


@dataclass
class TransferMetrics:
    total_requested_assets: int = 0
    local_cache_hits: int = 0
    network_transferred_assets: int = 0
    total_requested_bytes: int = 0
    local_cache_hit_bytes: int = 0
    network_transferred_bytes: int = 0
    byte_hit_ratio: float = 0.0
    network_savings_percent: float = 0.0
    average_transfer_throughput_mbps: float = 0.0
    resumption_events: int = 0
    failover_events: int = 0

    def compute_ratios(self) -> None:
        if self.total_requested_bytes > 0:
            self.byte_hit_ratio = round(self.local_cache_hit_bytes / self.total_requested_bytes, 4)
            self.network_savings_percent = round(self.byte_hit_ratio * 100.0, 2)
        else:
            self.byte_hit_ratio = 1.0 if self.local_cache_hits > 0 else 0.0
            self.network_savings_percent = self.byte_hit_ratio * 100.0


# ============================================================================
# 2. Mock CAS Adapter
# ============================================================================

class MockCASAdapter:
    """In-memory and filesystem-backed Content Addressed Storage adapter."""

    def __init__(self, cas_dir: Path | str) -> None:
        self.cas_dir = Path(cas_dir)
        self.objects_dir = self.cas_dir / "objects"
        self.staging_dir = self.cas_dir / "staging"
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    def _get_path_for_hash(self, sha256_hex: str) -> Path:
        normalized = sha256_hex.lower()
        shard = normalized[:2]
        return self.objects_dir / shard / normalized

    def has_asset(self, sha256_hex: str) -> bool:
        path = self._get_path_for_hash(sha256_hex)
        return path.is_file() and path.stat().st_size >= 0

    def get_missing_hashes(self, required_hashes: Iterable[str]) -> Set[str]:
        return {h for h in required_hashes if not self.has_asset(h)}

    def get_asset_path(self, sha256_hex: str) -> Optional[Path]:
        path = self._get_path_for_hash(sha256_hex)
        return path if path.is_file() else None

    def get_asset_size(self, sha256_hex: str) -> Optional[int]:
        path = self.get_asset_path(sha256_hex)
        return path.stat().st_size if path else None

    def list_cached_hashes(self) -> Set[str]:
        hashes = set()
        if self.objects_dir.exists():
            for shard in self.objects_dir.iterdir():
                if shard.is_dir():
                    for obj in shard.iterdir():
                        if obj.is_file():
                            hashes.add(obj.name)
        return hashes

    def open_asset_stream(self, sha256_hex: str, offset: int = 0) -> BinaryIO:
        if offset < 0:
            raise OSError(f"Negative seek offset {offset} is invalid")
        path = self.get_asset_path(sha256_hex)
        if not path:
            raise FileNotFoundError(f"Asset {sha256_hex} not found in CAS")
        handle = path.open("rb")
        if offset > 0:
            handle.seek(offset)
        return handle

    def store_bytes(self, data: bytes) -> str:
        sha256_hex = hashlib.sha256(data).hexdigest()
        path = self._get_path_for_hash(sha256_hex)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_staging = self.staging_dir / f"store_{uuid.uuid4().hex}.tmp"
        tmp_staging.write_bytes(data)
        os.replace(tmp_staging, path)
        return sha256_hex

    def commit_staged_file(self, staged_file_path: Path, expected_sha256: str) -> bool:
        if not staged_file_path.exists():
            return False
        hasher = hashlib.sha256()
        with staged_file_path.open("rb") as f:
            while chunk := f.read(1024 * 1024):
                hasher.update(chunk)
        actual_hash = hasher.hexdigest().lower()
        if actual_hash != expected_sha256.lower():
            if staged_file_path.exists():
                staged_file_path.unlink()
            return False

        target_path = self._get_path_for_hash(expected_sha256)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged_file_path, target_path)
        return True

    def prune_staging(self) -> int:
        count = 0
        if self.staging_dir.exists():
            for f in self.staging_dir.iterdir():
                if f.is_file():
                    try:
                        f.unlink()
                        count += 1
                    except OSError:
                        pass
        return count


# ============================================================================
# 3. Candidate Prioritizer
# ============================================================================

class MockCandidatePrioritizer:
    LOCALITY_WEIGHTS = {
        "loopback": 1000.0,
        "subnet": 500.0,
        "lan": 200.0,
        "wan": 50.0,
    }

    def __init__(self, ema_alpha: float = 0.3) -> None:
        self.ema_alpha = ema_alpha
        self.rtt_cache: Dict[str, float] = {}

    def classify_locality(self, requester_ip: str, candidate_ip: str) -> str:
        if (requester_ip.startswith("127.") or requester_ip in ("::1", "localhost")) and \
           (candidate_ip.startswith("127.") or candidate_ip in ("::1", "localhost")):
            return "loopback"
        if requester_ip == candidate_ip:
            return "loopback"

        r_parts = requester_ip.split(".")
        c_parts = candidate_ip.split(".")
        if len(r_parts) == 4 and len(c_parts) == 4:
            if r_parts[:3] == c_parts[:3]:
                return "subnet"
            if (r_parts[0] == "10" and c_parts[0] == "10") or                (r_parts[0] == "192" and r_parts[1] == "168" and c_parts[0] == "192" and c_parts[1] == "168") or                (r_parts[0] == "172" and 16 <= int(r_parts[1]) <= 31 and c_parts[0] == "172" and 16 <= int(c_parts[1]) <= 31):
                return "lan"

        return "wan"

    def update_rtt(self, worker_id: str, sample_ms: float) -> float:
        if worker_id not in self.rtt_cache:
            self.rtt_cache[worker_id] = sample_ms
        else:
            prev = self.rtt_cache[worker_id]
            self.rtt_cache[worker_id] = (self.ema_alpha * sample_ms) + ((1.0 - self.ema_alpha) * prev)
        return self.rtt_cache[worker_id]

    def rank_candidates(
        self,
        requester_ip: str,
        requester_id: str,
        candidates: List[WorkerInfo],
        penalties: Optional[Dict[str, float]] = None,
    ) -> List[CandidateSource]:
        penalties = penalties or {}
        results: List[CandidateSource] = []

        for w in candidates:
            if w.worker_id == requester_id or w.status == "OFFLINE":
                continue

            tier = self.classify_locality(requester_ip, w.ip_address)
            loc_score = self.LOCALITY_WEIGHTS.get(tier, 50.0)
            rtt = self.rtt_cache.get(w.worker_id, w.estimated_rtt_ms)
            load = (w.used_bytes / w.capacity_bytes) if w.capacity_bytes > 0 else 0.0
            penalty = penalties.get(w.worker_id, 0.0)

            score = loc_score - (2.0 * rtt) - (50.0 * load) - (100.0 * penalty)
            if w.status == "UNHEALTHY":
                score -= 500.0

            results.append(CandidateSource(
                worker_id=w.worker_id,
                endpoint_url=w.endpoint_url,
                locality_tier=tier,
                estimated_rtt_ms=rtt,
                load_factor=load,
                score=score,
            ))

        results.sort(key=lambda c: c.score, reverse=True)
        return results


# ============================================================================
# 4. Worker Registry & Coordinator
# ============================================================================

class MockWorkerRegistry:
    def __init__(self, prioritizer: Optional[MockCandidatePrioritizer] = None) -> None:
        self.workers: Dict[str, WorkerInfo] = {}
        self.asset_index: Dict[str, Set[str]] = {}
        self.penalties: Dict[str, float] = {}
        self.prioritizer = prioritizer or MockCandidatePrioritizer()

    def register(self, worker: WorkerInfo, initial_hashes: Optional[Set[str]] = None) -> bool:
        self.workers[worker.worker_id] = worker
        initial_hashes = initial_hashes or worker.inventory_hashes
        for h in initial_hashes:
            norm_h = h.lower()
            self.asset_index.setdefault(norm_h, set()).add(worker.worker_id)
            worker.inventory_hashes.add(norm_h)
        return True

    def heartbeat(self, worker_id: str, payload: Optional[HeartbeatPayload] = None) -> bool:
        if worker_id not in self.workers:
            return False
        w = self.workers[worker_id]
        w.last_heartbeat_utc = payload.timestamp_utc if payload else time.time()
        w.status = "ACTIVE"
        return True

    def update_inventory(self, worker_id: str, added: Optional[Set[str]] = None, removed: Optional[Set[str]] = None) -> None:
        if worker_id not in self.workers:
            return
        w = self.workers[worker_id]
        if added:
            for h in added:
                norm_h = h.lower()
                w.inventory_hashes.add(norm_h)
                self.asset_index.setdefault(norm_h, set()).add(worker_id)
        if removed:
            for h in removed:
                norm_h = h.lower()
                w.inventory_hashes.discard(norm_h)
                if norm_h in self.asset_index:
                    self.asset_index[norm_h].discard(worker_id)
                    if not self.asset_index[norm_h]:
                        del self.asset_index[norm_h]

    def locate_asset(self, sha256: str) -> Set[str]:
        return self.asset_index.get(sha256.lower(), set()).copy()

    def record_worker_error(self, worker_id: str, penalty_amount: float = 1.0) -> None:
        self.penalties[worker_id] = self.penalties.get(worker_id, 0.0) + penalty_amount

    def decay_penalties(self, factor: float = 0.9) -> None:
        for w_id in list(self.penalties.keys()):
            self.penalties[w_id] *= factor
            if self.penalties[w_id] < 0.01:
                del self.penalties[w_id]

    def prune_stale_workers(self, unhealthy_timeout: float = 15.0, eviction_timeout: float = 45.0, current_time: Optional[float] = None) -> Dict[str, List[str]]:
        now = current_time if current_time is not None else time.time()
        unhealthy: List[str] = []
        evicted: List[str] = []

        for w_id, w in list(self.workers.items()):
            delta = now - w.last_heartbeat_utc
            if delta > eviction_timeout:
                evicted.append(w_id)
                for h in list(w.inventory_hashes):
                    if h in self.asset_index:
                        self.asset_index[h].discard(w_id)
                        if not self.asset_index[h]:
                            del self.asset_index[h]
                del self.workers[w_id]
            elif delta > unhealthy_timeout:
                w.status = "UNHEALTHY"
                unhealthy.append(w_id)

        return {"unhealthy": unhealthy, "evicted": evicted}


class MockCoordinator:
    def __init__(self, coordinator_id: str = "coord-01", prioritizer: Optional[MockCandidatePrioritizer] = None) -> None:
        self.coordinator_id = coordinator_id
        self.prioritizer = prioritizer or MockCandidatePrioritizer()
        self.registry = MockWorkerRegistry(self.prioritizer)
        self.metrics = TransferMetrics()

    def handle_register(self, worker: WorkerInfo, initial_hashes: Optional[Set[str]] = None) -> Dict[str, Any]:
        success = self.registry.register(worker, initial_hashes)
        return {
            "status": "registered" if success else "failed",
            "coordinator_id": self.coordinator_id,
            "heartbeat_interval_seconds": 5.0,
            "heartbeat_timeout_seconds": 15.0,
            "registered_at": time.time(),
        }

    def handle_heartbeat(self, worker_id: str, payload: HeartbeatPayload) -> Dict[str, Any]:
        success = self.registry.heartbeat(worker_id, payload)
        return {
            "status": "healthy" if success else "unknown_worker",
            "acknowledged_at": time.time(),
        }

    def handle_locate(self, req: LocateAssetsRequest, requester_ip: str = "127.0.0.1") -> LocateAssetsResponse:
        locations: Dict[str, List[CandidateSource]] = {}
        unresolved: List[str] = []

        for h in req.missing_hashes:
            norm_h = h.lower()
            worker_ids = self.registry.locate_asset(norm_h)
            candidates = [self.registry.workers[wid] for wid in worker_ids if wid in self.registry.workers]
            ranked = self.prioritizer.rank_candidates(requester_ip, req.requester_worker_id, candidates, self.registry.penalties)
            if ranked:
                locations[norm_h] = ranked
            else:
                unresolved.append(norm_h)
                locations[norm_h] = []

        return LocateAssetsResponse(locations=locations, unresolved_hashes=unresolved)


# ============================================================================
# 5. Streaming Engine & Mock Worker Node
# ============================================================================

CHUNK_SIZE = 1024 * 1024  # 1 MiB

class MockStreamingServer:
    def __init__(self, cas_adapter: MockCASAdapter) -> None:
        self.cas = cas_adapter
        self.corrupt_assets: Set[str] = set()
        self.drop_offset: Optional[int] = None
        self.delay_per_chunk: float = 0.0

    def stream_chunks(self, sha256_hex: str, offset: int = 0) -> Iterator[bytes]:
        norm_h = sha256_hex.lower()
        if not self.cas.has_asset(norm_h):
            raise FileNotFoundError(f"Asset {sha256_hex} not found")

        path = self.cas.get_asset_path(norm_h)
        total_size = path.stat().st_size
        if offset >= total_size and total_size > 0:
            raise IndexError("Offset beyond total file size")

        bytes_sent = offset
        with self.cas.open_asset_stream(norm_h, offset=offset) as f:
            while chunk := f.read(CHUNK_SIZE):
                if self.delay_per_chunk > 0:
                    time.sleep(self.delay_per_chunk)

                if self.drop_offset is not None and bytes_sent + len(chunk) >= self.drop_offset:
                    partial = chunk[: max(0, self.drop_offset - bytes_sent)]
                    if partial:
                        yield partial
                    raise ConnectionResetError("Simulated network connection drop")

                if norm_h in self.corrupt_assets:
                    chunk = bytes([b ^ 0xFF for b in chunk[:1]]) + chunk[1:]
                    self.corrupt_assets.discard(norm_h)

                bytes_sent += len(chunk)
                yield chunk


class MockWorkerNode:
    def __init__(
        self,
        worker_id: str,
        cas_dir: Path | str,
        ip_address: str = "127.0.0.1",
        port: int = 8000,
        coordinator: Optional[MockCoordinator] = None,
    ) -> None:
        self.worker_id = worker_id
        self.cas = MockCASAdapter(cas_dir)
        self.ip_address = ip_address
        self.port = port
        self.endpoint_url = f"http://{ip_address}:{port}"
        self.coordinator = coordinator
        self.server = MockStreamingServer(self.cas)
        self.metrics = TransferMetrics()

        self.info = WorkerInfo(
            worker_id=self.worker_id,
            endpoint_url=self.endpoint_url,
            ip_address=self.ip_address,
            port=self.port,
            inventory_hashes=self.cas.list_cached_hashes(),
            capabilities=WorkerCapabilities(),
        )

        if self.coordinator:
            self.coordinator.handle_register(self.info)

    def heartbeat(self) -> Dict[str, Any]:
        if not self.coordinator:
            return {"status": "no_coordinator"}
        payload = HeartbeatPayload(worker_id=self.worker_id, timestamp_utc=time.time())
        return self.coordinator.handle_heartbeat(self.worker_id, payload)

    def calculate_missing_set(self, required_hashes: Iterable[str]) -> Set[str]:
        return self.cas.get_missing_hashes(required_hashes)

    def download_from_server(
        self,
        server: MockStreamingServer,
        sha256_hex: str,
        expected_size: Optional[int] = None,
        resume_from_offset: int = 0,
        max_retries: int = 3,
        backoff_base: float = 0.01,
    ) -> bool:
        staging_file = self.cas.staging_dir / f"{sha256_hex}_{uuid.uuid4().hex}.tmp"
        hasher = hashlib.sha256()

        bytes_written = 0
        mode = "wb"
        if resume_from_offset > 0 and staging_file.exists():
            mode = "ab"
            bytes_written = staging_file.stat().st_size
            with staging_file.open("rb") as f:
                while chk := f.read(CHUNK_SIZE):
                    hasher.update(chk)

        for attempt in range(max_retries):
            try:
                start_t = time.time()
                with staging_file.open(mode) as f:
                    for chunk in server.stream_chunks(sha256_hex, offset=bytes_written):
                        hasher.update(chunk)
                        f.write(chunk)
                        bytes_written += len(chunk)

                actual_hash = hasher.hexdigest().lower()
                if actual_hash != sha256_hex.lower():
                    if staging_file.exists():
                        staging_file.unlink()
                    raise ValueError(f"Checksum mismatch: expected {sha256_hex}, got {actual_hash}")

                committed = self.cas.commit_staged_file(staging_file, sha256_hex)
                if committed:
                    self.info.inventory_hashes.add(sha256_hex.lower())
                    if self.coordinator:
                        self.coordinator.registry.update_inventory(self.worker_id, added={sha256_hex.lower()})
                    elapsed = max(0.001, time.time() - start_t)
                    mbps = (bytes_written / (1024 * 1024)) / elapsed
                    self.metrics.network_transferred_assets += 1
                    self.metrics.network_transferred_bytes += bytes_written
                    self.metrics.average_transfer_throughput_mbps = round(mbps, 2)
                    return True
                return False

            except (ConnectionResetError, TimeoutError, OSError):
                if attempt < max_retries - 1:
                    time.sleep(backoff_base * (2 ** attempt))
                    mode = "ab"
                    resume_from_offset = bytes_written
                    continue
                else:
                    if staging_file.exists():
                        staging_file.unlink()
                    raise

            except Exception:
                if staging_file.exists():
                    staging_file.unlink()
                raise

        return False

    def sync_missing_assets(self, required_hashes: Iterable[str], peer_workers: Dict[str, MockWorkerNode]) -> Dict[str, bool]:
        missing = self.calculate_missing_set(required_hashes)
        total_req = list(required_hashes)
        self.metrics.total_requested_assets = len(total_req)

        for h in total_req:
            if h not in missing:
                self.metrics.local_cache_hits += 1
                size = self.cas.get_asset_size(h) or 0
                self.metrics.local_cache_hit_bytes += size
                self.metrics.total_requested_bytes += size

        if not missing:
            self.metrics.compute_ratios()
            return {h: True for h in total_req}

        loc_resp = self.coordinator.handle_locate(
            LocateAssetsRequest(requester_worker_id=self.worker_id, missing_hashes=list(missing)),
            requester_ip=self.ip_address,
        )

        results: Dict[str, bool] = {h: True for h in total_req if h not in missing}
        for h in missing:
            candidates = loc_resp.locations.get(h, [])
            transferred = False
            for cand in candidates:
                if cand.worker_id in peer_workers:
                    peer = peer_workers[cand.worker_id]
                    try:
                        success = self.download_from_server(peer.server, h)
                        if success:
                            transferred = True
                            break
                    except Exception:
                        self.metrics.failover_events += 1
                        self.coordinator.registry.record_worker_error(cand.worker_id)
                        continue

            results[h] = transferred
            if transferred:
                size = self.cas.get_asset_size(h) or 0
                self.metrics.total_requested_bytes += size

        self.metrics.compute_ratios()
        return results


# ============================================================================
# 6. Pytest Fixtures
# ============================================================================

@pytest.fixture
def temp_cas_dir(tmp_path: Path) -> Path:
    cas = tmp_path / "cas"
    cas.mkdir(parents=True, exist_ok=True)
    return cas


@pytest.fixture
def two_cas_dirs(tmp_path: Path) -> Tuple[Path, Path]:
    cas_a = tmp_path / "cas_worker_a"
    cas_b = tmp_path / "cas_worker_b"
    cas_a.mkdir(parents=True, exist_ok=True)
    cas_b.mkdir(parents=True, exist_ok=True)
    return cas_a, cas_b


@pytest.fixture
def payload_100b() -> Tuple[bytes, str]:
    data = b"X" * 100
    sha256 = hashlib.sha256(data).hexdigest()
    return data, sha256


@pytest.fixture
def payload_1kib() -> Tuple[bytes, str]:
    data = b"K" * 1024
    sha256 = hashlib.sha256(data).hexdigest()
    return data, sha256


@pytest.fixture
def payload_1mib() -> Tuple[bytes, str]:
    data = b"M" * (1024 * 1024)
    sha256 = hashlib.sha256(data).hexdigest()
    return data, sha256


@pytest.fixture
def payload_5mib() -> Tuple[bytes, str]:
    data = b"F" * (5 * 1024 * 1024)
    sha256 = hashlib.sha256(data).hexdigest()
    return data, sha256


@pytest.fixture
def payload_10mib() -> Tuple[bytes, str]:
    data = b"T" * (10 * 1024 * 1024)
    sha256 = hashlib.sha256(data).hexdigest()
    return data, sha256


@pytest.fixture
def zero_byte_payload() -> Tuple[bytes, str]:
    data = b""
    sha256 = hashlib.sha256(data).hexdigest()
    return data, sha256


@pytest.fixture
def mock_prioritizer() -> MockCandidatePrioritizer:
    return MockCandidatePrioritizer()


@pytest.fixture
def mock_registry(mock_prioritizer: MockCandidatePrioritizer) -> MockWorkerRegistry:
    return MockWorkerRegistry(mock_prioritizer)


@pytest.fixture
def mock_coordinator(mock_prioritizer: MockCandidatePrioritizer) -> MockCoordinator:
    return MockCoordinator(prioritizer=mock_prioritizer)


@pytest.fixture
def two_workers(tmp_path: Path, mock_coordinator: MockCoordinator) -> Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]:
    cas_a = tmp_path / "cas_a"
    cas_b = tmp_path / "cas_b"
    worker_a = MockWorkerNode("worker-a", cas_a, ip_address="192.168.1.101", coordinator=mock_coordinator)
    worker_b = MockWorkerNode("worker-b", cas_b, ip_address="192.168.1.102", coordinator=mock_coordinator)
    return mock_coordinator, worker_a, worker_b
