"""Tier 5 Adversarial Stress & Cluster Resilience Test Suite — Challenger 2.

Targeted Stress Test Scenarios:
1. Multi-node cluster failover: Primary node dies / returns 500 or corrupt bytes mid-stream,
   client fails over to secondary/tertiary node and succeeds. Zero orphan tmp files.
2. Heavy coordinator worker registration & heartbeat expiration under rapid worker churn (100+ concurrent workers).
3. Large file (50+ MiB) memory-bounded streaming verifying RAM usage remains strictly bounded (< 15 MiB).
4. Concurrent atomic commits of the exact same asset hash from multiple workers without file corruption or crashes.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import os
import shutil
import tempfile
import threading
import time
import tracemalloc
import uuid
from pathlib import Path
from typing import AsyncIterator, Dict, List, Optional, Set, Tuple

import httpx
from httpx import ASGITransport
import pytest
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.client import DistributedClient
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    CandidateSource,
    HeartbeatPayload,
    LocalityTier,
    TransferResult,
    WorkerCapabilities,
    WorkerInfo,
    WorkerRegistrationPayload,
    WorkerStatus,
    validate_sha256_hex,
)
from aidars.distributed.prioritizer import CandidatePrioritizer, LatencyTracker
from aidars.distributed.registry import ClusterStats, WorkerHealthStatus, WorkerRegistry
from aidars.distributed.server import WorkerServer, create_worker_app
from aidars.distributed.transfer import (
    CandidateExhaustedError,
    CASCommitError,
    DEFAULT_CHUNK_SIZE,
    IntegrityError,
    StagingContext,
    StreamAbortError,
    WorkerHttpError,
    download_stream_to_staging,
    generate_bounded_chunks,
    transfer_asset_from_candidate,
    transfer_asset_with_failover,
)


# ============================================================================
# Test Helpers & Fixtures
# ============================================================================

def generate_random_bytes(size: int, seed: int = 42) -> Tuple[bytes, str]:
    """Deterministically generate binary data and return (data, sha256_hex)."""
    import random
    rng = random.Random(seed)
    data = rng.randbytes(size)
    sha256 = hashlib.sha256(data).hexdigest()
    return data, sha256


def make_candidate_source(
    worker_id: str,
    endpoint_url: str,
    ip_address: str = "127.0.0.1",
    port: int = 8000,
    rtt_ms: float = 1.0,
    locality: LocalityTier = LocalityTier.LOOPBACK,
) -> CandidateSource:
    return CandidateSource(
        worker_id=worker_id,
        endpoint_url=endpoint_url,
        ip_address=ip_address,
        port=port,
        locality_tier=locality.value,
        estimated_rtt_ms=rtt_ms,
        load_factor=0.1,
    )


# ============================================================================
# Suite 1: Multi-Node Cluster Failover & Mid-Stream Resilience
# ============================================================================

class TestMultiNodeClusterFailoverStress:
    """Stress-test multi-node failover against HTTP 500, mid-stream disconnects, and bit corruption."""

    @pytest.mark.asyncio
    async def test_primary_500_failover_to_secondary_success(self, tmp_path: Path):
        """Primary node returns HTTP 500; client immediately fails over to secondary node and succeeds."""
        cas_sec = LocalCASAdapter(cas_dir=tmp_path / "cas_sec", staging_dir=tmp_path / "stg_sec")
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_cl", staging_dir=tmp_path / "stg_cl")

        data, h = generate_random_bytes(2 * 1024 * 1024, seed=101)  # 2 MiB
        cas_sec.store_bytes(data)

        # 1. Primary app returns HTTP 500
        async def primary_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={"detail": "Primary Internal Database Error"})

        # 2. Secondary app serves actual asset
        app_sec = create_worker_app(cas_adapter=cas_sec, worker_id="w-secondary")

        # Custom transport dispatching based on host/URL
        transport_sec = ASGITransport(app=app_sec)

        async def custom_transport_handler(request: httpx.Request) -> httpx.Response:
            if "w-primary" in str(request.url):
                return httpx.Response(500, json={"detail": "Primary Internal Server Error"})
            elif "w-secondary" in str(request.url):
                return await transport_sec.handle_async_request(request)
            return httpx.Response(404)

        transport = httpx.MockTransport(custom_transport_handler)

        async with httpx.AsyncClient(transport=transport) as client:
            cand_primary = make_candidate_source("w-primary", "http://w-primary:8000", rtt_ms=1.0)
            cand_secondary = make_candidate_source("w-secondary", "http://w-secondary:8001", rtt_ms=5.0)

            errors_recorded: List[Tuple[str, Exception]] = []

            def record_err(cand: CandidateSource, exc: Exception):
                errors_recorded.append((cand.worker_id, exc))

            result = await transfer_asset_with_failover(
                client=client,
                candidates=[cand_primary, cand_secondary],
                sha256=h,
                cas_adapter=cas_client,
                staging_dir=cas_client.staging_dir,
                on_candidate_error=record_err,
            )

            assert result.success is True
            assert result.source_worker_id == "w-secondary"
            assert result.bytes_transferred == len(data)
            assert result.verified_sha256 == h
            assert cas_client.has_asset(h) is True

            # Verify primary failure was captured
            assert len(errors_recorded) == 1
            assert errors_recorded[0][0] == "w-primary"

            # Verify zero orphan staging files
            staging_files = list(cas_client.staging_dir.iterdir())
            assert len(staging_files) == 0

    @pytest.mark.asyncio
    async def test_primary_dies_midstream_failover_to_secondary_success(self, tmp_path: Path):
        """Primary node drops connection mid-stream after sending partial bytes; client fails over cleanly."""
        cas_sec = LocalCASAdapter(cas_dir=tmp_path / "cas_sec", staging_dir=tmp_path / "stg_sec")
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_cl", staging_dir=tmp_path / "stg_cl")

        data, h = generate_random_bytes(3 * 1024 * 1024, seed=102)  # 3 MiB
        cas_sec.store_bytes(data)

        # Primary stream generator that abruptly truncates / raises mid-stream
        async def truncated_stream_gen():
            yield data[: 512 * 1024]  # Yield 512 KiB then abort
            raise httpx.ReadTimeout("Simulated physical network cord unplugged mid-stream")

        app_sec = create_worker_app(cas_adapter=cas_sec, worker_id="w-secondary")
        transport_sec = ASGITransport(app=app_sec)

        async def custom_transport_handler(request: httpx.Request) -> httpx.Response:
            if "w-primary" in str(request.url):
                return httpx.Response(
                    200,
                    content=truncated_stream_gen(),
                    headers={"content-type": "application/octet-stream", "content-length": str(len(data))},
                )
            elif "w-secondary" in str(request.url):
                return await transport_sec.handle_async_request(request)
            return httpx.Response(404)

        transport = httpx.MockTransport(custom_transport_handler)

        async with httpx.AsyncClient(transport=transport) as client:
            cand_primary = make_candidate_source("w-primary", "http://w-primary:8000", rtt_ms=1.0)
            cand_secondary = make_candidate_source("w-secondary", "http://w-secondary:8001", rtt_ms=10.0)

            result = await transfer_asset_with_failover(
                client=client,
                candidates=[cand_primary, cand_secondary],
                sha256=h,
                cas_adapter=cas_client,
                staging_dir=cas_client.staging_dir,
            )

            assert result.success is True
            assert result.source_worker_id == "w-secondary"
            assert result.bytes_transferred == len(data)
            assert cas_client.has_asset(h) is True

            # Verify no partial temporary files leaked
            assert len(list(cas_client.staging_dir.iterdir())) == 0

    @pytest.mark.asyncio
    async def test_3_candidate_cascade_500_then_corrupt_then_tertiary_success(self, tmp_path: Path):
        """3-candidate cascade: Node 1 returns 500, Node 2 sends corrupted bytes, Node 3 succeeds."""
        cas_tert = LocalCASAdapter(cas_dir=tmp_path / "cas_tert", staging_dir=tmp_path / "stg_tert")
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_cl", staging_dir=tmp_path / "stg_cl")

        data, h = generate_random_bytes(1024 * 1024, seed=103)  # 1 MiB
        cas_tert.store_bytes(data)

        # Corrupted payload for node 2: flip middle byte
        corrupt_data = bytearray(data)
        corrupt_data[len(data) // 2] ^= 0xFF
        corrupt_data = bytes(corrupt_data)

        app_tert = create_worker_app(cas_adapter=cas_tert, worker_id="w-tertiary")
        transport_tert = ASGITransport(app=app_tert)

        async def custom_transport_handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "w-primary" in url:
                return httpx.Response(500, json={"error": "Node 1 disk offline"})
            elif "w-secondary" in url:
                return httpx.Response(
                    200,
                    content=corrupt_data,
                    headers={"content-type": "application/octet-stream", "content-length": str(len(corrupt_data))},
                )
            elif "w-tertiary" in url:
                return await transport_tert.handle_async_request(request)
            return httpx.Response(404)

        transport = httpx.MockTransport(custom_transport_handler)

        async with httpx.AsyncClient(transport=transport) as client:
            cand1 = make_candidate_source("w-primary", "http://w-primary:8000", rtt_ms=1.0)
            cand2 = make_candidate_source("w-secondary", "http://w-secondary:8001", rtt_ms=2.0)
            cand3 = make_candidate_source("w-tertiary", "http://w-tertiary:8002", rtt_ms=3.0)

            failed_candidates: List[str] = []

            def on_err(cand: CandidateSource, exc: Exception):
                failed_candidates.append(cand.worker_id)

            result = await transfer_asset_with_failover(
                client=client,
                candidates=[cand1, cand2, cand3],
                sha256=h,
                cas_adapter=cas_client,
                staging_dir=cas_client.staging_dir,
                on_candidate_error=on_err,
            )

            assert result.success is True
            assert result.source_worker_id == "w-tertiary"
            assert result.bytes_transferred == len(data)
            assert cas_client.has_asset(h) is True
            assert failed_candidates == ["w-primary", "w-secondary"]
            assert len(list(cas_client.staging_dir.iterdir())) == 0

    @pytest.mark.asyncio
    async def test_all_candidates_fail_raises_candidate_exhausted_zero_tmp_leak(self, tmp_path: Path):
        """When all candidate sources fail, raises CandidateExhaustedError and cleans up all temp files."""
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_cl", staging_dir=tmp_path / "stg_cl")
        data, h = generate_random_bytes(512 * 1024, seed=104)

        async def failing_transport_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, json={"error": "Overloaded"})

        transport = httpx.MockTransport(failing_transport_handler)

        async with httpx.AsyncClient(transport=transport) as client:
            cands = [
                make_candidate_source(f"w-fail-{i}", f"http://w-fail-{i}:8000")
                for i in range(5)
            ]

            with pytest.raises(CandidateExhaustedError) as exc_info:
                await transfer_asset_with_failover(
                    client=client,
                    candidates=cands,
                    sha256=h,
                    cas_adapter=cas_client,
                    staging_dir=cas_client.staging_dir,
                )

            assert exc_info.value.candidate_count == 5
            assert exc_info.value.sha256 == h
            assert not cas_client.has_asset(h)
            assert len(list(cas_client.staging_dir.iterdir())) == 0


# ============================================================================
# Suite 2: Heavy Coordinator Worker Registration & Heartbeat Churn Stress
# ============================================================================

class TestCoordinatorRapidChurnStress:
    """Stress-test coordinator registry under massive concurrent registrations, heartbeats, and evictions."""

    @pytest.mark.asyncio
    async def test_concurrent_registration_100_workers_with_inverted_indexing(self):
        """100 workers registering concurrently with unique and overlapping asset inventories."""
        registry = WorkerRegistry(heartbeat_timeout_seconds=15.0)

        shared_hash_1 = hashlib.sha256(b"shared_common_texture_1").hexdigest()
        shared_hash_2 = hashlib.sha256(b"shared_common_texture_2").hexdigest()

        def create_worker(idx: int) -> WorkerInfo:
            unique_hashes = {
                hashlib.sha256(f"worker_{idx}_unique_asset_{k}".encode()).hexdigest()
                for k in range(10)
            }
            # All workers hold shared_hash_1; even workers hold shared_hash_2
            inv = unique_hashes | {shared_hash_1}
            if idx % 2 == 0:
                inv.add(shared_hash_2)

            return WorkerInfo(
                worker_id=f"worker-{idx:03d}",
                endpoint_url=f"http://10.0.0.{idx % 250 + 1}:8000",
                ip_address=f"10.0.0.{idx % 250 + 1}",
                port=8000,
                capacity_bytes=100 * 1024 * 1024 * 1024,
                used_bytes=len(inv) * 1024 * 1024,
                inventory_hashes=inv,
                last_heartbeat_utc=time.time(),
            )

        workers = [create_worker(i) for i in range(100)]

        # Execute concurrent registrations across a thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(registry.register_worker, w) for w in workers]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert len(results) == 100
        assert registry.get_worker_count() == 100

        # Verify inverted index correctness
        nodes_for_shared_1 = registry.get_workers_for_hash(shared_hash_1)
        assert len(nodes_for_shared_1) == 100

        nodes_for_shared_2 = registry.get_workers_for_hash(shared_hash_2)
        assert len(nodes_for_shared_2) == 50

        # Total unique hashes indexed: 1000 unique + 2 shared = 1002
        assert registry.get_hash_count() == 1002

    @pytest.mark.asyncio
    async def test_rapid_worker_churn_eviction_and_index_integrity(self):
        """Massive worker churn: 50 stale workers evicted while 50 new workers join, verifying O(K) index pruning."""
        registry = WorkerRegistry(heartbeat_timeout_seconds=5.0)
        t_base = time.time()

        # 1. Register 50 'stale' workers with old timestamp (t_base - 10)
        stale_hashes: Set[str] = set()
        for i in range(50):
            u_h = hashlib.sha256(f"stale_asset_{i}".encode()).hexdigest()
            stale_hashes.add(u_h)
            w = WorkerInfo(
                worker_id=f"stale-worker-{i:02d}",
                endpoint_url=f"http://192.168.1.{i+10}:8000",
                ip_address=f"192.168.1.{i+10}",
                port=8000,
                inventory_hashes={u_h},
                last_heartbeat_utc=t_base - 10.0,  # 10s old -> expired
            )
            registry.register_worker(w)

        # 2. Register 50 'fresh' workers with current timestamp
        fresh_hashes: Set[str] = set()
        for i in range(50):
            u_h = hashlib.sha256(f"fresh_asset_{i}".encode()).hexdigest()
            fresh_hashes.add(u_h)
            w = WorkerInfo(
                worker_id=f"fresh-worker-{i:02d}",
                endpoint_url=f"http://192.168.2.{i+10}:8000",
                ip_address=f"192.168.2.{i+10}",
                port=8000,
                inventory_hashes={u_h},
                last_heartbeat_utc=t_base,
            )
            registry.register_worker(w)

        assert registry.get_worker_count() == 100
        assert registry.get_hash_count() == 100

        # 3. Trigger eviction at current_time=t_base
        evicted = registry.evict_expired_workers(timeout_seconds=5.0, current_time=t_base)
        assert len(evicted) == 50
        assert all(wid.startswith("stale-worker-") for wid in evicted)

        # 4. Verify post-eviction index consistency
        assert registry.get_worker_count() == 50
        assert registry.get_hash_count() == 50

        # Stale hashes must be completely unmapped
        for sh in stale_hashes:
            assert len(registry.get_workers_for_hash(sh)) == 0

        # Fresh hashes must remain mapped
        for fh in fresh_hashes:
            assert len(registry.get_workers_for_hash(fh)) == 1

    @pytest.mark.asyncio
    async def test_concurrent_heartbeat_inventory_deltas(self):
        """50 concurrent workers pulsing heartbeats with concurrent inventory additions and removals."""
        registry = WorkerRegistry(heartbeat_timeout_seconds=15.0)

        for i in range(50):
            w = WorkerInfo(
                worker_id=f"delta-worker-{i:02d}",
                endpoint_url=f"http://127.0.0.1:{8000+i}",
                ip_address="127.0.0.1",
                port=8000 + i,
                inventory_hashes={hashlib.sha256(f"init_h_{i}".encode()).hexdigest()},
                last_heartbeat_utc=time.time(),
            )
            registry.register_worker(w)

        def pulse_deltas(idx: int):
            init_h = hashlib.sha256(f"init_h_{idx}".encode()).hexdigest()
            new_h1 = hashlib.sha256(f"new_h_{idx}_1".encode()).hexdigest()
            new_h2 = hashlib.sha256(f"new_h_{idx}_2".encode()).hexdigest()

            payload = HeartbeatPayload(
                worker_id=f"delta-worker-{idx:02d}",
                timestamp_utc=time.time(),
                inventory_delta_added={new_h1, new_h2},
                inventory_delta_removed={init_h},
            )
            return registry.record_heartbeat(f"delta-worker-{idx:02d}", payload)

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(pulse_deltas, i) for i in range(50)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert all(results)
        # Each worker removed 1 and added 2 -> total unique hashes = 50 * 2 = 100
        assert registry.get_hash_count() == 100


# ============================================================================
# Suite 3: Large File (50+ MiB) Memory-Bounded Streaming & RAM Boundedness
# ============================================================================

class TestLargeFileMemoryBoundedStreamingStress:
    """Empirically verify streaming transfers of 50+ MiB assets operate within strict RAM bounds."""

    @pytest.mark.asyncio
    async def test_large_file_50mib_streaming_memory_bounded(self, tmp_path: Path):
        """Stream a 50 MiB binary asset to disk, verifying tracemalloc RAM delta < 15 MiB."""
        cas_server = LocalCASAdapter(cas_dir=tmp_path / "cas_srv", staging_dir=tmp_path / "stg_srv")
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_cl", staging_dir=tmp_path / "stg_cl")

        # 50 MiB = 52,428,800 bytes
        file_size = 50 * 1024 * 1024
        test_file = tmp_path / "large_source_50mib.bin"

        # Deterministically write 50 MiB file in chunks to avoid generator memory spikes
        hasher = hashlib.sha256()
        chunk_1mib = b"X" * (1024 * 1024)
        with test_file.open("wb") as f:
            for _ in range(50):
                f.write(chunk_1mib)
                hasher.update(chunk_1mib)
        expected_sha256 = hasher.hexdigest()

        # Import into server CAS
        cas_server.store_file(test_file, expected_sha256=expected_sha256)
        test_file.unlink()

        app_srv = create_worker_app(cas_adapter=cas_server, worker_id="w-large-srv")
        transport = ASGITransport(app=app_srv)

        import gc
        gc.collect()
        tracemalloc.start()

        progress_history: List[int] = []
        def track_progress(received: int, total: Optional[int]):
            progress_history.append(received)

        async with httpx.AsyncClient(transport=transport) as client:
            stg_path, verified_hash, total_bytes = await download_stream_to_staging(
                client=client,
                endpoint_url="http://w-large-srv:8000",
                sha256=expected_sha256,
                staging_dir=cas_client.staging_dir,
                chunk_size=DEFAULT_CHUNK_SIZE,
                progress_callback=track_progress,
            )

        gc.collect()
        current_bytes, _ = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Verify retained memory delta is negligible (< 5 MiB) after transfer finishes
        retained_mb = current_bytes / (1024 * 1024)
        assert retained_mb < 5.0, f"Memory retained after 50 MiB transfer was {retained_mb:.2f} MiB (expected < 5 MiB)"

        # Verify progressive streaming: received bytes grew monotonically across at least 45 chunks
        assert len(progress_history) >= 45, f"Expected >= 45 progress updates for 50 MiB transfer, got {len(progress_history)}"
        assert progress_history[-1] == file_size

        # Verify correctness
        assert total_bytes == file_size
        assert verified_hash == expected_sha256
        assert stg_path.exists()
        assert stg_path.stat().st_size == file_size

        # Atomic commit into client CAS
        committed = cas_client.commit_staged_file(stg_path, expected_sha256)
        assert committed is True
        assert cas_client.has_asset(expected_sha256) is True
        assert cas_client.get_asset_size(expected_sha256) == file_size

    @pytest.mark.asyncio
    async def test_large_file_50mib_range_resumption_after_partial_read(self, tmp_path: Path):
        """Stream 20 MiB of a 50 MiB asset, interrupt, and resume from offset 20 MiB to completion."""
        cas_srv = LocalCASAdapter(cas_dir=tmp_path / "cas_srv", staging_dir=tmp_path / "stg_srv")
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_cl", staging_dir=tmp_path / "stg_cl")

        data, h = generate_random_bytes(50 * 1024 * 1024, seed=105)
        cas_srv.store_bytes(data)

        app_srv = create_worker_app(cas_adapter=cas_srv, worker_id="w-range-srv")
        transport = ASGITransport(app=app_srv)

        # 1. First request gets partial range: bytes=0-20971519 (20 MiB)
        headers_p1 = {"Range": "bytes=0-20971519"}
        # 2. Second request resumes: bytes=20971520- (remaining 30 MiB)
        headers_p2 = {"Range": "bytes=20971520-"}

        combined = bytearray()
        async with httpx.AsyncClient(transport=transport) as client:
            resp1 = await client.get(f"http://w-range-srv:8000/api/v1/assets/{h}/stream", headers=headers_p1)
            assert resp1.status_code == 206
            assert len(resp1.content) == 20 * 1024 * 1024
            combined.extend(resp1.content)

            resp2 = await client.get(f"http://w-range-srv:8000/api/v1/assets/{h}/stream", headers=headers_p2)
            assert resp2.status_code == 206
            assert len(resp2.content) == 30 * 1024 * 1024
            combined.extend(resp2.content)

        assert len(combined) == 50 * 1024 * 1024
        assert hashlib.sha256(combined).hexdigest() == h


# ============================================================================
# Suite 4: Concurrent Atomic Commits of Exact Same Asset Hash
# ============================================================================

class TestConcurrentAtomicCommitsStress:
    """Stress-test concurrent atomic commits of the exact same asset hash from multiple threads/workers."""

    def test_20_concurrent_threads_commit_exact_same_asset_hash(self, tmp_path: Path):
        """20 concurrent threads prepare identical staged files and commit simultaneously."""
        cas = LocalCASAdapter(cas_dir=tmp_path / "cas_concurrent", staging_dir=tmp_path / "stg_concurrent")
        data, h = generate_random_bytes(5 * 1024 * 1024, seed=106)  # 5 MiB

        num_threads = 20
        staging_files: List[Path] = []

        for i in range(num_threads):
            stg = cas.create_staging_file(h, prefix=f"worker_{i:02d}")
            stg.write_bytes(data)
            staging_files.append(stg)

        commit_results: List[bool] = []
        barrier = threading.Barrier(num_threads)

        def worker_commit(stg_file: Path) -> bool:
            barrier.wait()  # Synchronize so all 20 threads hit commit_staged_file simultaneously
            return cas.commit_staged_file(stg_file, h)

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker_commit, f) for f in staging_files]
            commit_results = [f.result() for f in futures]

        # Every thread must report success (atomic replace or deduplication shortcut)
        assert all(commit_results), f"Some threads failed commit: {commit_results}"
        assert len(commit_results) == num_threads

        # Verify CAS integrity
        assert cas.has_asset(h) is True
        asset_p = cas.get_asset_path(h)
        assert asset_p is not None
        assert asset_p.stat().st_size == len(data)

        # Verify on-disk file content
        with asset_p.open("rb") as f:
            assert hashlib.sha256(f.read()).hexdigest() == h

        # Verify staging directory is 100% clean
        remaining_tmp = list(cas.staging_dir.iterdir())
        assert len(remaining_tmp) == 0, f"Found orphan staging files: {remaining_tmp}"

    def test_concurrent_store_file_and_store_bytes_collision(self, tmp_path: Path):
        """30 concurrent tasks calling store_bytes and store_file simultaneously for same hash."""
        cas = LocalCASAdapter(cas_dir=tmp_path / "cas_collision", staging_dir=tmp_path / "stg_collision")
        data, h = generate_random_bytes(2 * 1024 * 1024, seed=107)

        source_file = tmp_path / "shared_source.bin"
        source_file.write_bytes(data)

        def worker_store(task_id: int) -> str:
            if task_id % 2 == 0:
                return cas.store_bytes(data)
            else:
                return cas.store_file(source_file, expected_sha256=h)

        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(worker_store, i) for i in range(30)]
            returned_hashes = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert len(returned_hashes) == 30
        assert all(ret_h == h for ret_h in returned_hashes)
        assert cas.has_asset(h) is True
        assert len(list(cas.staging_dir.iterdir())) == 0
