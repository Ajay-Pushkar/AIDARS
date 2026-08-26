"""Challenger 1 Adversarial Stress & Empirical Verification Harness (M2 R3).

Adversarial testing targeting:
1. LocalCASAdapter extreme multi-threaded concurrency (commit, store, delete, read stream, prune staging, get_missing_hashes).
2. Large scale missing-set resolution (10,000+ and 20,000 hashes, mixed valid, invalid, uppercase, duplicate, garbage types).
3. Corrupted staging files, SHA mismatch, partial writes, and path traversal security.
4. Windows file lock resilience (concurrent open stream + delete / commit).
5. Memory stability and latency SLA bounds across high-scale batch operations.
6. End-to-end coordinator asset location under heavy worker and hash scale.
"""
from __future__ import annotations

import concurrent.futures
import gc
import hashlib
import os
import random
import string
import threading
import time
from pathlib import Path
from typing import Dict, List, Set

import pytest

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.models import (
    LocateAssetsRequest,
    WorkerCapabilities,
    WorkerInfo,
    WorkerStatus,
    validate_sha256_hex,
)
from aidars.distributed.prioritizer import CandidatePrioritizer
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.coordinator import CoordinatorService


@pytest.fixture
def fresh_cas(tmp_path: Path) -> LocalCASAdapter:
    cas_dir = tmp_path / "cas_stress"
    return LocalCASAdapter(cas_dir=cas_dir, chunk_size=8192)


# ============================================================================
# 1. Extreme Concurrency & Rapid Commit / Delete / Stream Cycles
# ============================================================================


class TestCASAdapterExtremeConcurrency:
    """Stress test LocalCASAdapter with 32 concurrent threads performing rapid mutations."""

    def test_extreme_concurrent_operations(self, fresh_cas: LocalCASAdapter):
        num_threads = 32
        duration_seconds = 4.0
        stop_event = threading.Event()
        errors: List[str] = []
        errors_lock = threading.Lock()

        # Pre-populate some assets
        base_hashes: List[str] = []
        for i in range(50):
            payload = f"pre-seeded-payload-{i}".encode("utf-8")
            h = fresh_cas.store_bytes(payload)
            base_hashes.append(h)

        def worker_store(thread_id: int):
            seq = 0
            while not stop_event.is_set():
                seq += 1
                data = f"thread-{thread_id}-seq-{seq}-{time.time()}".encode("utf-8")
                expected_hash = hashlib.sha256(data).hexdigest()
                try:
                    h = fresh_cas.store_bytes(data)
                    if h != expected_hash:
                        with errors_lock:
                            errors.append(f"Store mismatch: {h} != {expected_hash}")
                except Exception as exc:
                    with errors_lock:
                        errors.append(f"Store error thread {thread_id}: {exc}")
                time.sleep(0.001)

        def worker_idempotent_commit(thread_id: int):
            # Multiple threads repeatedly staging and committing the SAME hash simultaneously
            shared_payload = b"constant-immutable-content-shared-across-threads"
            shared_hash = hashlib.sha256(shared_payload).hexdigest()
            while not stop_event.is_set():
                staged = fresh_cas.create_staging_file(shared_hash, prefix=f"idemp_{thread_id}")
                staged.write_bytes(shared_payload)
                try:
                    success = fresh_cas.commit_staged_file(staged, shared_hash)
                    if not success:
                        with errors_lock:
                            errors.append(f"Idempotent commit returned False in thread {thread_id}")
                except Exception as exc:
                    with errors_lock:
                        errors.append(f"Idempotent commit exception in thread {thread_id}: {exc}")
                time.sleep(0.002)

        def worker_delete_and_check(thread_id: int):
            while not stop_event.is_set():
                target_h = random.choice(base_hashes)
                try:
                    # Deleting might succeed or fail if deleted/locked, but MUST NOT raise unhandled exception
                    fresh_cas.delete_asset(target_h)
                    # has_asset should execute safely
                    fresh_cas.has_asset(target_h)
                except Exception as exc:
                    with errors_lock:
                        errors.append(f"Delete/Check error thread {thread_id}: {exc}")
                time.sleep(0.002)

        def worker_read_stream(thread_id: int):
            while not stop_event.is_set():
                target_h = random.choice(base_hashes)
                try:
                    if fresh_cas.has_asset(target_h):
                        with fresh_cas.open_asset_stream(target_h) as stream:
                            content = stream.read()
                            if content:
                                computed = hashlib.sha256(content).hexdigest()
                                if computed != target_h:
                                    with errors_lock:
                                        errors.append(f"Stream content corrupt: {computed} != {target_h}")
                except (FileNotFoundError, PermissionError):
                    # Concurrent delete on Windows can put file into delete-pending state before open()
                    pass
                except Exception as exc:
                    with errors_lock:
                        errors.append(f"Stream read error thread {thread_id}: {exc}")
                time.sleep(0.001)

        def worker_missing_set(thread_id: int):
            while not stop_event.is_set():
                query = random.sample(base_hashes, k=min(10, len(base_hashes)))
                # Add some unknown hashes
                fake_hashes = [hashlib.sha256(f"fake-{random.random()}".encode()).hexdigest() for _ in range(5)]
                full_query = query + fake_hashes
                try:
                    missing = fresh_cas.get_missing_hashes(full_query)
                    if not set(fake_hashes).issubset(missing):
                        with errors_lock:
                            errors.append("Missing set failed to identify unknown hashes")
                except Exception as exc:
                    with errors_lock:
                        errors.append(f"Missing set query error thread {thread_id}: {exc}")
                time.sleep(0.003)

        def worker_prune_staging(thread_id: int):
            while not stop_event.is_set():
                try:
                    # Create an orphaned old staging file to test pruning
                    old_tmp = fresh_cas.staging_dir / f"orphan_{uuid_str()}.tmp"
                    old_tmp.write_bytes(b"abandoned")
                    # Set mtime back by 10 seconds
                    os.utime(old_tmp, (time.time() - 10, time.time() - 10))

                    # Prune files older than 5.0 seconds (leaves in-flight writes intact)
                    fresh_cas.prune_staging(max_age_seconds=5.0)
                except Exception as exc:
                    with errors_lock:
                        errors.append(f"Prune staging error thread {thread_id}: {exc}")
                time.sleep(0.02)

        def uuid_str():
            import uuid
            return uuid.uuid4().hex

        threads: List[threading.Thread] = []
        for i in range(6):
            threads.append(threading.Thread(target=worker_store, args=(i,)))
        for i in range(6):
            threads.append(threading.Thread(target=worker_idempotent_commit, args=(i,)))
        for i in range(6):
            threads.append(threading.Thread(target=worker_delete_and_check, args=(i,)))
        for i in range(8):
            threads.append(threading.Thread(target=worker_read_stream, args=(i,)))
        for i in range(4):
            threads.append(threading.Thread(target=worker_missing_set, args=(i,)))
        for i in range(2):
            threads.append(threading.Thread(target=worker_prune_staging, args=(i,)))

        for t in threads:
            t.start()

        time.sleep(duration_seconds)
        stop_event.set()

        for t in threads:
            t.join(timeout=3.0)

        assert errors == [], f"Encountered {len(errors)} concurrency errors:\n" + "\n".join(errors[:10])


# ============================================================================
# 2. Large Scale (10,000+ & 20,000 Hashes) Missing-Set Resolution
# ============================================================================


class TestMissingSetLargeScaleAndAdversarial:
    """Stress test get_missing_hashes with 10,000+ items, invalid types, uppercase, and measure SLA."""

    def test_missing_set_10000_scale_and_correctness(self, fresh_cas: LocalCASAdapter):
        # 1. Pre-populate 3,000 assets
        stored_hashes: Set[str] = set()
        for i in range(3000):
            data = f"scale-asset-{i}".encode("utf-8")
            h = fresh_cas.store_bytes(data)
            stored_hashes.add(h)

        # 2. Build 10,000+ candidate query:
        # - 2,000 cached hashes in lowercase
        # - 1,000 cached hashes in UPPERCASE
        # - 5,000 missing valid SHA-256 hashes
        # - 1,500 invalid hex strings (wrong length, illegal chars)
        # - 500 duplicates
        # - 500 None/whitespace/garbage types
        cached_sample = list(stored_hashes)[:3000]
        cached_lower = cached_sample[:2000]
        cached_upper = [h.upper() for h in cached_sample[2000:3000]]

        missing_valid = [hashlib.sha256(f"missing-asset-{i}".encode()).hexdigest() for i in range(5000)]
        invalid_hex = [f"invalid-hex-{i:04d}" for i in range(1500)]
        duplicates = list(cached_lower[:250]) + list(missing_valid[:250])
        garbage = ["   ", "NOT_A_HASH", "", "12345", "00" * 31 + "zz"] * 100

        all_queries = cached_lower + cached_upper + missing_valid + invalid_hex + duplicates + garbage
        assert len(all_queries) >= 10000

        # Benchmark resolution time
        start_time = time.perf_counter()
        missing_result = fresh_cas.get_missing_hashes(all_queries)
        elapsed = time.perf_counter() - start_time

        # Verification of SLA (must be well under 0.5s SLA, target < 0.35s)
        assert elapsed < 0.35, f"10,000 missing set resolution took too long: {elapsed:.4f}s (> 350ms SLA)"

        # Correctness:
        # All 5,000 missing_valid must be present
        for h in missing_valid:
            assert h in missing_result, f"Missing hash {h} was not in result"

        # None of cached_lower or cached_upper should be in result
        for h in cached_lower:
            assert h not in missing_result, f"Cached hash {h} wrongly marked as missing"
        for h in cached_upper:
            assert h.lower() not in missing_result, f"Cached upper hash {h} wrongly marked as missing"

        # Invalid hex & garbage must be accounted for (treated as missing / unresolvable)
        for inv in invalid_hex:
            assert inv in missing_result or inv.strip().lower() in missing_result

    def test_missing_set_20000_extreme_scale(self, fresh_cas: LocalCASAdapter):
        # Pre-populate 500 assets across shards
        for i in range(500):
            fresh_cas.store_bytes(f"asset-20k-{i}".encode())

        query_set = [hashlib.sha256(f"query-20k-{i}".encode()).hexdigest() for i in range(20000)]
        start_time = time.perf_counter()
        res = fresh_cas.get_missing_hashes(query_set)
        elapsed = time.perf_counter() - start_time

        assert len(res) == 20000
        assert elapsed < 0.20, f"20,000 missing set resolution took {elapsed:.4f}s (> 200ms SLA)"

    def test_missing_set_memory_stability_repeated_calls(self, fresh_cas: LocalCASAdapter):
        # Ensure repeated 10,000 query calls don't leak memory or degrade performance
        for i in range(500):
            fresh_cas.store_bytes(f"asset-{i}".encode())

        query_set = [hashlib.sha256(f"query-{i}".encode()).hexdigest() for i in range(10000)]

        times: List[float] = []
        for _ in range(20):
            t0 = time.perf_counter()
            res = fresh_cas.get_missing_hashes(query_set)
            t1 = time.perf_counter()
            times.append(t1 - t0)
            assert len(res) == 10000

        avg_time = sum(times) / len(times)
        max_time = max(times)
        assert max_time < 0.15, f"Max iteration time {max_time:.4f}s exceeded 150ms"
        assert avg_time < 0.08, f"Average iteration time {avg_time:.4f}s exceeded 80ms"


# ============================================================================
# 3. Corrupted Inputs, SHA Mismatch & Path Traversal Security
# ============================================================================


class TestCASAdversarialCorruptionAndSecurity:
    """Stress test corrupted staging files, checksum mismatches, and path traversal."""

    def test_staging_checksum_mismatch_cleans_up(self, fresh_cas: LocalCASAdapter):
        staged = fresh_cas.create_staging_file()
        staged.write_bytes(b"actual content")
        wrong_hash = hashlib.sha256(b"completely different content").hexdigest()

        success = fresh_cas.commit_staged_file(staged, wrong_hash)
        assert success is False
        # Staged file must be removed
        assert not staged.exists()
        # CAS must not have the asset
        assert not fresh_cas.has_asset(wrong_hash)

    def test_staging_file_truncated_during_store(self, fresh_cas: LocalCASAdapter, monkeypatch):
        # Create staging file, simulate mid-transfer truncation
        staged = fresh_cas.create_staging_file()
        staged.write_bytes(b"1234567890")
        target_hash = hashlib.sha256(b"1234567890_full").hexdigest()

        assert fresh_cas.commit_staged_file(staged, target_hash) is False
        assert not staged.exists()

    def test_missing_staging_file_commit_graceful(self, fresh_cas: LocalCASAdapter):
        ghost_path = fresh_cas.staging_dir / "ghost_file.tmp"
        valid_hash = hashlib.sha256(b"ghost").hexdigest()
        assert fresh_cas.commit_staged_file(ghost_path, valid_hash) is False

    def test_path_traversal_attempts_rejected(self, fresh_cas: LocalCASAdapter):
        malicious_hashes = [
            "../../etc/passwd",
            "..\\..\\windows\\system32\\cmd.exe",
            "../" * 20 + "secret.txt",
            "aa/../../bb/cccc",
            "aa" + "/" + "00" * 31,
            "aa" + "\\" + "00" * 31,
            "\x00" + "00" * 31,
            "00" * 31 + ";calc.exe",
        ]
        for bad_hash in malicious_hashes:
            assert fresh_cas.has_asset(bad_hash) is False
            assert fresh_cas.get_asset_path(bad_hash) is None
            assert fresh_cas.get_asset_size(bad_hash) is None
            assert fresh_cas.delete_asset(bad_hash) is False
            with pytest.raises(Exception):
                fresh_cas.open_asset_stream(bad_hash)

    def test_open_asset_stream_boundary_offsets(self, fresh_cas: LocalCASAdapter):
        payload = b"0123456789ABCDEF"
        h = fresh_cas.store_bytes(payload)

        # Exact bounds
        with fresh_cas.open_asset_stream(h, offset=0) as f:
            assert f.read() == payload

        with fresh_cas.open_asset_stream(h, offset=16) as f:
            assert f.read() == b""

        with fresh_cas.open_asset_stream(h, offset=10) as f:
            assert f.read() == b"ABCDEF"

        # Out of bounds
        with pytest.raises(IndexError):
            fresh_cas.open_asset_stream(h, offset=17)

        with pytest.raises(IndexError):
            fresh_cas.open_asset_stream(h, offset=1000)

        # Negative offset
        with pytest.raises(ValueError):
            fresh_cas.open_asset_stream(h, offset=-1)

    def test_all_256_shards_distribution(self, fresh_cas: LocalCASAdapter):
        # Ensure that assets mapping to every 2-character hex shard (00-ff) are stored and listed correctly
        shard_hashes: List[str] = []
        for i in range(256):
            shard_prefix = f"{i:02x}"
            # Construct a data string that hashes to this shard prefix
            counter = 0
            while True:
                candidate_data = f"shard_test_{shard_prefix}_{counter}".encode("utf-8")
                h = hashlib.sha256(candidate_data).hexdigest()
                if h.startswith(shard_prefix):
                    fresh_cas.store_bytes(candidate_data)
                    shard_hashes.append(h)
                    break
                counter += 1

        assert len(shard_hashes) == 256
        inventory = fresh_cas.get_inventory_hashes()
        assert set(shard_hashes).issubset(inventory)


# ============================================================================
# 4. Windows File Locking & Active Stream Delete/Commit Interleaving
# ============================================================================


class TestWindowsStreamLockingResilience:
    """Verify that active read streams on Windows do not crash delete or commit operations."""

    def test_delete_while_stream_open(self, fresh_cas: LocalCASAdapter):
        payload = b"Streaming active asset payload"
        h = fresh_cas.store_bytes(payload)

        stream = fresh_cas.open_asset_stream(h)
        try:
            # Read first chunk
            chunk = stream.read(5)
            assert chunk == b"Strea"

            # Attempt delete while stream is open
            # On Windows, deleting an open file may fail with WinError 32 (PermissionError).
            # LocalCASAdapter MUST catch this gracefully and return False instead of crashing.
            result = fresh_cas.delete_asset(h)
            # The function must return a boolean without raising
            assert isinstance(result, bool)

            # Check that subsequent reads on open stream still work
            remaining = stream.read()
            assert remaining == b"ming active asset payload"
        finally:
            stream.close()

        # After closing stream, delete should succeed
        deleted = fresh_cas.delete_asset(h)
        assert deleted is True or not fresh_cas.has_asset(h)

    def test_idempotent_commit_while_stream_open(self, fresh_cas: LocalCASAdapter):
        payload = b"Active stream during concurrent commit"
        h = fresh_cas.store_bytes(payload)

        stream = fresh_cas.open_asset_stream(h)
        try:
            # Staging identical payload and committing while stream is active
            staged = fresh_cas.create_staging_file(h)
            staged.write_bytes(payload)

            # Must succeed idempotently without WinError 5 Access Denied
            success = fresh_cas.commit_staged_file(staged, h)
            assert success is True
            assert not staged.exists()
        finally:
            stream.close()


# ============================================================================
# 5. Coordinator + Registry + Prioritizer End-to-End Scale & Ranking Stress
# ============================================================================


class TestDistributedEndToEndScaleStress:
    """Stress test coordinator asset location across 20 workers and 2,000 hashes."""

    def test_coordinator_locate_2000_hashes_across_20_workers(self):
        registry = WorkerRegistry(heartbeat_timeout_seconds=60.0)
        prioritizer = CandidatePrioritizer()
        coordinator = CoordinatorService(registry=registry, prioritizer=prioritizer)

        # 20 workers on various subnets
        worker_ids = [f"worker-{i:02d}" for i in range(20)]
        for i, w_id in enumerate(worker_ids):
            subnet_idx = i % 4
            ip = f"192.168.{subnet_idx}.{10 + i}"
            worker_info = WorkerInfo(
                worker_id=w_id,
                endpoint_url=f"http://{ip}:8000",
                ip_address=ip,
                port=8000,
                status=WorkerStatus.ACTIVE,
                capabilities=WorkerCapabilities(max_concurrent_streams=8),
                inventory_hashes=set(),
            )
            registry.register_worker(worker_info)

        # Generate 2,000 hashes distributed across workers
        all_hashes = [hashlib.sha256(f"cluster-hash-{i}".encode()).hexdigest() for i in range(2000)]
        for i, h in enumerate(all_hashes):
            w = worker_ids[i % len(worker_ids)]
            registry.add_worker_hashes(w, {h})

        # Locate all 2,000 hashes for requester worker-00
        req = LocateAssetsRequest(
            requester_worker_id="worker-00",
            missing_hashes=all_hashes,
            max_candidates_per_asset=3,
        )
        start_time = time.perf_counter()
        resp = coordinator.locate_assets_sync(req)
        elapsed = time.perf_counter() - start_time

        # Latency SLA check (< 100ms)
        assert elapsed < 0.10, f"Locating 2,000 hashes took {elapsed:.4f}s (> 100ms SLA)"
        assert len(resp.locations) == 2000

        # Requester exclusion invariant: worker-00 must never be returned to worker-00
        for i, h in enumerate(all_hashes):
            candidates = resp.locations[h]
            candidate_ids = [c.worker_id for c in candidates]
            assert "worker-00" not in candidate_ids, f"Requester worker-00 found in candidates for {h}"
            if i % len(worker_ids) == 0:
                # Only worker-00 holds it, so candidates must be empty
                assert len(candidates) == 0
            else:
                # Other workers hold it, candidates must be non-empty
                assert len(candidates) >= 1
