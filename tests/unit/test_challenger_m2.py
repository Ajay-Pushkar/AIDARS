"""Adversarial challenge test suite for Milestone 2 (LocalCASAdapter and CoordinatorService).

Stress tests:
1. Path traversal and injection attacks on hash inputs.
2. Corrupted staging file commits and leak verification (zero file leak guarantee).
3. Concurrent read/write race conditions on identical and distinct hashes.
4. Missing-set computation and candidate location resolution edge cases.
"""
from __future__ import annotations

import concurrent.futures
import hashlib
import os
import random
import string
import tempfile
import threading
import time
from pathlib import Path
from typing import List, Set

import pytest
from fastapi.testclient import TestClient

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    HeartbeatPayload,
    LocateAssetsRequest,
    LocateAssetsResponse,
    WorkerCapabilities,
    WorkerInfo,
    WorkerRegistrationPayload,
    WorkerStatus,
    validate_sha256_hex,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def cas_fixture(tmp_path: Path) -> LocalCASAdapter:
    cas_dir = tmp_path / "adv_cas"
    staging_dir = tmp_path / "adv_staging"
    return LocalCASAdapter(cas_dir=cas_dir, staging_dir=staging_dir, chunk_size=2048)


@pytest.fixture
def coordinator_fixture() -> CoordinatorService:
    return CoordinatorService(
        coordinator_id="adv-coord-01",
        heartbeat_interval_seconds=2.0,
        heartbeat_timeout_seconds=5.0,
        eviction_interval_seconds=1.0,
    )


# ============================================================================
# 1. Path Traversal & Input Injection Attack Suite
# ============================================================================


class TestPathTraversalAndInjection:
    """Adversarial injection payloads targeting CAS filesystem resolution."""

    MALICIOUS_HASH_PAYLOADS = [
        # Relative traversal
        "../../../../../../../../../../etc/passwd",
        r"..\..\..\..\windows\system32\cmd.exe",
        "objects/../../secret.txt",
        "/etc/shadow",
        r"C:\Windows\System32\drivers\etc\hosts",
        # Null bytes and control chars
        "a" * 64 + "\x00.exe",
        "\x00" + "a" * 63,
        "a" * 60 + "\r\n\t",
        # Non-hex characters
        "g" * 64,
        "z" * 64,
        "!@#$%^&*()" + "a" * 54,
        "🚀" * 32,
        # Invalid lengths
        "",
        "a" * 63,
        "a" * 65,
        "a" * 128,
        "   ",
        "0" * 63 + "/",
        "0" * 63 + "\\",
    ]

    def test_local_cas_path_traversal_rejection(self, cas_fixture: LocalCASAdapter):
        for payload in self.MALICIOUS_HASH_PAYLOADS:
            # has_asset must return False
            assert cas_fixture.has_asset(payload) is False, f"has_asset failed on: {payload!r}"

            # get_asset_path must return None
            assert cas_fixture.get_asset_path(payload) is None, f"get_asset_path failed on: {payload!r}"

            # get_asset_size must return None
            assert cas_fixture.get_asset_size(payload) is None, f"get_asset_size failed on: {payload!r}"

            # delete_asset must return False without side effects
            assert cas_fixture.delete_asset(payload) is False, f"delete_asset failed on: {payload!r}"

            # open_asset_stream must raise (ValueError or FileNotFoundError)
            with pytest.raises((ValueError, FileNotFoundError)):
                cas_fixture.open_asset_stream(payload)

    def test_uppercase_and_whitespace_normalization(self, cas_fixture: LocalCASAdapter):
        data = b"Uppercase and whitespace normalization test"
        real_hash = hashlib.sha256(data).hexdigest()

        cas_fixture.store_bytes(data)

        # Uppercase query
        upper_hash = real_hash.upper()
        assert cas_fixture.has_asset(upper_hash) is True
        assert cas_fixture.get_asset_path(upper_hash) is not None

        # Padded with whitespace
        padded_hash = f"  {real_hash}  "
        assert cas_fixture.has_asset(padded_hash) is True

        # Missing-set normalization
        missing = cas_fixture.get_missing_hashes([upper_hash, padded_hash])
        assert missing == set()

    def test_coordinator_locate_rejects_injection_via_pydantic(self, coordinator_fixture: CoordinatorService):
        client = TestClient(coordinator_fixture.app)

        for bad_hash in ["../../etc/passwd", "g" * 64, "short_hash"]:
            body = {
                "requester_worker_id": "test-node",
                "missing_hashes": [bad_hash],
            }
            resp = client.post("/api/v1/assets/locate", json=body)
            assert resp.status_code in (400, 422), f"Expected HTTP 422/400 for {bad_hash!r}, got {resp.status_code}"


# ============================================================================
# 2. Corrupted Staging File Commits & Leak Verification
# ============================================================================


class TestCorruptStagingAndZeroLeak:
    """Verify immediate deletion and zero residual leak upon corrupt or mismatched commit."""

    def test_corrupt_data_commit_deletes_staging_and_leaves_no_cas_file(
        self, cas_fixture: LocalCASAdapter
    ):
        expected_data = b"Expected legitimate payload 12345"
        expected_hash = hashlib.sha256(expected_data).hexdigest()

        # Write corrupted content (1 byte flipped)
        corrupted_data = b"Expected legitimate payload 12346"
        staging_file = cas_fixture.create_staging_file(expected_hash, prefix="corrupt_test")
        staging_file.write_bytes(corrupted_data)

        assert staging_file.exists()

        success = cas_fixture.commit_staged_file(staging_file, expected_hash)
        assert success is False

        # Staging file MUST be deleted
        assert not staging_file.exists(), "Staging file leaked after corruption mismatch!"

        # CAS object MUST NOT exist
        assert cas_fixture.has_asset(expected_hash) is False

        # Staging directory must be completely clean
        staging_files = list(cas_fixture.staging_dir.iterdir())
        assert len(staging_files) == 0, f"Residual files found in staging dir: {staging_files}"

    def test_commit_with_invalid_hash_deletes_staging(self, cas_fixture: LocalCASAdapter):
        staging_file = cas_fixture.create_staging_file(prefix="bad_hash")
        staging_file.write_bytes(b"some random data")

        success = cas_fixture.commit_staged_file(staging_file, "../../bad/path")
        assert success is False
        assert not staging_file.exists(), "Staging file leaked on invalid hash!"

    def test_store_file_mismatch_cleans_staging(self, cas_fixture: LocalCASAdapter, tmp_path: Path):
        src_file = tmp_path / "src_mismatch.bin"
        src_file.write_bytes(b"Original content")
        wrong_hash = "0" * 64

        with pytest.raises(ValueError):
            cas_fixture.store_file(src_file, expected_sha256=wrong_hash)

        # Verify zero staging files remain
        staging_files = list(cas_fixture.staging_dir.iterdir())
        assert len(staging_files) == 0, f"Staging leak on store_file failure: {staging_files}"

    def test_multiple_sequential_corruptions_zero_leak(self, cas_fixture: LocalCASAdapter):
        for i in range(20):
            data = f"Corrupt payload attempt {i}".encode()
            fake_hash = hashlib.sha256(f"Different payload {i}".encode()).hexdigest()

            staged = cas_fixture.create_staging_file(fake_hash)
            staged.write_bytes(data)
            assert cas_fixture.commit_staged_file(staged, fake_hash) is False

        # Ensure staging dir is completely empty
        assert len(list(cas_fixture.staging_dir.iterdir())) == 0
        assert cas_fixture.get_cas_stats()["total_assets"] == 0


# ============================================================================
# 3. High-Concurrency Stress Harness
# ============================================================================


class TestConcurrencyStress:
    """Concurrent multi-threaded read/write stress on same and different hashes."""

    def test_concurrent_writes_to_same_hash(self, cas_fixture: LocalCASAdapter):
        """Multiple threads concurrently writing/committing the exact same asset."""
        payload = b"Shared immutable deterministic payload for concurrency test" * 100
        target_hash = hashlib.sha256(payload).hexdigest()

        num_threads = 16
        results: List[bool] = []
        lock = threading.Lock()

        def concurrent_writer():
            staged = cas_fixture.create_staging_file(target_hash, prefix="race")
            staged.write_bytes(payload)
            res = cas_fixture.commit_staged_file(staged, target_hash)
            with lock:
                results.append(res)

        threads = [threading.Thread(target=concurrent_writer) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads should report success or atomic commit without crashing
        assert len(results) == num_threads
        assert all(results)

        # Asset must exist and be intact
        assert cas_fixture.has_asset(target_hash) is True
        with cas_fixture.open_asset_stream(target_hash) as s:
            assert s.read() == payload

        # Zero leaked staging files
        assert len(list(cas_fixture.staging_dir.iterdir())) == 0

    def test_concurrent_simultaneous_reads_and_writes(self, cas_fixture: LocalCASAdapter):
        """Threads reading an asset while other threads write new distinct assets."""
        # Pre-seed base asset
        base_data = b"Base readable asset data" * 500
        base_hash = cas_fixture.store_bytes(base_data)

        num_readers = 10
        num_writers = 10
        iterations = 15

        read_errors: List[str] = []
        write_hashes: List[str] = []
        lock = threading.Lock()

        def reader_task():
            for _ in range(iterations):
                try:
                    with cas_fixture.open_asset_stream(base_hash) as stream:
                        content = stream.read()
                        if content != base_data:
                            with lock:
                                read_errors.append("Data corruption during read")
                except Exception as exc:
                    with lock:
                        read_errors.append(f"Read exception: {exc}")

        def writer_task(tid: int):
            for i in range(iterations):
                w_data = f"Writer-{tid}-iteration-{i}".encode() * 200
                h = cas_fixture.store_bytes(w_data)
                with lock:
                    write_hashes.append(h)

        threads: List[threading.Thread] = []
        for _ in range(num_readers):
            threads.append(threading.Thread(target=reader_task))
        for tid in range(num_writers):
            threads.append(threading.Thread(target=writer_task, args=(tid,)))

        random.shuffle(threads)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(read_errors) == 0, f"Read errors during concurrent writes: {read_errors}"
        assert len(write_hashes) == num_writers * iterations
        assert cas_fixture.has_asset(base_hash) is True

    def test_concurrent_writes_and_deletes(self, cas_fixture: LocalCASAdapter):
        """Threads creating assets while other threads delete created assets."""
        num_workers = 8
        items = 20
        created_hashes: Set[str] = set()
        lock = threading.Lock()

        def worker_cycle(wid: int):
            for i in range(items):
                data = f"Worker-{wid}-item-{i}-{time.time()}".encode()
                h = cas_fixture.store_bytes(data)
                with lock:
                    created_hashes.add(h)

                # Delete half of the time
                if i % 2 == 0:
                    cas_fixture.delete_asset(h)

        threads = [threading.Thread(target=worker_cycle, args=(w,)) for w in range(num_workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # No crashes, inventory reflects existing files
        inventory = cas_fixture.get_inventory_hashes()
        for h in inventory:
            assert cas_fixture.has_asset(h) is True


# ============================================================================
# 4. Missing-Set & Coordinator Location Resolution Edge Cases
# ============================================================================


class TestMissingSetAndLocationResolution:
    """Boundary edge cases in missing-set computation and coordinator candidate ranking."""

    def test_missing_set_large_batch(self, cas_fixture: LocalCASAdapter):
        """Verify set difference computation on a large batch (1000 items)."""
        cached_hashes = []
        for i in range(200):
            h = cas_fixture.store_bytes(f"Cached item {i}".encode())
            cached_hashes.append(h)

        missing_hashes = [hashlib.sha256(f"Missing item {i}".encode()).hexdigest() for i in range(800)]
        all_required = cached_hashes + missing_hashes
        random.shuffle(all_required)

        result_missing = cas_fixture.get_missing_hashes(all_required)
        assert result_missing == set(missing_hashes)
        assert len(result_missing) == 800

    def test_coordinator_locate_multiple_workers_ranking(self, coordinator_fixture: CoordinatorService):
        """Verify multi-candidate ranking: Subnet > LAN > WAN."""
        h_common = "a" * 64

        # Register Subnet worker (192.168.1.50)
        w_subnet = WorkerRegistrationPayload(
            worker_id="worker-subnet",
            endpoint_url="http://192.168.1.50:8002",
            ip_address="192.168.1.50",
            port=8002,
            inventory_hashes={h_common},
        )
        # Register Private LAN worker (10.0.0.5)
        w_lan = WorkerRegistrationPayload(
            worker_id="worker-lan",
            endpoint_url="http://10.0.0.5:8003",
            ip_address="10.0.0.5",
            port=8003,
            inventory_hashes={h_common},
        )
        # Register WAN worker (8.8.8.8)
        w_wan = WorkerRegistrationPayload(
            worker_id="worker-wan",
            endpoint_url="http://8.8.8.8:8004",
            ip_address="8.8.8.8",
            port=8004,
            inventory_hashes={h_common},
        )

        coordinator_fixture.register_worker_sync(w_subnet)
        coordinator_fixture.register_worker_sync(w_lan)
        coordinator_fixture.register_worker_sync(w_wan)

        # Query from requester at 192.168.1.10
        req = LocateAssetsRequest(
            requester_worker_id="requester-node",
            requester_ip="192.168.1.10",
            missing_hashes=[h_common],
            max_candidates_per_asset=10,
        )
        resp = coordinator_fixture.locate_assets_sync(req)

        candidates = resp.locations[h_common]
        assert len(candidates) == 3
        # Candidate 0 should be Subnet (192.168.1.50)
        assert candidates[0].worker_id == "worker-subnet"
        assert candidates[0].locality_tier == "subnet"
        # Candidate 1 should be LAN (10.0.0.5)
        assert candidates[1].worker_id == "worker-lan"
        assert candidates[1].locality_tier == "lan"
        # Candidate 2 should be WAN (8.8.8.8)
        assert candidates[2].worker_id == "worker-wan"
        assert candidates[2].locality_tier == "wan"

    def test_coordinator_locate_excludes_requester(self, coordinator_fixture: CoordinatorService):
        h1 = "b" * 64
        w_self = WorkerRegistrationPayload(
            worker_id="node-self",
            endpoint_url="http://192.168.1.10:8000",
            ip_address="192.168.1.10",
            port=8000,
            inventory_hashes={h1},
        )
        coordinator_fixture.register_worker_sync(w_self)

        req = LocateAssetsRequest(
            requester_worker_id="node-self",
            missing_hashes=[h1],
        )
        resp = coordinator_fixture.locate_assets_sync(req)
        # Should be empty because only node-self has it, and node-self is excluded
        assert resp.locations[h1] == []
        assert h1 in resp.unresolved_hashes
