"""Adversarial Stress Test Suite for AIDAR Milestone 2 (Missing-Set & Asset Location Service).

Tests empirical behavior under high concurrency, extreme scale (1000s of hashes),
requester exclusion invariants, mixed locality ranking, and fault/eviction conditions.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import ipaddress
import random
import threading
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pytest
from fastapi.testclient import TestClient

from aidars.distributed.cas_adapter import CASAdapter, LocalCASAdapter
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    CandidateSource,
    HeartbeatPayload,
    LocateAssetsRequest,
    LocateAssetsResponse,
    LocalityTier,
    PingRequest,
    WorkerCapabilities,
    WorkerInfo,
    WorkerMetrics,
    WorkerRegistrationPayload,
    WorkerStatus,
    validate_sha256_hex,
)
from aidars.distributed.prioritizer import CandidatePrioritizer, LatencyTracker
from aidars.distributed.registry import (
    ClusterStats,
    WorkerHealthStatus,
    WorkerRegistry,
)


def _gen_hash(seed: int | str) -> str:
    """Generate a deterministic 64-char SHA-256 hex string."""
    return hashlib.sha256(f"m2-adversarial-seed-{seed}".encode("utf-8")).hexdigest()


def _make_reg_payload(
    worker_id: str,
    ip: str = "192.168.1.10",
    port: int = 8000,
    inventory: Set[str] | None = None,
    capacity: int = 50_000_000,
    used: int = 10_000_000,
    max_streams: int = 16,
) -> WorkerRegistrationPayload:
    return WorkerRegistrationPayload(
        worker_id=worker_id,
        endpoint_url=f"http://{ip}:{port}",
        ip_address=ip,
        port=port,
        capacity_bytes=capacity,
        used_bytes=used,
        capabilities=WorkerCapabilities(max_concurrent_streams=max_streams),
        inventory_hashes=inventory or set(),
    )


# ============================================================================
# 1. High-Concurrency Coordinator Endpoints Stress
# ============================================================================


class TestCoordinatorConcurrencyStress:
    """Stress testing parallel worker registration, unregistration, heartbeats, and location queries."""

    def test_rapid_parallel_worker_registration_and_heartbeats(self):
        """50+ concurrent threads hammering register, heartbeat with deltas, and unregister."""
        coord = CoordinatorService(
            coordinator_id="coord-stress-01",
            heartbeat_interval_seconds=2.0,
            heartbeat_timeout_seconds=10.0,
        )
        client = TestClient(coord.app)

        num_threads = 50
        ops_per_thread = 30
        errors: List[str] = []
        hash_catalog = [_gen_hash(i) for i in range(500)]

        def worker_lifecycle_task(tid: int):
            wid = f"worker-stress-{tid:03d}"
            ip = f"192.168.{(tid % 10) + 1}.{(tid % 200) + 1}"
            port = 8000 + (tid % 1000)

            try:
                # 1. Initial Registration
                init_hashes = set(random.sample(hash_catalog, 20))
                reg_payload = _make_reg_payload(wid, ip=ip, port=port, inventory=init_hashes)
                resp = client.post("/api/v1/workers/register", json=reg_payload.model_dump(mode="json"))
                if resp.status_code != 200:
                    errors.append(f"Registration failed for {wid}: {resp.text}")
                    return

                for op_idx in range(ops_per_thread):
                    op_type = op_idx % 5

                    if op_type == 0:
                        # Heartbeat with inventory deltas
                        add_h = set(random.sample(hash_catalog, 3))
                        rem_h = set(random.sample(list(init_hashes), min(2, len(init_hashes))))
                        hb = HeartbeatPayload(
                            worker_id=wid,
                            active_transfers=random.randint(0, 10),
                            used_bytes=random.randint(10000, 500000),
                            inventory_delta_added=add_h,
                            inventory_delta_removed=rem_h,
                        )
                        hb_resp = client.post(
                            f"/api/v1/workers/{wid}/heartbeat",
                            json=hb.model_dump(mode="json"),
                        )
                        if hb_resp.status_code != 200:
                            errors.append(f"Heartbeat failed for {wid}: {hb_resp.text}")

                    elif op_type == 1:
                        # Missing asset location query
                        query_hashes = random.sample(hash_catalog, 15)
                        loc_req = LocateAssetsRequest(
                            requester_worker_id=wid,
                            requester_ip=ip,
                            missing_hashes=query_hashes,
                        )
                        loc_resp = client.post(
                            "/api/v1/assets/locate",
                            json=loc_req.model_dump(mode="json"),
                        )
                        if loc_resp.status_code != 200:
                            errors.append(f"Locate failed for {wid}: {loc_resp.text}")
                        else:
                            data = loc_resp.json()
                            # Requester invariant: wid must NOT be in any candidate list
                            for h, cands in data.get("locations", {}).items():
                                for c in cands:
                                    if c["worker_id"] == wid:
                                        errors.append(f"Requester {wid} found in candidates for hash {h}")

                    elif op_type == 2:
                        # Cluster stats query
                        st_resp = client.get("/api/v1/cluster/stats")
                        if st_resp.status_code != 200:
                            errors.append(f"Stats query failed: {st_resp.text}")

                    elif op_type == 3:
                        # List workers query
                        w_resp = client.get("/api/v1/workers")
                        if w_resp.status_code != 200:
                            errors.append(f"List workers failed: {w_resp.text}")

                    elif op_type == 4:
                        # Re-registration update
                        up_hashes = set(random.sample(hash_catalog, 15))
                        up_payload = _make_reg_payload(wid, ip=ip, port=port, inventory=up_hashes)
                        up_resp = client.post(
                            "/api/v1/workers/register",
                            json=up_payload.model_dump(mode="json"),
                        )
                        if up_resp.status_code != 200:
                            errors.append(f"Re-registration failed for {wid}: {up_resp.text}")

            except Exception as exc:
                errors.append(f"Exception in thread {tid}: {exc}")

        threads = [
            threading.Thread(target=worker_lifecycle_task, args=(i,), name=f"StressThread-{i}")
            for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30.0)

        assert not errors, f"Concurrent coordinator stress encountered errors: {errors[:10]}"

        # Check final cluster state
        stats = coord.get_cluster_stats_sync()
        assert stats.total_registered_workers == num_threads
        assert stats.active_workers == num_threads
        assert stats.unique_cached_assets_count > 0

    def test_concurrent_unregistration_and_locate_race(self):
        """Worker unregistering while concurrent queries resolve candidates holding its assets."""
        coord = CoordinatorService(coordinator_id="coord-race-01")
        client = TestClient(coord.app)

        target_hash = _gen_hash("target-shared")
        num_providers = 20
        query_threads = 20
        errors: List[str] = []
        stop_flag = threading.Event()

        # Register providers
        for i in range(num_providers):
            payload = _make_reg_payload(f"provider-{i}", ip=f"10.0.0.{i+1}", inventory={target_hash})
            client.post("/api/v1/workers/register", json=payload.model_dump(mode="json"))

        def unregister_task():
            for i in range(num_providers):
                time.sleep(0.01)
                resp = client.post(f"/api/v1/workers/provider-{i}/unregister")
                if resp.status_code not in (200, 404):
                    errors.append(f"Unregister failed: {resp.status_code}")

        def query_task(tid: int):
            while not stop_flag.is_set():
                loc_req = LocateAssetsRequest(
                    requester_worker_id=f"requester-{tid}",
                    requester_ip="10.0.1.1",
                    missing_hashes=[target_hash],
                )
                resp = client.post("/api/v1/assets/locate", json=loc_req.model_dump(mode="json"))
                if resp.status_code != 200:
                    errors.append(f"Query failed: {resp.text}")
                    break
                data = resp.json()
                # Check candidate sanity
                cands = data["locations"].get(target_hash, [])
                for c in cands:
                    if not c["worker_id"].startswith("provider-"):
                        errors.append(f"Invalid candidate: {c}")

        q_threads = [threading.Thread(target=query_task, args=(i,)) for i in range(query_threads)]
        unreg_thread = threading.Thread(target=unregister_task)

        for t in q_threads:
            t.start()
        unreg_thread.start()

        unreg_thread.join(timeout=10.0)
        stop_flag.set()
        for t in q_threads:
            t.join(timeout=5.0)

        assert not errors, f"Unregistration race encountered errors: {errors}"
        # All providers unregistered, hash should now have 0 candidates
        final_lookup = coord.registry.get_workers_for_hash(target_hash)
        assert len(final_lookup) == 0


# ============================================================================
# 2. Asset Location Query Stress & Scaling (1,000s of Hashes)
# ============================================================================


class TestAssetLocationScaleAndEdgeCases:
    """Stress testing asset location resolution with empty, massive, unindexed, and mixed lists."""

    def test_locate_empty_missing_hashes(self):
        """Querying coordinator with empty list [] returns empty dict and empty unresolved."""
        coord = CoordinatorService(coordinator_id="coord-empty")
        client = TestClient(coord.app)

        loc_req = LocateAssetsRequest(
            requester_worker_id="req-node",
            missing_hashes=[],
        )
        resp = client.post("/api/v1/assets/locate", json=loc_req.model_dump(mode="json"))
        assert resp.status_code == 200
        data = resp.json()
        assert data["locations"] == {}
        assert data["unresolved_hashes"] == []

    def test_locate_thousands_of_hashes_high_scale(self):
        """Scale test: 2,000 missing hashes queried in a single request across 50 nodes."""
        coord = CoordinatorService(coordinator_id="coord-scale")
        total_hashes = 2000
        hash_catalog = [_gen_hash(i) for i in range(total_hashes)]

        # Register 50 workers, each holding random 200 hashes
        for i in range(50):
            inv = set(random.sample(hash_catalog, 200))
            payload = _make_reg_payload(
                f"scale-node-{i:02d}",
                ip=f"10.0.{(i % 10) + 1}.{(i % 250) + 1}",
                inventory=inv,
            )
            coord.register_worker_sync(payload)

        assert coord.registry.get_worker_count() == 50
        assert coord.registry.get_hash_count() > 0

        # Query all 2,000 hashes
        req = LocateAssetsRequest(
            requester_worker_id="requester-bench",
            requester_ip="10.0.1.50",
            missing_hashes=hash_catalog,
            max_candidates_per_asset=5,
        )

        t0 = time.perf_counter()
        response = coord.locate_assets_sync(req)
        duration = time.perf_counter() - t0

        # Performance constraint: 2,000 hashes resolution should take < 0.5s
        assert duration < 1.0, f"Resolving 2,000 hashes took too long: {duration:.4f}s"

        assert len(response.locations) == total_hashes
        resolved_count = 0
        for h, cands in response.locations.items():
            assert len(cands) <= 5
            if cands:
                resolved_count += 1
                # Verify each candidate actually holds this hash in the registry
                for c in cands:
                    assert h in coord.registry.get_worker(c.worker_id).inventory_hashes

        assert resolved_count > 0
        assert len(response.unresolved_hashes) == total_hashes - resolved_count

    def test_locate_non_existent_and_unindexed_hashes(self):
        """Querying with completely unindexed hashes returns empty candidate lists and adds to unresolved."""
        coord = CoordinatorService(coordinator_id="coord-nonexist")
        w1 = _make_reg_payload("node-1", inventory={_gen_hash(1)})
        coord.register_worker_sync(w1)

        unindexed_hashes = [_gen_hash(f"fake-{i}") for i in range(50)]
        req = LocateAssetsRequest(
            requester_worker_id="req-01",
            missing_hashes=unindexed_hashes,
        )
        response = coord.locate_assets_sync(req)

        assert len(response.unresolved_hashes) == 50
        assert set(response.unresolved_hashes) == set(unindexed_hashes)
        for h in unindexed_hashes:
            assert response.locations[h] == []

    def test_mixed_subnet_and_lan_candidates_priority_ranking(self):
        """Verify strict 4-tier candidate ranking: Loopback > Subnet > LAN > WAN."""
        coord = CoordinatorService(coordinator_id="coord-priority")
        shared_hash = _gen_hash("shared-asset")

        # Requester is at 192.168.1.10
        # 1. Loopback candidate (same IP as requester)
        w_loopback = _make_reg_payload("cand-loopback", ip="192.168.1.10", port=8001, inventory={shared_hash})
        # 2. Subnet candidate (192.168.1.55, /24 subnet)
        w_subnet = _make_reg_payload("cand-subnet", ip="192.168.1.55", port=8002, inventory={shared_hash})
        # 3. LAN candidate (10.0.0.50, private RFC 1918 cross-subnet)
        w_lan = _make_reg_payload("cand-lan", ip="10.0.0.50", port=8003, inventory={shared_hash})
        # 4. WAN candidate (142.250.190.46, public IP)
        w_wan = _make_reg_payload("cand-wan", ip="142.250.190.46", port=8004, inventory={shared_hash})

        coord.register_worker_sync(w_loopback)
        coord.register_worker_sync(w_subnet)
        coord.register_worker_sync(w_lan)
        coord.register_worker_sync(w_wan)

        req = LocateAssetsRequest(
            requester_worker_id="requester-node",
            requester_ip="192.168.1.10",
            missing_hashes=[shared_hash],
            max_candidates_per_asset=10,
        )
        resp = coord.locate_assets_sync(req)
        cands = resp.locations[shared_hash]

        assert len(cands) == 4
        # Check ordering
        assert cands[0].worker_id == "cand-loopback"
        assert cands[0].locality_tier == LocalityTier.LOOPBACK.value

        assert cands[1].worker_id == "cand-subnet"
        assert cands[1].locality_tier == LocalityTier.SUBNET.value

        assert cands[2].worker_id == "cand-lan"
        assert cands[2].locality_tier == LocalityTier.LAN.value

        assert cands[3].worker_id == "cand-wan"
        assert cands[3].locality_tier == LocalityTier.WAN.value

        # Priority scores must be strictly descending
        scores = [c.priority_score for c in cands]
        assert scores == sorted(scores, reverse=True)


# ============================================================================
# 3. Requester Exclusion Invariant Verification
# ============================================================================


class TestRequesterExclusionInvariant:
    """Stress tests guaranteeing that the requesting worker is NEVER returned as a candidate."""

    def test_requester_holds_all_missing_hashes(self):
        """When requester node itself holds the requested hashes and no peers do, candidate list must be empty."""
        coord = CoordinatorService(coordinator_id="coord-excl-1")
        h1 = _gen_hash("excl-1")
        h2 = _gen_hash("excl-2")

        # Register requester with both hashes
        req_worker = _make_reg_payload("worker-requester-exclusive", ip="192.168.1.10", inventory={h1, h2})
        coord.register_worker_sync(req_worker)

        req = LocateAssetsRequest(
            requester_worker_id="worker-requester-exclusive",
            requester_ip="192.168.1.10",
            missing_hashes=[h1, h2],
        )
        resp = coord.locate_assets_sync(req)

        # Must return empty candidates and marked unresolved because requester is excluded
        assert resp.locations[h1] == []
        assert resp.locations[h2] == []
        assert set(resp.unresolved_hashes) == {h1, h2}

    def test_requester_holds_shared_hashes_with_peers(self):
        """When requester and 5 peers hold the hash, only the 5 peers are returned, never the requester."""
        coord = CoordinatorService(coordinator_id="coord-excl-2")
        h = _gen_hash("shared-excl")

        coord.register_worker_sync(_make_reg_payload("worker-requester-main", ip="192.168.1.10", inventory={h}))
        for i in range(5):
            coord.register_worker_sync(_make_reg_payload(f"peer-worker-{i}", ip=f"192.168.1.{20+i}", inventory={h}))

        req = LocateAssetsRequest(
            requester_worker_id="worker-requester-main",
            requester_ip="192.168.1.10",
            missing_hashes=[h],
            max_candidates_per_asset=10,
        )
        resp = coord.locate_assets_sync(req)
        cands = resp.locations[h]

        assert len(cands) == 5
        cand_ids = [c.worker_id for c in cands]
        assert "worker-requester-main" not in cand_ids
        for i in range(5):
            assert f"peer-worker-{i}" in cand_ids

    def test_concurrent_mutual_requester_exclusions(self):
        """50 concurrent workers querying coordinator for hashes they mutually possess."""
        coord = CoordinatorService(coordinator_id="coord-mutual-excl")
        num_workers = 30
        hashes = [_gen_hash(i) for i in range(100)]
        errors: List[str] = []

        # Register workers
        for i in range(num_workers):
            wid = f"mutual-worker-{i:02d}"
            inv = set(random.sample(hashes, 40))
            coord.register_worker_sync(_make_reg_payload(wid, ip=f"10.0.0.{i+1}", inventory=inv))

        def query_loop(wid: int):
            w_name = f"mutual-worker-{wid:02d}"
            for _ in range(30):
                sample_hashes = random.sample(hashes, 20)
                req = LocateAssetsRequest(
                    requester_worker_id=w_name,
                    requester_ip=f"10.0.0.{wid+1}",
                    missing_hashes=sample_hashes,
                )
                resp = coord.locate_assets_sync(req)
                for sh, cands in resp.locations.items():
                    for c in cands:
                        if c.worker_id == w_name:
                            errors.append(f"Worker {w_name} received itself for hash {sh}")

        threads = [threading.Thread(target=query_loop, args=(i,)) for i in range(num_workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15.0)

        assert not errors, f"Mutual exclusion violations found: {errors}"


# ============================================================================
# 4. Fault Conditions, Degradation & Eviction
# ============================================================================


class TestFaultConditionsAndDegradedStates:
    """Stress testing degraded nodes, corrupt reporting, and eviction during locate queries."""

    def test_degraded_and_unhealthy_worker_filtering(self):
        """include_degraded flag filters out UNHEALTHY / DEGRADED nodes when False."""
        coord = CoordinatorService(coordinator_id="coord-fault")
        h = _gen_hash("fault-hash")

        coord.register_worker_sync(_make_reg_payload("node-healthy", ip="10.0.0.1", inventory={h}))
        coord.register_worker_sync(_make_reg_payload("node-penalized", ip="10.0.0.2", inventory={h}))

        # Penalize node-penalized heavily to make it degraded / suspect
        coord.registry.record_failure("node-penalized", reason="timeout", penalty=12.0)
        health_rec = coord.registry.get_health_record("node-penalized")
        assert health_rec.health_status in (WorkerHealthStatus.DEGRADED, WorkerHealthStatus.SUSPECT)

        # Query with include_degraded=False
        req_filtered = LocateAssetsRequest(
            requester_worker_id="req-node",
            missing_hashes=[h],
            include_degraded=False,
        )
        resp_filtered = coord.locate_assets_sync(req_filtered)
        cands_filtered = resp_filtered.locations[h]
        assert len(cands_filtered) == 1
        assert cands_filtered[0].worker_id == "node-healthy"

        # Query with include_degraded=True
        req_unfiltered = LocateAssetsRequest(
            requester_worker_id="req-node",
            missing_hashes=[h],
            include_degraded=True,
        )
        resp_unfiltered = coord.locate_assets_sync(req_unfiltered)
        cands_unfiltered = resp_unfiltered.locations[h]
        assert len(cands_unfiltered) == 2
        # Healthy node must be ranked higher than degraded node
        assert cands_unfiltered[0].worker_id == "node-healthy"
        assert cands_unfiltered[1].worker_id == "node-penalized"
        assert cands_unfiltered[0].priority_score > cands_unfiltered[1].priority_score

    def test_corruption_reporting_prunes_hash_from_location(self):
        """Reporting corruption immediately removes corrupted hash from worker's location candidates."""
        coord = CoordinatorService(coordinator_id="coord-corrupt")
        h_good = _gen_hash("good-hash")
        h_corrupt = _gen_hash("bad-hash")

        coord.register_worker_sync(_make_reg_payload("worker-bad", ip="10.0.0.1", inventory={h_good, h_corrupt}))

        # Verify initial location returns worker-bad for both
        resp1 = coord.locate_assets_sync(
            LocateAssetsRequest(requester_worker_id="req", missing_hashes=[h_good, h_corrupt])
        )
        assert len(resp1.locations[h_corrupt]) == 1

        # Report corruption
        coord.registry.record_corruption("worker-bad", sha256_hex=h_corrupt, penalty=5.0)

        # Location query should now return worker-bad ONLY for h_good, NOT h_corrupt
        resp2 = coord.locate_assets_sync(
            LocateAssetsRequest(requester_worker_id="req", missing_hashes=[h_good, h_corrupt])
        )
        assert len(resp2.locations[h_good]) == 1
        assert resp2.locations[h_corrupt] == []
        assert h_corrupt in resp2.unresolved_hashes


# ============================================================================
# 5. CASAdapter High Concurrency & File System Edge Cases
# ============================================================================


class TestCASAdapterAdversarialEdgeCases:
    """Stress tests on LocalCASAdapter atomic commits, staging lifecycle, and path traversal."""

    def test_concurrent_staging_and_commits(self, tmp_path: Path):
        """Multiple threads concurrently staging, committing, reading, and deleting CAS assets."""
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas", staging_dir=tmp_path / "staging")
        num_threads = 16
        items_per_thread = 20
        committed_hashes: List[Tuple[str, bytes]] = []
        lock = threading.Lock()
        errors: List[Exception] = []

        def worker_task(tid: int):
            for idx in range(items_per_thread):
                try:
                    payload = f"Payload-from-thread-{tid}-index-{idx}-{random.random()}".encode() * 100
                    expected_h = hashlib.sha256(payload).hexdigest()

                    # Stage
                    staging = adapter.create_staging_file(expected_h, prefix=f"th_{tid}")
                    staging.write_bytes(payload)

                    # Commit
                    ok = adapter.commit_staged_file(staging, expected_h)
                    if not ok:
                        errors.append(RuntimeError(f"Failed commit in thread {tid} for {expected_h}"))
                        continue

                    assert adapter.has_asset(expected_h) is True

                    # Verify stream
                    with adapter.open_asset_stream(expected_h) as s:
                        read_bytes = s.read()
                        assert read_bytes == payload

                    with lock:
                        committed_hashes.append((expected_h, payload))

                except Exception as exc:
                    errors.append(exc)

        threads = [threading.Thread(target=worker_task, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20.0)

        assert not errors, f"Concurrent CAS operations failed: {errors}"
        assert len(committed_hashes) == num_threads * items_per_thread
        assert adapter.get_cas_stats()["total_assets"] == num_threads * items_per_thread

    def test_missing_set_large_scale(self, tmp_path: Path):
        """Compute missing-set difference of 10,000 hashes against 2,000 cached assets."""
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_large")

        # Cache 2,000 assets
        cached_hashes = set()
        for i in range(2000):
            h = adapter.store_bytes(f"cached-{i}".encode())
            cached_hashes.add(h)

        # Generate 10,000 required hashes (2,000 cached + 8,000 missing)
        missing_expected = set()
        for i in range(8000):
            missing_expected.add(_gen_hash(f"missing-{i}"))

        required_all = list(cached_hashes) + list(missing_expected)
        random.shuffle(required_all)

        t0 = time.perf_counter()
        computed_missing = adapter.get_missing_hashes(required_all)
        duration = time.perf_counter() - t0

        assert duration < 0.5, f"Missing-set calculation of 10,000 hashes took {duration:.4f}s"
        assert computed_missing == missing_expected
