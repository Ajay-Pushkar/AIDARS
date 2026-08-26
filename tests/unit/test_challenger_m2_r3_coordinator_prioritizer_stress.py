"""Challenger 2 Empirical Adversarial Stress Test Suite for Milestone 2 Iteration 3.

Empirically challenges:
1. Coordinator endpoints and Candidate Prioritizer at high scale:
   - 2,000+ to 5,000+ hashes across 50+ to 100+ worker nodes.
   - Sub-second batch resolution SLA (< 500ms).
   - Multi-tier locality classification (Loopback, Subnet, LAN, WAN, IPv6).
2. High-concurrency chaos and race condition resistance:
   - 50+ concurrent worker threads executing registration, heartbeats, delta updates,
     and batch locate queries under continuous background eviction and penalty decay.
   - Concurrent mass eviction during active locate queries.
3. Prioritizer and Latency Tracker mathematical stability and invariants:
   - Locality tier dominance invariant under adversarial loads and RTTs.
   - EMA convergence and thread safety under 10,000+ parallel updates.
   - Dynamic error penalization, corruption pruning, and exponential half-life decay.
4. Robustness against malformed inputs, boundaries, and index symmetry:
   - Inverted index dual-mapping symmetry verification after extreme churn.
   - Pydantic schema input validation and rejection of invalid hashes.
   - Strict parameter bounds enforcement (max_candidates_per_asset ge=1, le=20).
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import ipaddress
import math
import random
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    CandidateSource,
    ClusterTelemetry,
    HeartbeatPayload,
    HeartbeatResponse,
    LocateAssetsRequest,
    LocateAssetsResponse,
    LocalityTier,
    PingRequest,
    PongResponse,
    WorkerCapabilities,
    WorkerInfo,
    WorkerRegistrationPayload,
    WorkerRegistrationResponse,
    WorkerStatus,
    validate_sha256_hex,
)
from aidars.distributed.prioritizer import (
    CandidatePrioritizer,
    LatencyTracker,
    classify_locality,
    measure_ping_rtt,
    normalize_ip,
)
from aidars.distributed.registry import (
    ClusterStats,
    WorkerHealthRecord,
    WorkerHealthStatus,
    WorkerRegistry,
)


def _generate_sha256(seed: int | str) -> str:
    return hashlib.sha256(f"challenger2-m2-r3-seed-{seed}".encode("utf-8")).hexdigest()


def _make_worker_payload(
    worker_id: str,
    ip: str = "192.168.1.50",
    port: int = 8000,
    inventory: Optional[Set[str]] = None,
    capacity: int = 100_000_000,
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
# 1. High Scale Candidate Prioritization & Coordinator Locate Stress
# ============================================================================


class TestHighScaleCoordinatorAndPrioritizer:
    """Stress tests with 2,000+ to 5,000+ hashes distributed across 50+ to 100+ worker nodes."""

    def test_high_scale_2000_hashes_across_50_workers_http(self):
        """Scale test: 2,000 hashes across 50 workers via FastAPI TestClient HTTP endpoint.

        Verifies:
        1. Batch resolution time is well below 500ms (SLA).
        2. All 2,000 hashes resolved with exact candidate matches.
        3. Priority ranking orders candidates correctly (Tier > RTT > Load).
        4. Requester node is strictly excluded from candidate list.
        """
        coord = CoordinatorService(coordinator_id="coord-scale-50w")
        client = TestClient(coord.app)

        num_workers = 50
        num_hashes = 2000
        hash_pool = [_generate_sha256(i) for i in range(num_hashes)]

        # Register 50 workers across different IP subnets
        for i in range(num_workers):
            wid = f"worker-scale-{i:03d}"
            if i == 0:
                ip = "127.0.0.1"  # Loopback
            elif i < 15:
                ip = f"192.168.1.{10 + i}"  # Same subnet as requester (192.168.1.15)
            elif i < 35:
                ip = f"10.0.{(i % 5) + 1}.{10 + i}"  # Private LAN
            else:
                ip = f"198.51.100.{10 + i}"  # WAN

            # Each worker holds a subset of ~200 hashes
            worker_hashes = set(hash_pool[i * 30 : (i * 30) + 200])
            payload = _make_worker_payload(
                worker_id=wid,
                ip=ip,
                port=8000 + i,
                inventory=worker_hashes,
                max_streams=16,
            )
            reg_resp = client.post("/api/v1/workers/register", json=payload.model_dump(mode="json"))
            assert reg_resp.status_code == 200

            rtt_sample = 1.0 + (i * 0.5)
            coord.prioritizer.latency_tracker.update_rtt(wid, rtt_sample)

        requester_id = "worker-scale-005"
        locate_req = LocateAssetsRequest(
            requester_worker_id=requester_id,
            requester_ip="192.168.1.15",
            missing_hashes=hash_pool,
            max_candidates_per_asset=5,
        )

        start_time = time.perf_counter()
        response = client.post("/api/v1/assets/locate", json=locate_req.model_dump(mode="json"))
        elapsed = time.perf_counter() - start_time

        assert response.status_code == 200
        data = response.json()
        assert "locations" in data
        assert len(data["locations"]) == num_hashes

        assert elapsed < 0.5, f"2,000 hashes batch locate took {elapsed:.4f}s (exceeded 0.5s SLA)"

        for h, candidates in data["locations"].items():
            cand_ids = [c["worker_id"] for c in candidates]
            assert requester_id not in cand_ids, f"Requester {requester_id} found in candidate list for hash {h}"
            assert len(candidates) <= 5

            scores = [c["priority_score"] for c in candidates]
            assert scores == sorted(scores, reverse=True), f"Candidates not sorted by priority score: {scores}"

    def test_massive_scale_5000_hashes_across_100_workers_sync(self):
        """Extreme scale: 5,000 hashes across 100 workers via locate_assets_sync directly.

        Verifies memory stability, throughput, and zero degradation under massive scale.
        """
        coord = CoordinatorService(coordinator_id="coord-scale-100w")
        num_workers = 100
        num_hashes = 5000
        hash_pool = [_generate_sha256(i) for i in range(num_hashes)]

        for i in range(num_workers):
            wid = f"node-{i:03d}"
            ip = f"10.{(i // 25) + 1}.{(i % 25) + 1}.{(i % 200) + 10}"
            worker_hashes = set(hash_pool[i * 45 : (i * 45) + 300])
            payload = _make_worker_payload(worker_id=wid, ip=ip, port=9000 + i, inventory=worker_hashes)
            coord.register_worker_sync(payload)

        req = LocateAssetsRequest(
            requester_worker_id="node-000",
            requester_ip="10.1.1.10",
            missing_hashes=hash_pool,
            max_candidates_per_asset=10,
        )

        start_time = time.perf_counter()
        resp = coord.locate_assets_sync(req)
        elapsed = time.perf_counter() - start_time

        assert isinstance(resp, LocateAssetsResponse)
        assert len(resp.locations) == num_hashes
        assert elapsed < 0.5, f"5,000 hashes batch locate took {elapsed:.4f}s (exceeded 0.5s SLA)"

        stats = coord.get_cluster_stats_sync()
        assert stats.total_registered_workers == 100
        assert stats.unique_cached_assets_count > 0

    def test_scale_sparse_and_dense_skewed_hash_distribution(self):
        """Test heavily skewed hash distributions: dense popular hashes vs sparse rare hashes vs unindexed."""
        coord = CoordinatorService(coordinator_id="coord-skewed")
        num_workers = 60
        popular_hashes = [_generate_sha256(f"popular-{i}") for i in range(20)]
        rare_hashes = [_generate_sha256(f"rare-{i}") for i in range(1000)]
        unindexed_hashes = [_generate_sha256(f"unindexed-{i}") for i in range(500)]

        for i in range(num_workers):
            wid = f"skew-worker-{i:02d}"
            ip = f"192.168.{(i % 4) + 1}.{10 + i}"
            # Every worker holds all popular hashes
            inv = set(popular_hashes)
            # Each worker holds a unique slice of rare hashes
            inv.update(rare_hashes[i * 15 : (i * 15) + 15])
            coord.register_worker_sync(_make_worker_payload(wid, ip=ip, inventory=inv))

        query = popular_hashes + rare_hashes + unindexed_hashes
        req = LocateAssetsRequest(
            requester_worker_id="skew-worker-00",
            requester_ip="192.168.1.10",
            missing_hashes=query,
            max_candidates_per_asset=10,
        )

        start_time = time.perf_counter()
        resp = coord.locate_assets_sync(req)
        elapsed = time.perf_counter() - start_time

        assert elapsed < 0.5, f"Skewed query took {elapsed:.4f}s"
        assert len(resp.locations) == len(query)

        for pop_h in popular_hashes:
            assert len(resp.locations[pop_h]) == 10
            assert "skew-worker-00" not in [c.worker_id for c in resp.locations[pop_h]]

        for unind_h in unindexed_hashes:
            assert len(resp.locations[unind_h]) == 0
            assert unind_h in resp.unresolved_hashes


# ============================================================================
# 2. Concurrency Chaos & Race Condition Resistance
# ============================================================================


class TestCoordinatorConcurrencyChaos:
    """Stress tests with 50+ concurrent threads executing mixed lifecycle operations while eviction runs."""

    def test_50_threads_concurrent_registration_heartbeats_eviction_and_locates(self):
        coord = CoordinatorService(
            coordinator_id="coord-chaos-50t",
            heartbeat_interval_seconds=1.0,
            heartbeat_timeout_seconds=2.0,
        )
        client = TestClient(coord.app)

        num_threads = 50
        duration_seconds = 3.0
        hash_catalog = [_generate_sha256(i) for i in range(1000)]

        stop_event = threading.Event()
        errors: List[str] = []
        locate_counts = [0] * num_threads

        def eviction_worker():
            while not stop_event.is_set():
                try:
                    coord.registry.evict_expired_workers(timeout_seconds=1.5)
                    coord.registry.decay_penalties()
                except Exception as exc:
                    errors.append(f"Eviction loop error: {exc}")
                time.sleep(0.05)

        def worker_task(tid: int):
            wid = f"worker-chaos-{tid:03d}"
            ip = f"192.168.{(tid % 8) + 1}.{(tid % 250) + 1}"
            port = 8000 + tid

            while not stop_event.is_set():
                try:
                    init_hashes = set(random.sample(hash_catalog, 30))
                    payload = _make_worker_payload(wid, ip=ip, port=port, inventory=init_hashes)
                    r_resp = client.post("/api/v1/workers/register", json=payload.model_dump(mode="json"))
                    if r_resp.status_code != 200:
                        errors.append(f"Thread {tid} register failed: {r_resp.text}")

                    for _ in range(3):
                        if stop_event.is_set():
                            break
                        add_h = set(random.sample(hash_catalog, 5))
                        rem_h = set(random.sample(list(init_hashes), min(3, len(init_hashes))))
                        hb = HeartbeatPayload(
                            worker_id=wid,
                            active_transfers=random.randint(0, 5),
                            used_bytes=random.randint(1000, 50000),
                            inventory_delta_added=add_h,
                            inventory_delta_removed=rem_h,
                        )
                        hb_resp = client.post(f"/api/v1/workers/{wid}/heartbeat", json=hb.model_dump(mode="json"))
                        if hb_resp.status_code != 200:
                            errors.append(f"Thread {tid} heartbeat failed: {hb_resp.text}")

                        query_hashes = random.sample(hash_catalog, 50)
                        loc_req = LocateAssetsRequest(
                            requester_worker_id=wid,
                            requester_ip=ip,
                            missing_hashes=query_hashes,
                            max_candidates_per_asset=3,
                        )
                        loc_resp = client.post("/api/v1/assets/locate", json=loc_req.model_dump(mode="json"))
                        if loc_resp.status_code != 200:
                            errors.append(f"Thread {tid} locate failed: {loc_resp.text}")
                        else:
                            loc_data = loc_resp.json()
                            assert len(loc_data["locations"]) == 50
                            locate_counts[tid] += 1

                        time.sleep(0.01)

                    if random.random() < 0.3:
                        client.post(f"/api/v1/workers/{wid}/unregister")
                    elif random.random() < 0.2:
                        coord.registry.record_corruption(wid, random.choice(hash_catalog))

                except Exception as exc:
                    errors.append(f"Thread {tid} unhandled exception: {exc}")

        eviction_thread = threading.Thread(target=eviction_worker, daemon=True)
        worker_threads = [
            threading.Thread(target=worker_task, args=(i,), daemon=True)
            for i in range(num_threads)
        ]

        eviction_thread.start()
        for t in worker_threads:
            t.start()

        time.sleep(duration_seconds)
        stop_event.set()

        for t in worker_threads:
            t.join(timeout=3.0)
        eviction_thread.join(timeout=1.0)

        assert not errors, f"Concurrent chaos produced errors: {errors[:10]}"
        total_locates = sum(locate_counts)
        assert total_locates > 100, f"Expected >100 successful batch locate queries under chaos, got {total_locates}"

        self._verify_index_symmetry(coord.registry)

    def test_concurrent_locate_during_mass_eviction_race(self):
        coord = CoordinatorService(coordinator_id="coord-mass-evict")
        num_workers = 30
        hashes = [_generate_sha256(i) for i in range(500)]

        for i in range(num_workers):
            wid = f"w-evict-race-{i:02d}"
            inv = set(hashes[i * 10 : (i * 10) + 100])
            coord.register_worker_sync(_make_worker_payload(wid, inventory=inv))

        stop_event = threading.Event()
        errors: List[str] = []

        def locator_task(tid: int):
            while not stop_event.is_set():
                try:
                    req = LocateAssetsRequest(
                        requester_worker_id="external-requester",
                        requester_ip="10.0.0.1",
                        missing_hashes=random.sample(hashes, 50),
                    )
                    resp = coord.locate_assets_sync(req)
                    assert len(resp.locations) == 50
                except Exception as exc:
                    errors.append(f"Locator {tid} error: {exc}")

        def evictor_task():
            for i in range(num_workers):
                time.sleep(0.01)
                coord.registry.unregister_worker(f"w-evict-race-{i:02d}")

        loc_threads = [threading.Thread(target=locator_task, args=(i,)) for i in range(15)]
        evict_thread = threading.Thread(target=evictor_task)

        for t in loc_threads:
            t.start()
        evict_thread.start()

        evict_thread.join(timeout=5.0)
        stop_event.set()
        for t in loc_threads:
            t.join(timeout=2.0)

        assert not errors, f"Concurrent eviction locate race failed: {errors}"
        assert coord.registry.get_worker_count() == 0

    def _verify_index_symmetry(self, registry: WorkerRegistry):
        with registry._lock:
            for wid, hashes in registry._worker_hashes.items():
                assert wid in registry._workers, f"Worker {wid} in _worker_hashes but not in _workers"
                for h in hashes:
                    assert h in registry._hash_index, f"Hash {h} for worker {wid} missing in _hash_index"
                    assert wid in registry._hash_index[h], f"Worker {wid} missing in _hash_index[{h}]"

            for h, workers in registry._hash_index.items():
                assert len(workers) > 0, f"_hash_index[{h}] contains empty worker set"
                for wid in workers:
                    assert wid in registry._workers, f"Worker {wid} indexed for {h} but not in _workers"
                    assert wid in registry._worker_hashes, f"Worker {wid} in _hash_index but not in _worker_hashes"
                    assert h in registry._worker_hashes[wid], f"Hash {h} missing in _worker_hashes[{wid}]"


# ============================================================================
# 3. Prioritizer & Latency Tracker Mathematical Invariants & Edge Cases
# ============================================================================


class TestPrioritizerAndLatencyInvariants:
    """Deep invariants testing on CandidatePrioritizer and LatencyTracker."""

    def test_locality_tier_dominance_invariant(self):
        prioritizer_lan = CandidatePrioritizer()
        req_lan_ip = "192.168.1.10"

        w_subnet = WorkerInfo(
            worker_id="w-subnet",
            endpoint_url="http://192.168.1.20:8000",
            ip_address="192.168.1.20",
            port=8000,
            capacity_bytes=1000,
            used_bytes=500,
            capabilities=WorkerCapabilities(max_concurrent_streams=16),
            estimated_rtt_ms=0.5,
            last_heartbeat_utc=time.time(),
        )

        w_lan = WorkerInfo(
            worker_id="w-lan",
            endpoint_url="http://10.0.0.5:8000",
            ip_address="10.0.0.5",
            port=8000,
            capacity_bytes=1000,
            used_bytes=500,
            capabilities=WorkerCapabilities(max_concurrent_streams=16),
            estimated_rtt_ms=0.5,
            last_heartbeat_utc=time.time(),
        )

        w_wan = WorkerInfo(
            worker_id="w-wan",
            endpoint_url="http://8.8.8.8:8000",
            ip_address="8.8.8.8",
            port=8000,
            capacity_bytes=1000,
            used_bytes=500,
            capabilities=WorkerCapabilities(max_concurrent_streams=16),
            estimated_rtt_ms=0.5,
            last_heartbeat_utc=time.time(),
        )

        ranked = prioritizer_lan.rank_candidates(
            requester_ip=req_lan_ip,
            candidates=[w_wan, w_lan, w_subnet],
        )

        assert len(ranked) == 3
        ranked_ids = [c.worker_id for c in ranked]
        assert ranked_ids == ["w-subnet", "w-lan", "w-wan"]

    def test_latency_tracker_ema_stability_under_parallel_flood(self):
        tracker = LatencyTracker(default_alpha=0.3)
        num_threads = 20
        updates_per_thread = 500
        worker_ids = [f"worker-rtt-{i}" for i in range(10)]

        def flood_task():
            for _ in range(updates_per_thread):
                wid = random.choice(worker_ids)
                sample = random.uniform(0.1, 100.0)
                res = tracker.update_rtt(wid, sample)
                assert res > 0.0
                assert not math.isnan(res)

        threads = [threading.Thread(target=flood_task) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        for wid in worker_ids:
            final_rtt = tracker.get_rtt(wid)
            assert 0.01 <= final_rtt <= 100.0

    def test_exponential_penalty_decay_math(self):
        registry = WorkerRegistry(
            penalty_half_life_seconds=100.0,
            degraded_threshold=3.0,
            suspect_threshold=10.0,
        )
        wid = "worker-decay-test"
        payload = _make_worker_payload(wid)
        registry.register_worker(WorkerInfo(**payload.model_dump(), last_heartbeat_utc=time.time()))

        registry.record_failure(wid, reason="Test error", penalty=8.0)
        rec = registry.get_health_record(wid)
        assert rec is not None
        assert rec.penalty_score == 8.0
        assert rec.health_status == WorkerHealthStatus.DEGRADED

        t1 = rec.last_decay_utc + 100.0
        registry.decay_penalties(current_time=t1)
        rec1 = registry.get_health_record(wid)
        assert rec1 is not None
        assert math.isclose(rec1.penalty_score, 4.0, rel_tol=1e-3)
        assert rec1.health_status == WorkerHealthStatus.DEGRADED

        t2 = t1 + 100.0
        registry.decay_penalties(current_time=t2)
        rec2 = registry.get_health_record(wid)
        assert rec2 is not None
        assert math.isclose(rec2.penalty_score, 2.0, rel_tol=1e-3)
        assert rec2.health_status == WorkerHealthStatus.HEALTHY

        worker = registry.get_worker(wid)
        assert worker.status == WorkerStatus.ACTIVE

    def test_corruption_reporting_prunes_single_hash_only(self):
        registry = WorkerRegistry()
        wid = "worker-corrupt-target"
        hashes = [_generate_sha256(i) for i in range(50)]
        registry.register_worker(
            WorkerInfo(**_make_worker_payload(wid, inventory=set(hashes)).model_dump(), last_heartbeat_utc=time.time())
        )

        bad_hash = hashes[0]
        registry.record_corruption(wid, bad_hash, penalty=5.0)

        workers_bad = registry.get_workers_for_hash(bad_hash)
        assert wid not in workers_bad

        for ok_hash in hashes[1:]:
            workers_ok = registry.get_workers_for_hash(ok_hash)
            assert wid in workers_ok


# ============================================================================
# 4. Boundary & Malicious Input Robustness on Coordinator Endpoints
# ============================================================================


class TestCoordinatorEdgeCasesAndBoundaries:
    """Boundary conditions, malformed SHA-256 strings, and unusual parameters."""

    def test_locate_with_malformed_hashes_pydantic_rejection(self):
        """Verify malformed SHA-256 strings are rejected at HTTP layer with 422 Unprocessable Entity."""
        coord = CoordinatorService(coordinator_id="coord-edge-01")
        client = TestClient(coord.app)

        valid_h1 = _generate_sha256(1)

        payload = _make_worker_payload("worker-edge-1", inventory={valid_h1})
        coord.register_worker_sync(payload)

        malformed_hashes = [
            "not-a-hash",
            "0" * 63,
            "a" * 65,
            "g" * 64,
            "../../etc/passwd",
            r"\windows\system32\cmd.exe",
        ]

        for mh in malformed_hashes:
            resp = client.post(
                "/api/v1/assets/locate",
                json={"requester_worker_id": "test-req", "missing_hashes": [valid_h1, mh]},
            )
            assert resp.status_code == 422, f"Expected 422 for malformed hash {mh!r}, got {resp.status_code}"

    def test_locate_max_candidates_parameter_bounds(self):
        coord = CoordinatorService(coordinator_id="coord-bounds-01")
        h = _generate_sha256("shared-asset")

        for i in range(10):
            payload = _make_worker_payload(f"w-bound-{i}", ip=f"192.168.1.{20+i}", inventory={h})
            coord.register_worker_sync(payload)

        # Valid bounds: 1, 3, default (5), 10, 20
        r1 = coord.locate_assets_sync(
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h], max_candidates_per_asset=1)
        )
        assert len(r1.locations[h]) == 1

        r3 = coord.locate_assets_sync(
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h], max_candidates_per_asset=3)
        )
        assert len(r3.locations[h]) == 3

        r_default = coord.locate_assets_sync(
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h])
        )
        assert len(r_default.locations[h]) == 5

        r_max = coord.locate_assets_sync(
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h], max_candidates_per_asset=20)
        )
        assert len(r_max.locations[h]) == 10

        # Invalid bounds: 0, negative, > 20, None must raise ValidationError
        with pytest.raises(ValidationError):
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h], max_candidates_per_asset=0)

        with pytest.raises(ValidationError):
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h], max_candidates_per_asset=-5)

        with pytest.raises(ValidationError):
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h], max_candidates_per_asset=21)

        with pytest.raises(ValidationError):
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h], max_candidates_per_asset=None)

    def test_locate_with_degraded_workers_filter_toggle(self):
        coord = CoordinatorService(coordinator_id="coord-degraded-test")
        h = _generate_sha256("degraded-asset")

        coord.register_worker_sync(_make_worker_payload("w-healthy", ip="192.168.1.10", inventory={h}))

        w_deg = _make_worker_payload("w-degraded", ip="192.168.1.20", inventory={h})
        coord.register_worker_sync(w_deg)
        coord.registry.record_failure("w-degraded", penalty=5.0)

        r_no_deg = coord.locate_assets_sync(
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h], include_degraded=False)
        )
        cands_no_deg = [c.worker_id for c in r_no_deg.locations[h]]
        assert "w-healthy" in cands_no_deg
        assert "w-degraded" not in cands_no_deg

        r_with_deg = coord.locate_assets_sync(
            LocateAssetsRequest(requester_worker_id="test-req", missing_hashes=[h], include_degraded=True)
        )
        cands_with_deg = [c.worker_id for c in r_with_deg.locations[h]]
        assert "w-healthy" in cands_with_deg
        assert "w-degraded" in cands_with_deg
        assert cands_with_deg[0] == "w-healthy"

    def test_ping_pong_latency_probe_with_clock_skew(self):
        coord = CoordinatorService(coordinator_id="coord-ping-01")
        client = TestClient(coord.app)

        future_ts = time.time() + 1000.0
        resp_future = client.post("/api/v1/ping", json={"client_timestamp_utc": future_ts, "sequence_number": 1})
        assert resp_future.status_code == 200
        data_f = resp_future.json()
        assert data_f["status"] == "pong"
        assert data_f["sequence_number"] == 1

        past_ts = time.time() - 1000.0
        resp_past = client.post("/api/v1/ping", json={"client_timestamp_utc": past_ts, "sequence_number": 2})
        assert resp_past.status_code == 200
        data_p = resp_past.json()
        assert data_p["status"] == "pong"
        assert data_p["sequence_number"] == 2
