"""Adversarial stress and edge-case test suite for AIDAR Milestone 1.

Focus areas:
1. Concurrent multi-threaded registration, heartbeat updates, simultaneous queries,
   and eviction under high thread contention (50+ threads).
2. Dual inverted hash index consistency under heavy churn with oracle verification.
3. Locality tier classification edge cases and RTT EMA calculations with extreme outliers.
4. Candidate prioritizer stress testing under adverse concurrency and zero/extreme parameters.
"""
from __future__ import annotations

import concurrent.futures
import ipaddress
import math
import random
import threading
import time
from typing import Dict, List, Set, Tuple

import pytest

from aidars.distributed.models import (
    CandidateSource,
    HeartbeatPayload,
    LocalityTier,
    WorkerCapabilities,
    WorkerInfo,
    WorkerMetrics,
    WorkerRegistrationPayload,
    WorkerStatus,
    validate_ip_address,
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


def _gen_hash(seed: int) -> str:
    """Generate a deterministic valid 64-char SHA-256 hex string."""
    import hashlib
    return hashlib.sha256(f"seed-{seed}".encode("utf-8")).hexdigest()


def _make_worker(
    worker_id: str,
    ip: str = "192.168.1.10",
    port: int = 8000,
    inventory: Set[str] | None = None,
    capacity: int = 10_000_000,
    used: int = 1_000_000,
    active_transfers: int = 0,
    max_streams: int = 16,
    penalty: float = 0.0,
    rtt: float = 5.0,
    status: WorkerStatus = WorkerStatus.ACTIVE,
) -> WorkerInfo:
    return WorkerInfo(
        worker_id=worker_id,
        endpoint_url=f"http://{ip}:{port}",
        ip_address=ip,
        port=port,
        capacity_bytes=capacity,
        used_bytes=used,
        inventory_hashes=inventory or set(),
        active_transfers=active_transfers,
        capabilities=WorkerCapabilities(max_concurrent_streams=max_streams),
        penalty_score=penalty,
        estimated_rtt_ms=rtt,
        status=status,
    )


# ============================================================================
# 1. High-Contention Multi-Threaded Stress Tests (50+ Threads)
# ============================================================================


class TestConcurrentRegistryContention:
    """Stress tests simulating 50+ concurrent threads contending on WorkerRegistry."""

    def test_massive_concurrent_registration_and_queries(self):
        """50+ threads performing simultaneous registration, heartbeats, locate queries, and stats."""
        registry = WorkerRegistry(heartbeat_timeout_seconds=5.0)
        num_threads = 60
        ops_per_thread = 40
        errors: List[Exception] = []

        # Pre-generate a shared pool of 200 hashes
        hash_pool = [_gen_hash(i) for i in range(200)]

        def worker_lifecycle_thread(tid: int):
            try:
                wid = f"worker-contention-{tid}"
                # Initial inventory of 15 hashes
                init_hashes = set(random.sample(hash_pool, 15))
                w_info = _make_worker(wid, ip=f"192.168.{(tid % 10) + 1}.{(tid % 200) + 1}", inventory=init_hashes)
                registry.register_worker(w_info)

                for op_idx in range(ops_per_thread):
                    op_type = op_idx % 7

                    if op_type == 0:
                        # Heartbeat with delta addition
                        new_hashes = set(random.sample(hash_pool, 3))
                        payload = HeartbeatPayload(
                            worker_id=wid,
                            active_transfers=random.randint(0, 16),
                            used_bytes=random.randint(1000, 500000),
                            inventory_delta_added=new_hashes,
                        )
                        registry.record_heartbeat(wid, payload=payload)

                    elif op_type == 1:
                        # Locate batch query
                        query_sample = random.sample(hash_pool, 10)
                        locs = registry.locate_hashes(query_sample)
                        assert isinstance(locs, dict)

                    elif op_type == 2:
                        # Success / Failure telemetry
                        if op_idx % 2 == 0:
                            registry.record_success(wid, bytes_transferred=65536)
                        else:
                            registry.record_failure(wid, reason="transient timeout", penalty=0.5)

                    elif op_type == 3:
                        # Single hash query
                        target_h = random.choice(hash_pool)
                        workers = registry.get_workers_for_hash(target_h)
                        assert isinstance(workers, set)

                    elif op_type == 4:
                        # Cluster stats computation under contention
                        stats = registry.get_cluster_stats()
                        assert stats.total_workers >= 0

                    elif op_type == 5:
                        # Inventory sync
                        new_inventory = set(random.sample(hash_pool, 10))
                        registry.sync_worker_inventory(wid, new_inventory)

                    elif op_type == 6:
                        # Eviction & penalty decay pass (simulating background workers)
                        if tid == 0:
                            registry.evict_expired_workers()
                            registry.decay_penalties()

            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=worker_lifecycle_thread, args=(i,), name=f"ContentionThread-{i}")
            for i in range(num_threads)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30.0)

        assert not errors, f"Contention threads encountered exceptions: {errors}"
        # All 60 workers should be registered
        assert registry.get_worker_count() == num_threads

    def test_concurrent_eviction_and_unregistration_under_load(self):
        """Simultaneous aggressive eviction, unregistration, and active queries."""
        registry = WorkerRegistry(heartbeat_timeout_seconds=0.2)
        num_workers = 40
        num_query_threads = 20
        hash_pool = [_gen_hash(i) for i in range(100)]
        stop_event = threading.Event()
        query_errors: List[Exception] = []

        # Register workers
        for i in range(num_workers):
            hashes = set(random.sample(hash_pool, 10))
            registry.register_worker(_make_worker(f"w-evict-{i}", inventory=hashes))

        def continuous_queries():
            while not stop_event.is_set():
                try:
                    q = random.sample(hash_pool, 5)
                    registry.locate_hashes(q)
                    registry.get_cluster_stats()
                    registry.list_workers(active_only=True)
                    registry.get_all_indexed_hashes()
                    time.sleep(0.001)
                except Exception as exc:
                    query_errors.append(exc)

        def evictor_and_mutator(tid: int):
            for step in range(25):
                # Update heartbeats on even workers only
                if tid % 2 == 0:
                    wid = f"w-evict-{tid}"
                    registry.record_heartbeat(wid, current_time=time.time())
                else:
                    # Unregister some odd workers
                    if step == 5:
                        registry.unregister_worker(f"w-evict-{tid}")
                # Periodic eviction
                registry.evict_expired_workers(timeout_seconds=0.05, current_time=time.time() + 1.0)
                time.sleep(0.005)

        q_threads = [threading.Thread(target=continuous_queries) for _ in range(num_query_threads)]
        m_threads = [threading.Thread(target=evictor_and_mutator, args=(i,)) for i in range(num_workers)]

        for t in q_threads + m_threads:
            t.start()

        for t in m_threads:
            t.join(timeout=15.0)

        stop_event.set()
        for t in q_threads:
            t.join(timeout=5.0)

        assert not query_errors, f"Query threads failed during concurrent eviction: {query_errors}"


# ============================================================================
# 2. Inverted Hash Index Consistency & Oracle Verification
# ============================================================================


class TestInvertedIndexConsistencyOracle:
    """Adversarial oracle-based testing of the dual inverted index for zero orphaned entries."""

    def test_dual_index_oracle_differential_fuzzing(self):
        """Perform thousands of randomized mutations comparing registry against an independent ground truth oracle."""
        registry = WorkerRegistry()
        oracle: Dict[str, Set[str]] = {}  # worker_id -> Set[sha256]

        total_hashes = 2000
        hash_catalog = [_gen_hash(i) for i in range(total_hashes)]
        worker_pool = [f"oracle-worker-{i:03d}" for i in range(50)]

        for op in range(1000):
            action = random.choice([
                "register", "re_register", "add_hashes", "remove_hashes",
                "sync_inventory", "unregister", "corruption", "delta_heartbeat"
            ])

            wid = random.choice(worker_pool)

            if action == "register" or (action == "re_register" and wid in oracle):
                inv_size = random.randint(10, 100)
                hashes = set(random.sample(hash_catalog, inv_size))
                w_info = _make_worker(wid, inventory=hashes)
                registry.register_worker(w_info)
                oracle[wid] = set(hashes)

            elif action == "add_hashes" and wid in oracle:
                to_add = set(random.sample(hash_catalog, random.randint(1, 20)))
                registry.add_worker_hashes(wid, to_add)
                oracle[wid].update(to_add)

            elif action == "remove_hashes" and wid in oracle and oracle[wid]:
                count = min(len(oracle[wid]), random.randint(1, 15))
                to_remove = set(random.sample(list(oracle[wid]), count))
                registry.remove_worker_hashes(wid, to_remove)
                oracle[wid].difference_update(to_remove)

            elif action == "sync_inventory" and wid in oracle:
                new_target = set(random.sample(hash_catalog, random.randint(5, 50)))
                registry.sync_worker_inventory(wid, new_target)
                oracle[wid] = set(new_target)

            elif action == "unregister" and wid in oracle:
                registry.unregister_worker(wid)
                del oracle[wid]

            elif action == "corruption" and wid in oracle and oracle[wid]:
                corrupt_h = random.choice(list(oracle[wid]))
                registry.record_corruption(wid, corrupt_h)
                oracle[wid].discard(corrupt_h)

            elif action == "delta_heartbeat" and wid in oracle:
                add_set = set(random.sample(hash_catalog, 5))
                rem_set = set(random.sample(list(oracle[wid]), min(len(oracle[wid]), 3))) if oracle[wid] else set()
                payload = HeartbeatPayload(
                    worker_id=wid,
                    inventory_delta_added=add_set,
                    inventory_delta_removed=rem_set,
                )
                registry.record_heartbeat(wid, payload=payload)
                oracle[wid].update(add_set)
                oracle[wid].difference_update(rem_set)

        # --------------------------------------------------------------------
        # ORACLE INVARIANT VERIFICATION
        # --------------------------------------------------------------------
        # 1. Total registered workers match
        assert registry.get_worker_count() == len(oracle)

        # 2. Reconstruct inverted index from oracle
        expected_inverted: Dict[str, Set[str]] = {}
        for w_id, w_hashes in oracle.items():
            for h in w_hashes:
                expected_inverted.setdefault(h, set()).add(w_id)

        # 3. Verify total unique indexed hashes count
        assert registry.get_hash_count() == len(expected_inverted)
        assert registry.get_all_indexed_hashes() == set(expected_inverted.keys())

        # 4. Verify exact worker sets for all hashes (Zero orphaned hashes)
        for h, expected_workers in expected_inverted.items():
            actual_workers = registry.get_workers_for_hash(h)
            assert actual_workers == expected_workers, f"Mismatch for hash {h}: expected {expected_workers}, got {actual_workers}"

        # 5. Verify batch location lookup consistency
        all_indexed = list(expected_inverted.keys())
        sample_query = random.sample(all_indexed, min(len(all_indexed), 200))
        located_results = registry.locate_hashes(sample_query)
        for h in sample_query:
            assert located_results[h] == expected_inverted[h]

        # 6. Verify unregistering ALL remaining workers leaves zero orphaned hashes
        for wid in list(oracle.keys()):
            registry.unregister_worker(wid)

        assert registry.get_worker_count() == 0
        assert registry.get_hash_count() == 0
        assert len(registry.get_all_indexed_hashes()) == 0
        assert registry.get_cluster_stats().total_unique_hashes == 0

    def test_registering_thousands_of_hashes_per_worker(self):
        """Register workers with 5,000+ distinct hashes and verify O(K) teardown performance."""
        registry = WorkerRegistry()
        large_inventory = {_gen_hash(i) for i in range(5000)}

        t0 = time.time()
        registry.register_worker(_make_worker("w-large-1", inventory=large_inventory))
        t_reg = time.time() - t0
        assert t_reg < 2.0, f"Registration of 5,000 hashes took too long: {t_reg:.3f}s"

        assert registry.get_hash_count() == 5000

        t1 = time.time()
        registry.unregister_worker("w-large-1")
        t_unreg = time.time() - t1
        assert t_unreg < 1.0, f"Unregistration of 5,000 hashes took too long: {t_unreg:.3f}s"

        assert registry.get_hash_count() == 0
        assert len(registry.get_all_indexed_hashes()) == 0


# ============================================================================
# 3. Locality Classification Edge Cases & RTT EMA Calculations
# ============================================================================


class TestLocalityClassificationEdgeCases:
    """Stress tests on IP parsing, subnet masking, and edge case network classifications."""

    def test_classify_locality_edge_cases(self):
        # Empty and loopback aliases
        assert classify_locality("", "") == LocalityTier.LOOPBACK
        assert classify_locality("localhost", "127.0.0.1") == LocalityTier.LOOPBACK
        assert classify_locality("ip6-localhost", "[::1]") == LocalityTier.LOOPBACK
        assert classify_locality("::1", "127.0.0.1") == LocalityTier.WAN  # IPv6 vs IPv4 loopback are different IPs

        # Identical non-loopback IP (same machine) -> LOOPBACK
        assert classify_locality("10.0.0.50", "10.0.0.50") == LocalityTier.LOOPBACK
        assert classify_locality("192.168.1.100", "192.168.1.100") == LocalityTier.LOOPBACK
        assert classify_locality("8.8.8.8", "8.8.8.8") == LocalityTier.LOOPBACK

        # Malformed / Unparseable IPs -> Fallback to WAN
        assert classify_locality("invalid-ip", "192.168.1.1") == LocalityTier.WAN
        assert classify_locality("999.999.999.999", "10.0.0.1") == LocalityTier.WAN
        assert classify_locality("192.168.1.1:8000", "192.168.1.2:8000") == LocalityTier.WAN

        # IPv4 Subnet boundaries (/24 default)
        assert classify_locality("192.168.1.1", "192.168.1.254") == LocalityTier.SUBNET
        assert classify_locality("192.168.1.255", "192.168.2.0") == LocalityTier.LAN

        # Custom subnet prefixes (/16, /28, /32)
        assert classify_locality("10.0.1.5", "10.0.2.5", ipv4_subnet_prefix=16) == LocalityTier.SUBNET
        assert classify_locality("10.0.1.5", "10.0.1.20", ipv4_subnet_prefix=28) == LocalityTier.LAN  # across /28 boundary
        assert classify_locality("10.0.1.5", "10.0.1.6", ipv4_subnet_prefix=28) == LocalityTier.SUBNET

        # IPv6 Subnet boundaries (/64 default)
        assert (
            classify_locality("2001:db8:1::1", "2001:db8:1::ffff:ffff")
            == LocalityTier.SUBNET
        )
        assert (
            classify_locality("2001:db8:1::1", "2001:db8:2::1")
            == LocalityTier.WAN  # Public IPv6 cross /64
        )

        # Private IPv6 (Unique Local Addresses fc00::/7 / fd00::/8)
        assert (
            classify_locality("fd00:abcd:1::1", "fd00:abcd:2::1")
            == LocalityTier.LAN  # Cross subnet but private
        )

        # Public WAN pairs
        assert classify_locality("142.250.190.46", "172.217.16.206") == LocalityTier.WAN


class TestLatencyTrackerAndPingEMAOutliers:
    """Stress tests on RTT EMA smoothing with extreme values, zero/negative inputs, and outliers."""

    def test_measure_ping_rtt_clock_skew_and_extreme_timestamps(self):
        # Normal 10ms measurement
        assert pytest.approx(measure_ping_rtt(100.0, current_time=100.010), rel=1e-3) == 10.0

        # Future timestamp (Clock skew where client timestamp > coordinator time)
        # Should be safely clamped to minimum 0.01 ms
        assert measure_ping_rtt(105.0, current_time=100.0) == 0.01

        # Zero or negative client timestamp
        rtt_zero = measure_ping_rtt(0.0, current_time=10.0)
        assert rtt_zero == 10000.0

        # Sub-millisecond RTT (e.g. 5 microseconds -> 0.005 ms clamped to 0.01 or recorded accurately)
        assert measure_ping_rtt(100.0, current_time=100.000005) == 0.01

    def test_latency_tracker_ema_outliers_and_clamping(self):
        tracker = LatencyTracker(default_alpha=0.3)

        # 1. Zero and negative sample RTT values clamped to 0.01 ms
        rtt_neg = tracker.update_rtt("w-test", -50.0)
        assert rtt_neg == 0.01

        rtt_zero = tracker.update_rtt("w-test2", 0.0)
        assert rtt_zero == 0.01

        # 2. Extreme outlier spike (e.g. 100,000 ms = 100 seconds)
        # First baseline = 10.0 ms
        tracker.update_rtt("w-spike", 10.0)
        # Spike of 100,000 ms with alpha 0.3: 0.3 * 100000 + 0.7 * 10 = 30000 + 7 = 30007.0
        smoothed = tracker.update_rtt("w-spike", 100_000.0)
        assert pytest.approx(smoothed, rel=1e-3) == 30007.0

        # 3. Rapid decay back to normal over multiple samples
        for _ in range(10):
            smoothed = tracker.update_rtt("w-spike", 10.0)
        assert smoothed < 1000.0

        # 4. Extreme alpha values: alpha=0.0 (no update), alpha=1.0 (instant overwrite)
        tracker.update_rtt("w-alpha", 20.0)
        assert tracker.update_rtt("w-alpha", 100.0, alpha=0.0) == 20.0
        assert tracker.update_rtt("w-alpha", 100.0, alpha=1.0) == 100.0

    def test_latency_tracker_concurrent_updates(self):
        """50 threads concurrently updating RTT for the same worker."""
        tracker = LatencyTracker()
        num_threads = 50
        updates_per_thread = 50

        def update_task(tid: int):
            for i in range(updates_per_thread):
                tracker.update_rtt("shared-worker", float((tid * 10) + i + 1))
                tracker.get_rtt("shared-worker")

        threads = [threading.Thread(target=update_task, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final_rtt = tracker.get_rtt("shared-worker")
        assert final_rtt > 0.0


# ============================================================================
# 4. Candidate Prioritizer Robustness & Edge Cases
# ============================================================================


class TestCandidatePrioritizerEdgeCases:
    """Adversarial tests on candidate ranking, zero division defenses, and tie breaking."""

    def test_zero_max_concurrent_streams_defense(self):
        """Worker advertising max_concurrent_streams=0 (invalid but possible from malicious client)."""
        prioritizer = CandidatePrioritizer()
        # Worker with max_concurrent_streams=0 or 1
        w = _make_worker("w-zero-stream", max_streams=1, active_transfers=5)
        # Artificially modify max_concurrent_streams to 0 to test defense
        w.capabilities.max_concurrent_streams = 0

        ranked = prioritizer.rank_candidates("192.168.1.1", [w])
        assert len(ranked) == 1
        # Load factor must safely compute max(0, 1) without ZeroDivisionError
        assert ranked[0].load_factor == 5.0

    def test_ranking_with_empty_and_all_filtered_candidates(self):
        prioritizer = CandidatePrioritizer()
        assert prioritizer.rank_candidates("192.168.1.1", []) == []

        # All candidates offline or degraded with include_degraded=False
        w_off = _make_worker("w-off", status=WorkerStatus.OFFLINE)
        w_deg = _make_worker("w-deg", status=WorkerStatus.DEGRADED)
        w_unh = _make_worker("w-unh", status=WorkerStatus.UNHEALTHY)

        ranked = prioritizer.rank_candidates(
            "192.168.1.1",
            [w_off, w_deg, w_unh],
            include_degraded=False,
        )
        assert len(ranked) == 0

    def test_ranking_requester_exclusion(self):
        prioritizer = CandidatePrioritizer()
        w1 = _make_worker("requester-self", ip="192.168.1.100")
        w2 = _make_worker("peer-1", ip="192.168.1.101")

        ranked = prioritizer.rank_candidates(
            "192.168.1.100",
            [w1, w2],
            exclude_worker_id="requester-self",
        )
        assert len(ranked) == 1
        assert ranked[0].worker_id == "peer-1"

    def test_max_candidates_parameter_bounds(self):
        prioritizer = CandidatePrioritizer()
        candidates = [_make_worker(f"w-{i}", ip=f"192.168.1.{10+i}") for i in range(20)]

        # max_candidates = 0 (ignored/returns all)
        assert len(prioritizer.rank_candidates("192.168.1.1", candidates, max_candidates=0)) == 20
        # max_candidates = 5
        assert len(prioritizer.rank_candidates("192.168.1.1", candidates, max_candidates=5)) == 5
        # max_candidates = 100 (larger than candidates pool)
        assert len(prioritizer.rank_candidates("192.168.1.1", candidates, max_candidates=100)) == 20

    def test_concurrent_prioritizer_and_error_mutation(self):
        prioritizer = CandidatePrioritizer()
        candidates = [_make_worker(f"w-race-{i}", ip=f"192.168.1.{20+i}") for i in range(10)]
        num_threads = 20

        def mutate_and_rank(tid: int):
            wid = f"w-race-{tid % 10}"
            for _ in range(50):
                prioritizer.record_error(wid)
                ranked = prioritizer.rank_candidates("192.168.1.1", candidates)
                assert len(ranked) == 10
                prioritizer.clear_errors(wid)

        threads = [threading.Thread(target=mutate_and_rank, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
