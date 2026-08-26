"""Unit tests for locality classification, LatencyTracker EMA, and CandidatePrioritizer."""
from __future__ import annotations

import threading
import time
from typing import List

import pytest

from aidars.distributed.models import (
    CandidateSource,
    LocalityTier,
    WorkerCapabilities,
    WorkerInfo,
    WorkerStatus,
)
from aidars.distributed.prioritizer import (
    CandidatePrioritizer,
    LatencyTracker,
    classify_locality,
    measure_ping_rtt,
    normalize_ip,
)


def make_worker(
    worker_id: str,
    ip: str,
    active_transfers: int = 0,
    max_streams: int = 16,
    status: WorkerStatus = WorkerStatus.ACTIVE,
    penalty: float = 0.0,
    rtt: float = 0.0,
) -> WorkerInfo:
    return WorkerInfo(
        worker_id=worker_id,
        endpoint_url=f"http://{ip}:8000",
        ip_address=ip,
        port=8000,
        active_transfers=active_transfers,
        capabilities=WorkerCapabilities(max_concurrent_streams=max_streams),
        status=status,
        penalty_score=penalty,
        estimated_rtt_ms=rtt,
    )


# ============================================================================
# IP Normalization & Locality Classification Tests
# ============================================================================


def test_normalize_ip():
    assert normalize_ip("") == "127.0.0.1"
    assert normalize_ip("localhost") == "127.0.0.1"
    assert normalize_ip("ip6-localhost") == "::1"
    assert normalize_ip("[::1]") == "::1"
    assert normalize_ip("  192.168.1.1  ") == "192.168.1.1"


def test_classify_locality_loopback():
    assert classify_locality("127.0.0.1", "127.0.0.1") == LocalityTier.LOOPBACK
    assert classify_locality("127.0.0.1", "127.0.0.2") == LocalityTier.LOOPBACK
    assert classify_locality("localhost", "127.0.0.1") == LocalityTier.LOOPBACK
    assert classify_locality("::1", "::1") == LocalityTier.LOOPBACK
    assert classify_locality("192.168.1.50", "192.168.1.50") == LocalityTier.LOOPBACK


def test_classify_locality_subnet():
    # Same IPv4 /24
    assert classify_locality("192.168.1.10", "192.168.1.50") == LocalityTier.SUBNET
    assert classify_locality("10.0.5.1", "10.0.5.254") == LocalityTier.SUBNET

    # Same IPv6 /64
    assert (
        classify_locality(
            "2001:db8:abcd:0012::1",
            "2001:db8:abcd:0012::99",
        )
        == LocalityTier.SUBNET
    )


def test_classify_locality_lan():
    # Different /24 subnets, but both RFC-1918 private
    assert classify_locality("192.168.1.10", "192.168.2.10") == LocalityTier.LAN
    assert classify_locality("10.0.1.5", "172.16.50.1") == LocalityTier.LAN
    assert classify_locality("172.16.1.1", "192.168.1.1") == LocalityTier.LAN


def test_classify_locality_wan():
    # Public Internet addresses
    assert classify_locality("8.8.8.8", "1.1.1.1") == LocalityTier.WAN
    assert classify_locality("192.168.1.10", "8.8.8.8") == LocalityTier.WAN
    assert classify_locality("invalid_ip", "192.168.1.10") == LocalityTier.WAN


def test_measure_ping_rtt():
    t_start = 100.0
    t_end = 100.025  # 25 ms
    rtt = measure_ping_rtt(t_start, current_time=t_end)
    assert pytest.approx(rtt, rel=1e-3) == 25.0


# ============================================================================
# Latency Tracker EMA Tests
# ============================================================================


def test_latency_tracker_ema_smoothing():
    tracker = LatencyTracker(default_alpha=0.3)

    # First sample: seeds baseline (no prior history)
    rtt_1 = tracker.update_rtt("w-1", 10.0)
    assert rtt_1 == 10.0

    # Second sample with 20.0 ms: EMA = 0.3 * 20 + 0.7 * 10 = 6 + 7 = 13.0
    rtt_2 = tracker.update_rtt("w-1", 20.0)
    assert pytest.approx(rtt_2, rel=1e-3) == 13.0

    # Third sample with 10.0 ms: EMA = 0.3 * 10 + 0.7 * 13 = 3 + 9.1 = 12.1
    rtt_3 = tracker.update_rtt("w-1", 10.0)
    assert pytest.approx(rtt_3, rel=1e-3) == 12.1

    # Custom alpha override (alpha=1.0 instant update)
    rtt_4 = tracker.update_rtt("w-1", 50.0, alpha=1.0)
    assert pytest.approx(rtt_4, rel=1e-3) == 50.0

    assert tracker.get_rtt("w-1") == 50.0
    assert tracker.get_rtt("unknown-w", default=15.0) == 15.0

    tracker.remove_worker("w-1")
    assert tracker.get_rtt("w-1", default=10.0) == 10.0


# ============================================================================
# Candidate Prioritizer Tests
# ============================================================================


def test_tier_dominance_over_load():
    tracker = LatencyTracker()
    prioritizer = CandidatePrioritizer(latency_tracker=tracker)

    # loopback worker under heavy load (16/16 transfers)
    w_loop = make_worker("w-loop", "127.0.0.1", active_transfers=16, max_streams=16)
    # subnet worker with zero load (0/16 transfers)
    w_subnet = make_worker("w-sub", "192.168.1.50", active_transfers=0, max_streams=16)
    # LAN worker with zero load
    w_lan = make_worker("w-lan", "10.0.1.50", active_transfers=0, max_streams=16)
    # WAN worker with zero load
    w_wan = make_worker("w-wan", "8.8.8.8", active_transfers=0, max_streams=16)

    requester_ip = "192.168.1.100"
    # Even if loopback is local host, requester on 192.168.1.100:
    # let's set requester_ip="192.168.1.100" and candidate on "192.168.1.100"
    w_local = make_worker("w-local", "192.168.1.100", active_transfers=16, max_streams=16)

    ranked = prioritizer.rank_candidates(
        requester_ip=requester_ip,
        candidates=[w_wan, w_lan, w_subnet, w_local],
    )

    # Hierarchy must strictly be: w-local (LOOPBACK) > w-sub (SUBNET) > w-lan (LAN) > w-wan (WAN)
    assert ranked[0].worker_id == "w-local"
    assert ranked[0].locality_tier == LocalityTier.LOOPBACK.value

    assert ranked[1].worker_id == "w-sub"
    assert ranked[1].locality_tier == LocalityTier.SUBNET.value

    assert ranked[2].worker_id == "w-lan"
    assert ranked[2].locality_tier == LocalityTier.LAN.value

    assert ranked[3].worker_id == "w-wan"
    assert ranked[3].locality_tier == LocalityTier.WAN.value


def test_subnet_load_and_rtt_ranking():
    tracker = LatencyTracker()
    prioritizer = CandidatePrioritizer(latency_tracker=tracker)

    # Both on same subnet
    w_busy = make_worker("w-busy", "192.168.1.20", active_transfers=12, max_streams=16)
    w_idle = make_worker("w-idle", "192.168.1.30", active_transfers=1, max_streams=16)

    tracker.update_rtt("w-busy", 2.0)
    tracker.update_rtt("w-idle", 2.0)

    ranked = prioritizer.rank_candidates(
        requester_ip="192.168.1.10",
        candidates=[w_busy, w_idle],
    )

    assert ranked[0].worker_id == "w-idle"
    assert ranked[1].worker_id == "w-busy"


def test_error_penalization():
    prioritizer = CandidatePrioritizer()

    w1 = make_worker("w-clean", "192.168.1.20")
    w2 = make_worker("w-error", "192.168.1.30")

    # Record error on w2
    prioritizer.record_error("w-error")
    assert prioritizer.get_error_count("w-error") == 1

    ranked = prioritizer.rank_candidates(
        requester_ip="192.168.1.10",
        candidates=[w2, w1],
    )

    assert ranked[0].worker_id == "w-clean"
    assert ranked[1].worker_id == "w-error"

    # Clearing errors restores ranking equality / alphabetical or load
    prioritizer.clear_errors("w-error")
    assert prioritizer.get_error_count("w-error") == 0


def test_exclude_requester_and_filter_offline():
    prioritizer = CandidatePrioritizer()

    w_self = make_worker("w-self", "192.168.1.10")
    w_off = make_worker("w-off", "192.168.1.20", status=WorkerStatus.OFFLINE)
    w_active = make_worker("w-active", "192.168.1.30", status=WorkerStatus.ACTIVE)

    ranked = prioritizer.rank_candidates(
        requester_ip="192.168.1.10",
        candidates=[w_self, w_off, w_active],
        exclude_worker_id="w-self",
    )

    assert len(ranked) == 1
    assert ranked[0].worker_id == "w-active"


def test_max_candidates_limit():
    prioritizer = CandidatePrioritizer()
    candidates = [
        make_worker(f"w-{i}", f"192.168.1.{10+i}")
        for i in range(10)
    ]

    ranked = prioritizer.rank_candidates(
        requester_ip="192.168.1.1",
        candidates=candidates,
        max_candidates=3,
    )

    assert len(ranked) == 3


def test_concurrent_prioritizer_access():
    tracker = LatencyTracker()
    prioritizer = CandidatePrioritizer(latency_tracker=tracker)
    num_threads = 6

    def access_task(thread_id: int):
        wid = f"w-{thread_id}"
        w = make_worker(wid, f"192.168.1.{20+thread_id}")
        for i in range(50):
            tracker.update_rtt(wid, float(i + 1))
            prioritizer.record_error(wid)
            prioritizer.rank_candidates("192.168.1.1", [w])
            prioritizer.clear_errors(wid)

    threads = [threading.Thread(target=access_task, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
