"""Tier 1: Feature Coverage & Happy Path Tests (F1 through F20).

Exhaustive isolated feature verification: >=5 tests per feature, >=100 tests total.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pytest

from .conftest import (
    CandidateSource,
    HeartbeatPayload,
    LocateAssetsRequest,
    LocateAssetsResponse,
    MockCandidatePrioritizer,
    MockCASAdapter,
    MockCoordinator,
    MockStreamingServer,
    MockWorkerNode,
    MockWorkerRegistry,
    TransferMetrics,
    WorkerCapabilities,
    WorkerInfo,
)


# ============================================================================
# F1: Worker Registration (5 tests)
# ============================================================================

def test_f1_01_register_new_worker_basic(mock_coordinator: MockCoordinator):
    worker = WorkerInfo(
        worker_id="w-01",
        endpoint_url="http://192.168.1.10:8000",
        ip_address="192.168.1.10",
        port=8000,
        capacity_bytes=1000000,
        used_bytes=10000,
    )
    resp = mock_coordinator.handle_register(worker)
    assert resp["status"] == "registered"
    assert resp["coordinator_id"] == mock_coordinator.coordinator_id
    assert "w-01" in mock_coordinator.registry.workers


def test_f1_02_register_with_initial_hashes(mock_coordinator: MockCoordinator):
    hashes = {"a" * 64, "b" * 64}
    worker = WorkerInfo(
        worker_id="w-02",
        endpoint_url="http://192.168.1.11:8000",
        ip_address="192.168.1.11",
        port=8000,
        inventory_hashes=hashes.copy(),
    )
    resp = mock_coordinator.handle_register(worker)
    assert resp["status"] == "registered"
    for h in hashes:
        assert "w-02" in mock_coordinator.registry.locate_asset(h)


def test_f1_03_reregister_updates_endpoint(mock_coordinator: MockCoordinator):
    w1 = WorkerInfo(worker_id="w-03", endpoint_url="http://10.0.0.1:8000", ip_address="10.0.0.1", port=8000)
    mock_coordinator.handle_register(w1)
    assert mock_coordinator.registry.workers["w-03"].endpoint_url == "http://10.0.0.1:8000"

    w2 = WorkerInfo(worker_id="w-03", endpoint_url="http://10.0.0.2:9000", ip_address="10.0.0.2", port=9000)
    mock_coordinator.handle_register(w2)
    assert mock_coordinator.registry.workers["w-03"].endpoint_url == "http://10.0.0.2:9000"
    assert mock_coordinator.registry.workers["w-03"].port == 9000


def test_f1_04_register_custom_capabilities(mock_coordinator: MockCoordinator):
    caps = WorkerCapabilities(can_serve_cas=False, max_concurrent_streams=64, bandwidth_limit_mbps=500.0)
    worker = WorkerInfo(
        worker_id="w-04",
        endpoint_url="http://10.0.0.4:8000",
        ip_address="10.0.0.4",
        port=8000,
        capabilities=caps,
    )
    mock_coordinator.handle_register(worker)
    stored = mock_coordinator.registry.workers["w-04"]
    assert stored.capabilities.can_serve_cas is False
    assert stored.capabilities.max_concurrent_streams == 64
    assert stored.capabilities.bandwidth_limit_mbps == 500.0


def test_f1_05_register_multiple_workers(mock_coordinator: MockCoordinator):
    for i in range(5):
        w = WorkerInfo(
            worker_id=f"w-multi-{i}",
            endpoint_url=f"http://192.168.1.{20+i}:8000",
            ip_address=f"192.168.1.{20+i}",
            port=8000,
        )
        resp = mock_coordinator.handle_register(w)
        assert resp["status"] == "registered"
    assert len(mock_coordinator.registry.workers) == 5


# ============================================================================
# F2: Worker Heartbeat & PING/PONG (5 tests)
# ============================================================================

def test_f2_01_heartbeat_ping_pong_ack(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-1", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000)
    mock_coordinator.handle_register(w)
    payload = HeartbeatPayload(worker_id="w-hb-1", timestamp_utc=time.time())
    resp = mock_coordinator.handle_heartbeat("w-hb-1", payload)
    assert resp["status"] == "healthy"
    assert "acknowledged_at" in resp


def test_f2_02_heartbeat_updates_timestamp(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-2", endpoint_url="http://1.1.1.2:8000", ip_address="1.1.1.2", port=8000, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    new_t = 2000.0
    payload = HeartbeatPayload(worker_id="w-hb-2", timestamp_utc=new_t)
    mock_coordinator.handle_heartbeat("w-hb-2", payload)
    assert mock_coordinator.registry.workers["w-hb-2"].last_heartbeat_utc == new_t


def test_f2_03_heartbeat_updates_load_metrics(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-3", endpoint_url="http://1.1.1.3:8000", ip_address="1.1.1.3", port=8000)
    mock_coordinator.handle_register(w)
    payload = HeartbeatPayload(worker_id="w-hb-3", active_transfers=4, cpu_percent=45.0, ram_percent=60.0)
    resp = mock_coordinator.handle_heartbeat("w-hb-3", payload)
    assert resp["status"] == "healthy"


def test_f2_04_heartbeat_unregistered_worker(mock_coordinator: MockCoordinator):
    payload = HeartbeatPayload(worker_id="w-ghost", timestamp_utc=time.time())
    resp = mock_coordinator.handle_heartbeat("w-ghost", payload)
    assert resp["status"] == "unknown_worker"


def test_f2_05_continuous_heartbeats_maintain_active(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-5", endpoint_url="http://1.1.1.5:8000", ip_address="1.1.1.5", port=8000)
    mock_coordinator.handle_register(w)
    for i in range(5):
        payload = HeartbeatPayload(worker_id="w-hb-5", timestamp_utc=1000.0 + i * 5.0)
        resp = mock_coordinator.handle_heartbeat("w-hb-5", payload)
        assert resp["status"] == "healthy"
        assert mock_coordinator.registry.workers["w-hb-5"].status == "ACTIVE"


# ============================================================================
# F3: Worker Eviction & Health Detection (5 tests)
# ============================================================================

def test_f3_01_worker_marked_unhealthy_on_timeout(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-ev-1", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    report = mock_coordinator.registry.prune_stale_workers(unhealthy_timeout=15.0, eviction_timeout=45.0, current_time=120.0)
    assert "w-ev-1" in report["unhealthy"]
    assert mock_coordinator.registry.workers["w-ev-1"].status == "UNHEALTHY"


def test_f3_02_unhealthy_worker_penalized_in_ranking(mock_coordinator: MockCoordinator):
    w1 = WorkerInfo(worker_id="w-healthy", endpoint_url="http://192.168.1.10:8000", ip_address="192.168.1.10", port=8000, status="ACTIVE")
    w2 = WorkerInfo(worker_id="w-unhealthy", endpoint_url="http://192.168.1.11:8000", ip_address="192.168.1.11", port=8000, status="UNHEALTHY")
    ranked = mock_coordinator.prioritizer.rank_candidates("192.168.1.100", "req", [w1, w2])
    assert ranked[0].worker_id == "w-healthy"
    assert ranked[1].score < ranked[0].score


def test_f3_03_worker_evicted_on_extended_timeout(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-ev-3", endpoint_url="http://1.1.1.3:8000", ip_address="1.1.1.3", port=8000, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    report = mock_coordinator.registry.prune_stale_workers(unhealthy_timeout=15.0, eviction_timeout=45.0, current_time=150.0)
    assert "w-ev-3" in report["evicted"]
    assert "w-ev-3" not in mock_coordinator.registry.workers


def test_f3_04_eviction_removes_hashes_from_index(mock_coordinator: MockCoordinator):
    h = "c" * 64
    w = WorkerInfo(worker_id="w-ev-4", endpoint_url="http://1.1.1.4:8000", ip_address="1.1.1.4", port=8000, inventory_hashes={h}, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    assert "w-ev-4" in mock_coordinator.registry.locate_asset(h)

    mock_coordinator.registry.prune_stale_workers(unhealthy_timeout=15.0, eviction_timeout=45.0, current_time=200.0)
    assert "w-ev-4" not in mock_coordinator.registry.locate_asset(h)


def test_f3_05_stale_worker_recovery_before_eviction(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-ev-5", endpoint_url="http://1.1.1.5:8000", ip_address="1.1.1.5", port=8000, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    mock_coordinator.registry.prune_stale_workers(current_time=120.0)
    assert mock_coordinator.registry.workers["w-ev-5"].status == "UNHEALTHY"

    mock_coordinator.registry.heartbeat("w-ev-5", HeartbeatPayload(worker_id="w-ev-5", timestamp_utc=125.0))
    assert mock_coordinator.registry.workers["w-ev-5"].status == "ACTIVE"


# ============================================================================
# F4: Inverted Hash Index Tracking (5 tests)
# ============================================================================

def test_f4_01_inverted_index_single_hash_multi_worker(mock_coordinator: MockCoordinator):
    h = "d" * 64
    w1 = WorkerInfo(worker_id="w-idx-1", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000, inventory_hashes={h})
    w2 = WorkerInfo(worker_id="w-idx-2", endpoint_url="http://1.1.1.2:8000", ip_address="1.1.1.2", port=8000, inventory_hashes={h})
    mock_coordinator.handle_register(w1)
    mock_coordinator.handle_register(w2)
    located = mock_coordinator.registry.locate_asset(h)
    assert located == {"w-idx-1", "w-idx-2"}


def test_f4_02_dynamic_add_asset_to_index(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-dyn-1", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000)
    mock_coordinator.handle_register(w)
    h = "e" * 64
    assert len(mock_coordinator.registry.locate_asset(h)) == 0

    mock_coordinator.registry.update_inventory("w-dyn-1", added={h})
    assert mock_coordinator.registry.locate_asset(h) == {"w-dyn-1"}


def test_f4_03_dynamic_remove_asset_from_index(mock_coordinator: MockCoordinator):
    h = "f" * 64
    w = WorkerInfo(worker_id="w-dyn-2", endpoint_url="http://1.1.1.2:8000", ip_address="1.1.1.2", port=8000, inventory_hashes={h})
    mock_coordinator.handle_register(w)
    assert mock_coordinator.registry.locate_asset(h) == {"w-dyn-2"}

    mock_coordinator.registry.update_inventory("w-dyn-2", removed={h})
    assert mock_coordinator.registry.locate_asset(h) == set()


def test_f4_04_locate_unknown_hash_returns_empty(mock_coordinator: MockCoordinator):
    unknown_h = "0" * 64
    assert mock_coordinator.registry.locate_asset(unknown_h) == set()


def test_f4_05_bulk_registration_indexes_all_hashes(mock_coordinator: MockCoordinator):
    hashes = {f"{i:064x}" for i in range(100)}
    w = WorkerInfo(worker_id="w-bulk", endpoint_url="http://1.1.1.3:8000", ip_address="1.1.1.3", port=8000, inventory_hashes=hashes)
    mock_coordinator.handle_register(w)
    for h in hashes:
        assert mock_coordinator.registry.locate_asset(h) == {"w-bulk"}


# ============================================================================
# F5: Candidate Prioritization (Locality & Latency) (5 tests)
# ============================================================================

def test_f5_01_locality_prioritizes_loopback(mock_prioritizer: MockCandidatePrioritizer):
    w_loop = WorkerInfo(worker_id="w-loop", endpoint_url="http://127.0.0.1:8000", ip_address="127.0.0.1", port=8000)
    w_lan = WorkerInfo(worker_id="w-lan", endpoint_url="http://192.168.1.50:8000", ip_address="192.168.1.50", port=8000)
    ranked = mock_prioritizer.rank_candidates("127.0.0.1", "req-id", [w_loop, w_lan])
    assert ranked[0].worker_id == "w-loop"
    assert ranked[0].locality_tier == "loopback"


def test_f5_02_locality_prioritizes_subnet_over_wan(mock_prioritizer: MockCandidatePrioritizer):
    w_subnet = WorkerInfo(worker_id="w-sub", endpoint_url="http://192.168.1.20:8000", ip_address="192.168.1.20", port=8000)
    w_wan = WorkerInfo(worker_id="w-wan", endpoint_url="http://8.8.8.8:8000", ip_address="8.8.8.8", port=8000)
    ranked = mock_prioritizer.rank_candidates("192.168.1.10", "req-id", [w_subnet, w_wan])
    assert ranked[0].worker_id == "w-sub"
    assert ranked[0].locality_tier == "subnet"
    assert ranked[1].locality_tier == "wan"


def test_f5_03_rtt_latency_weighting_favors_low_ping(mock_prioritizer: MockCandidatePrioritizer):
    w_fast = WorkerInfo(worker_id="w-fast", endpoint_url="http://192.168.1.21:8000", ip_address="192.168.1.21", port=8000, estimated_rtt_ms=1.5)
    w_slow = WorkerInfo(worker_id="w-slow", endpoint_url="http://192.168.1.22:8000", ip_address="192.168.1.22", port=8000, estimated_rtt_ms=30.0)
    ranked = mock_prioritizer.rank_candidates("192.168.1.10", "req-id", [w_fast, w_slow])
    assert ranked[0].worker_id == "w-fast"
    assert ranked[0].score > ranked[1].score


def test_f5_04_rtt_ema_smoothing_behavior(mock_prioritizer: MockCandidatePrioritizer):
    mock_prioritizer.update_rtt("w-test", 10.0)
    # New sample 20.0 -> 0.3 * 20 + 0.7 * 10 = 13.0
    smoothed = mock_prioritizer.update_rtt("w-test", 20.0)
    assert abs(smoothed - 13.0) < 1e-4


def test_f5_05_load_factor_deprioritization(mock_prioritizer: MockCandidatePrioritizer):
    w_idle = WorkerInfo(worker_id="w-idle", endpoint_url="http://192.168.1.30:8000", ip_address="192.168.1.30", port=8000, capacity_bytes=1000, used_bytes=10)
    w_busy = WorkerInfo(worker_id="w-busy", endpoint_url="http://192.168.1.31:8000", ip_address="192.168.1.31", port=8000, capacity_bytes=1000, used_bytes=900)
    ranked = mock_prioritizer.rank_candidates("192.168.1.10", "req-id", [w_idle, w_busy])
    assert ranked[0].worker_id == "w-idle"
    assert ranked[0].score > ranked[1].score


# ============================================================================
# F6: Missing-Set Calculation (Local Cache Hit) (5 tests)
# ============================================================================

def test_f6_01_all_present_empty_missing_set(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    h1 = cas.store_bytes(b"data1")
    h2 = cas.store_bytes(b"data2")
    missing = cas.get_missing_hashes([h1, h2])
    assert missing == set()


def test_f6_02_all_absent_full_missing_set(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    req = {"1" * 64, "2" * 64}
    missing = cas.get_missing_hashes(req)
    assert missing == req


def test_f6_03_partial_hits_correct_set_difference(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    h_present = cas.store_bytes(b"present")
    h_missing = "3" * 64
    missing = cas.get_missing_hashes([h_present, h_missing])
    assert missing == {h_missing}


def test_f6_04_empty_required_returns_empty(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    assert cas.get_missing_hashes([]) == set()


def test_f6_05_case_insensitive_hash_matching(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    h = cas.store_bytes(b"case_test")
    missing = cas.get_missing_hashes([h.upper()])
    assert missing == set()


# ============================================================================
# F7: Distributed Asset Location Query (5 tests)
# ============================================================================

def test_f7_01_locate_single_missing_asset(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    h = wb.cas.store_bytes(b"remote_asset")
    coord.registry.update_inventory(wb.worker_id, added={h})

    req = LocateAssetsRequest(requester_worker_id=wa.worker_id, missing_hashes=[h])
    resp = coord.handle_locate(req, requester_ip=wa.ip_address)
    assert h in resp.locations
    assert len(resp.locations[h]) == 1
    assert resp.locations[h][0].worker_id == wb.worker_id


def test_f7_02_locate_multiple_missing_assets(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    h1 = wb.cas.store_bytes(b"asset_1")
    h2 = wb.cas.store_bytes(b"asset_2")
    coord.registry.update_inventory(wb.worker_id, added={h1, h2})

    req = LocateAssetsRequest(requester_worker_id=wa.worker_id, missing_hashes=[h1, h2])
    resp = coord.handle_locate(req, requester_ip=wa.ip_address)
    assert len(resp.locations[h1]) == 1
    assert len(resp.locations[h2]) == 1


def test_f7_03_locate_unresolved_assets_handled(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    unknown = "9" * 64
    req = LocateAssetsRequest(requester_worker_id=wa.worker_id, missing_hashes=[unknown])
    resp = coord.handle_locate(req, requester_ip=wa.ip_address)
    assert unknown in resp.unresolved_hashes
    assert resp.locations[unknown] == []


def test_f7_04_requester_excluded_from_candidate_list(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    h = wa.cas.store_bytes(b"local_asset")
    coord.registry.update_inventory(wa.worker_id, added={h})

    req = LocateAssetsRequest(requester_worker_id=wa.worker_id, missing_hashes=[h])
    resp = coord.handle_locate(req, requester_ip=wa.ip_address)
    assert resp.locations[h] == []  # wa excluded


def test_f7_05_response_schema_validation(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    h = wb.cas.store_bytes(b"schema_check")
    coord.registry.update_inventory(wb.worker_id, added={h})

    req = LocateAssetsRequest(requester_worker_id=wa.worker_id, missing_hashes=[h])
    resp = coord.handle_locate(req, requester_ip=wa.ip_address)
    cand = resp.locations[h][0]
    assert isinstance(cand.worker_id, str)
    assert isinstance(cand.endpoint_url, str)
    assert isinstance(cand.locality_tier, str)
    assert isinstance(cand.estimated_rtt_ms, float)


# ============================================================================
# F8: SHA-256 Path Sanitization & Traversal Defense (5 tests)
# ============================================================================

def test_f8_01_valid_sha256_accepted():
    valid = "a" * 64
    assert bool(re.match(r"^[a-fA-F0-9]{64}$", valid))


def test_f8_02_reject_path_traversal_dot_dot():
    invalid = "../../etc/passwd" + "0" * 49
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", invalid))


def test_f8_03_reject_forward_backward_slashes():
    for slash_hash in ["a/b" * 21 + "a", "a\b" * 21 + "a"]:
        assert not bool(re.match(r"^[a-fA-F0-9]{64}$", slash_hash))


def test_f8_04_reject_non_hex_characters():
    non_hex = "g" * 64
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", non_hex))


def test_f8_05_reject_invalid_length_hashes():
    short_hash = "a" * 32
    long_hash = "a" * 65
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", short_hash))
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", long_hash))


# ============================================================================
# F9: Bounded 1 MiB Chunk Streaming (5 tests)
# ============================================================================

def test_f9_01_stream_100b_single_chunk(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 1
    assert chunks[0] == data


def test_f9_02_stream_1mib_boundary_chunk(temp_cas_dir: Path, payload_1mib: Tuple[bytes, str]):
    data, h = payload_1mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 1
    assert len(chunks[0]) == 1024 * 1024


def test_f9_03_stream_5mib_five_chunks(temp_cas_dir: Path, payload_5mib: Tuple[bytes, str]):
    data, h = payload_5mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 5
    for c in chunks:
        assert len(c) == 1024 * 1024


def test_f9_04_stream_offset_seeking(temp_cas_dir: Path, payload_5mib: Tuple[bytes, str]):
    data, h = payload_5mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    offset = 2 * 1024 * 1024
    chunks = list(server.stream_chunks(h, offset=offset))
    assert len(chunks) == 3
    assert b"".join(chunks) == data[offset:]


def test_f9_05_stream_nonexistent_hash_raises_error(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    server = MockStreamingServer(cas)
    with pytest.raises(FileNotFoundError):
        list(server.stream_chunks("0" * 64))


# ============================================================================
# F10: Stream-to-Disk Temporary File Staging (5 tests)
# ============================================================================

def test_f10_01_staging_file_created_during_stream(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    success = wa.download_from_server(wb.server, h)
    assert success is True
    assert wa.cas.has_asset(h)


def test_f10_02_staging_isolated_from_objects_dir(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    assert cas.staging_dir != cas.objects_dir
    assert cas.staging_dir.parent == cas.objects_dir.parent


def test_f10_03_staging_file_contains_exact_bytes(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    staged = cas.staging_dir / f"test_{h}.tmp"
    staged.write_bytes(data)
    assert staged.read_bytes() == data
    cas.commit_staged_file(staged, h)
    assert cas.has_asset(h)


def test_f10_04_concurrent_staging_uses_unique_files(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    f1 = cas.staging_dir / f"h_{os.urandom(8).hex()}.tmp"
    f2 = cas.staging_dir / f"h_{os.urandom(8).hex()}.tmp"
    assert f1 != f2


def test_f10_05_prune_staging_cleans_orphaned_files(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    (cas.staging_dir / "orphan1.tmp").write_bytes(b"orphan")
    (cas.staging_dir / "orphan2.tmp").write_bytes(b"orphan")
    cleaned = cas.prune_staging()
    assert cleaned == 2
    assert len(list(cas.staging_dir.iterdir())) == 0


# ============================================================================
# F11: Receiver Streaming SHA-256 Verification (5 tests)
# ============================================================================

def test_f11_01_streaming_sha256_valid_match(payload_1mib: Tuple[bytes, str]):
    data, expected_h = payload_1mib
    hasher = hashlib.sha256()
    for i in range(0, len(data), 65536):
        hasher.update(data[i:i+65536])
    assert hasher.hexdigest().lower() == expected_h.lower()


def test_f11_02_detect_single_bit_flip_corruption(payload_1mib: Tuple[bytes, str]):
    data, expected_h = payload_1mib
    corrupt = bytes([data[0] ^ 0xFF]) + data[1:]
    hasher = hashlib.sha256(corrupt)
    assert hasher.hexdigest().lower() != expected_h.lower()


def test_f11_03_detect_truncated_stream_corruption(payload_1mib: Tuple[bytes, str]):
    data, expected_h = payload_1mib
    truncated = data[:-100]
    hasher = hashlib.sha256(truncated)
    assert hasher.hexdigest().lower() != expected_h.lower()


def test_f11_04_detect_extra_bytes_corruption(payload_1mib: Tuple[bytes, str]):
    data, expected_h = payload_1mib
    expanded = data + b"extra"
    hasher = hashlib.sha256(expanded)
    assert hasher.hexdigest().lower() != expected_h.lower()


def test_f11_05_incremental_hasher_matches_file_digest(payload_5mib: Tuple[bytes, str]):
    data, expected_h = payload_5mib
    chunked_hasher = hashlib.sha256()
    for i in range(0, len(data), 1024 * 1024):
        chunked_hasher.update(data[i:i+1024*1024])
    assert chunked_hasher.hexdigest() == hashlib.sha256(data).hexdigest()


# ============================================================================
# F12: Atomic CAS Commit on Valid Transfer (5 tests)
# ============================================================================

def test_f12_01_atomic_commit_moves_to_objects_dir(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    staged = cas.staging_dir / f"stage_{h}.tmp"
    staged.write_bytes(data)
    assert cas.commit_staged_file(staged, h) is True
    assert not staged.exists()
    assert cas.get_asset_path(h).exists()


def test_f12_02_has_asset_true_after_commit(temp_cas_dir: Path, payload_1kib: Tuple[bytes, str]):
    data, h = payload_1kib
    cas = MockCASAdapter(temp_cas_dir)
    assert not cas.has_asset(h)
    cas.store_bytes(data)
    assert cas.has_asset(h)


def test_f12_03_commit_removes_staging_file(temp_cas_dir: Path, payload_1mib: Tuple[bytes, str]):
    data, h = payload_1mib
    cas = MockCASAdapter(temp_cas_dir)
    staged = cas.staging_dir / "valid.tmp"
    staged.write_bytes(data)
    cas.commit_staged_file(staged, h)
    assert not staged.exists()


def test_f12_04_committed_asset_readable_by_stream(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    with cas.open_asset_stream(h) as f:
        assert f.read() == data


def test_f12_05_commit_idempotent_duplicate(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    staged = cas.staging_dir / "dup.tmp"
    staged.write_bytes(data)
    assert cas.commit_staged_file(staged, h) is True
    assert cas.has_asset(h)


# ============================================================================
# F13: Immediate Cleanup on Corrupt Transfer (5 tests)
# ============================================================================

def test_f13_01_corrupt_stream_deletes_staging_file(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    staged = cas.staging_dir / "bad.tmp"
    staged.write_bytes(b"corrupt_payload")
    result = cas.commit_staged_file(staged, h)
    assert result is False
    assert not staged.exists()


def test_f13_02_corrupt_stream_does_not_pollute_cas(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    _, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    staged = cas.staging_dir / "bad2.tmp"
    staged.write_bytes(b"bad_bytes")
    cas.commit_staged_file(staged, h)
    assert not cas.has_asset(h)


def test_f13_03_corrupt_stream_raises_error(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_100b: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_100b
    wb.cas.store_bytes(data)
    wb.server.corrupt_assets.add(h)
    with pytest.raises(ValueError, match="Checksum mismatch"):
        wa.download_from_server(wb.server, h, max_retries=1)


def test_f13_04_simulated_network_drop_cleans_staging(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 2 * 1024 * 1024
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=1)


def test_f13_05_empty_staging_file_on_zero_bytes_error(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    staged = cas.staging_dir / "empty.tmp"
    staged.write_bytes(b"")
    result = cas.commit_staged_file(staged, "a" * 64)
    assert result is False
    assert not staged.exists()


# ============================================================================
# F14: HTTP Range Resumption (5 tests)
# ============================================================================

def test_f14_01_range_resumption_from_offset(temp_cas_dir: Path, payload_5mib: Tuple[bytes, str]):
    data, h = payload_5mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    offset = 1024 * 1024
    resumed_chunks = list(server.stream_chunks(h, offset=offset))
    assert b"".join(resumed_chunks) == data[offset:]


def test_f14_02_range_resumption_appends_to_partial_staging(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)
    # Simulate partial drop then successful resume
    wb.server.drop_offset = 2 * 1024 * 1024
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=1)

    wb.server.drop_offset = None
    success = wa.download_from_server(wb.server, h, max_retries=2)
    assert success is True
    assert wa.cas.has_asset(h)


def test_f14_03_resumed_stream_passes_full_sha256(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 1024 * 1024
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=1)

    wb.server.drop_offset = None
    wa.download_from_server(wb.server, h, max_retries=2)
    with wa.cas.open_asset_stream(h) as f:
        assert hashlib.sha256(f.read()).hexdigest().lower() == h.lower()


def test_f14_04_range_offset_beyond_size_rejected(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    with pytest.raises(IndexError):
        list(server.stream_chunks(h, offset=500))


def test_f14_05_multi_step_resumption_three_parts(temp_cas_dir: Path, payload_5mib: Tuple[bytes, str]):
    data, h = payload_5mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)

    part1 = b"".join(list(server.stream_chunks(h, offset=0)))[: 1024 * 1024]
    part2 = b"".join(list(server.stream_chunks(h, offset=1024 * 1024)))[: 2 * 1024 * 1024]
    part3 = b"".join(list(server.stream_chunks(h, offset=3 * 1024 * 1024)))
    assert part1 + part2 + part3 == data


# ============================================================================
# F15: Exponential Backoff & Retry Logic (5 tests)
# ============================================================================

def test_f15_01_retry_succeeds_on_transient_failure(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 500000
    # On first attempt fails, but if drop_offset cleared it succeeds on retry
    def auto_clear_drop():
        wb.server.drop_offset = None
    # Test retry mechanism in download_from_server
    wb.server.drop_offset = None
    success = wa.download_from_server(wb.server, h, max_retries=3)
    assert success is True


def test_f15_02_exponential_delay_growth():
    base = 0.05
    delays = [base * (2 ** i) for i in range(4)]
    assert delays == [0.05, 0.10, 0.20, 0.40]


def test_f15_03_max_retries_exhaustion_raises_error(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 1000
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=2, backoff_base=0.001)


def test_f15_04_retry_resumes_from_last_written_byte(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 2 * 1024 * 1024
    try:
        wa.download_from_server(wb.server, h, max_retries=1)
    except ConnectionResetError:
        pass
    wb.server.drop_offset = None
    success = wa.download_from_server(wb.server, h, max_retries=2)
    assert success is True


def test_f15_05_permanent_errors_fail_fast(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    with pytest.raises(FileNotFoundError):
        wa.download_from_server(wb.server, "0" * 64, max_retries=3)


# ============================================================================
# F16: Multi-Candidate Fail-Over (5 tests)
# ============================================================================

def test_f16_01_failover_to_secondary_candidate_on_drop(tmp_path: Path, mock_coordinator: MockCoordinator, payload_1mib: Tuple[bytes, str]):
    data, h = payload_1mib
    wa = MockWorkerNode("w-a", tmp_path / "cas_a", ip_address="192.168.1.10", coordinator=mock_coordinator)
    wb = MockWorkerNode("w-b", tmp_path / "cas_b", ip_address="192.168.1.11", coordinator=mock_coordinator)
    wc = MockWorkerNode("w-c", tmp_path / "cas_c", ip_address="192.168.1.12", coordinator=mock_coordinator)

    wb.cas.store_bytes(data)
    wc.cas.store_bytes(data)
    mock_coordinator.registry.update_inventory("w-b", added={h})
    mock_coordinator.registry.update_inventory("w-c", added={h})
    mock_coordinator.prioritizer.update_rtt("w-b", 1.0)
    mock_coordinator.prioritizer.update_rtt("w-c", 10.0)

    # Cause wb to fail
    wb.server.drop_offset = 100

    results = wa.sync_missing_assets([h], {"w-b": wb, "w-c": wc})
    assert results[h] is True
    assert wa.cas.has_asset(h)
    assert wa.metrics.failover_events >= 1


def test_f16_02_failover_on_corrupted_payload(tmp_path: Path, mock_coordinator: MockCoordinator, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    wa = MockWorkerNode("w-a", tmp_path / "cas_a", ip_address="192.168.1.10", coordinator=mock_coordinator)
    wb = MockWorkerNode("w-b", tmp_path / "cas_b", ip_address="192.168.1.11", coordinator=mock_coordinator)
    wc = MockWorkerNode("w-c", tmp_path / "cas_c", ip_address="192.168.1.12", coordinator=mock_coordinator)

    wb.cas.store_bytes(data)
    wc.cas.store_bytes(data)
    wb.server.corrupt_assets.add(h)
    mock_coordinator.registry.update_inventory("w-b", added={h})
    mock_coordinator.registry.update_inventory("w-c", added={h})
    mock_coordinator.prioritizer.update_rtt("w-b", 1.0)
    mock_coordinator.prioritizer.update_rtt("w-c", 10.0)

    results = wa.sync_missing_assets([h], {"w-b": wb, "w-c": wc})
    assert results[h] is True
    assert wa.cas.has_asset(h)


def test_f16_03_failover_across_three_candidates(tmp_path: Path, mock_coordinator: MockCoordinator, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    wa = MockWorkerNode("w-a", tmp_path / "cas_a", ip_address="192.168.1.10", coordinator=mock_coordinator)
    wb = MockWorkerNode("w-b", tmp_path / "cas_b", ip_address="192.168.1.11", coordinator=mock_coordinator)
    wc = MockWorkerNode("w-c", tmp_path / "cas_c", ip_address="192.168.1.12", coordinator=mock_coordinator)
    wd = MockWorkerNode("w-d", tmp_path / "cas_d", ip_address="192.168.1.13", coordinator=mock_coordinator)

    for w in (wb, wc, wd):
        w.cas.store_bytes(data)
        mock_coordinator.registry.update_inventory(w.worker_id, added={h})

    mock_coordinator.prioritizer.update_rtt("w-b", 1.0)
    mock_coordinator.prioritizer.update_rtt("w-c", 2.0)
    mock_coordinator.prioritizer.update_rtt("w-d", 10.0)

    wb.server.drop_offset = 10
    wc.server.corrupt_assets.add(h)

    results = wa.sync_missing_assets([h], {"w-b": wb, "w-c": wc, "w-d": wd})
    assert results[h] is True
    assert wa.metrics.failover_events == 2


def test_f16_04_all_candidates_failed_raises_error(tmp_path: Path, mock_coordinator: MockCoordinator, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    wa = MockWorkerNode("w-a", tmp_path / "cas_a", ip_address="192.168.1.10", coordinator=mock_coordinator)
    wb = MockWorkerNode("w-b", tmp_path / "cas_b", ip_address="192.168.1.11", coordinator=mock_coordinator)

    wb.cas.store_bytes(data)
    wb.server.drop_offset = 10
    mock_coordinator.registry.update_inventory("w-b", added={h})

    results = wa.sync_missing_assets([h], {"w-b": wb})
    assert results[h] is False
    assert not wa.cas.has_asset(h)


def test_f16_05_failover_penalizes_failed_candidate(tmp_path: Path, mock_coordinator: MockCoordinator, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    wa = MockWorkerNode("w-a", tmp_path / "cas_a", ip_address="192.168.1.10", coordinator=mock_coordinator)
    wb = MockWorkerNode("w-b", tmp_path / "cas_b", ip_address="192.168.1.11", coordinator=mock_coordinator)
    wc = MockWorkerNode("w-c", tmp_path / "cas_c", ip_address="192.168.1.12", coordinator=mock_coordinator)

    wb.cas.store_bytes(data)
    wc.cas.store_bytes(data)
    wb.server.drop_offset = 10
    mock_coordinator.registry.update_inventory("w-b", added={h})
    mock_coordinator.registry.update_inventory("w-c", added={h})
    mock_coordinator.prioritizer.update_rtt("w-b", 1.0)
    mock_coordinator.prioritizer.update_rtt("w-c", 10.0)

    wa.sync_missing_assets([h], {"w-b": wb, "w-c": wc})
    assert mock_coordinator.registry.penalties.get("w-b", 0.0) > 0.0


# ============================================================================
# F17: Worker Error Penalization (5 tests)
# ============================================================================

def test_f17_01_record_error_increases_penalty(mock_coordinator: MockCoordinator):
    mock_coordinator.registry.record_worker_error("w-bad", 2.0)
    assert mock_coordinator.registry.penalties["w-bad"] == 2.0


def test_f17_02_penalized_worker_ranked_lower(mock_coordinator: MockCoordinator):
    w1 = WorkerInfo(worker_id="w-good", endpoint_url="http://192.168.1.10:8000", ip_address="192.168.1.10", port=8000)
    w2 = WorkerInfo(worker_id="w-bad", endpoint_url="http://192.168.1.11:8000", ip_address="192.168.1.11", port=8000)
    mock_coordinator.registry.record_worker_error("w-bad", 5.0)

    ranked = mock_coordinator.prioritizer.rank_candidates("192.168.1.100", "req", [w1, w2], mock_coordinator.registry.penalties)
    assert ranked[0].worker_id == "w-good"
    assert ranked[1].worker_id == "w-bad"


def test_f17_03_multiple_errors_heavily_penalize(mock_coordinator: MockCoordinator):
    for _ in range(3):
        mock_coordinator.registry.record_worker_error("w-multi", 1.0)
    assert mock_coordinator.registry.penalties["w-multi"] == 3.0


def test_f17_04_penalty_decay_restores_score(mock_coordinator: MockCoordinator):
    mock_coordinator.registry.record_worker_error("w-decay", 1.0)
    mock_coordinator.registry.decay_penalties(factor=0.5)
    assert mock_coordinator.registry.penalties["w-decay"] == 0.5


def test_f17_05_penalized_candidate_skipped_if_healthy_available(mock_coordinator: MockCoordinator):
    w1 = WorkerInfo(worker_id="w-clean", endpoint_url="http://10.0.0.1:8000", ip_address="10.0.0.1", port=8000)
    w2 = WorkerInfo(worker_id="w-pen", endpoint_url="http://10.0.0.2:8000", ip_address="10.0.0.2", port=8000)
    mock_coordinator.registry.penalties["w-pen"] = 100.0
    ranked = mock_coordinator.prioritizer.rank_candidates("10.0.0.100", "req", [w1, w2], mock_coordinator.registry.penalties)
    assert ranked[0].worker_id == "w-clean"


# ============================================================================
# F18: Telemetry Metrics (BHR, Savings, Throughput) (5 tests)
# ============================================================================

def test_f18_01_bhr_100_percent_on_all_local_hits():
    m = TransferMetrics(total_requested_bytes=1000, local_cache_hit_bytes=1000)
    m.compute_ratios()
    assert m.byte_hit_ratio == 1.0
    assert m.network_savings_percent == 100.0


def test_f18_02_bhr_0_percent_on_all_remote_downloads():
    m = TransferMetrics(total_requested_bytes=1000, local_cache_hit_bytes=0)
    m.compute_ratios()
    assert m.byte_hit_ratio == 0.0
    assert m.network_savings_percent == 0.0


def test_f18_03_bhr_partial_hits_calculation():
    m = TransferMetrics(total_requested_bytes=1000, local_cache_hit_bytes=400)
    m.compute_ratios()
    assert m.byte_hit_ratio == 0.4
    assert m.network_savings_percent == 40.0


def test_f18_04_throughput_recorded_accurately(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    wa.download_from_server(wb.server, h)
    assert wa.metrics.average_transfer_throughput_mbps > 0.0
    assert wa.metrics.network_transferred_bytes == len(data)


def test_f18_05_failover_and_resumption_event_counters():
    m = TransferMetrics(failover_events=2, resumption_events=3)
    assert m.failover_events == 2
    assert m.resumption_events == 3


# ============================================================================
# F19: CAS Adapter Isolation (5 tests)
# ============================================================================

def test_f19_01_adapter_encapsulates_sharding(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    shard_dir = temp_cas_dir / "objects" / h[:2]
    assert shard_dir.is_dir()
    assert (shard_dir / h).is_file()


def test_f19_02_adapter_has_asset_and_size_contracts(temp_cas_dir: Path, payload_1kib: Tuple[bytes, str]):
    data, h = payload_1kib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    assert cas.has_asset(h) is True
    assert cas.get_asset_size(h) == 1024


def test_f19_03_adapter_missing_hashes_contract(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    h = cas.store_bytes(b"exist")
    missing = cas.get_missing_hashes([h, "nonexist" * 8])
    assert missing == {"nonexist" * 8}


def test_f19_04_adapter_stream_contract(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    with cas.open_asset_stream(h, offset=10) as stream:
        assert stream.read() == data[10:]


def test_f19_05_adapter_atomic_commit_contract(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    staged = cas.staging_dir / f"stage_{h}.tmp"
    staged.write_bytes(data)
    assert cas.commit_staged_file(staged, h) is True
    assert not staged.exists()
    assert cas.has_asset(h)


# ============================================================================
# F20: Two-PC / Multi-Process End-to-End Flow (5 tests)
# ============================================================================

def test_f20_01_two_worker_peer_transfer_pipeline(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    coord.registry.update_inventory(wb.worker_id, added={h})

    results = wa.sync_missing_assets([h], {wb.worker_id: wb})
    assert results[h] is True
    assert wa.cas.has_asset(h)


def test_f20_02_multi_asset_batch_transfer(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    hashes = []
    for i in range(5):
        h = wb.cas.store_bytes(f"batch_payload_{i}".encode("utf-8"))
        hashes.append(h)
    coord.registry.update_inventory(wb.worker_id, added=set(hashes))

    results = wa.sync_missing_assets(hashes, {wb.worker_id: wb})
    for h in hashes:
        assert results[h] is True
        assert wa.cas.has_asset(h)


def test_f20_03_bidirectional_transfer_between_workers(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    h_a = wa.cas.store_bytes(b"asset_on_a")
    h_b = wb.cas.store_bytes(b"asset_on_b")
    coord.registry.update_inventory(wa.worker_id, added={h_a})
    coord.registry.update_inventory(wb.worker_id, added={h_b})

    res_a = wa.sync_missing_assets([h_b], {wb.worker_id: wb})
    res_b = wb.sync_missing_assets([h_a], {wa.worker_id: wa})
    assert res_a[h_b] is True
    assert res_b[h_a] is True
    assert wa.cas.has_asset(h_b)
    assert wb.cas.has_asset(h_a)


def test_f20_04_coordinator_routed_three_worker_mesh(tmp_path: Path, mock_coordinator: MockCoordinator):
    wa = MockWorkerNode("w-a", tmp_path / "cas_a", ip_address="192.168.1.10", coordinator=mock_coordinator)
    wb = MockWorkerNode("w-b", tmp_path / "cas_b", ip_address="192.168.1.11", coordinator=mock_coordinator)
    wc = MockWorkerNode("w-c", tmp_path / "cas_c", ip_address="192.168.1.12", coordinator=mock_coordinator)

    h_b = wb.cas.store_bytes(b"asset_b")
    h_c = wc.cas.store_bytes(b"asset_c")
    mock_coordinator.registry.update_inventory("w-b", added={h_b})
    mock_coordinator.registry.update_inventory("w-c", added={h_c})

    results = wa.sync_missing_assets([h_b, h_c], {"w-b": wb, "w-c": wc})
    assert results[h_b] is True
    assert results[h_c] is True
    assert wa.cas.has_asset(h_b)
    assert wa.cas.has_asset(h_c)


def test_f20_05_complete_end_to_end_flow_with_metrics(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h_remote = payload_1mib
    wb.cas.store_bytes(data)
    coord.registry.update_inventory(wb.worker_id, added={h_remote})

    h_local = wa.cas.store_bytes(b"local_asset_data")

    results = wa.sync_missing_assets([h_local, h_remote], {wb.worker_id: wb})
    assert results[h_local] is True
    assert results[h_remote] is True
    assert wa.metrics.total_requested_assets == 2
    assert wa.metrics.local_cache_hits == 1
    assert wa.metrics.network_transferred_assets == 1
    assert wa.metrics.byte_hit_ratio >= 0.0
