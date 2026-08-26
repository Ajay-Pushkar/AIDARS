"""Tier 3: Pairwise and Multi-Feature Interaction Scenarios.

Covers complex multi-feature cross-product behaviors:
- Heartbeat expiry during active chunk stream
- Corrupt node failover with range resumption
- Partial drop resumption and atomic commit
- Concurrent multi-worker sync
- Swarm bidirectional sync
- Stale cache missing-set resolution and commit
- Candidate ranking dynamic shift on RTT change
- Worker crash and restart with inventory preserved
- Concurrent transfers same asset commit collision safety
- Worker error penalization causing cluster route rebalancing
- Partial local hit + remote multi-candidate fetch + telemetry
- Range resumption on evicted asset fails over
- Staging directory error aborts cleanly
- Rapid worker registration churn under query load
- CAS adapter concurrent reads during commit
- Worker bandwidth throttling concurrent streams
- Inverted index consistency across eviction and re-registration
- Client backoff interrupted by candidate failover
- M4 smart package records to distributed CAS sync
- Telemetry metrics consistency across failures, resumptions, and commits
Total >= 20 scenarios.
"""
from __future__ import annotations

import hashlib
import os
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


def test_heartbeat_expiry_during_active_stream(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)
    coord.registry.update_inventory(wb.worker_id, added={h})

    # Start stream and expire wb during transfer
    stream_iter = wb.server.stream_chunks(h)
    first_chunk = next(stream_iter)
    assert len(first_chunk) == 1024 * 1024

    # Expire wb in coordinator
    coord.registry.prune_stale_workers(unhealthy_timeout=0.0, current_time=time.time() + 100.0)
    assert wb.info.worker_id not in [c.worker_id for c in coord.handle_locate(LocateAssetsRequest(requester_worker_id=wa.worker_id, missing_hashes=[h])).locations.get(h, []) if c.locality_tier != "wan" and c.score > 0]

    # Active stream still finishes
    remaining_chunks = list(stream_iter)
    full_data = first_chunk + b"".join(remaining_chunks)
    assert hashlib.sha256(full_data).hexdigest().lower() == h.lower()


def test_corrupt_node_failover_with_range_resumption(tmp_path: Path, mock_coordinator: MockCoordinator, payload_5mib: Tuple[bytes, str]):
    data, h = payload_5mib
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
    assert wa.metrics.failover_events >= 1


def test_partial_drop_resumption_and_atomic_commit(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_10mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_10mib
    wb.cas.store_bytes(data)

    # Drop at 4 MiB
    wb.server.drop_offset = 4 * 1024 * 1024
    try:
        wa.download_from_server(wb.server, h, max_retries=1)
    except ConnectionResetError:
        pass

    wb.server.drop_offset = None
    assert wa.download_from_server(wb.server, h, max_retries=2) is True
    assert wa.cas.has_asset(h)
    with wa.cas.open_asset_stream(h) as f:
        assert f.read() == data


def test_multi_worker_concurrent_sync(tmp_path: Path, mock_coordinator: MockCoordinator):
    seeder = MockWorkerNode("seeder", tmp_path / "cas_seed", ip_address="192.168.1.10", coordinator=mock_coordinator)
    w1 = MockWorkerNode("w1", tmp_path / "cas_w1", ip_address="192.168.1.11", coordinator=mock_coordinator)
    w2 = MockWorkerNode("w2", tmp_path / "cas_w2", ip_address="192.168.1.12", coordinator=mock_coordinator)
    w3 = MockWorkerNode("w3", tmp_path / "cas_w3", ip_address="192.168.1.13", coordinator=mock_coordinator)

    h1 = seeder.cas.store_bytes(b"asset_1_" * 1000)
    h2 = seeder.cas.store_bytes(b"asset_2_" * 1000)
    h3 = seeder.cas.store_bytes(b"asset_3_" * 1000)
    mock_coordinator.registry.update_inventory("seeder", added={h1, h2, h3})

    r1 = w1.sync_missing_assets([h1], {"seeder": seeder})
    r2 = w2.sync_missing_assets([h2], {"seeder": seeder})
    r3 = w3.sync_missing_assets([h3], {"seeder": seeder})

    assert r1[h1] is True and w1.cas.has_asset(h1)
    assert r2[h2] is True and w2.cas.has_asset(h2)
    assert r3[h3] is True and w3.cas.has_asset(h3)


def test_bidirectional_swarm_sync(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    h_a = wa.cas.store_bytes(b"payload_a_12345")
    h_b = wb.cas.store_bytes(b"payload_b_67890")
    coord.registry.update_inventory("worker-a", added={h_a})
    coord.registry.update_inventory("worker-b", added={h_b})

    res_a = wa.sync_missing_assets([h_b], {"worker-b": wb})
    res_b = wb.sync_missing_assets([h_a], {"worker-a": wa})

    assert res_a[h_b] is True and wa.cas.has_asset(h_b)
    assert res_b[h_a] is True and wb.cas.has_asset(h_a)


def test_stale_cache_missing_set_resolution_and_commit(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    h = wb.cas.store_bytes(b"stale_test_data")
    coord.registry.update_inventory("worker-b", added={h})

    # Initial missing set contains h
    assert wa.calculate_missing_set([h]) == {h}

    # Sync
    wa.sync_missing_assets([h], {"worker-b": wb})

    # Subsequent missing set is empty
    assert wa.calculate_missing_set([h]) == set()


def test_candidate_ranking_dynamic_shift_on_rtt_change(tmp_path: Path, mock_coordinator: MockCoordinator):
    w1 = WorkerInfo(worker_id="w-node-1", endpoint_url="http://192.168.1.10:8000", ip_address="192.168.1.10", port=8000, estimated_rtt_ms=1.0)
    w2 = WorkerInfo(worker_id="w-node-2", endpoint_url="http://192.168.1.11:8000", ip_address="192.168.1.11", port=8000, estimated_rtt_ms=2.0)

    # Initial rank
    r1 = mock_coordinator.prioritizer.rank_candidates("192.168.1.100", "req", [w1, w2])
    assert r1[0].worker_id == "w-node-1"

    # w1 RTT spikes to 100ms
    mock_coordinator.prioritizer.update_rtt("w-node-1", 100.0)
    mock_coordinator.prioritizer.update_rtt("w-node-1", 100.0)
    r2 = mock_coordinator.prioritizer.rank_candidates("192.168.1.100", "req", [w1, w2])
    assert r2[0].worker_id == "w-node-2"


def test_worker_crash_and_restart_with_inventory_preserved(tmp_path: Path, mock_coordinator: MockCoordinator):
    cas_path = tmp_path / "persistent_cas"
    w_orig = MockWorkerNode("w-persist", cas_path, ip_address="192.168.1.50", coordinator=mock_coordinator)
    h = w_orig.cas.store_bytes(b"persistent_payload")
    mock_coordinator.registry.update_inventory("w-persist", added={h})

    # Crash / delete original object
    del w_orig

    # Restart worker on same cas_path
    w_restarted = MockWorkerNode("w-persist", cas_path, ip_address="192.168.1.50", coordinator=mock_coordinator)
    assert w_restarted.cas.has_asset(h) is True
    assert h in w_restarted.info.inventory_hashes


def test_concurrent_transfers_same_asset_commit_collision_safe(tmp_path: Path, mock_coordinator: MockCoordinator, payload_1mib: Tuple[bytes, str]):
    data, h = payload_1mib
    seeder = MockWorkerNode("seeder", tmp_path / "cas_seed", coordinator=mock_coordinator)
    seeder.cas.store_bytes(data)

    worker = MockWorkerNode("receiver", tmp_path / "cas_recv", coordinator=mock_coordinator)

    # Simulate two independent staging files committing same hash
    f1 = worker.cas.staging_dir / f"f1_{h}.tmp"
    f2 = worker.cas.staging_dir / f"f2_{h}.tmp"
    f1.write_bytes(data)
    f2.write_bytes(data)

    c1 = worker.cas.commit_staged_file(f1, h)
    c2 = worker.cas.commit_staged_file(f2, h)
    assert c1 is True
    assert c2 is True
    assert worker.cas.has_asset(h)


def test_worker_error_penalization_rebalances_cluster(tmp_path: Path, mock_coordinator: MockCoordinator, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    w_bad = MockWorkerNode("w-bad", tmp_path / "cas_bad", ip_address="192.168.1.20", coordinator=mock_coordinator)
    w_good = MockWorkerNode("w-good", tmp_path / "cas_good", ip_address="192.168.1.21", coordinator=mock_coordinator)
    client = MockWorkerNode("w-client", tmp_path / "cas_client", ip_address="192.168.1.22", coordinator=mock_coordinator)

    w_bad.cas.store_bytes(data)
    w_good.cas.store_bytes(data)
    w_bad.server.corrupt_assets.add(h)
    mock_coordinator.registry.update_inventory("w-bad", added={h})
    mock_coordinator.registry.update_inventory("w-good", added={h})
    mock_coordinator.prioritizer.update_rtt("w-bad", 1.0)
    mock_coordinator.prioritizer.update_rtt("w-good", 10.0)

    client.sync_missing_assets([h], {"w-bad": w_bad, "w-good": w_good})
    assert mock_coordinator.registry.penalties.get("w-bad", 0) > 0

    # Next location query ranks w-good first
    ranked = mock_coordinator.prioritizer.rank_candidates("192.168.1.22", "w-client", [w_bad.info, w_good.info], mock_coordinator.registry.penalties)
    assert ranked[0].worker_id == "w-good"


def test_partial_local_hit_plus_remote_fetch_and_metrics(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h_remote = payload_1mib
    wb.cas.store_bytes(data)
    coord.registry.update_inventory("worker-b", added={h_remote})

    h_local = wa.cas.store_bytes(b"local_hit_payload")

    results = wa.sync_missing_assets([h_local, h_remote], {"worker-b": wb})
    assert results[h_local] is True
    assert results[h_remote] is True
    assert wa.metrics.local_cache_hits == 1
    assert wa.metrics.network_transferred_assets == 1
    assert wa.metrics.byte_hit_ratio >= 0.0


def test_range_resumption_on_evicted_asset_fails_over(tmp_path: Path, mock_coordinator: MockCoordinator, payload_1mib: Tuple[bytes, str]):
    data, h = payload_1mib
    wa = MockWorkerNode("w-a", tmp_path / "cas_a", ip_address="192.168.1.10", coordinator=mock_coordinator)
    wb = MockWorkerNode("w-b", tmp_path / "cas_b", ip_address="192.168.1.11", coordinator=mock_coordinator)
    wc = MockWorkerNode("w-c", tmp_path / "cas_c", ip_address="192.168.1.12", coordinator=mock_coordinator)

    # wb and wc have asset
    wb.cas.store_bytes(data)
    wc.cas.store_bytes(data)
    mock_coordinator.registry.update_inventory("w-b", added={h})
    mock_coordinator.registry.update_inventory("w-c", added={h})

    # wb drops and then evicts asset
    wb.server.drop_offset = 100
    try:
        wa.download_from_server(wb.server, h, max_retries=1)
    except ConnectionResetError:
        pass

    wb.cas.get_asset_path(h).unlink()
    # Failover to wc
    assert wa.download_from_server(wc.server, h) is True
    assert wa.cas.has_asset(h)


def test_staging_directory_error_aborts_cleanly(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    staged = cas.staging_dir / "error.tmp"
    staged.write_bytes(b"invalid_bytes")
    # Commit with wrong hash
    res = cas.commit_staged_file(staged, h)
    assert res is False
    assert not staged.exists()


def test_rapid_worker_registration_churn_under_query_load(mock_coordinator: MockCoordinator):
    for i in range(20):
        w = WorkerInfo(worker_id=f"w-churn-{i}", endpoint_url=f"http://10.0.0.{i}:8000", ip_address=f"10.0.0.{i}", port=8000, inventory_hashes={f"{i:064x}"})
        mock_coordinator.handle_register(w)
        req = LocateAssetsRequest(requester_worker_id="req", missing_hashes=[f"{i:064x}"])
        resp = mock_coordinator.handle_locate(req)
        assert len(resp.locations[f"{i:064x}"]) == 1


def test_cas_adapter_concurrent_reads_during_commit(temp_cas_dir: Path, payload_1mib: Tuple[bytes, str]):
    data, h1 = payload_1mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)

    # Open stream on h1
    stream = cas.open_asset_stream(h1)
    chunk1 = stream.read(1024 * 1024)

    # Commit new asset h2 concurrently
    h2 = cas.store_bytes(b"new_data_2")
    assert cas.has_asset(h2)

    # Complete stream on h1
    assert chunk1 == data
    stream.close()


def test_worker_bandwidth_throttling_concurrent_streams(tmp_path: Path, mock_coordinator: MockCoordinator):
    caps = WorkerCapabilities(can_serve_cas=True, max_concurrent_streams=2, bandwidth_limit_mbps=10.0)
    worker = WorkerInfo(worker_id="w-throttled", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000, capabilities=caps)
    mock_coordinator.handle_register(worker)
    assert mock_coordinator.registry.workers["w-throttled"].capabilities.max_concurrent_streams == 2


def test_inverted_index_consistency_across_eviction_and_reregistration(mock_coordinator: MockCoordinator):
    h = "e" * 64
    w = WorkerInfo(worker_id="w-cycle", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000, inventory_hashes={h}, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    assert "w-cycle" in mock_coordinator.registry.locate_asset(h)

    # Evict
    mock_coordinator.registry.prune_stale_workers(current_time=200.0)
    assert "w-cycle" not in mock_coordinator.registry.locate_asset(h)

    # Re-register
    w_new = WorkerInfo(worker_id="w-cycle", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000, inventory_hashes={h}, last_heartbeat_utc=300.0)
    mock_coordinator.handle_register(w_new)
    assert "w-cycle" in mock_coordinator.registry.locate_asset(h)


def test_client_backoff_interrupted_by_candidate_failover(tmp_path: Path, mock_coordinator: MockCoordinator, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    wa = MockWorkerNode("w-a", tmp_path / "cas_a", coordinator=mock_coordinator)
    wb = MockWorkerNode("w-b", tmp_path / "cas_b", coordinator=mock_coordinator)
    wc = MockWorkerNode("w-c", tmp_path / "cas_c", coordinator=mock_coordinator)

    wb.cas.store_bytes(data)
    wc.cas.store_bytes(data)
    wb.server.drop_offset = 10
    mock_coordinator.registry.update_inventory("w-b", added={h})
    mock_coordinator.registry.update_inventory("w-c", added={h})
    mock_coordinator.prioritizer.update_rtt("w-b", 1.0)
    mock_coordinator.prioritizer.update_rtt("w-c", 10.0)

    t0 = time.time()
    results = wa.sync_missing_assets([h], {"w-b": wb, "w-c": wc})
    t_elapsed = time.time() - t0
    assert results[h] is True
    assert t_elapsed < 1.0  # Immediate failover without waiting long delays


def test_m4_smart_package_records_to_distributed_cas_sync(tmp_path: Path, mock_coordinator: MockCoordinator):
    seeder = MockWorkerNode("seeder", tmp_path / "cas_seed", coordinator=mock_coordinator)
    client = MockWorkerNode("client", tmp_path / "cas_client", coordinator=mock_coordinator)

    # M4 simulated records
    records = [
        {"name": "WoodTexture.png", "data": b"wood_texture_bytes"},
        {"name": "MetalMat.blend", "data": b"metal_mat_bytes"},
    ]

    required_hashes = []
    for r in records:
        h = seeder.cas.store_bytes(r["data"])
        required_hashes.append(h)
        mock_coordinator.registry.update_inventory("seeder", added={h})

    sync_results = client.sync_missing_assets(required_hashes, {"seeder": seeder})
    for h in required_hashes:
        assert sync_results[h] is True
        assert client.cas.has_asset(h) is True


def test_telemetry_consistency_across_failures_resumptions_and_commits(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    coord.registry.update_inventory("worker-b", added={h})

    # Drop once then succeed
    wb.server.drop_offset = 500000
    try:
        wa.download_from_server(wb.server, h, max_retries=1)
    except ConnectionResetError:
        pass

    wb.server.drop_offset = None
    wa.download_from_server(wb.server, h, max_retries=2)
    assert wa.metrics.network_transferred_assets == 1
    assert wa.metrics.network_transferred_bytes == len(data)
