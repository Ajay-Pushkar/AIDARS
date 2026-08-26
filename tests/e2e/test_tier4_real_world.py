"""Tier 4: Real-World Multi-Worker Cluster Scenarios.

Covers:
1. Multi-Worker Asset Distribution Pipeline
2. Partial Cache Hit + Remote Fill
3. Network Interruption with HTTP Range Resumption
4. Primary Worker Corruption with Automatic Failover
5. Live Simulated Socket Cluster Flow
6. Multi-Worker Swarm Convergence
7. Heavy Scene Asset Distribution Throughput
8. Asymmetric Network Topology (LAN vs WAN)
9. Worker Node Crash and Dynamic Recovery
10. Complete Render Farm End-to-End Pipeline
Total >= 10 realistic cluster scenarios.
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


def test_scenario_1_three_worker_render_cluster_distribution(tmp_path: Path, mock_coordinator: MockCoordinator):
    """Scenario 1: Realistic 3-worker render cluster distributing 30 assets."""
    w1 = MockWorkerNode("render-01", tmp_path / "cas_01", ip_address="192.168.1.101", coordinator=mock_coordinator)
    w2 = MockWorkerNode("render-02", tmp_path / "cas_02", ip_address="192.168.1.102", coordinator=mock_coordinator)
    w3 = MockWorkerNode("render-03", tmp_path / "cas_03", ip_address="192.168.1.103", coordinator=mock_coordinator)

    # Seed 30 assets across w1 and w2
    all_hashes = []
    for i in range(30):
        data = f"texture_asset_{i}_content_{'x'*500}".encode("utf-8")
        target_worker = w1 if i % 2 == 0 else w2
        h = target_worker.cas.store_bytes(data)
        all_hashes.append(h)
        mock_coordinator.registry.update_inventory(target_worker.worker_id, added={h})

    # w3 requires all 30 assets
    results = w3.sync_missing_assets(all_hashes, {"render-01": w1, "render-02": w2})
    assert len(results) == 30
    assert all(results.values())
    for h in all_hashes:
        assert w3.cas.has_asset(h) is True
    assert w3.metrics.network_transferred_assets == 30


def test_scenario_2_high_cache_hit_production_pipeline(tmp_path: Path, mock_coordinator: MockCoordinator):
    """Scenario 2: Production farm pipeline with 80% cache hit and 20% network fill."""
    w_seeder = MockWorkerNode("seeder", tmp_path / "cas_seeder", coordinator=mock_coordinator)
    w_receiver = MockWorkerNode("receiver", tmp_path / "cas_receiver", coordinator=mock_coordinator)

    # 10 assets: 8 pre-cached in receiver, 2 only in seeder
    all_hashes = []
    for i in range(10):
        data = f"production_asset_{i}_{'p'*1000}".encode("utf-8")
        if i < 8:
            h = w_receiver.cas.store_bytes(data)
        else:
            h = w_seeder.cas.store_bytes(data)
            mock_coordinator.registry.update_inventory("seeder", added={h})
        all_hashes.append(h)

    results = w_receiver.sync_missing_assets(all_hashes, {"seeder": w_seeder})
    assert all(results.values())
    assert w_receiver.metrics.total_requested_assets == 10
    assert w_receiver.metrics.local_cache_hits == 8
    assert w_receiver.metrics.network_transferred_assets == 2
    assert w_receiver.metrics.network_savings_percent == 80.0


def test_scenario_3_network_interruption_with_live_resumption(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    """Scenario 3: 5 MiB asset interrupted midway, resumed with Range header, verified."""
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)

    # Drop connection at 2.5 MiB
    wb.server.drop_offset = int(2.5 * 1024 * 1024)
    try:
        wa.download_from_server(wb.server, h, max_retries=1)
    except ConnectionResetError:
        pass

    # Clear drop, resume and complete
    wb.server.drop_offset = None
    assert wa.download_from_server(wb.server, h, max_retries=2) is True
    assert wa.cas.has_asset(h)
    with wa.cas.open_asset_stream(h) as f:
        assert f.read() == data


def test_scenario_4_corrupt_peer_automatic_failover(tmp_path: Path, mock_coordinator: MockCoordinator, payload_1mib: Tuple[bytes, str]):
    """Scenario 4: Malicious/corrupt peer sends corrupt chunks; client fails over to backup peer."""
    data, h = payload_1mib
    w_primary = MockWorkerNode("peer-primary", tmp_path / "cas_p", ip_address="192.168.1.10", coordinator=mock_coordinator)
    w_backup = MockWorkerNode("peer-backup", tmp_path / "cas_b", ip_address="192.168.1.11", coordinator=mock_coordinator)
    w_client = MockWorkerNode("peer-client", tmp_path / "cas_c", ip_address="192.168.1.12", coordinator=mock_coordinator)

    w_primary.cas.store_bytes(data)
    w_backup.cas.store_bytes(data)
    w_primary.server.corrupt_assets.add(h)  # Inject corruption on primary
    mock_coordinator.registry.update_inventory("peer-primary", added={h})
    mock_coordinator.registry.update_inventory("peer-backup", added={h})
    mock_coordinator.prioritizer.update_rtt("peer-primary", 1.0)
    mock_coordinator.prioritizer.update_rtt("peer-backup", 10.0)

    results = w_client.sync_missing_assets([h], {"peer-primary": w_primary, "peer-backup": w_backup})
    assert results[h] is True
    assert w_client.cas.has_asset(h) is True
    assert w_client.metrics.failover_events >= 1


def test_scenario_5_simulated_asgi_socket_cluster_flow(tmp_path: Path, mock_coordinator: MockCoordinator):
    """Scenario 5: Simulated cluster control plane REST + data plane streaming flow."""
    w1 = MockWorkerNode("asgi-node-1", tmp_path / "cas_1", ip_address="127.0.0.1", port=8001, coordinator=mock_coordinator)
    w2 = MockWorkerNode("asgi-node-2", tmp_path / "cas_2", ip_address="127.0.0.1", port=8002, coordinator=mock_coordinator)

    h = w1.cas.store_bytes(b"asgi_stream_test_payload")
    mock_coordinator.registry.update_inventory("asgi-node-1", added={h})

    # w2 heartbeats
    hb_resp = w2.heartbeat()
    assert hb_resp["status"] == "healthy"

    # w2 locates and downloads
    res = w2.sync_missing_assets([h], {"asgi-node-1": w1})
    assert res[h] is True
    assert w2.cas.has_asset(h)


def test_scenario_6_multi_worker_swarm_convergence(tmp_path: Path, mock_coordinator: MockCoordinator):
    """Scenario 6: 4 workers each holding 5 distinct assets converge until all hold all 20 assets."""
    workers = [MockWorkerNode(f"swarm-0{i}", tmp_path / f"cas_sw_{i}", coordinator=mock_coordinator) for i in range(4)]
    all_hashes = []

    for i, w in enumerate(workers):
        for j in range(5):
            h = w.cas.store_bytes(f"asset_node_{i}_item_{j}".encode("utf-8"))
            all_hashes.append(h)
            mock_coordinator.registry.update_inventory(w.worker_id, added={h})

    peer_map = {w.worker_id: w for w in workers}
    for w in workers:
        res = w.sync_missing_assets(all_hashes, peer_map)
        assert all(res.values())
        assert len(w.cas.list_cached_hashes()) == 20


def test_scenario_7_heavy_scene_asset_distribution_throughput(tmp_path: Path, mock_coordinator: MockCoordinator):
    """Scenario 7: 10 MiB scene asset distribution with throughput recording."""
    seeder = MockWorkerNode("heavy-seeder", tmp_path / "cas_hs", coordinator=mock_coordinator)
    client = MockWorkerNode("heavy-client", tmp_path / "cas_hc", coordinator=mock_coordinator)

    data = b"V" * (10 * 1024 * 1024)
    h = seeder.cas.store_bytes(data)
    mock_coordinator.registry.update_inventory("heavy-seeder", added={h})

    client.sync_missing_assets([h], {"heavy-seeder": seeder})
    assert client.cas.has_asset(h) is True
    assert client.metrics.average_transfer_throughput_mbps > 0.0


def test_scenario_8_asymmetric_network_topology_lan_vs_wan(tmp_path: Path, mock_coordinator: MockCoordinator, payload_100b: Tuple[bytes, str]):
    """Scenario 8: Candidate ranking prioritizes same-subnet LAN worker over WAN remote worker."""
    data, h = payload_100b
    w_lan = MockWorkerNode("w-lan", tmp_path / "cas_lan", ip_address="192.168.1.20", coordinator=mock_coordinator)
    w_wan = MockWorkerNode("w-wan", tmp_path / "cas_wan", ip_address="8.8.8.8", coordinator=mock_coordinator)
    w_req = MockWorkerNode("w-req", tmp_path / "cas_req", ip_address="192.168.1.10", coordinator=mock_coordinator)

    w_lan.cas.store_bytes(data)
    w_wan.cas.store_bytes(data)
    mock_coordinator.registry.update_inventory("w-lan", added={h})
    mock_coordinator.registry.update_inventory("w-wan", added={h})

    req = LocateAssetsRequest(requester_worker_id="w-req", missing_hashes=[h])
    resp = mock_coordinator.handle_locate(req, requester_ip="192.168.1.10")
    candidates = resp.locations[h]
    assert len(candidates) == 2
    assert candidates[0].worker_id == "w-lan"
    assert candidates[0].locality_tier == "subnet"
    assert candidates[1].worker_id == "w-wan"
    assert candidates[1].locality_tier == "wan"


def test_scenario_9_worker_node_crash_and_dynamic_recovery(tmp_path: Path, mock_coordinator: MockCoordinator, payload_100b: Tuple[bytes, str]):
    """Scenario 9: Worker node crashes, gets pruned, recovers, re-registers, and resumes serving."""
    data, h = payload_100b
    w_node = MockWorkerNode("w-crash", tmp_path / "cas_cr", ip_address="192.168.1.50", coordinator=mock_coordinator)
    w_node.cas.store_bytes(data)
    mock_coordinator.registry.update_inventory("w-crash", added={h})

    # Stale timeout -> eviction
    mock_coordinator.registry.prune_stale_workers(current_time=time.time() + 100.0)
    assert len(mock_coordinator.registry.locate_asset(h)) == 0

    # Node recovers and re-registers
    mock_coordinator.handle_register(w_node.info)
    assert len(mock_coordinator.registry.locate_asset(h)) == 1

    # Client can now sync
    client = MockWorkerNode("w-client", tmp_path / "cas_cl", coordinator=mock_coordinator)
    res = client.sync_missing_assets([h], {"w-crash": w_node})
    assert res[h] is True
    assert client.cas.has_asset(h)


def test_scenario_10_complete_render_farm_end_to_end_pipeline(tmp_path: Path, mock_coordinator: MockCoordinator):
    """Scenario 10: Complete render farm scene sync pipeline from scene models to CAS verification."""
    coordinator = mock_coordinator
    worker_a = MockWorkerNode("worker-a", tmp_path / "cas_a", ip_address="192.168.1.101", coordinator=coordinator)
    worker_b = MockWorkerNode("worker-b", tmp_path / "cas_b", ip_address="192.168.1.102", coordinator=coordinator)

    # 1. Simulate scene assets
    scene_assets = {
        "tex_albedo": b"albedo_texture_rgba_" * 125,
        "tex_roughness": b"roughness_greyscale_" * 125,
        "mesh_geom": b"mesh_geom_triangles_" * 125,
        "hdri_env": b"hdri_env_lighting__" * 125,
    }

    # 2. Worker A holds albedo and roughness; Worker B holds mesh and hdri
    worker_a.cas.store_bytes(scene_assets["tex_albedo"])
    worker_a.cas.store_bytes(scene_assets["tex_roughness"])
    h_alb = hashlib.sha256(scene_assets["tex_albedo"]).hexdigest()
    h_rgh = hashlib.sha256(scene_assets["tex_roughness"]).hexdigest()
    coordinator.registry.update_inventory("worker-a", added={h_alb, h_rgh})

    worker_b.cas.store_bytes(scene_assets["mesh_geom"])
    worker_b.cas.store_bytes(scene_assets["hdri_env"])
    h_msh = hashlib.sha256(scene_assets["mesh_geom"]).hexdigest()
    h_hdr = hashlib.sha256(scene_assets["hdri_env"]).hexdigest()
    coordinator.registry.update_inventory("worker-b", added={h_msh, h_hdr})

    all_scene_hashes = [h_alb, h_rgh, h_msh, h_hdr]

    # 3. Worker A assigned render task -> needs full set of 4 assets
    missing_for_a = worker_a.calculate_missing_set(all_scene_hashes)
    assert missing_for_a == {h_msh, h_hdr}

    # 4. Worker A syncs missing assets from Worker B
    res_a = worker_a.sync_missing_assets(all_scene_hashes, {"worker-b": worker_b})
    assert all(res_a.values())

    # 5. Verify Worker A is now 100% ready for render
    assert worker_a.calculate_missing_set(all_scene_hashes) == set()
    assert worker_a.metrics.local_cache_hits == 2
    assert worker_a.metrics.network_transferred_assets == 2
    assert worker_a.metrics.byte_hit_ratio >= 0.5
    assert worker_a.metrics.network_savings_percent >= 50.0
