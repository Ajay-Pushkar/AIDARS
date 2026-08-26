"""Unit tests for CoordinatorService and FastAPI control plane REST endpoints."""
from __future__ import annotations

import asyncio
import time
from typing import Dict, List

import pytest
from fastapi.testclient import TestClient

from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    HeartbeatPayload,
    LocateAssetsRequest,
    PingRequest,
    WorkerCapabilities,
    WorkerInfo,
    WorkerMetrics,
    WorkerRegistrationPayload,
    WorkerStatus,
)
from aidars.distributed.prioritizer import CandidatePrioritizer, LatencyTracker
from aidars.distributed.registry import WorkerHealthStatus, WorkerRegistry

HASH_1 = "1" * 64
HASH_2 = "2" * 64
HASH_3 = "3" * 64
HASH_4 = "4" * 64


@pytest.fixture
def coordinator_service() -> CoordinatorService:
    service = CoordinatorService(
        coordinator_id="test-coord-01",
        heartbeat_interval_seconds=2.0,
        heartbeat_timeout_seconds=5.0,
        eviction_interval_seconds=1.0,
    )
    return service


@pytest.fixture
def client(coordinator_service: CoordinatorService) -> TestClient:
    return TestClient(coordinator_service.app)


def make_reg_payload(
    worker_id: str,
    ip: str = "192.168.1.10",
    port: int = 8000,
    inventory: set | None = None,
    capacity: int = 50000000,
    used: int = 10000000,
) -> WorkerRegistrationPayload:
    return WorkerRegistrationPayload(
        worker_id=worker_id,
        endpoint_url=f"http://{ip}:{port}",
        ip_address=ip,
        port=port,
        capacity_bytes=capacity,
        used_bytes=used,
        inventory_hashes=inventory or set(),
    )


# ============================================================================
# Worker Registration Endpoint Tests
# ============================================================================


def test_register_worker_endpoint(client: TestClient, coordinator_service: CoordinatorService):
    payload = make_reg_payload("node-alpha", ip="192.168.1.100", inventory={HASH_1, HASH_2})
    resp = client.post("/api/v1/workers/register", json=payload.model_dump(mode="json"))

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "registered"
    assert data["worker_id"] == "node-alpha"
    assert data["coordinator_id"] == "test-coord-01"
    assert data["acknowledged_inventory_count"] == 2

    # Verify registry state
    worker = coordinator_service.registry.get_worker("node-alpha")
    assert worker is not None
    assert worker.ip_address == "192.168.1.100"
    assert coordinator_service.registry.get_workers_for_hash(HASH_1) == {"node-alpha"}


def test_register_worker_invalid_payload(client: TestClient):
    # Invalid IP address
    bad_payload = {
        "worker_id": "bad-node",
        "endpoint_url": "http://invalid-ip:8000",
        "ip_address": "not-an-ip",
        "port": 8000,
    }
    resp = client.post("/api/v1/workers/register", json=bad_payload)
    assert resp.status_code in (400, 422)


def test_reregister_worker_updates_endpoint(client: TestClient, coordinator_service: CoordinatorService):
    payload1 = make_reg_payload("node-beta", ip="10.0.0.1", port=8001, inventory={HASH_1})
    resp1 = client.post("/api/v1/workers/register", json=payload1.model_dump(mode="json"))
    assert resp1.status_code == 200

    payload2 = make_reg_payload("node-beta", ip="10.0.0.2", port=8002, inventory={HASH_2})
    resp2 = client.post("/api/v1/workers/register", json=payload2.model_dump(mode="json"))
    assert resp2.status_code == 200

    worker = coordinator_service.registry.get_worker("node-beta")
    assert worker is not None
    assert worker.ip_address == "10.0.0.2"
    assert worker.port == 8002
    assert coordinator_service.registry.get_workers_for_hash(HASH_1) == set()
    assert coordinator_service.registry.get_workers_for_hash(HASH_2) == {"node-beta"}


# ============================================================================
# Worker Heartbeat Endpoint Tests
# ============================================================================


def test_heartbeat_registered_worker(client: TestClient, coordinator_service: CoordinatorService):
    payload = make_reg_payload("node-gamma", inventory={HASH_1})
    client.post("/api/v1/workers/register", json=payload.model_dump(mode="json"))

    hb = HeartbeatPayload(
        worker_id="node-gamma",
        active_transfers=3,
        used_bytes=15000000,
        inventory_delta_added={HASH_3},
        inventory_delta_removed={HASH_1},
    )
    resp = client.post("/api/v1/workers/node-gamma/heartbeat", json=hb.model_dump(mode="json"))
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["re_register_required"] is False

    # Check updated node state in registry
    worker = coordinator_service.registry.get_worker("node-gamma")
    assert worker is not None
    assert worker.active_transfers == 3
    assert worker.used_bytes == 15000000
    assert coordinator_service.registry.get_workers_for_hash(HASH_1) == set()
    assert coordinator_service.registry.get_workers_for_hash(HASH_3) == {"node-gamma"}


def test_heartbeat_unregistered_worker(client: TestClient):
    hb = HeartbeatPayload(worker_id="unregistered-node")
    resp = client.post("/api/v1/workers/unregistered-node/heartbeat", json=hb.model_dump(mode="json"))
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "re_register_required"
    assert data["re_register_required"] is True


def test_heartbeat_without_body(client: TestClient):
    payload = make_reg_payload("node-empty-hb")
    client.post("/api/v1/workers/register", json=payload.model_dump(mode="json"))

    resp = client.post("/api/v1/workers/node-empty-hb/heartbeat")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"


# ============================================================================
# Worker Deregistration Endpoint Tests
# ============================================================================


def test_unregister_worker_endpoint(client: TestClient, coordinator_service: CoordinatorService):
    payload = make_reg_payload("node-delta", inventory={HASH_1})
    client.post("/api/v1/workers/register", json=payload.model_dump(mode="json"))

    assert coordinator_service.registry.has_worker("node-delta") is True

    resp = client.post("/api/v1/workers/node-delta/unregister")
    assert resp.status_code == 200
    assert resp.json() == {"status": "unregistered", "worker_id": "node-delta"}
    assert coordinator_service.registry.has_worker("node-delta") is False
    assert coordinator_service.registry.get_workers_for_hash(HASH_1) == set()

    # Second unregister should return 404
    resp_404 = client.post("/api/v1/workers/node-delta/unregister")
    assert resp_404.status_code == 404


# ============================================================================
# Missing-Set Asset Location Endpoint Tests
# ============================================================================


def test_locate_assets_endpoint(client: TestClient, coordinator_service: CoordinatorService):
    # Register Node 1: Loopback (127.0.0.1) with HASH_1
    w1 = make_reg_payload("node-local", ip="127.0.0.1", port=8001, inventory={HASH_1})
    # Register Node 2: Same Subnet (192.168.1.50) with HASH_1, HASH_2
    w2 = make_reg_payload("node-subnet", ip="192.168.1.50", port=8002, inventory={HASH_1, HASH_2})
    # Register Node 3: WAN (8.8.8.8) with HASH_2
    w3 = make_reg_payload("node-wan", ip="8.8.8.8", port=8003, inventory={HASH_2})

    client.post("/api/v1/workers/register", json=w1.model_dump(mode="json"))
    client.post("/api/v1/workers/register", json=w2.model_dump(mode="json"))
    client.post("/api/v1/workers/register", json=w3.model_dump(mode="json"))

    locate_req = LocateAssetsRequest(
        requester_worker_id="requester-01",
        requester_ip="192.168.1.10",
        missing_hashes=[HASH_1, HASH_2, HASH_4],
    )

    resp = client.post("/api/v1/assets/locate", json=locate_req.model_dump(mode="json"))
    assert resp.status_code == 200
    data = resp.json()

    # HASH_1 should have candidates node-local and node-subnet (subnet ranked higher than WAN)
    assert HASH_1 in data["locations"]
    candidates_1 = data["locations"][HASH_1]
    assert len(candidates_1) == 2
    # Node subnet (192.168.1.50 vs requester 192.168.1.10) is Subnet tier (score ~5000)
    assert candidates_1[0]["worker_id"] == "node-subnet"
    assert candidates_1[0]["locality_tier"] == "subnet"

    # HASH_2 should have node-subnet and node-wan
    assert HASH_2 in data["locations"]
    candidates_2 = data["locations"][HASH_2]
    assert len(candidates_2) == 2
    assert candidates_2[0]["worker_id"] == "node-subnet"
    assert candidates_2[1]["worker_id"] == "node-wan"
    assert candidates_2[1]["locality_tier"] == "wan"

    # HASH_4 is unindexed
    assert HASH_4 in data["unresolved_hashes"]
    assert data["locations"][HASH_4] == []


def test_locate_assets_excludes_requester(client: TestClient):
    w1 = make_reg_payload("node-requester", ip="192.168.1.20", inventory={HASH_1})
    w2 = make_reg_payload("node-peer", ip="192.168.1.21", inventory={HASH_1})

    client.post("/api/v1/workers/register", json=w1.model_dump(mode="json"))
    client.post("/api/v1/workers/register", json=w2.model_dump(mode="json"))

    locate_req = LocateAssetsRequest(
        requester_worker_id="node-requester",
        missing_hashes=[HASH_1],
    )
    resp = client.post("/api/v1/assets/locate", json=locate_req.model_dump(mode="json"))
    assert resp.status_code == 200
    data = resp.json()

    candidates = data["locations"][HASH_1]
    candidate_ids = [c["worker_id"] for c in candidates]
    assert "node-peer" in candidate_ids
    assert "node-requester" not in candidate_ids


def test_locate_assets_malformed_hash(client: TestClient):
    req_payload = {
        "requester_worker_id": "requester-node",
        "missing_hashes": ["e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "malformed_hash"],
    }
    # Pydantic validation rejects malformed_hash at HTTP boundary
    resp = client.post("/api/v1/assets/locate", json=req_payload)
    assert resp.status_code in (400, 422)


# ============================================================================
# Cluster Stats & Workers List Endpoint Tests
# ============================================================================


def test_cluster_stats_and_worker_queries(client: TestClient):
    w1 = make_reg_payload("node-1", capacity=100000, used=20000, inventory={HASH_1})
    w2 = make_reg_payload("node-2", capacity=200000, used=50000, inventory={HASH_1, HASH_2})

    client.post("/api/v1/workers/register", json=w1.model_dump(mode="json"))
    client.post("/api/v1/workers/register", json=w2.model_dump(mode="json"))

    # Test /cluster/stats
    resp_stats = client.get("/api/v1/cluster/stats")
    assert resp_stats.status_code == 200
    stats = resp_stats.json()
    assert stats["total_registered_workers"] == 2
    assert stats["active_workers"] == 2
    assert stats["total_cluster_capacity_bytes"] == 300000
    assert stats["total_cluster_used_bytes"] == 70000
    assert stats["unique_cached_assets_count"] == 2
    assert stats["total_inventory_records"] == 3

    # Test /workers
    resp_workers = client.get("/api/v1/workers")
    assert resp_workers.status_code == 200
    workers = resp_workers.json()
    assert len(workers) == 2

    # Test /workers/{id}
    resp_single = client.get("/api/v1/workers/node-1")
    assert resp_single.status_code == 200
    assert resp_single.json()["worker_id"] == "node-1"

    # Test /workers/{id} 404
    resp_missing = client.get("/api/v1/workers/non-existent-worker")
    assert resp_missing.status_code == 404


# ============================================================================
# Ping / Pong Diagnostic Tests
# ============================================================================


def test_ping_pong_endpoints(client: TestClient):
    # Test POST /ping
    ping_body = PingRequest(client_timestamp_utc=12345.67, sequence_number=99)
    resp_post = client.post("/api/v1/ping", json=ping_body.model_dump(mode="json"))
    assert resp_post.status_code == 200
    pong = resp_post.json()
    assert pong["status"] == "pong"
    assert pong["client_timestamp_utc"] == 12345.67
    assert pong["sequence_number"] == 99
    assert pong["worker_id"] == "test-coord-01"

    # Test GET /ping
    resp_get = client.get("/api/v1/ping")
    assert resp_get.status_code == 200
    assert resp_get.json()["status"] == "pong"


# ============================================================================
# Eviction Lifecycle Tests
# ============================================================================


@pytest.mark.asyncio
async def test_coordinator_lifecycle_and_eviction():
    service = CoordinatorService(
        coordinator_id="coord-lifecycle",
        heartbeat_timeout_seconds=0.2,
        eviction_interval_seconds=0.1,
    )

    # Register worker
    payload = make_reg_payload("node-expiring", inventory={HASH_1})
    service.register_worker_sync(payload)
    assert service.registry.has_worker("node-expiring") is True

    # Start eviction loop
    await service.start()

    # Wait for eviction timeout
    await asyncio.sleep(0.4)

    # Worker should be evicted
    assert service.registry.has_worker("node-expiring") is False
    assert service.registry.get_workers_for_hash(HASH_1) == set()

    # Stop service
    await service.stop()
    assert service._running is False
