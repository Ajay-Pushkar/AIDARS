"""Phase 4B.2 / 4B.3: Master-as-asset-source and end-to-end asset delivery.

Exercises the real existing components -- CoordinatorService, WorkerRegistry,
DistributedWorker (as both a normal worker and a CAS-only "Master asset
source"), WorkloadOrchestrator, PlacementEngine, and DistributedClient's real
HTTP transfer path -- rather than mocking the lifecycle. No new CAS, no new
transfer protocol, no new registry: this proves the existing pipeline already
supports a registered node that serves/receives assets but is never selected
for compute placement.
"""
from __future__ import annotations
import time

import asyncio
import hashlib
from pathlib import Path

import httpx
import pytest
from httpx import ASGITransport

from aidars.core.assets.manager import AssetManager
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.client import DistributedClient
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    WorkerInfo,
    WorkerResourceProfile,
    WorkerRegistrationPayload,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.server import create_worker_app
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRegistry


# ============================================================================
# Section A: can_execute_workloads plumbing through registration
# ============================================================================


def test_worker_registration_payload_defaults_can_execute_workloads_true():
    payload = WorkerRegistrationPayload(
        worker_id="w-1", endpoint_url="http://127.0.0.1:8001", ip_address="127.0.0.1", port=8001,
    )
    assert payload.can_execute_workloads is True


def test_register_worker_sync_propagates_can_execute_workloads_false():
    coord = CoordinatorService(registry=WorkerRegistry())
    payload = WorkerRegistrationPayload(
        worker_id="w-master", endpoint_url="http://127.0.0.1:9000", ip_address="127.0.0.1", port=9000,
        can_execute_workloads=False,
    )
    coord.register_worker_sync(payload)

    info = coord.registry.get_worker("w-master")
    assert info is not None
    assert info.can_execute_workloads is False


# ============================================================================
# Section B: DistributedWorker reused, unmodified, as a Master asset source
# ============================================================================


@pytest.mark.asyncio
async def test_distributed_worker_master_flag_reaches_registration(tmp_path: Path):
    """DistributedWorker(can_execute_workloads=False) -- the same class real
    compute workers use -- registers itself with that flag intact, using the
    existing registration mechanism (no new daemon class)."""
    coord = CoordinatorService(registry=WorkerRegistry())
    transport = ASGITransport(app=coord.app)
    http_client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    master = DistributedWorker(
        worker_id="master-cas",
        cas_dir=tmp_path / "master_cas",
        ip_address="127.0.0.1",
        port=9500,
        coordinator_url="http://coordinator",
        http_client=http_client,
        can_execute_workloads=False,
    )
    assert master.can_execute_workloads is False

    await master.register()

    info = coord.registry.get_worker("master-cas")
    assert info is not None
    assert info.can_execute_workloads is False

    await http_client.aclose()


# ============================================================================
# Section C: inventory synchronization -- ingest -> heartbeat -> registry
# ============================================================================


@pytest.mark.asyncio
async def test_master_heartbeat_synchronizes_newly_ingested_asset(tmp_path: Path):
    """A hash present in the Master's CAS but never reported must not count
    as done. This proves the full lifecycle: ingest into CAS after
    registration -> heartbeat reports the delta -> WorkerRegistry can locate
    it -- using the existing HeartbeatPayload.inventory_delta_added field and
    WorkerRegistry.record_heartbeat(), not a manual registry write."""
    coord = CoordinatorService(registry=WorkerRegistry())
    transport = ASGITransport(app=coord.app)
    http_client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    master_cas = LocalCASAdapter(cas_dir=tmp_path / "master_cas")
    master = DistributedWorker(
        worker_id="master-cas",
        cas_adapter=master_cas,
        ip_address="127.0.0.1",
        port=9501,
        coordinator_url="http://coordinator",
        http_client=http_client,
        can_execute_workloads=False,
    )

    # Register with an empty CAS -- the asset does not exist yet.
    await master.register()
    data = b"phase-4b-inventory-sync-asset-bytes"
    expected_hash = hashlib.sha256(data).hexdigest()
    assert coord.registry.get_workers_for_hash(expected_hash) == set()

    # Ingest happens after registration (e.g. AssetManager.upload_asset_file
    # having just run against this same CAS).
    manager = AssetManager(master_cas)
    stored_hash = await manager.upload_assets({"late-asset": data})
    assert expected_hash in stored_hash

    # Not yet visible to the registry -- no heartbeat has reported it.
    assert coord.registry.get_workers_for_hash(expected_hash) == set()

    await master.send_heartbeat()

    assert coord.registry.get_workers_for_hash(expected_hash) == {"master-cas"}

    await http_client.aclose()


# ============================================================================
# Section D: compute safety -- Master excluded, normal worker unaffected
# ============================================================================


@pytest.mark.asyncio
async def test_master_excluded_from_placement_normal_worker_selected():
    """The Master asset source, even with vastly more advertised resources
    than a real worker, must never be selected for compute; the real worker
    must be placed exactly as it would without the Master registered."""
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    orchestrator = WorkloadOrchestrator(registry=registry, workload_registry=workload_registry)

    registry.register_worker(WorkerInfo(
        worker_id="w-real", endpoint_url="http://real-worker", ip_address="127.0.0.1", port=8002,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
        resource_profile=WorkerResourceProfile(
            timestamp_utc=time.time(), worker_id="w-real", endpoint_url="http://real-worker",
            ip_address="127.0.0.1", cpu_cores_total=4, cpu_utilization_percent=0.0,
            ram_total_bytes=4096, ram_available_bytes=4096,
        ),
    ))
    registry.register_worker(WorkerInfo(
        worker_id="w-master", endpoint_url="http://master", ip_address="127.0.0.1", port=9000,
        status=WorkerStatus.ACTIVE, capacity_bytes=999_000_000_000, used_bytes=0,
        can_execute_workloads=False,
    ))

    dispatched_to = []

    def handler(request: httpx.Request) -> httpx.Response:
        dispatched_to.append(str(request.url))
        if "real-worker" not in str(request.url):
            raise AssertionError(f"Master must never receive a dispatch: {request.url}")
        result = WorkloadExecutionResult(
            workload_id="task-e2e-placement",
            worker_id="w-real",
            success=True,
            output_asset_hashes=set(),
            execution_duration_seconds=0.01,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    spec = WorkloadSpec(workload_id="task-e2e-placement", task_type="test", min_ram_bytes=1024)
    workload_registry.add_workload(spec)
    await orchestrator._process_workload("task-e2e-placement")

    record = workload_registry.get_workload("task-e2e-placement")
    assert record.placement_decision is not None
    assert record.placement_decision.selected_worker_id == "w-real"
    assert record.state.value == "completed"
    assert dispatched_to == ["http://real-worker/api/v1/workloads/execute"]


def test_normal_worker_placement_unaffected_by_can_execute_workloads_field():
    """Regression: a normal WorkerInfo (no can_execute_workloads override)
    still participates in placement exactly as before Phase 4B.0/4B.2."""
    engine = PlacementEngine()
    from aidars.distributed.models import WorkerResourceProfile

    spec = WorkloadSpec(workload_id="task-regress", task_type="test", min_ram_bytes=1024)
    profile = WorkerResourceProfile(timestamp_utc=time.time(),
        worker_id="w-normal", endpoint_url="http://127.0.0.1:8001", ip_address="127.0.0.1",
        cpu_cores_total=4, cpu_utilization_percent=10.0,
        ram_total_bytes=4096, ram_available_bytes=4096,
    )
    decision = engine.evaluate(spec, [profile])
    assert decision is not None
    assert decision.selected_worker_id == "w-normal"


# ============================================================================
# Section E (4B.3): full real asset delivery, Master CAS -> worker CAS
# ============================================================================


@pytest.mark.asyncio
async def test_end_to_end_master_asset_delivered_to_worker(tmp_path: Path):
    """The complete real path: a physical asset resolved the way M4 resolves
    it is ingested into a Master CAS, the Master registers and advertises it,
    the coordinator locates the Master as a candidate source, and a worker's
    real sync_assets()/DistributedClient download pipeline fetches the exact
    bytes into its own CAS -- all through existing, unmodified transfer code
    (transfer_asset_with_failover / WorkerServer's streaming route)."""
    coord = CoordinatorService(registry=WorkerRegistry())

    # 1-3: a real M4-resolved asset, ingested into Master CAS via AssetManager.
    master_cas = LocalCASAdapter(cas_dir=tmp_path / "master_cas")
    asset_bytes = b"phase-4b3-end-to-end-real-asset-payload"
    expected_hash = hashlib.sha256(asset_bytes).hexdigest()
    source_file = tmp_path / "resolved_asset.bin"
    source_file.write_bytes(asset_bytes)

    manager = AssetManager(master_cas)
    stored_hash = manager.upload_asset_file(source_file, expected_hash)
    assert stored_hash == expected_hash
    assert master_cas.has_asset(expected_hash) is True

    # Master's own data-plane server, exposing the existing streaming route.
    master_app = create_worker_app(
        cas_adapter=master_cas, worker_id="master-cas", endpoint_url="http://master-cas",
    )

    # 4-6: Master registers (can_execute_workloads=False) and advertises the hash.
    coord.register_worker_sync(WorkerRegistrationPayload(
        worker_id="master-cas", endpoint_url="http://master-cas", ip_address="127.0.0.1", port=9000,
        inventory_hashes={expected_hash}, can_execute_workloads=False,
    ))
    assert coord.registry.get_workers_for_hash(expected_hash) == {"master-cas"}

    locate_req_hashes = [expected_hash]
    from aidars.distributed.models import LocateAssetsRequest
    locate_resp = coord.locate_assets_sync(LocateAssetsRequest(
        requester_worker_id="w-consumer", missing_hashes=locate_req_hashes,
    ))
    candidates = locate_resp.locations.get(expected_hash, [])
    assert any(c.worker_id == "master-cas" for c in candidates)

    # 7-9: a real worker, with an empty CAS, fetches the missing asset
    # through the existing DistributedClient.sync_assets() pipeline.
    async def cluster_app(scope, receive, send):
        headers = dict(scope.get("headers", []))
        host = headers.get(b"host", b"").decode()
        if "master-cas" in host:
            await master_app(scope, receive, send)
        else:
            await coord.app(scope, receive, send)

    transport = ASGITransport(app=cluster_app)
    mock_http = httpx.AsyncClient(transport=transport)

    worker_cas = LocalCASAdapter(cas_dir=tmp_path / "worker_cas")
    assert worker_cas.has_asset(expected_hash) is False

    async with DistributedClient(
        cas_adapter=worker_cas, coordinator_url="http://coordinator", http_client=mock_http,
    ) as client:
        results = await client.sync_assets([expected_hash])

    # 9-10: bytes physically present in the worker's own CAS, matching hash.
    assert results[expected_hash].success is True
    assert results[expected_hash].source_worker_id == "master-cas"
    assert worker_cas.has_asset(expected_hash) is True
    with worker_cas.open_asset_stream(expected_hash) as stream:
        assert stream.read() == asset_bytes
