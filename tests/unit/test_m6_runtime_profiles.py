"""M6 runtime resource telemetry from workers through placement."""
from __future__ import annotations

import time
from types import SimpleNamespace

import httpx
import pytest
from httpx import ASGITransport

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    HeartbeatPayload,
    WorkerInfo,
    WorkerResourceProfile,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.resources import WorkerResourceMonitor
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.workload_registry import WorkloadState


def _profile(worker_id="w-1", **overrides):
    values = dict(
        worker_id=worker_id,
        endpoint_url=f"http://{worker_id}",
        ip_address="127.0.0.1",
        cpu_cores_total=8,
        cpu_utilization_percent=0.0,
        ram_total_bytes=16_000,
        ram_available_bytes=12_000,
        gpu_available=True,
        gpu_device_name="GPU-X",
        vram_total_bytes=8_000,
        vram_available_bytes=6_000,
        timestamp_utc=time.time(),
    )
    values.update(overrides)
    return WorkerResourceProfile(**values)


def test_resource_monitor_reports_cpu_ram_and_gpu(monkeypatch):
    import aidars.distributed.resources as resources

    fake_nvml = SimpleNamespace(
        nvmlInit=lambda: None,
        nvmlDeviceGetHandleByIndex=lambda index: "gpu0",
        nvmlDeviceGetName=lambda handle: b"Test NVIDIA",
        nvmlDeviceGetMemoryInfo=lambda handle: SimpleNamespace(total=8192, free=4096),
    )
    monkeypatch.setattr(resources, "_HAS_PYNVML", True)
    monkeypatch.setattr(resources, "pynvml", fake_nvml, raising=False)
    monkeypatch.setattr(resources.psutil, "cpu_count", lambda logical=True: 12)
    monkeypatch.setattr(resources.psutil, "cpu_percent", lambda interval=None: 25.0)
    monkeypatch.setattr(
        resources.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(total=64_000, available=48_000),
    )

    profile = WorkerResourceMonitor("w-1", "http://w-1", "127.0.0.1").get_profile()
    assert profile.cpu_cores_total == 12
    assert profile.cpu_utilization_percent == 25.0
    assert profile.ram_total_bytes == 64_000
    assert profile.ram_available_bytes == 48_000
    assert profile.gpu_available is True
    assert profile.gpu_device_name == "Test NVIDIA"
    assert profile.vram_total_bytes == 8192
    assert profile.vram_available_bytes == 4096
    assert profile.timestamp_utc is not None


def test_resource_monitor_handles_missing_nvml(monkeypatch):
    import aidars.distributed.resources as resources

    monkeypatch.setattr(resources, "_HAS_PYNVML", False)
    monkeypatch.setattr(resources.psutil, "cpu_count", lambda logical=True: 4)
    monkeypatch.setattr(resources.psutil, "cpu_percent", lambda interval=None: 0.0)
    monkeypatch.setattr(
        resources.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(total=32_000, available=20_000),
    )
    profile = WorkerResourceMonitor("w-1", "http://w-1", "127.0.0.1").get_profile()
    assert profile.gpu_available is False
    assert profile.gpu_device_name is None
    assert profile.vram_total_bytes == profile.vram_available_bytes == 0


def test_resource_monitor_handles_nvml_initialization_failure(monkeypatch):
    import aidars.distributed.resources as resources

    class BrokenNvml:
        @staticmethod
        def nvmlInit():
            raise RuntimeError("driver unavailable")

    monkeypatch.setattr(resources, "_HAS_PYNVML", True)
    monkeypatch.setattr(resources, "pynvml", BrokenNvml, raising=False)
    monkeypatch.setattr(resources.psutil, "cpu_count", lambda logical=True: 2)
    monkeypatch.setattr(resources.psutil, "cpu_percent", lambda interval=None: 0.0)
    monkeypatch.setattr(
        resources.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(total=8_000, available=4_000),
    )
    profile = WorkerResourceMonitor("w-1", "http://w-1", "127.0.0.1").get_profile()
    assert profile.gpu_available is False
    assert profile.vram_available_bytes == 0


def test_placement_enforces_hard_resource_requirements():
    from aidars.distributed.placement import PlacementEngine

    engine = PlacementEngine()
    spec = WorkloadSpec(
        workload_id="requirements",
        task_type="test",
        min_cpu_cores=4,
        min_ram_bytes=8_000,
        requires_gpu=True,
        min_vram_bytes=4_000,
    )
    base = _profile()
    assert engine.evaluate(spec, [base]) is not None

    rejected = [
        base.model_copy(update={"cpu_cores_total": 3}),
        base.model_copy(update={"ram_available_bytes": 7_999}),
        base.model_copy(update={"gpu_available": False}),
        base.model_copy(update={"vram_available_bytes": 3_999}),
        base.model_copy(update={"active_workload_count": 10}),
        base.model_copy(update={"status": WorkerStatus.DEGRADED}),
    ]
    assert all(engine.evaluate(spec, [candidate]) is None for candidate in rejected)


def test_placement_requires_fresh_timestamp_and_accepts_exact_ten_second_boundary(monkeypatch):
    import aidars.distributed.placement as placement

    now = 10_000.0
    monkeypatch.setattr(placement.time, "time", lambda: now)
    engine = placement.PlacementEngine()
    spec = WorkloadSpec(workload_id="freshness", task_type="test", min_ram_bytes=1)

    assert engine.evaluate(spec, [_profile(timestamp_utc=now - 10.0)]) is not None
    assert engine.evaluate(spec, [_profile(timestamp_utc=now - 10.0001)]) is None
    assert engine.evaluate(spec, [_profile(timestamp_utc=None)]) is None


def test_cpu_headroom_ranks_only_after_minimum_cpu_is_satisfied():
    from aidars.distributed.placement import PlacementEngine

    engine = PlacementEngine()
    spec = WorkloadSpec(workload_id="cpu-rank", task_type="test", min_cpu_cores=2, min_ram_bytes=1)
    low_headroom = _profile("low", cpu_cores_total=4, cpu_utilization_percent=40.0)
    high_headroom = _profile("high", cpu_cores_total=8, cpu_utilization_percent=0.0)
    decision = engine.evaluate(spec, [low_headroom, high_headroom])
    assert decision is not None
    assert decision.selected_worker_id == "high"


def test_build_profiles_merges_telemetry_and_attempt_count():
    service = CoordinatorService()
    service.attempt_registry.create_attempt("assigned")
    service.attempt_registry.mark_assigned("assigned#1", "w-1")
    service.attempt_registry.create_attempt("running")
    service.attempt_registry.mark_assigned("running#1", "w-1")
    service.attempt_registry.mark_running("running#1")
    service.attempt_registry.create_attempt("queued")

    info = WorkerInfo(
        worker_id="w-1",
        endpoint_url="http://w-1",
        ip_address="127.0.0.1",
        port=8000,
        capacity_bytes=999_999,
        used_bytes=999_998,
        resource_profile=_profile("untrusted-id", endpoint_url="http://wrong", ram_total_bytes=16_000, ram_available_bytes=12_000),
    )
    profiles = service.orchestrator._build_worker_profiles([info])
    assert len(profiles) == 1
    result = profiles[0]
    assert result.worker_id == "w-1"
    assert result.endpoint_url == "http://w-1"
    assert result.cpu_cores_total == 8
    assert result.ram_total_bytes == 16_000
    assert result.ram_available_bytes == 12_000
    assert result.gpu_available is True
    assert result.vram_available_bytes == 6_000
    assert result.timestamp_utc == info.resource_profile.timestamp_utc
    assert result.active_workload_count == 2
    assert result.ram_total_bytes != info.capacity_bytes


@pytest.mark.asyncio
async def test_registration_heartbeat_profile_placement_and_cas_only_worker(monkeypatch, tmp_path):
    monkeypatch.setenv("AIDAR_INSECURE_MODE", "1")
    service = CoordinatorService()
    worker = DistributedWorker(
        worker_id="worker-live",
        cas_adapter=LocalCASAdapter(cas_dir=tmp_path / "cas"),
        ip_address="127.0.0.1",
        port=8001,
        coordinator_url="http://coordinator",
    )
    snapshots = iter([
        _profile("worker-live", cpu_cores_total=6, ram_total_bytes=40_000, ram_available_bytes=30_000),
        _profile("worker-live", cpu_cores_total=6, ram_total_bytes=40_000, ram_available_bytes=28_000),
    ])
    monkeypatch.setattr(worker.resource_monitor, "get_profile", lambda **kwargs: next(snapshots))
    worker.client.http_client = httpx.AsyncClient(
        transport=ASGITransport(app=service.app), base_url="http://coordinator"
    )

    await worker.register()
    registered = service.registry.get_worker("worker-live")
    assert registered.resource_profile.ram_available_bytes == 30_000
    first_received_at = registered.resource_profile.timestamp_utc

    await worker.send_heartbeat()
    refreshed = service.registry.get_worker("worker-live")
    assert refreshed.resource_profile.ram_available_bytes == 28_000
    assert refreshed.resource_profile.timestamp_utc >= first_received_at

    # Register a second worker whose telemetry fails the GPU requirement.
    bad_worker = DistributedWorker(
        worker_id="worker-bad",
        cas_adapter=LocalCASAdapter(cas_dir=tmp_path / "cas-bad"),
        ip_address="127.0.0.2",
        port=8002,
        coordinator_url="http://coordinator",
    )
    bad_snapshots = iter([
        _profile("worker-bad", gpu_available=False, vram_available_bytes=0),
        _profile("worker-bad", gpu_available=False, vram_available_bytes=0),
    ])
    monkeypatch.setattr(bad_worker.resource_monitor, "get_profile", lambda **kwargs: next(bad_snapshots))
    bad_worker.client.http_client = worker.client.http_client
    await bad_worker.register()
    await bad_worker.send_heartbeat()

    def dispatch(request):
        selected = "worker-live" if request.url.port == 8001 else "worker-bad"
        result = WorkloadExecutionResult(
            workload_id="gpu-task", worker_id=selected, success=True,
            output_asset_hashes=set(), execution_duration_seconds=0.1,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    service.orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(dispatch))
    spec = WorkloadSpec(
        workload_id="gpu-task", task_type="test", min_cpu_cores=4,
        min_ram_bytes=10_000, requires_gpu=True, min_vram_bytes=2_000,
    )
    service.workload_registry.add_workload(spec)
    await service.orchestrator._process_workload(spec.workload_id)
    decision = service.workload_registry.get_workload(spec.workload_id).placement_decision
    assert decision is not None
    assert decision.selected_worker_id == "worker-live"

    # A worker without compute telemetry remains registered and can still
    # serve a CAS hash through the existing inverted asset index.
    cas_hash = "a" * 64
    service.registry.register_worker(WorkerInfo(
        worker_id="cas-only",
        endpoint_url="http://cas-only",
        ip_address="127.0.0.3",
        port=8003,
        inventory_hashes={cas_hash},
        can_execute_workloads=False,
    ))
    assert "cas-only" in service.registry.get_workers_for_hash(cas_hash)
    assert service.orchestrator._build_worker_profiles(
        [service.registry.get_worker("cas-only")]
    ) == []


def test_worker_profile_round_trips_and_restored_worker_stays_offline(tmp_path):
    db_path = tmp_path / "coordinator.db"
    store = CoordinatorStateStore(db_path)
    original = WorkerInfo(
        worker_id="persisted",
        endpoint_url="http://persisted",
        ip_address="127.0.0.1",
        port=8000,
        resource_profile=_profile("persisted", timestamp_utc=1234.5),
    )
    store.save_worker(original)
    restored = store.load_workers()[0]
    assert restored.resource_profile == original.resource_profile

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()
    live = service.registry.get_worker("persisted")
    assert live.status == WorkerStatus.OFFLINE
    service.registry.record_heartbeat(
        "persisted",
        HeartbeatPayload(worker_id="persisted", resource_profile=_profile("persisted")),
    )
    assert service.registry.get_worker("persisted").status == WorkerStatus.ACTIVE


def test_missing_heartbeat_profile_clears_compute_eligibility():
    service = CoordinatorService()
    service.registry.register_worker(WorkerInfo(
        worker_id="w-1",
        endpoint_url="http://w-1",
        ip_address="127.0.0.1",
        port=8000,
        resource_profile=_profile(),
    ))
    service.registry.record_heartbeat("w-1", HeartbeatPayload(worker_id="w-1"))
    info = service.registry.get_worker("w-1")
    assert info.status == WorkerStatus.ACTIVE
    assert info.resource_profile is None
    assert service.orchestrator._build_worker_profiles([info]) == []


@pytest.mark.asyncio
async def test_concurrent_dispatches_do_not_exceed_worker_workload_limit():
    import asyncio

    service = CoordinatorService()
    service.registry.register_worker(WorkerInfo(
        worker_id="limited",
        endpoint_url="http://limited:8000",
        ip_address="127.0.0.1",
        port=8000,
        resource_profile=_profile("limited", max_concurrent_workloads=1),
    ))

    class Response:
        def __init__(self, workload_id):
            self.workload_id = workload_id

        def raise_for_status(self):
            pass

        def json(self):
            return WorkloadExecutionResult(
                workload_id=self.workload_id,
                worker_id="limited",
                success=True,
                output_asset_hashes=set(),
                execution_duration_seconds=0.01,
            ).model_dump(mode="json")

    class SlowClient:
        async def post(self, url, json, timeout):
            await asyncio.sleep(0.03)
            return Response(json["workload_id"])

    service.orchestrator.http_client = SlowClient()
    for workload_id in ("first", "second"):
        service.workload_registry.add_workload(
            WorkloadSpec(workload_id=workload_id, task_type="test", min_ram_bytes=1)
        )
    await asyncio.gather(
        service.orchestrator._process_workload("first"),
        service.orchestrator._process_workload("second"),
    )
    states = {
        service.workload_registry.get_workload(wid).state
        for wid in ("first", "second")
    }
    assert states == {WorkloadState.COMPLETED, WorkloadState.SUBMITTED}
