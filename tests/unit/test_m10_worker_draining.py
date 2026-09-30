"""M10.8/M10.19: worker draining.

Audits the pre-existing drain_worker() bug fix (record.placement ->
record.placement_decision), confirms PlacementEngine already excludes
DRAINING workers from NEW placement (VALIDATE ONLY -- no change needed
there), and exercises the new DRAINING -> ACTIVE revival path added to
CoordinatorService._evaluate_cluster_health_loop.
"""
from __future__ import annotations
import time

import httpx
import pytest

from aidars.distributed.models import (
    WorkerInfo,
    WorkerResourceProfile,
    WorkerStatus,
    WorkloadSpec,
)
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


# ============================================================================
# drain_worker() bug fix
# ============================================================================


@pytest.mark.asyncio
async def test_drain_worker_finds_and_triggers_checkpoint_for_placed_workloads():
    """Pre-M10, this always no-op'd: WorkloadRecord has no `.placement`
    attribute (it's `.placement_decision`), so the old filter raised
    AttributeError on every record it examined and drain_worker()'s
    active_workloads list was always empty. Confirms the fix actually
    finds the workload and calls its worker's checkpoint endpoint."""
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    orchestrator = WorkloadOrchestrator(registry=registry, workload_registry=workload_registry)

    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.DRAINING, capacity_bytes=4096, used_bytes=0,
    ))

    from aidars.distributed.models import PlacementDecision
    spec = WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024)
    record = workload_registry.add_workload(spec)
    workload_registry.set_placement("task-1", PlacementDecision(
        workload_id="task-1", selected_worker_id="w-1", placement_score=1.0,
        score_breakdown={}, missing_assets_on_worker=set(), execution_tier="lan",
    ))
    workload_registry.update_state("task-1", WorkloadState.PLACED)

    checkpoint_calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        checkpoint_calls.append(str(request.url))
        return httpx.Response(200, json={"status": "checkpointing"})

    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    await orchestrator.drain_worker("w-1")

    assert len(checkpoint_calls) == 1
    assert "task-1" in checkpoint_calls[0]
    assert "checkpoint" in checkpoint_calls[0]


@pytest.mark.asyncio
async def test_drain_worker_ignores_workloads_placed_on_other_workers():
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    orchestrator = WorkloadOrchestrator(registry=registry, workload_registry=workload_registry)

    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.DRAINING, capacity_bytes=4096, used_bytes=0,
    ))

    from aidars.distributed.models import PlacementDecision
    spec = WorkloadSpec(workload_id="task-elsewhere", task_type="test", min_ram_bytes=1024)
    workload_registry.add_workload(spec)
    workload_registry.set_placement("task-elsewhere", PlacementDecision(
        workload_id="task-elsewhere", selected_worker_id="w-OTHER", placement_score=1.0,
        score_breakdown={}, missing_assets_on_worker=set(), execution_tier="lan",
    ))
    workload_registry.update_state("task-elsewhere", WorkloadState.PLACED)

    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json={"status": "checkpointing"})

    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await orchestrator.drain_worker("w-1")

    assert calls == []


@pytest.mark.asyncio
async def test_drain_worker_no_op_when_worker_has_no_active_workloads():
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    orchestrator = WorkloadOrchestrator(registry=registry, workload_registry=workload_registry)
    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.DRAINING, capacity_bytes=4096, used_bytes=0,
    ))
    # Must not raise even with no workload_registry entries at all.
    await orchestrator.drain_worker("w-1")


# ============================================================================
# Placement excludes DRAINING workers (VALIDATE ONLY -- pre-existing)
# ============================================================================


def test_placement_engine_excludes_draining_workers():
    engine = PlacementEngine()
    spec = WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024, min_cpu_cores=1)

    draining_profile = WorkerResourceProfile(timestamp_utc=time.time(),
        worker_id="w-draining", endpoint_url="http://w1", ip_address="127.0.0.1",
        cpu_cores_total=8, cpu_utilization_percent=0.0,
        ram_total_bytes=16_000_000_000, ram_available_bytes=16_000_000_000,
        status=WorkerStatus.DRAINING,
    )
    active_profile = WorkerResourceProfile(timestamp_utc=time.time(),
        worker_id="w-active", endpoint_url="http://w2", ip_address="127.0.0.2",
        cpu_cores_total=8, cpu_utilization_percent=0.0,
        ram_total_bytes=16_000_000_000, ram_available_bytes=16_000_000_000,
        status=WorkerStatus.ACTIVE,
    )

    decision = engine.evaluate(spec, [draining_profile, active_profile])
    assert decision is not None
    assert decision.selected_worker_id == "w-active"


def test_placement_engine_returns_none_when_only_draining_workers_exist():
    engine = PlacementEngine()
    spec = WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024)
    draining_profile = WorkerResourceProfile(timestamp_utc=time.time(),
        worker_id="w-draining", endpoint_url="http://w1", ip_address="127.0.0.1",
        cpu_cores_total=8, cpu_utilization_percent=0.0,
        ram_total_bytes=16_000_000_000, ram_available_bytes=16_000_000_000,
        status=WorkerStatus.DRAINING,
    )
    assert engine.evaluate(spec, [draining_profile]) is None


# ============================================================================
# DRAINING -> ACTIVE revival (new in M10.8)
# ============================================================================


def test_no_existing_code_path_revives_draining_without_the_new_health_loop_logic():
    """Documents the gap this fixes: WorkerRegistry.record_heartbeat()
    only promotes OFFLINE -> ACTIVE, never DRAINING -> ACTIVE."""
    registry = WorkerRegistry()
    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://w1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.DRAINING, capacity_bytes=4096, used_bytes=0,
    ))
    registry.record_heartbeat("w-1")
    assert registry.get_worker("w-1").status == WorkerStatus.DRAINING


@pytest.mark.asyncio
async def test_coordinator_revives_draining_worker_when_m7_behavior_recovers_to_stable():
    from aidars.distributed.coordinator import CoordinatorService
    from aidars.m7.telemetry import EMA, WorkerTemporalState

    service = CoordinatorService()
    service.registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://w1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.DRAINING, capacity_bytes=4096, used_bytes=0,
    ))

    # Seed M7 memory with a worker state that BehaviorInferencer will
    # classify as STABLE (low utilization, low failure rate, low latency).
    service.m7_memory.workers["w-1"] = WorkerTemporalState(
        worker_id="w-1",
        cpu_utilization_ema=EMA(value=0.1, alpha=0.3, initialized=True),
        ram_utilization_ema=EMA(value=0.1, alpha=0.3, initialized=True),
        failure_rate_ema=EMA(value=0.0, alpha=0.3, initialized=True),
        latency_ema=EMA(value=1.0, alpha=0.3, initialized=True),
    )

    service._running = True
    # Run exactly one health-loop pass body without the sleep -- call the
    # loop's internal evaluation logic once by invoking the coroutine and
    # cancelling it after one iteration via a very short-lived task.
    import asyncio

    async def _one_pass():
        service.eviction_interval_seconds = 0.001
        task = asyncio.create_task(service._evaluate_cluster_health_loop())
        await asyncio.sleep(0.05)
        service._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await _one_pass()

    assert service.registry.get_worker("w-1").status == WorkerStatus.ACTIVE
