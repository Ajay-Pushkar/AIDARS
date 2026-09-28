"""Phase 5.0: WorkloadOrchestrator._process_workload() must persist the
complete WorkloadExecutionResult it receives from a worker via
WorkloadRegistry.set_result(), not just extract success/error_message for
a state transition and discard the rest (output_asset_hashes in
particular). Uses the real WorkerRegistry/WorkloadRegistry/
WorkloadOrchestrator/PlacementEngine, with only the HTTP dispatch mocked
via httpx.MockTransport -- the same pattern already established in
tests/unit/test_master_asset_source.py.
"""
from __future__ import annotations

import httpx
import pytest

from aidars.distributed.models import (
    WorkerInfo,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


def _make_orchestrator_with_one_worker(handler):
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    orchestrator = WorkloadOrchestrator(registry=registry, workload_registry=workload_registry)

    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
    ))
    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return orchestrator, workload_registry


@pytest.mark.asyncio
async def test_successful_execution_persists_complete_result():
    output_hashes = {"a" * 64, "b" * 64}

    def handler(request: httpx.Request) -> httpx.Response:
        result = WorkloadExecutionResult(
            workload_id="task-success",
            worker_id="w-1",
            success=True,
            output_asset_hashes=output_hashes,
            execution_duration_seconds=12.5,
            stdout_snippet="render complete",
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator, workload_registry = _make_orchestrator_with_one_worker(handler)
    spec = WorkloadSpec(workload_id="task-success", task_type="test", min_ram_bytes=1024)
    workload_registry.add_workload(spec)

    await orchestrator._process_workload("task-success")

    record = workload_registry.get_workload("task-success")
    assert record.execution_result is not None
    assert record.execution_result.success is True
    assert record.execution_result.output_asset_hashes == output_hashes
    assert record.execution_result.execution_duration_seconds == 12.5
    assert record.execution_result.stdout_snippet == "render complete"


@pytest.mark.asyncio
async def test_output_asset_hashes_retrievable_after_success():
    expected_hash = "c" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        result = WorkloadExecutionResult(
            workload_id="task-hashes",
            worker_id="w-1",
            success=True,
            output_asset_hashes={expected_hash},
            execution_duration_seconds=1.0,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator, workload_registry = _make_orchestrator_with_one_worker(handler)
    spec = WorkloadSpec(workload_id="task-hashes", task_type="test", min_ram_bytes=1024)
    workload_registry.add_workload(spec)

    await orchestrator._process_workload("task-hashes")

    record = workload_registry.get_workload("task-hashes")
    # Before this fix, execution_result was always None here -- the hash
    # was computed by the worker, returned over HTTP, and then discarded.
    assert record.execution_result is not None
    assert expected_hash in record.execution_result.output_asset_hashes


@pytest.mark.asyncio
async def test_failed_execution_persists_complete_result():
    def handler(request: httpx.Request) -> httpx.Response:
        result = WorkloadExecutionResult(
            workload_id="task-failed",
            worker_id="w-1",
            success=False,
            output_asset_hashes=set(),
            execution_duration_seconds=0.5,
            error_message="render crashed",
            stderr_snippet="Traceback...",
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator, workload_registry = _make_orchestrator_with_one_worker(handler)
    spec = WorkloadSpec(workload_id="task-failed", task_type="test", min_ram_bytes=1024)
    workload_registry.add_workload(spec)

    await orchestrator._process_workload("task-failed")

    record = workload_registry.get_workload("task-failed")
    assert record.execution_result is not None
    assert record.execution_result.success is False
    assert record.execution_result.error_message == "render crashed"
    assert record.execution_result.stderr_snippet == "Traceback..."
    # The pre-existing top-level convenience field is still populated too.
    assert record.error_message == "render crashed"


@pytest.mark.asyncio
async def test_existing_state_transitions_unchanged_for_success():
    def handler(request: httpx.Request) -> httpx.Response:
        result = WorkloadExecutionResult(
            workload_id="task-state-ok", worker_id="w-1", success=True,
            output_asset_hashes=set(), execution_duration_seconds=1.0,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator, workload_registry = _make_orchestrator_with_one_worker(handler)
    spec = WorkloadSpec(workload_id="task-state-ok", task_type="test", min_ram_bytes=1024)
    workload_registry.add_workload(spec)

    await orchestrator._process_workload("task-state-ok")

    record = workload_registry.get_workload("task-state-ok")
    assert record.state == WorkloadState.COMPLETED
    assert record.completed_at is not None
    assert record.placement_decision is not None
    assert record.placement_decision.selected_worker_id == "w-1"


@pytest.mark.asyncio
async def test_existing_state_transitions_unchanged_for_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        result = WorkloadExecutionResult(
            workload_id="task-state-fail", worker_id="w-1", success=False,
            output_asset_hashes=set(), execution_duration_seconds=1.0,
            error_message="boom",
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator, workload_registry = _make_orchestrator_with_one_worker(handler)
    spec = WorkloadSpec(workload_id="task-state-fail", task_type="test", min_ram_bytes=1024)
    workload_registry.add_workload(spec)

    await orchestrator._process_workload("task-state-fail")

    record = workload_registry.get_workload("task-state-fail")
    assert record.state == WorkloadState.FAILED
    assert record.completed_at is not None
    assert record.error_message == "boom"


@pytest.mark.asyncio
async def test_checkpointed_result_does_not_persist_execution_result():
    """Scope boundary: a checkpointed/migrating result reports success=True
    (per ExecutionManager) even though it isn't actually complete, so it
    must NOT be persisted via set_result() -- doing so would incorrectly
    flip state to COMPLETED. The MIGRATING transition is unchanged."""
    def handler(request: httpx.Request) -> httpx.Response:
        result = WorkloadExecutionResult(
            workload_id="task-checkpoint", worker_id="w-1", success=True,
            output_asset_hashes=set(), execution_duration_seconds=1.0,
            was_checkpointed=True, checkpoint_hash="d" * 64,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator, workload_registry = _make_orchestrator_with_one_worker(handler)
    spec = WorkloadSpec(workload_id="task-checkpoint", task_type="test", min_ram_bytes=1024)
    workload_registry.add_workload(spec)

    # The migrating branch resubmits via asyncio.create_task; run once and
    # inspect state immediately rather than letting it recurse.
    await orchestrator._process_workload("task-checkpoint")

    record = workload_registry.get_workload("task-checkpoint")
    assert record.state == WorkloadState.MIGRATING
    assert record.execution_result is None


def test_workload_registry_set_result_no_regression():
    """Direct WorkloadRegistry.set_result() behavior is unchanged for
    existing callers/tests: unknown workload_id returns False, known one
    stores the result and transitions state exactly as documented."""
    registry = WorkloadRegistry()
    spec = WorkloadSpec(workload_id="task-direct", task_type="test")
    registry.add_workload(spec)

    assert registry.set_result("nonexistent", WorkloadExecutionResult(
        workload_id="nonexistent", worker_id="w-x", success=True,
        output_asset_hashes=set(), execution_duration_seconds=0.0,
    )) is False

    result = WorkloadExecutionResult(
        workload_id="task-direct", worker_id="w-x", success=True,
        output_asset_hashes={"e" * 64}, execution_duration_seconds=2.0,
    )
    assert registry.set_result("task-direct", result) is True

    record = registry.get_workload("task-direct")
    assert record.execution_result is result
    assert record.state == WorkloadState.COMPLETED
    assert record.completed_at is not None
