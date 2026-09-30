"""M10.1/M10.5/M10.19: WorkloadOrchestrator's attempt creation and
retry-budget loop, exercised against a real WorkerRegistry/
WorkloadRegistry/AttemptRegistry/PlacementEngine with only the HTTP
dispatch mocked -- same pattern as test_workload_result_persistence.py.
"""
from __future__ import annotations

import httpx
import pytest
import time

from aidars.distributed.attempt import AttemptRegistry, AttemptStatus
from aidars.distributed.models import (
    FailureCategory,
    WorkerInfo,
    WorkerResourceProfile,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.retry import DEFAULT_MAX_ATTEMPTS
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


def _resource_profile(worker_id, endpoint_url, ip_address):
    return WorkerResourceProfile(
        timestamp_utc=time.time(), worker_id=worker_id, endpoint_url=endpoint_url,
        ip_address=ip_address, cpu_cores_total=8, cpu_utilization_percent=0.0,
        ram_total_bytes=16 * 1024**3, ram_available_bytes=16 * 1024**3,
    )


def _make_orchestrator(handler, max_attempts=DEFAULT_MAX_ATTEMPTS):
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    attempt_registry = AttemptRegistry()
    orchestrator = WorkloadOrchestrator(
        registry=registry, workload_registry=workload_registry,
        attempt_registry=attempt_registry, max_attempts=max_attempts,
    )
    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
        resource_profile=_resource_profile("w-1", "http://worker-1", "127.0.0.1"),
    ))
    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return orchestrator, workload_registry, attempt_registry


def _success_result(workload_id: str) -> WorkloadExecutionResult:
    return WorkloadExecutionResult(
        workload_id=workload_id, worker_id="w-1", success=True,
        output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0,
    )


def _failed_result(workload_id: str, category: FailureCategory) -> WorkloadExecutionResult:
    return WorkloadExecutionResult(
        workload_id=workload_id, worker_id="w-1", success=False,
        output_asset_hashes=set(), execution_duration_seconds=1.0,
        error_message="synthetic failure", failure_category=category,
    )


# ============================================================================
# Execution Attempts
# ============================================================================


@pytest.mark.asyncio
async def test_successful_workload_creates_exactly_one_attempt():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_success_result("task-1").model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))
    await orchestrator._process_workload("task-1")

    attempts = attempt_registry.list_attempts_for_workload("task-1")
    assert len(attempts) == 1
    assert attempts[0].attempt_number == 1
    assert attempts[0].status == AttemptStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_attempt_worker_id_matches_placement_decision():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_success_result("task-1").model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))
    await orchestrator._process_workload("task-1")

    attempt = attempt_registry.get_latest_attempt("task-1")
    assert attempt.worker_id == "w-1"


@pytest.mark.asyncio
async def test_attempt_result_association():
    """The attempt's execution_result matches exactly what the worker
    reported for that attempt."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_success_result("task-1").model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))
    await orchestrator._process_workload("task-1")

    attempt = attempt_registry.get_latest_attempt("task-1")
    assert attempt.execution_result is not None
    assert attempt.execution_result.output_asset_hashes == {"a" * 64}


@pytest.mark.asyncio
async def test_none_attempt_registry_preserves_pre_m10_behavior():
    """Orchestrators constructed without an attempt_registry (every
    pre-M10 test/caller) must still work exactly as before -- attempt
    tracking is additive, not a hard requirement."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_success_result("task-1").model_dump(mode="json"))

    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    orchestrator = WorkloadOrchestrator(registry=registry, workload_registry=workload_registry)
    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
        resource_profile=_resource_profile("w-1", "http://worker-1", "127.0.0.1"),
    ))
    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))

    await orchestrator._process_workload("task-1")
    record = workload_registry.get_workload("task-1")
    assert record.state == WorkloadState.COMPLETED
    assert orchestrator.attempt_registry is None


# ============================================================================
# Retry: retryable failures
# ============================================================================


@pytest.mark.asyncio
async def test_retryable_failure_creates_a_second_attempt_with_same_workload_id():
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(200, json=_failed_result(
                "task-1", FailureCategory.TEMPORARY_CAS_FAILURE
            ).model_dump(mode="json"))
        return httpx.Response(200, json=_success_result("task-1").model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))
    await orchestrator._process_workload("task-1")

    attempts = attempt_registry.list_attempts_for_workload("task-1")
    assert len(attempts) == 2
    assert attempts[0].workload_id == attempts[1].workload_id == "task-1"
    assert attempts[0].status == AttemptStatus.FAILED
    assert attempts[0].failure_category == FailureCategory.TEMPORARY_CAS_FAILURE
    assert attempts[1].status == AttemptStatus.SUCCEEDED
    assert call_count == 2

    record = workload_registry.get_workload("task-1")
    assert record.state == WorkloadState.COMPLETED
    # workload_id must never change across retries.
    assert record.spec.workload_id == "task-1"


@pytest.mark.asyncio
async def test_dispatch_exception_is_worker_unavailable_and_retryable():
    """A transport-level failure (can't reach the worker at all) never
    produces a WorkloadExecutionResult -- it's classified inline as
    WORKER_UNAVAILABLE and is retryable."""
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    attempt_registry = AttemptRegistry()
    orchestrator = WorkloadOrchestrator(
        registry=registry, workload_registry=workload_registry, attempt_registry=attempt_registry,
    )
    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
        resource_profile=_resource_profile("w-1", "http://worker-1", "127.0.0.1"),
    ))
    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))

    await orchestrator._process_workload("task-1")

    attempts = attempt_registry.list_attempts_for_workload("task-1")
    # Only one worker exists, and it's excluded from this workload's
    # remaining attempts after failing once. With M11 admission semantics,
    # temporary lack of an eligible worker leaves the original workload
    # pending for reconsideration and does not manufacture another attempt.
    assert attempts[0].failure_category == FailureCategory.WORKER_UNAVAILABLE
    assert attempts[0].status == AttemptStatus.FAILED
    assert len(attempts) == 1
    record = workload_registry.get_workload("task-1")
    assert record.state == WorkloadState.SUBMITTED


# ============================================================================
# Retry: non-retryable failures
# ============================================================================


@pytest.mark.asyncio
async def test_non_retryable_failure_creates_exactly_one_attempt_and_fails_workload():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_failed_result(
            "task-1", FailureCategory.APPLICATION_ERROR
        ).model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))
    await orchestrator._process_workload("task-1")

    attempts = attempt_registry.list_attempts_for_workload("task-1")
    assert len(attempts) == 1
    assert attempts[0].status == AttemptStatus.FAILED

    record = workload_registry.get_workload("task-1")
    assert record.state == WorkloadState.FAILED
    assert record.execution_result is not None
    assert record.execution_result.failure_category == FailureCategory.APPLICATION_ERROR


# ============================================================================
# Retry: budget exhaustion
# ============================================================================


@pytest.mark.asyncio
async def test_retry_budget_exhaustion_records_workload_failed_explicitly():
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json=_failed_result(
            "task-1", FailureCategory.RESOURCE_EXHAUSTION
        ).model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler, max_attempts=3)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))
    await orchestrator._process_workload("task-1")

    assert call_count == 3, "must stop exactly at the configured budget -- no infinite retry"
    attempts = attempt_registry.list_attempts_for_workload("task-1")
    assert len(attempts) == 3
    assert all(a.status == AttemptStatus.FAILED for a in attempts)
    assert all(a.attempt_number == i + 1 for i, a in enumerate(attempts))

    record = workload_registry.get_workload("task-1")
    assert record.state == WorkloadState.FAILED


@pytest.mark.asyncio
async def test_no_infinite_retry_loop_with_a_tiny_budget():
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json=_failed_result(
            "task-1", FailureCategory.WORKER_UNAVAILABLE
        ).model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler, max_attempts=1)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))
    await orchestrator._process_workload("task-1")

    assert call_count == 1
    assert attempt_registry.count_attempts("task-1") == 1
    assert workload_registry.get_workload("task-1").state == WorkloadState.FAILED


@pytest.mark.asyncio
async def test_placement_recovery_retries_a_different_worker_after_dispatch_failure():
    """End-to-end version of the classic B1 placement-recovery scenario,
    now driven through the explicit attempt/retry loop instead of the
    old one-shot inline fallback."""
    def handler(request: httpx.Request) -> httpx.Response:
        if "worker-b" in str(request.url):
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json=_success_result("task-1").model_dump(mode="json"))

    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    attempt_registry = AttemptRegistry()
    orchestrator = WorkloadOrchestrator(
        registry=registry, workload_registry=workload_registry, attempt_registry=attempt_registry,
    )
    registry.register_worker(WorkerInfo(
        worker_id="worker-b", endpoint_url="http://worker-b", ip_address="1.1.1.1", port=8001,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
        resource_profile=_resource_profile("worker-b", "http://worker-b", "1.1.1.1"),
    ))
    registry.register_worker(WorkerInfo(
        worker_id="worker-c", endpoint_url="http://worker-c", ip_address="2.2.2.2", port=8002,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
        resource_profile=_resource_profile("worker-c", "http://worker-c", "2.2.2.2"),
    ))
    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))

    await orchestrator._process_workload("task-1")

    record = workload_registry.get_workload("task-1")
    assert record.state == WorkloadState.COMPLETED
    assert record.placement_decision.selected_worker_id == "worker-c"

    attempts = attempt_registry.list_attempts_for_workload("task-1")
    assert len(attempts) == 2
    assert attempts[0].worker_id == "worker-b"
    assert attempts[0].status == AttemptStatus.FAILED
    assert attempts[1].worker_id == "worker-c"
    assert attempts[1].status == AttemptStatus.SUCCEEDED
