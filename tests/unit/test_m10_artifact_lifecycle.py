"""M10.9/M10.19: audits that M8's Artifact lifecycle/GC primitives are
correctly wired to the NEW attempt/retry loop -- artifacts must reflect
only the FINAL successful attempt's output, never a failed/retried
attempt's, and GC-eligibility/active-job protection semantics from M8
are unaffected by the loop now being attempt-driven.

Full artifact lifecycle mechanics themselves (compute_gc_eligible,
mark_gc_eligible, mark_deleted transitions) are already covered by M8's
own test suite (test_artifact_registry.py) and are unmodified by M10 --
this file only covers the M10-specific wiring seam.
"""
from __future__ import annotations

import httpx
import pytest
import time

from aidars.distributed.artifact import ArtifactRegistry
from aidars.distributed.attempt import AttemptRegistry
from aidars.distributed.job_registry import CompletionPolicy, JobRegistry
from aidars.distributed.models import FailureCategory, WorkerInfo, WorkerResourceProfile, WorkerStatus, WorkloadExecutionResult, WorkloadSpec
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


def _make_full_orchestrator(handler):
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    attempt_registry = AttemptRegistry()
    job_registry = JobRegistry(workload_registry)
    artifact_registry = ArtifactRegistry()
    orchestrator = WorkloadOrchestrator(
        registry=registry, workload_registry=workload_registry,
        attempt_registry=attempt_registry, job_registry=job_registry,
        artifact_registry=artifact_registry,
    )
    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
        resource_profile=WorkerResourceProfile(
            timestamp_utc=time.time(), worker_id="w-1", endpoint_url="http://worker-1",
            ip_address="127.0.0.1", cpu_cores_total=4, cpu_utilization_percent=0.0,
            ram_total_bytes=16 * 1024**3, ram_available_bytes=16 * 1024**3,
        ),
    ))
    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return orchestrator, workload_registry, attempt_registry, job_registry, artifact_registry


@pytest.mark.asyncio
async def test_only_the_final_successful_attempts_output_becomes_an_artifact():
    """The FIRST attempt fails and reports no output; the retry succeeds
    with real output. Only the successful attempt's hashes must be
    recorded as Artifacts -- a failed attempt's (empty) output must
    never leak into provenance."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            result = WorkloadExecutionResult(
                workload_id="task-1", worker_id="w-1", success=False,
                output_asset_hashes=set(), execution_duration_seconds=1.0,
                error_message="transient", failure_category=FailureCategory.TEMPORARY_CAS_FAILURE,
            )
        else:
            result = WorkloadExecutionResult(
                workload_id="task-1", worker_id="w-1", success=True,
                output_asset_hashes={"b" * 64}, execution_duration_seconds=1.0,
            )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry, job_registry, artifact_registry = (
        _make_full_orchestrator(handler)
    )
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))
    await orchestrator._process_workload("task-1")

    assert workload_registry.get_workload("task-1").state == WorkloadState.COMPLETED
    artifacts = artifact_registry.list_artifacts()
    assert len(artifacts) == 1
    assert artifacts[0].content_hash == "b" * 64
    assert artifacts[0].producer_workload_id == "task-1"


@pytest.mark.asyncio
async def test_a_workload_that_exhausts_its_retry_budget_records_no_artifacts():
    def handler(request: httpx.Request) -> httpx.Response:
        result = WorkloadExecutionResult(
            workload_id="task-1", worker_id="w-1", success=False,
            output_asset_hashes={"should-never-be-recorded"}, execution_duration_seconds=1.0,
            error_message="always fails", failure_category=FailureCategory.RESOURCE_EXHAUSTION,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry, job_registry, artifact_registry = (
        _make_full_orchestrator(handler)
    )
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))
    await orchestrator._process_workload("task-1")

    assert workload_registry.get_workload("task-1").state == WorkloadState.FAILED
    assert artifact_registry.list_artifacts() == []


@pytest.mark.asyncio
async def test_job_aggregate_reflects_final_attempt_not_every_attempt():
    """M8's Job aggregation reads WorkloadRegistry live and must
    continue to see exactly ONE completed workload -- not one per
    attempt -- confirming the retry loop never creates a second logical
    workload (Part 5's explicit requirement)."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            result = WorkloadExecutionResult(
                workload_id="task-1", worker_id="w-1", success=False,
                output_asset_hashes=set(), execution_duration_seconds=1.0,
                error_message="transient", failure_category=FailureCategory.WORKER_UNAVAILABLE,
            )
        else:
            result = WorkloadExecutionResult(
                workload_id="task-1", worker_id="w-1", success=True,
                output_asset_hashes={"c" * 64}, execution_duration_seconds=1.0,
            )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator, workload_registry, attempt_registry, job_registry, artifact_registry = (
        _make_full_orchestrator(handler)
    )
    workload_registry.add_workload(WorkloadSpec(workload_id="task-1", job_id="job-1", task_type="test", min_ram_bytes=1024))
    job_registry.create_job("job-1", {"task-1"}, CompletionPolicy.ALL_REQUIRED)

    await orchestrator._process_workload("task-1")

    aggregate = job_registry.get_aggregate("job-1")
    assert aggregate.total == 1  # never inflated by attempt count
    assert aggregate.completed == 1
    assert aggregate.failed == 0

    # But the full retry history IS visible via AttemptRegistry, for
    # observability -- it's just never conflated with Job/Workload counting.
    assert attempt_registry.count_attempts("task-1") == 3
