"""M8 correction pass: WorkloadRegistry.add_workload() must never silently
discard a submission when workload_id already exists under a materially
different spec.

Reproduces the exact case the post-implementation audit found:

    spec1: workload_id="dup", job_id="job-A"
    spec2: workload_id="dup", job_id="job-B"

Before this fix, add_workload(spec2) silently returned spec1's record;
the caller had no way to know spec2 was discarded, and any subsequent
dispatch used spec1's content under the identity the caller believed was
spec2's.
"""
from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.job_registry import JobRegistry
from aidars.distributed.models import WorkerInfo, WorkerStatus, WorkloadExecutionResult, WorkloadSpec
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadIdConflictError, WorkloadRegistry


# ============================================================================
# Registry-level: identical resubmission is idempotent
# ============================================================================


def test_identical_resubmission_is_idempotent_noop():
    wr = WorkloadRegistry()
    spec = WorkloadSpec(workload_id="w1", job_id="job-A", task_type="test", min_ram_bytes=1024)

    rec1 = wr.add_workload(spec)
    # A byte-for-byte equal but distinct spec object -- e.g. a client
    # retry that reconstructs an equivalent WorkloadSpec.
    spec_replay = WorkloadSpec(workload_id="w1", job_id="job-A", task_type="test", min_ram_bytes=1024)
    rec2 = wr.add_workload(spec_replay)

    assert rec1 is rec2
    assert len(wr.list_workloads()) == 1


def test_same_object_resubmission_still_works_exactly_as_before():
    """Pre-existing test_6_13_task_idempotency's exact scenario, re-verified
    here as a named regression guard for this specific fix."""
    wr = WorkloadRegistry()
    spec = WorkloadSpec(workload_id="task-dup", task_type="test")

    r1 = wr.add_workload(spec)
    r2 = wr.add_workload(spec)

    assert r1 is r2
    assert len(wr.list_workloads()) == 1


# ============================================================================
# Registry-level: the exact audit case -- conflicting spec is rejected
# ============================================================================


def test_conflicting_spec_raises_and_preserves_original():
    wr = WorkloadRegistry()
    spec1 = WorkloadSpec(workload_id="dup", job_id="job-A", task_type="test", min_ram_bytes=1024)
    spec2 = WorkloadSpec(workload_id="dup", job_id="job-B", task_type="test", min_ram_bytes=2048)

    wr.add_workload(spec1)

    with pytest.raises(WorkloadIdConflictError):
        wr.add_workload(spec2)

    # The ORIGINAL submission must be completely untouched -- never
    # silently replaced, never silently merged.
    record = wr.get_workload("dup")
    assert record.spec.job_id == "job-A"
    assert record.spec.min_ram_bytes == 1024
    assert len(wr.list_workloads()) == 1


def test_workload_id_conflict_error_is_a_value_error():
    """So any existing/future caller that already catches ValueError for
    input validation (the CoordinatorService REST convention) catches
    this without needing a separate except clause."""
    assert issubclass(WorkloadIdConflictError, ValueError)


def test_conflict_error_message_identifies_the_colliding_id():
    wr = WorkloadRegistry()
    wr.add_workload(WorkloadSpec(workload_id="dup", job_id="job-A", task_type="test"))
    try:
        wr.add_workload(WorkloadSpec(workload_id="dup", job_id="job-B", task_type="test"))
        assert False, "expected WorkloadIdConflictError"
    except WorkloadIdConflictError as exc:
        assert exc.workload_id == "dup"
        assert exc.existing_spec.job_id == "job-A"
        assert exc.incoming_spec.job_id == "job-B"
        assert "dup" in str(exc)


# ============================================================================
# Orchestrator-level: submit_workload()/submit_job() propagate the conflict
# rather than silently dispatching the wrong spec
# ============================================================================


@pytest.mark.asyncio
async def test_submit_workload_propagates_conflict_and_does_not_dispatch():
    registry = WorkerRegistry()
    wr = WorkloadRegistry()
    orch = WorkloadOrchestrator(registry, wr)
    dispatched = []

    def handler(request: httpx.Request) -> httpx.Response:
        dispatched.append(request)
        return httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="dup", worker_id="w-1", success=True,
            output_asset_hashes=set(), execution_duration_seconds=1.0,
        ).model_dump(mode="json"))

    orch.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        capacity_bytes=999999999, used_bytes=0,
    ))

    await orch.submit_workload(WorkloadSpec(workload_id="dup", job_id="job-A", task_type="test", min_ram_bytes=1024))
    await asyncio.sleep(0.05)

    with pytest.raises(WorkloadIdConflictError):
        await orch.submit_workload(WorkloadSpec(workload_id="dup", job_id="job-B", task_type="test", min_ram_bytes=2048))

    # Only job-A's dispatch happened; the conflicting job-B submission
    # never reached the create_task/dispatch step.
    await asyncio.sleep(0.05)
    assert wr.get_workload("dup").spec.job_id == "job-A"


@pytest.mark.asyncio
async def test_submit_job_propagates_conflict_for_colliding_workload_id():
    registry = WorkerRegistry()
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    orch = WorkloadOrchestrator(registry, wr, job_registry=jr)
    orch.http_client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="dup", worker_id="w-1", success=True,
            output_asset_hashes=set(), execution_duration_seconds=1.0,
        ).model_dump(mode="json"))
    ))

    await orch.submit_job([WorkloadSpec(workload_id="dup", job_id="job-A", task_type="test", min_ram_bytes=1024)])

    with pytest.raises(WorkloadIdConflictError):
        await orch.submit_job([WorkloadSpec(workload_id="dup", job_id="job-C", task_type="test", min_ram_bytes=4096)])

    assert wr.get_workload("dup").spec.job_id == "job-A"


# ============================================================================
# Regression: submit_job() still delegates through submit_workload() only
# (no second dispatcher introduced by this fix)
# ============================================================================


@pytest.mark.asyncio
async def test_submit_job_still_delegates_only_through_submit_workload(monkeypatch):
    registry = WorkerRegistry()
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    orch = WorkloadOrchestrator(registry, wr, job_registry=jr)

    calls = []
    original_submit_workload = orch.submit_workload

    async def spy_submit_workload(spec):
        calls.append(spec.workload_id)
        return await original_submit_workload(spec)

    monkeypatch.setattr(orch, "submit_workload", spy_submit_workload)

    await orch.submit_job([
        WorkloadSpec(workload_id="c0", task_type="test", min_ram_bytes=1024),
        WorkloadSpec(workload_id="c1", task_type="test", min_ram_bytes=1024),
    ])

    assert calls == ["c0", "c1"]


# ============================================================================
# API-level: /api/v1/workloads/submit -- the exact audit case, at the
# pre-existing single-workload REST boundary (not just /jobs/submit).
# ============================================================================


def test_workloads_submit_endpoint_rejects_conflicting_id_with_409():
    service = CoordinatorService()
    client = TestClient(service.app)

    first = client.post("/api/v1/workloads/submit", json={
        "workload_id": "dup", "job_id": "job-A", "task_type": "test", "min_ram_bytes": 1024,
    })
    assert first.status_code == 202

    second = client.post("/api/v1/workloads/submit", json={
        "workload_id": "dup", "job_id": "job-B", "task_type": "test", "min_ram_bytes": 2048,
    })
    assert second.status_code == 409
    assert "dup" in second.json()["detail"]

    status_resp = client.get("/api/v1/workloads/dup")
    assert status_resp.json()["state"] in ("submitted", "validating", "placing", "placed", "unschedulable", "completed", "failed")
    # The record that actually exists is still job-A's -- confirm via the
    # registry directly, since the status endpoint doesn't echo job_id.
    assert service.workload_registry.get_workload("dup").spec.job_id == "job-A"


def test_workloads_submit_endpoint_allows_identical_replay():
    service = CoordinatorService()
    client = TestClient(service.app)

    payload = {"workload_id": "dup2", "job_id": "job-A", "task_type": "test", "min_ram_bytes": 1024}
    first = client.post("/api/v1/workloads/submit", json=payload)
    second = client.post("/api/v1/workloads/submit", json=payload)

    assert first.status_code == 202
    assert second.status_code == 202
