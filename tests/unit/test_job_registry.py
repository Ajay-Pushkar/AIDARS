"""M8.1-M8.5: Job identity, JobRegistry, aggregation, and completion policy.

Uses the real WorkloadRegistry (not a mock) since JobAggregate is derived
live from it -- the whole point of this design is that there is nothing
else to fake.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from aidars.distributed.job_registry import (
    CompletionPolicy,
    JobRegistry,
    JobState,
)
from aidars.distributed.models import WorkloadExecutionResult, WorkloadSpec
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


def _spec(workload_id: str, job_id=None, **overrides) -> WorkloadSpec:
    defaults = dict(workload_id=workload_id, job_id=job_id, task_type="test", min_ram_bytes=1024)
    defaults.update(overrides)
    return WorkloadSpec(**defaults)


# ============================================================================
# Job creation / membership / job_id propagation
# ============================================================================


def test_job_creation_and_membership():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)

    job = jr.create_job("job-1", {"w1", "w2", "w3"})
    assert job.job_id == "job-1"
    assert job.workload_ids == {"w1", "w2", "w3"}
    assert job.completion_policy == CompletionPolicy.ALL_REQUIRED  # PRD default


def test_create_job_is_idempotent_by_job_id():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)

    jr.create_job("job-1", {"w1"})
    jr.create_job("job-1", {"different", "membership"})  # ignored -- existing wins

    assert jr.get_job("job-1").workload_ids == {"w1"}


def test_threshold_policy_requires_a_threshold():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    with pytest.raises(ValueError):
        jr.create_job("job-1", {"w1"}, completion_policy=CompletionPolicy.THRESHOLD)


@pytest.mark.asyncio
async def test_job_id_propagates_to_workload_specs_via_submit_job():
    registry = WorkerRegistry()
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    orch = WorkloadOrchestrator(registry, wr, job_registry=jr)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="w1", worker_id="w-1", success=True,
            output_asset_hashes=set(), execution_duration_seconds=1.0,
        ).model_dump(mode="json"))

    orch.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    registry.register_worker(__import__("aidars.distributed.models", fromlist=["WorkerInfo"]).WorkerInfo(
        worker_id="w-1", endpoint_url="http://w1", ip_address="127.0.0.1", port=8001,
        capacity_bytes=999999999, used_bytes=0,
    ))

    job_id = await orch.submit_job([_spec("w1")])
    assert wr.get_workload("w1").spec.job_id == job_id


def test_submit_job_reuses_shared_job_id_from_specs():
    """BlenderAdapter.evaluate_request() already stamps a shared job_id on
    every chunk it returns; submit_job() must reuse it, not mint a
    conflicting second one."""
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    registry = WorkerRegistry()
    orch = WorkloadOrchestrator(registry, wr, job_registry=jr)
    orch.http_client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="ignored", worker_id="w-1", success=True,
            output_asset_hashes=set(), execution_duration_seconds=1.0,
        ).model_dump(mode="json"))
    ))
    specs = [_spec("c0", job_id="job-preassigned"), _spec("c1", job_id="job-preassigned")]

    job_id = asyncio.run(orch.submit_job(specs))
    assert job_id == "job-preassigned"


def test_submit_job_rejects_specs_with_conflicting_job_ids():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    orch = WorkloadOrchestrator(WorkerRegistry(), wr, job_registry=jr)
    specs = [_spec("c0", job_id="job-a"), _spec("c1", job_id="job-b")]

    with pytest.raises(ValueError):
        asyncio.run(orch.submit_job(specs))


# ============================================================================
# Backward compatibility: single-workload submission unaffected
# ============================================================================


@pytest.mark.asyncio
async def test_single_workload_submission_remains_backward_compatible():
    """submit_workload() (no Job involved) must work exactly as before,
    including with an orchestrator that never received a job_registry."""
    registry = WorkerRegistry()
    wr = WorkloadRegistry()
    orch = WorkloadOrchestrator(registry, wr)  # no job_registry/artifact_registry passed
    assert orch.job_registry is None
    assert orch.artifact_registry is None

    workload_id = await orch.submit_workload(_spec("solo"))
    assert workload_id == "solo"
    assert wr.get_workload("solo").spec.job_id is None


# ============================================================================
# Aggregation: total/completed/failed/pending/running + derived state
# ============================================================================


def _record_result(wr: WorkloadRegistry, workload_id: str, success: bool, hashes=None):
    wr.set_result(workload_id, WorkloadExecutionResult(
        workload_id=workload_id, worker_id="w-x", success=success,
        output_asset_hashes=hashes or set(), execution_duration_seconds=1.0,
        error_message=None if success else "boom",
    ))


def test_aggregate_all_pending():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    for wid in ("w1", "w2"):
        wr.add_workload(_spec(wid))
    jr.create_job("job-1", {"w1", "w2"})

    agg = jr.get_aggregate("job-1")
    assert (agg.total, agg.completed, agg.failed, agg.pending, agg.running) == (2, 0, 0, 2, 0)
    assert agg.state == JobState.SUBMITTED


def test_aggregate_all_workloads_complete():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    for wid in ("w1", "w2"):
        wr.add_workload(_spec(wid))
        _record_result(wr, wid, True, {f"{wid}-hash".ljust(64, "0")})
    jr.create_job("job-1", {"w1", "w2"})

    agg = jr.get_aggregate("job-1")
    assert agg.state == JobState.COMPLETED
    assert agg.completed == 2


def test_aggregate_partial_completion_still_running():
    """PRD example: total=10-ish, some complete, some running, one failed
    -- must NOT report COMPLETED while workloads are still active."""
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    wr.add_workload(_spec("w1")); _record_result(wr, "w1", True)
    wr.add_workload(_spec("w2")); _record_result(wr, "w2", False)
    wr.add_workload(_spec("w3")); wr.update_state("w3", WorkloadState.PLACED)  # running
    wr.add_workload(_spec("w4"))  # pending
    jr.create_job("job-1", {"w1", "w2", "w3", "w4"})

    agg = jr.get_aggregate("job-1")
    assert agg.state != JobState.COMPLETED
    assert agg.state == JobState.RUNNING
    assert (agg.completed, agg.failed, agg.running, agg.pending) == (1, 1, 1, 1)


def test_aggregate_all_required_fails_once_terminal_with_any_failure():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    wr.add_workload(_spec("w1")); _record_result(wr, "w1", True)
    wr.add_workload(_spec("w2")); _record_result(wr, "w2", False)
    jr.create_job("job-1", {"w1", "w2"}, completion_policy=CompletionPolicy.ALL_REQUIRED)

    agg = jr.get_aggregate("job-1")
    assert agg.state == JobState.FAILED  # not COMPLETED -- one failure disqualifies ALL_REQUIRED


def test_aggregate_unschedulable_counts_as_failed():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    wr.add_workload(_spec("w1"))
    wr.update_state("w1", WorkloadState.UNSCHEDULABLE, error_message="no workers")
    jr.create_job("job-1", {"w1"})

    agg = jr.get_aggregate("job-1")
    assert agg.failed == 1
    assert agg.state == JobState.FAILED


@pytest.mark.parametrize("policy,completed,total,expected", [
    (CompletionPolicy.ANY_SUCCESS, 1, 2, JobState.COMPLETED),
    (CompletionPolicy.ANY_SUCCESS, 0, 2, JobState.FAILED),
    (CompletionPolicy.BEST_EFFORT, 1, 2, JobState.PARTIALLY_COMPLETED),
    (CompletionPolicy.BEST_EFFORT, 2, 2, JobState.COMPLETED),
    (CompletionPolicy.BEST_EFFORT, 0, 2, JobState.FAILED),
])
def test_completion_policies(policy, completed, total, expected):
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    workload_ids = set()
    for i in range(total):
        wid = f"w{i}"
        workload_ids.add(wid)
        wr.add_workload(_spec(wid))
        _record_result(wr, wid, success=(i < completed))
    jr.create_job("job-1", workload_ids, completion_policy=policy)

    assert jr.get_aggregate("job-1").state == expected


def test_threshold_policy():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    for i, ok in enumerate([True, True, False]):
        wid = f"w{i}"
        wr.add_workload(_spec(wid))
        _record_result(wr, wid, success=ok)
    jr.create_job("job-1", {"w0", "w1", "w2"}, completion_policy=CompletionPolicy.THRESHOLD, threshold=2)

    assert jr.get_aggregate("job-1").state == JobState.COMPLETED  # 2 of 3 met the threshold


# ============================================================================
# Output hash aggregation / deduplication
# ============================================================================


def test_output_hash_aggregation_deduplicates():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    shared_hash = "a" * 64
    wr.add_workload(_spec("w1")); _record_result(wr, "w1", True, {shared_hash, "b" * 64})
    wr.add_workload(_spec("w2")); _record_result(wr, "w2", True, {shared_hash, "c" * 64})
    jr.create_job("job-1", {"w1", "w2"})

    agg = jr.get_aggregate("job-1")
    assert agg.output_asset_hashes == {shared_hash, "b" * 64, "c" * 64}  # set union, not a list with dupes


def test_get_aggregate_unknown_job_returns_none():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    assert jr.get_aggregate("does-not-exist") is None


# ============================================================================
# Persistence round-trip (state_store direct)
# ============================================================================


def test_job_persists_through_state_store(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    wr = WorkloadRegistry()
    jr = JobRegistry(wr, state_store=store)

    jr.create_job("job-1", {"w1", "w2"}, completion_policy=CompletionPolicy.THRESHOLD, threshold=1)

    loaded = store.load_jobs()
    assert len(loaded) == 1
    assert loaded[0].job_id == "job-1"
    assert loaded[0].workload_ids == {"w1", "w2"}
    assert loaded[0].completion_policy == CompletionPolicy.THRESHOLD
    assert loaded[0].threshold == 1


def test_job_registry_without_store_requires_no_sqlite():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)  # no state_store
    jr.create_job("job-1", {"w1"})
    assert jr.get_job("job-1") is not None
