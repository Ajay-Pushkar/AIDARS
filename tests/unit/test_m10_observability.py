"""M10.11/M10.19: execution observability timeline.

Covers WorkloadExecutionResult's per-phase duration fields (populated by
execution.py) and AttemptRecord's coordinator-side timeline
(queued/assigned/started/finished), including that a FAILED attempt
still exposes timing -- not just a successful one.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from aidars.distributed.attempt import AttemptRegistry
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.models import FailureCategory, WorkloadExecutionResult, WorkloadSpec
from aidars.distributed.runtime import GenericSubprocessRuntime


# ============================================================================
# WorkloadExecutionResult phase timing
# ============================================================================


@pytest.mark.asyncio
async def test_successful_execution_reports_every_phase_duration(tmp_path: Path):
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    spec = WorkloadSpec(
        workload_id="task-1", task_type="test",
        parameters={"command": "echo hi > outputs/result.txt"},
    )
    result = await manager.execute_workload(spec, "w-1", GenericSubprocessRuntime())

    assert result.success is True
    assert result.staging_duration_seconds >= 0.0
    assert result.execution_duration_seconds >= 0.0
    assert result.output_ingestion_duration_seconds >= 0.0
    assert result.verification_duration_seconds >= 0.0
    assert result.total_duration_seconds == pytest.approx(
        result.staging_duration_seconds + result.execution_duration_seconds
        + result.output_ingestion_duration_seconds + result.verification_duration_seconds
    )


@pytest.mark.asyncio
async def test_failed_execution_still_reports_staging_duration(tmp_path: Path):
    """A failed attempt must still expose timing -- not just successful ones."""
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    missing_hash = "a" * 64
    spec = WorkloadSpec(workload_id="task-2", task_type="test", input_asset_hashes={missing_hash})

    result = await manager.execute_workload(spec, "w-1", GenericSubprocessRuntime())

    assert result.success is False
    assert result.failure_category == FailureCategory.TEMPORARY_CAS_FAILURE
    assert result.staging_duration_seconds >= 0.0


@pytest.mark.asyncio
async def test_timeout_failure_still_reports_execution_duration(tmp_path: Path):
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    spec = WorkloadSpec(
        workload_id="task-timeout", task_type="test",
        estimated_duration_seconds=0.01,  # timeout = 0.03s
        parameters={"command": "sleep 5"},
    )
    result = await manager.execute_workload(spec, "w-1", GenericSubprocessRuntime())

    assert result.success is False
    assert result.failure_category == FailureCategory.EXECUTION_TIMEOUT
    assert result.execution_duration_seconds >= 0.0
    assert "timed out" in (result.stderr_snippet or "")


def test_total_duration_seconds_is_a_pure_derived_property_not_a_stored_field():
    """Confirms this doesn't duplicate execution_duration_seconds as a
    second persisted total -- it's purely computed from the other four."""
    result = WorkloadExecutionResult(
        workload_id="w-1", worker_id="worker-x", success=True,
        output_asset_hashes=set(), execution_duration_seconds=2.0,
        staging_duration_seconds=1.0, output_ingestion_duration_seconds=0.5,
        verification_duration_seconds=0.25,
    )
    assert result.total_duration_seconds == 3.75
    assert "total_duration_seconds" not in WorkloadExecutionResult.model_fields


# ============================================================================
# AttemptRecord coordinator-side timeline
# ============================================================================


def test_attempt_timeline_progresses_through_each_timestamp():
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    assert attempt.queued_at is not None
    assert attempt.assigned_at is None
    assert attempt.started_at is None
    assert attempt.finished_at is None

    reg.mark_assigned(attempt.attempt_id, "worker-x")
    a = reg.get_attempt(attempt.attempt_id)
    assert a.assigned_at is not None
    assert a.assigned_at >= a.queued_at

    reg.mark_running(attempt.attempt_id)
    a = reg.get_attempt(attempt.attempt_id)
    assert a.started_at is not None
    assert a.started_at >= a.assigned_at

    reg.mark_succeeded(attempt.attempt_id, WorkloadExecutionResult(
        workload_id="w-1", worker_id="worker-x", success=True,
        output_asset_hashes=set(), execution_duration_seconds=1.0,
    ))
    a = reg.get_attempt(attempt.attempt_id)
    assert a.finished_at is not None
    assert a.finished_at >= a.started_at


def test_failed_attempt_still_exposes_full_timeline():
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    reg.mark_assigned(attempt.attempt_id, "worker-x")
    reg.mark_running(attempt.attempt_id)
    reg.mark_failed(attempt.attempt_id, FailureCategory.APPLICATION_ERROR, "boom")

    a = reg.get_attempt(attempt.attempt_id)
    assert a.total_duration_seconds is not None
    assert a.queued_duration_seconds is not None


def test_attempt_summary_dict_distinguishes_scheduler_from_runtime_timing():
    """The summary must let a caller distinguish coordinator-side
    scheduling latency (queued_duration_seconds) from the total wall
    time (total_duration_seconds), satisfying Part 11's "distinguish
    scheduler/network/CAS/runtime/storage bottlenecks" requirement at
    the attempt level -- worker-side phase breakdown lives on
    execution_result (see WorkloadExecutionResult's own fields, tested
    above)."""
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    reg.mark_assigned(attempt.attempt_id, "worker-x")
    reg.mark_running(attempt.attempt_id)
    result = WorkloadExecutionResult(
        workload_id="w-1", worker_id="worker-x", success=True,
        output_asset_hashes=set(), execution_duration_seconds=1.0,
        staging_duration_seconds=0.5,
    )
    reg.mark_succeeded(attempt.attempt_id, result)

    summary = reg.get_attempt(attempt.attempt_id).to_summary_dict()
    assert "queued_duration_seconds" in summary
    assert "total_duration_seconds" in summary
    assert summary["queued_duration_seconds"] <= summary["total_duration_seconds"]
