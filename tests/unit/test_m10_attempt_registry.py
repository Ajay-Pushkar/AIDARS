"""M10.1/M10.19: AttemptRecord/AttemptRegistry in isolation -- no HTTP,
no orchestrator. Integration with the dispatch loop lives in
test_m10_execution_attempts.py; persistence round-tripping lives in
test_m10_persistence.py.
"""
from __future__ import annotations

from aidars.distributed.attempt import (
    AttemptRegistry,
    AttemptStatus,
    TERMINAL_ATTEMPT_STATUSES,
    make_attempt_id,
)
from aidars.distributed.models import FailureCategory, WorkloadExecutionResult


def test_make_attempt_id_is_stable_and_deterministic():
    assert make_attempt_id("w-1", 1) == "w-1#1"
    assert make_attempt_id("w-1", 2) == "w-1#2"


def test_first_attempt_is_numbered_one():
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    assert attempt.attempt_number == 1
    assert attempt.status == AttemptStatus.QUEUED
    assert attempt.workload_id == "w-1"


def test_successive_attempts_for_same_workload_increment_and_get_separate_ids():
    reg = AttemptRegistry()
    a1 = reg.create_attempt("w-1")
    a2 = reg.create_attempt("w-1")
    a3 = reg.create_attempt("w-1")
    assert [a.attempt_number for a in (a1, a2, a3)] == [1, 2, 3]
    assert len({a1.attempt_id, a2.attempt_id, a3.attempt_id}) == 3


def test_workload_id_remains_stable_across_every_attempt():
    """Core M10.4 invariant: the logical workload_id never changes across
    retries -- only attempt_number and attempt_id do."""
    reg = AttemptRegistry()
    attempts = [reg.create_attempt("stable-workload") for _ in range(5)]
    assert all(a.workload_id == "stable-workload" for a in attempts)


def test_attempts_for_different_workloads_are_independently_numbered():
    reg = AttemptRegistry()
    reg.create_attempt("w-1")
    reg.create_attempt("w-1")
    only_attempt_w2 = reg.create_attempt("w-2")
    assert only_attempt_w2.attempt_number == 1


def test_count_attempts_reflects_creation_count():
    reg = AttemptRegistry()
    assert reg.count_attempts("w-1") == 0
    reg.create_attempt("w-1")
    reg.create_attempt("w-1")
    assert reg.count_attempts("w-1") == 2
    assert reg.count_attempts("w-nonexistent") == 0


def test_list_attempts_for_workload_returns_creation_order():
    reg = AttemptRegistry()
    reg.create_attempt("w-1")
    reg.create_attempt("w-1")
    attempts = reg.list_attempts_for_workload("w-1")
    assert [a.attempt_number for a in attempts] == [1, 2]


def test_get_latest_attempt_returns_the_most_recent():
    reg = AttemptRegistry()
    reg.create_attempt("w-1")
    second = reg.create_attempt("w-1")
    assert reg.get_latest_attempt("w-1").attempt_id == second.attempt_id


def test_get_latest_attempt_none_when_no_attempts():
    reg = AttemptRegistry()
    assert reg.get_latest_attempt("w-nonexistent") is None


def test_mark_assigned_sets_worker_and_timestamp():
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    updated = reg.mark_assigned(attempt.attempt_id, "worker-x")
    assert updated.status == AttemptStatus.ASSIGNED
    assert updated.worker_id == "worker-x"
    assert updated.assigned_at is not None


def test_mark_running_sets_started_at():
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    reg.mark_assigned(attempt.attempt_id, "worker-x")
    updated = reg.mark_running(attempt.attempt_id)
    assert updated.status == AttemptStatus.RUNNING
    assert updated.started_at is not None


def test_mark_succeeded_attaches_execution_result_and_is_terminal():
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    result = WorkloadExecutionResult(
        workload_id="w-1", worker_id="worker-x", success=True,
        output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0,
    )
    updated = reg.mark_succeeded(attempt.attempt_id, result)
    assert updated.status == AttemptStatus.SUCCEEDED
    # A deep copy, not the same object -- matches the established
    # snapshot-inside-lock pattern used by every registry in this
    # package (see AttemptRegistry._update()'s docstring/implementation).
    assert updated.execution_result == result
    assert updated.finished_at is not None
    assert updated.status in TERMINAL_ATTEMPT_STATUSES


def test_mark_failed_records_category_and_reason():
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    updated = reg.mark_failed(attempt.attempt_id, FailureCategory.TEMPORARY_CAS_FAILURE, "disk full")
    assert updated.status == AttemptStatus.FAILED
    assert updated.failure_category == FailureCategory.TEMPORARY_CAS_FAILURE
    assert updated.failure_reason == "disk full"
    assert updated.status in TERMINAL_ATTEMPT_STATUSES


def test_mark_lost_uses_worker_unavailable_category():
    """M10.6: LOST always classifies as WORKER_UNAVAILABLE -- it's
    definitionally about the worker vanishing, not the workload."""
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    updated = reg.mark_lost(attempt.attempt_id, "worker evicted")
    assert updated.status == AttemptStatus.LOST
    assert updated.failure_category == FailureCategory.WORKER_UNAVAILABLE
    assert updated.status in TERMINAL_ATTEMPT_STATUSES


def test_mark_on_unknown_attempt_id_returns_none():
    reg = AttemptRegistry()
    assert reg.mark_running("does-not-exist") is None
    assert reg.mark_failed("does-not-exist", FailureCategory.APPLICATION_ERROR, "x") is None


def test_list_running_attempts_for_worker_only_returns_assigned_and_running():
    reg = AttemptRegistry()
    a1 = reg.create_attempt("w-1")
    reg.mark_assigned(a1.attempt_id, "worker-x")

    a2 = reg.create_attempt("w-2")
    reg.mark_assigned(a2.attempt_id, "worker-x")
    reg.mark_running(a2.attempt_id)

    a3 = reg.create_attempt("w-3")
    reg.mark_assigned(a3.attempt_id, "worker-x")
    reg.mark_succeeded(a3.attempt_id, WorkloadExecutionResult(
        workload_id="w-3", worker_id="worker-x", success=True,
        output_asset_hashes=set(), execution_duration_seconds=1.0,
    ))

    a4 = reg.create_attempt("w-4")
    reg.mark_assigned(a4.attempt_id, "worker-y")  # different worker

    running = reg.list_running_attempts_for_worker("worker-x")
    running_ids = {a.attempt_id for a in running}
    assert running_ids == {a1.attempt_id, a2.attempt_id}


def test_queued_and_total_duration_properties():
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    assert attempt.queued_duration_seconds is None  # not yet assigned
    assert attempt.total_duration_seconds is None  # not yet finished

    reg.mark_assigned(attempt.attempt_id, "worker-x")
    updated = reg.get_attempt(attempt.attempt_id)
    assert updated.queued_duration_seconds is not None
    assert updated.queued_duration_seconds >= 0.0

    reg.mark_failed(attempt.attempt_id, FailureCategory.APPLICATION_ERROR, "boom")
    finished = reg.get_attempt(attempt.attempt_id)
    assert finished.total_duration_seconds is not None
    assert finished.total_duration_seconds >= 0.0


def test_to_summary_dict_never_leaks_worker_credential_shaped_fields():
    """M10.17: the API-facing summary must not carry anything beyond
    worker_id -- no credential/token-shaped field should ever be added
    to this method without deliberate review."""
    reg = AttemptRegistry()
    attempt = reg.create_attempt("w-1")
    reg.mark_assigned(attempt.attempt_id, "worker-x")
    summary = reg.get_attempt(attempt.attempt_id).to_summary_dict()
    forbidden_keys = {"credential", "token", "secret", "authorization", "password"}
    assert not (forbidden_keys & set(summary.keys()))


def test_restore_attempt_does_not_persist_and_rebuilds_workload_index():
    """Mirrors WorkloadRegistry.restore_workload()/ArtifactRegistry.
    restore_artifact()'s convention: restoring inserts into memory only."""
    class ExplodingStateStore:
        def save_attempt(self, *_args, **_kwargs):
            raise AssertionError("restore_attempt must never persist")

    reg = AttemptRegistry(state_store=ExplodingStateStore())
    from aidars.distributed.attempt import AttemptRecord

    record = AttemptRecord(attempt_id="w-1#1", workload_id="w-1", attempt_number=1)
    reg.restore_attempt(record)

    assert reg.get_attempt("w-1#1") is record
    assert reg.count_attempts("w-1") == 1
    assert reg.list_attempts_for_workload("w-1") == [record]
