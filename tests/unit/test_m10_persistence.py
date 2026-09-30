"""M10.15/M10.18/M10.19: attempts-table persistence/recovery, and the
single-writer coordinator-startup safety lock.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from aidars.distributed.attempt import AttemptRecord, AttemptRegistry, AttemptStatus
from aidars.distributed.models import FailureCategory, WorkloadExecutionResult
from aidars.distributed.state_store import (
    CoordinatorAlreadyRunningError,
    CoordinatorStateStore,
)


# ============================================================================
# Attempts table: round-trip fidelity
# ============================================================================


def test_fresh_sqlite_handle_starts_with_no_attempts(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    assert store.load_attempts() == []


def test_attempt_round_trips_through_a_fresh_store_handle(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store_before = CoordinatorStateStore(db_path)
    registry = AttemptRegistry(state_store=store_before)

    a = registry.create_attempt("task-1")
    registry.mark_assigned(a.attempt_id, "w-1")
    registry.mark_running(a.attempt_id)
    registry.mark_succeeded(a.attempt_id, WorkloadExecutionResult(
        workload_id="task-1", worker_id="w-1", success=True,
        output_asset_hashes={"a" * 64}, execution_duration_seconds=2.5,
        staging_duration_seconds=0.1, was_checkpointed=False,
    ))
    store_before.close()

    store_after = CoordinatorStateStore(db_path)
    loaded = store_after.load_attempts()
    assert len(loaded) == 1
    restored = loaded[0]
    assert restored.attempt_id == a.attempt_id
    assert restored.workload_id == "task-1"
    assert restored.attempt_number == 1
    assert restored.status == AttemptStatus.SUCCEEDED
    assert restored.worker_id == "w-1"
    assert restored.execution_result is not None
    assert restored.execution_result.execution_duration_seconds == 2.5
    assert restored.execution_result.output_asset_hashes == {"a" * 64}


def test_failed_attempt_round_trips_with_failure_category_and_reason(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = CoordinatorStateStore(db_path)
    registry = AttemptRegistry(state_store=store)
    a = registry.create_attempt("task-1")
    registry.mark_failed(a.attempt_id, FailureCategory.TEMPORARY_CAS_FAILURE, "disk unavailable")
    store.close()

    reloaded = CoordinatorStateStore(db_path).load_attempts()
    assert len(reloaded) == 1
    assert reloaded[0].failure_category == FailureCategory.TEMPORARY_CAS_FAILURE
    assert reloaded[0].failure_reason == "disk unavailable"
    assert reloaded[0].status == AttemptStatus.FAILED


def test_checkpoint_metadata_round_trips(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = CoordinatorStateStore(db_path)
    registry = AttemptRegistry(state_store=store)
    a = registry.create_attempt("task-1")
    registry.mark_succeeded(a.attempt_id, WorkloadExecutionResult(
        workload_id="task-1", worker_id="w-1", success=True,
        output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0,
        was_checkpointed=True, checkpoint_hash="c" * 64,
        checkpoint_runtime_type="FakeRuntime", checkpoint_format_version=1,
    ))
    store.close()

    reloaded = CoordinatorStateStore(db_path).load_attempts()[0]
    assert reloaded.checkpoint_hash == "c" * 64
    assert reloaded.checkpoint_runtime_type == "FakeRuntime"
    assert reloaded.checkpoint_format_version == 1


def test_multiple_attempts_for_same_workload_all_persist_independently(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = CoordinatorStateStore(db_path)
    registry = AttemptRegistry(state_store=store)
    registry.create_attempt("task-1")
    registry.create_attempt("task-1")
    registry.create_attempt("task-1")
    store.close()

    reloaded = CoordinatorStateStore(db_path).load_attempts()
    assert len(reloaded) == 3
    assert sorted(a.attempt_number for a in reloaded) == [1, 2, 3]
    assert all(a.workload_id == "task-1" for a in reloaded)


# ============================================================================
# Malformed row handling (mirrors the existing _safe_reconstruct pattern
# already exercised for workers/workloads/jobs/artifacts)
# ============================================================================


def test_malformed_attempt_row_is_skipped_not_fatal(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = CoordinatorStateStore(db_path)
    registry = AttemptRegistry(state_store=store)
    good = registry.create_attempt("task-good")

    # Directly corrupt a second row's status column with an invalid enum value.
    with store._lock:
        store._conn.execute("""
            INSERT INTO attempts (
                attempt_id, workload_id, attempt_number, status, worker_id,
                queued_at, assigned_at, started_at, finished_at,
                failure_category, failure_reason, execution_result_json,
                checkpoint_hash, checkpoint_runtime_type, checkpoint_format_version,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "task-bad#1", "task-bad", 1, "not-a-real-status", None,
            time.time(), None, None, None, None, None, None, None, None, None,
            time.time(),
        ))
        store._conn.commit()

    loaded = store.load_attempts()
    assert len(loaded) == 1
    assert loaded[0].attempt_id == good.attempt_id


def test_malformed_execution_result_json_is_skipped_not_fatal(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = CoordinatorStateStore(db_path)
    registry = AttemptRegistry(state_store=store)
    good = registry.create_attempt("task-good")

    with store._lock:
        store._conn.execute("""
            INSERT INTO attempts (
                attempt_id, workload_id, attempt_number, status, worker_id,
                queued_at, assigned_at, started_at, finished_at,
                failure_category, failure_reason, execution_result_json,
                checkpoint_hash, checkpoint_runtime_type, checkpoint_format_version,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "task-bad#1", "task-bad", 1, "succeeded", "w-1",
            time.time(), time.time(), time.time(), time.time(), None, None,
            "{not valid json", None, None, None, time.time(),
        ))
        store._conn.commit()

    loaded = store.load_attempts()
    assert len(loaded) == 1
    assert loaded[0].attempt_id == good.attempt_id


# ============================================================================
# Backward compatibility: pre-M10 tables are untouched
# ============================================================================


def test_pre_m10_tables_still_load_correctly_alongside_the_new_attempts_table(tmp_path: Path):
    from aidars.distributed.job_registry import CompletionPolicy, JobRecord
    from aidars.distributed.models import WorkerInfo, WorkerStatus

    db_path = tmp_path / "state.db"
    store = CoordinatorStateStore(db_path)
    store.save_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://w1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.ACTIVE, capacity_bytes=1024, used_bytes=0,
    ))
    store.save_job(JobRecord("job-1", {"task-1"}, CompletionPolicy.ALL_REQUIRED))
    registry = AttemptRegistry(state_store=store)
    registry.create_attempt("task-1")
    store.close()

    reopened = CoordinatorStateStore(db_path)
    assert len(reopened.load_workers()) == 1
    assert len(reopened.load_jobs()) == 1
    assert len(reopened.load_attempts()) == 1


# ============================================================================
# Single-writer safety lock (M10.15)
# ============================================================================


def test_second_store_on_same_path_is_refused(tmp_path: Path):
    db_path = tmp_path / "state.db"
    first = CoordinatorStateStore(db_path)
    try:
        with pytest.raises(CoordinatorAlreadyRunningError):
            CoordinatorStateStore(db_path)
    finally:
        first.close()


def test_after_close_a_new_store_can_open_the_same_path(tmp_path: Path):
    db_path = tmp_path / "state.db"
    first = CoordinatorStateStore(db_path)
    first.close()
    second = CoordinatorStateStore(db_path)  # must not raise
    second.close()


def test_different_paths_never_conflict(tmp_path: Path):
    a = CoordinatorStateStore(tmp_path / "a.db")
    b = CoordinatorStateStore(tmp_path / "b.db")  # must not raise
    a.close()
    b.close()


def test_enforce_single_writer_false_opts_out_of_the_lock(tmp_path: Path):
    """Escape hatch for callers that intentionally want multiple handles
    to the same file within one process (e.g. tests) -- explicit opt-out
    only, never the default."""
    db_path = tmp_path / "state.db"
    first = CoordinatorStateStore(db_path, enforce_single_writer=False)
    second = CoordinatorStateStore(db_path, enforce_single_writer=False)  # must not raise
    first.close()
    second.close()


def test_lock_is_released_on_del_even_without_explicit_close(tmp_path: Path):
    import gc

    db_path = tmp_path / "state.db"

    def _open_and_drop():
        store = CoordinatorStateStore(db_path)
        return None  # let it go out of scope

    _open_and_drop()
    gc.collect()

    second = CoordinatorStateStore(db_path)  # must not raise if __del__ released the lock
    second.close()
