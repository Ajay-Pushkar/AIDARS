"""M10.6/M10.18/M10.19: worker-failure recovery and coordinator-restart
recovery for the new Attempt model, integrated into the EXISTING
heartbeat/eviction/restore machinery (not a second recovery engine).
"""
from __future__ import annotations

import time
from pathlib import Path

import httpx
import pytest

from aidars.distributed.attempt import AttemptRegistry, AttemptStatus
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    HeartbeatPayload,
    WorkerInfo,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
    WorkerResourceProfile,
)
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadState


# ============================================================================
# Live worker eviction -> attempt reclassification (M10.6)
# ============================================================================


def _make_worker_info(worker_id: str = "w-1", status: WorkerStatus = WorkerStatus.ACTIVE) -> WorkerInfo:
    return WorkerInfo(
        worker_id=worker_id, endpoint_url=f"http://{worker_id}", ip_address="127.0.0.1", port=8001,
        status=status, capacity_bytes=4096, used_bytes=0, last_heartbeat_utc=time.time(),
    )


def _resource_profile(worker_id: str = "w-1") -> WorkerResourceProfile:
    return WorkerResourceProfile(
        worker_id=worker_id,
        endpoint_url=f"http://{worker_id}",
        ip_address="127.0.0.1",
        cpu_cores_total=4,
        cpu_utilization_percent=0.0,
        ram_total_bytes=8 * 1024**3,
        ram_available_bytes=8 * 1024**3,
        gpu_available=False,
        vram_total_bytes=0,
        vram_available_bytes=0,
        timestamp_utc=time.time(),
    )


def test_handle_worker_lost_marks_running_and_assigned_attempts_as_lost():
    service = CoordinatorService()
    service.registry.register_worker(_make_worker_info("w-1"))

    a1 = service.attempt_registry.create_attempt("task-1")
    service.attempt_registry.mark_assigned(a1.attempt_id, "w-1")
    service.attempt_registry.mark_running(a1.attempt_id)

    a2 = service.attempt_registry.create_attempt("task-2")
    service.attempt_registry.mark_assigned(a2.attempt_id, "w-1")
    # a2 stays ASSIGNED (never reached RUNNING) -- must also be reclassified.

    a3 = service.attempt_registry.create_attempt("task-3")
    service.attempt_registry.mark_assigned(a3.attempt_id, "w-OTHER")
    service.attempt_registry.mark_running(a3.attempt_id)
    # a3 belongs to a DIFFERENT worker -- must be unaffected.

    service.orchestrator.handle_worker_lost("w-1")

    assert service.attempt_registry.get_attempt(a1.attempt_id).status == AttemptStatus.LOST
    assert service.attempt_registry.get_attempt(a2.attempt_id).status == AttemptStatus.LOST
    assert service.attempt_registry.get_attempt(a3.attempt_id).status == AttemptStatus.RUNNING


def test_handle_worker_lost_is_a_no_op_without_an_attempt_registry():
    """Orchestrators built without attempt tracking (pre-M10 callers)
    must not error when a worker is lost."""
    from aidars.distributed.registry import WorkerRegistry
    from aidars.distributed.workload import WorkloadOrchestrator
    from aidars.distributed.workload_registry import WorkloadRegistry

    registry = WorkerRegistry()
    orchestrator = WorkloadOrchestrator(registry=registry, workload_registry=WorkloadRegistry())
    orchestrator.handle_worker_lost("w-nonexistent")  # must not raise


@pytest.mark.asyncio
async def test_eviction_loop_calls_handle_worker_lost_for_expired_workers():
    service = CoordinatorService(heartbeat_timeout_seconds=0.05, eviction_interval_seconds=0.02)
    service.registry.register_worker(_make_worker_info("w-1"))
    service.registry._workers["w-1"].last_heartbeat_utc = time.time() - 10.0  # force-expire

    attempt = service.attempt_registry.create_attempt("task-1")
    service.attempt_registry.mark_assigned(attempt.attempt_id, "w-1")
    service.attempt_registry.mark_running(attempt.attempt_id)

    service._running = True
    import asyncio
    task = asyncio.create_task(service._run_eviction_loop())
    await asyncio.sleep(0.15)
    service._running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert service.registry.has_worker("w-1") is False
    assert service.attempt_registry.get_attempt(attempt.attempt_id).status == AttemptStatus.LOST


# ============================================================================
# Coordinator restart recovery: attempts restored, in-flight ones LOST
# ============================================================================


def test_restart_restores_terminal_attempts_unchanged(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    attempt_registry = AttemptRegistry(state_store=store)
    a = attempt_registry.create_attempt("task-1")
    attempt_registry.mark_assigned(a.attempt_id, "w-1")
    attempt_registry.mark_running(a.attempt_id)
    attempt_registry.mark_succeeded(a.attempt_id, WorkloadExecutionResult(
        workload_id="task-1", worker_id="w-1", success=True,
        output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0,
    ))
    store.close()

    service = CoordinatorService(state_store=CoordinatorStateStore(tmp_path / "state.db"))
    service._restore_persisted_state()

    restored = service.attempt_registry.get_attempt(a.attempt_id)
    assert restored.status == AttemptStatus.SUCCEEDED  # unchanged -- already terminal


def test_restart_marks_in_flight_attempts_as_lost(tmp_path: Path):
    """The M10.6-required scenario: the coordinator crashed while an
    attempt was RUNNING. The worker may have actually finished it, but
    the coordinator has no way to know -- restart must not leave it
    looking perpetually 'in progress'."""
    store = CoordinatorStateStore(tmp_path / "state.db")
    attempt_registry = AttemptRegistry(state_store=store)
    a = attempt_registry.create_attempt("task-1")
    attempt_registry.mark_assigned(a.attempt_id, "w-1")
    attempt_registry.mark_running(a.attempt_id)  # crash happens here -- never resolved
    store.close()

    service = CoordinatorService(state_store=CoordinatorStateStore(tmp_path / "state.db"))
    service._restore_persisted_state()

    restored = service.attempt_registry.get_attempt(a.attempt_id)
    assert restored.status == AttemptStatus.LOST
    assert restored.failure_category is not None
    assert restored.finished_at is not None


def test_restart_marks_merely_assigned_attempts_as_lost_too(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    attempt_registry = AttemptRegistry(state_store=store)
    a = attempt_registry.create_attempt("task-1")
    attempt_registry.mark_assigned(a.attempt_id, "w-1")  # crash before RUNNING was ever set
    store.close()

    service = CoordinatorService(state_store=CoordinatorStateStore(tmp_path / "state.db"))
    service._restore_persisted_state()

    assert service.attempt_registry.get_attempt(a.attempt_id).status == AttemptStatus.LOST


def _make_workload_record(workload_id: str, state: WorkloadState) -> WorkloadRecord:
    spec = WorkloadSpec(workload_id=workload_id, task_type="test", min_ram_bytes=1024)
    record = WorkloadRecord(spec)
    record.state = state
    return record


@pytest.mark.asyncio
async def test_redrive_after_restart_creates_a_new_attempt_not_a_new_workload(tmp_path: Path):
    """End-to-end: a coordinator restart redrives a non-terminal
    workload through the existing _process_workload() dispatcher, and
    the result is a NEW, correctly-numbered attempt for the SAME
    workload_id -- never a second logical workload.

    Uses pytest's tmp_path fixture rather than a manual
    tempfile.TemporaryDirectory(), matching the convention every other
    test in this suite already uses: `service`'s CoordinatorStateStore
    is intentionally left open for the rest of the test (a coordinator
    keeps its state store open for its whole process lifetime; nothing
    here calls service.stop()/close()), and a bare
    tempfile.TemporaryDirectory() tries to remove its directory
    synchronously while that connection is still live -- on Windows,
    SQLite's VFS opens the file without FILE_SHARE_DELETE, so directory
    removal fails with WinError 32 as long as the connection is open
    (harmless on POSIX, which allows unlinking open files). tmp_path
    defers cleanup past the end of the test function, avoiding the race
    entirely -- this is a test resource-management fix, not a change to
    what the test verifies.
    """
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        result = WorkloadExecutionResult(
            workload_id="task-1", worker_id="w-1", success=True,
            output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    db_path = tmp_path / "state.db"
    store = CoordinatorStateStore(db_path)
    store.save_workload(_make_workload_record("task-1", WorkloadState.PLACED))
    attempt_registry = AttemptRegistry(state_store=store)
    stale = attempt_registry.create_attempt("task-1")
    attempt_registry.mark_assigned(stale.attempt_id, "w-1")
    attempt_registry.mark_running(stale.attempt_id)
    worker = _make_worker_info("w-1")
    worker.resource_profile = _resource_profile("w-1")
    store.save_worker(worker)
    store.close()

    service = CoordinatorService(state_store=CoordinatorStateStore(db_path))
    service.orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    pending = service._restore_persisted_state()

    assert pending == ["task-1"]
    assert service.attempt_registry.get_attempt(stale.attempt_id).status == AttemptStatus.LOST

    service.registry.record_heartbeat("w-1", payload=HeartbeatPayload(
        worker_id="w-1",
        resource_profile=_resource_profile("w-1"),
    ))
    profiles = service.orchestrator._build_worker_profiles(
    service.registry.list_workers(active_only=False)
)

    decision = service.orchestrator.placement_engine.evaluate(
        service.workload_registry.get_workload("task-1").spec,
        profiles,
        worker_tags={
            w.worker_id: dict(w.tags)
            for w in service.registry.list_workers(active_only=False)
        },
    )

    print("PROFILES:", profiles)
    print("EVALUATION:", service.orchestrator.placement_engine.last_evaluation)
    print("DECISION:", decision)
    await service.orchestrator._process_workload("task-1")

    assert call_count == 1
    record = service.workload_registry.get_workload("task-1")
    assert record.state == WorkloadState.COMPLETED
    assert record.spec.workload_id == "task-1"  # stable workload_id

    attempts = service.attempt_registry.list_attempts_for_workload("task-1")
    assert len(attempts) == 2  # the restored (now LOST) one, plus the new one
    assert attempts[0].attempt_id == stale.attempt_id
    assert attempts[0].status == AttemptStatus.LOST
    assert attempts[1].status == AttemptStatus.SUCCEEDED
    assert attempts[1].attempt_number == 2

    service.state_store.close()


# ============================================================================
# Duplicate execution under at-least-once (M10.6's explicit test scenario)
# ============================================================================


@pytest.mark.asyncio
async def test_duplicate_execution_of_identical_output_remains_semantically_correct():
    """Worker completes work but the coordinator doesn't observe it
    (simulated: two independent dispatches both 'succeed' and report the
    SAME content-addressed output hash). CAS content-addressing makes
    the duplicate write a no-op; result correctness must not depend on
    only ever executing once."""
    from aidars.distributed.cas_adapter import LocalCASAdapter
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        cas = LocalCASAdapter(cas_dir=Path(td) / "cas")
        h1 = cas.store_bytes(b"deterministic render output")
        h2 = cas.store_bytes(b"deterministic render output")  # simulates a second, redundant execution
        assert h1 == h2
        assert cas.has_asset(h1)
