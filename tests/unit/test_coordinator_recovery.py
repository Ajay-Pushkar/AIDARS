"""Phase 5.2C: coordinator startup recovery + persistent worker lifecycle.

Covers:
 - CoordinatorStateStore.delete_worker() (closes the Phase 5.2B gap).
 - WorkerRegistry.unregister_worker()/evict_expired_workers() now remove
   the persisted row, not just the in-memory entry.
 - CoordinatorService._restore_persisted_state(): loads persisted workers
   as OFFLINE/unverified and persisted workloads as-is, and identifies
   which non-terminal workloads need re-driving.
 - CoordinatorService._redrive_recovered_workloads(): re-dispatches
   through the existing WorkloadOrchestrator._process_workload() path
   (no second dispatcher), with AT-LEAST-ONCE semantics.

Scope reminder: M8/job_id/RenderJobRecord, exactly-once execution, HA
coordinator, and distributed transactions are not exercised here.
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path

import httpx
import pytest

from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    HeartbeatPayload,
    WorkerInfo,
    WorkerRegistrationPayload,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadRegistry, WorkloadState


def _make_worker_info(worker_id: str = "w-1", **overrides) -> WorkerInfo:
    defaults = dict(
        worker_id=worker_id,
        endpoint_url="http://127.0.0.1:8001",
        ip_address="127.0.0.1",
        port=8001,
        status=WorkerStatus.ACTIVE,
        capacity_bytes=4096,
        used_bytes=0,
        inventory_hashes=set(),
    )
    defaults.update(overrides)
    return WorkerInfo(**defaults)


def _make_record(workload_id: str, state: WorkloadState, **result_kwargs) -> WorkloadRecord:
    spec = WorkloadSpec(workload_id=workload_id, task_type="test", min_ram_bytes=1024)
    record = WorkloadRecord(spec)
    record.state = state
    if result_kwargs:
        record.execution_result = WorkloadExecutionResult(
            workload_id=workload_id, worker_id="w-1", **result_kwargs
        )
    return record


# ============================================================================
# 1. delete_worker persistence
# ============================================================================


def test_delete_worker_removes_persisted_row(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_worker(_make_worker_info())
    assert len(store.load_workers()) == 1

    store.delete_worker("w-1")

    assert store.load_workers() == []


def test_delete_worker_nonexistent_is_a_no_op(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.delete_worker("does-not-exist")  # must not raise
    assert store.load_workers() == []


# ============================================================================
# 2 & 3. unregister_worker / evict_expired_workers remove persisted worker
# ============================================================================


def test_unregister_worker_removes_persisted_row(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkerRegistry(state_store=store)
    registry.register_worker(_make_worker_info())
    assert len(store.load_workers()) == 1

    registry.unregister_worker("w-1")

    assert len(store.load_workers()) == 0


def test_evict_expired_workers_removes_persisted_row(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkerRegistry(state_store=store)
    registry.register_worker(_make_worker_info())
    registry.record_heartbeat("w-1", current_time=100.0)
    assert len(store.load_workers()) == 1

    evicted = registry.evict_expired_workers(timeout_seconds=15.0, current_time=200.0)

    assert evicted == ["w-1"]
    assert store.load_workers() == []


def test_no_store_mode_unregister_and_evict_unchanged():
    """13/15-style guard: without a store, unregister/evict behave exactly
    as before -- no SQLite dependency, no errors."""
    registry = WorkerRegistry()
    registry.register_worker(_make_worker_info())
    registry.record_heartbeat("w-1", current_time=100.0)

    assert registry.unregister_worker("w-1") is not None
    registry.register_worker(_make_worker_info())
    registry.record_heartbeat("w-1", current_time=100.0)
    assert registry.evict_expired_workers(timeout_seconds=15.0, current_time=200.0) == ["w-1"]


# ============================================================================
# 4/5/6. Fresh coordinator loads persisted workers; restored workers are
# unverified until a fresh heartbeat or registration.
# ============================================================================


def test_fresh_coordinator_loads_persisted_workers(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_worker(_make_worker_info(status=WorkerStatus.ACTIVE))

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()

    assert service.registry.has_worker("w-1")


def test_restored_worker_is_not_immediately_trusted(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_worker(_make_worker_info(status=WorkerStatus.ACTIVE))

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()

    restored = service.registry.get_worker("w-1")
    assert restored.status == WorkerStatus.OFFLINE
    # Excluded from the same active_only listing WorkloadOrchestrator uses
    # to build placement candidates, and from locate_assets_sync's scan.
    assert "w-1" not in {w.worker_id for w in service.registry.list_workers(active_only=True)}


def test_restored_worker_heartbeat_grace_window_survives_first_eviction_pass(tmp_path: Path):
    """The persisted last_heartbeat_utc is stale (pre-crash); restoring it
    unmodified would let the very next eviction pass purge the worker
    before it can prove liveness. Restore must reset the grace window."""
    store = CoordinatorStateStore(tmp_path / "state.db")
    stale_info = _make_worker_info(status=WorkerStatus.ACTIVE)
    stale_info.last_heartbeat_utc = time.time() - 10_000  # long "crashed" gap
    store.save_worker(stale_info)

    service = CoordinatorService(state_store=store, heartbeat_timeout_seconds=15.0)
    service._restore_persisted_state()

    evicted = service.registry.evict_expired_workers(timeout_seconds=15.0)
    assert evicted == []
    assert service.registry.has_worker("w-1")


def test_fresh_heartbeat_makes_restored_worker_usable_again(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_worker(_make_worker_info(status=WorkerStatus.ACTIVE))

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()
    assert service.registry.get_worker("w-1").status == WorkerStatus.OFFLINE

    service.registry.record_heartbeat("w-1", payload=HeartbeatPayload(worker_id="w-1"))

    revived = service.registry.get_worker("w-1")
    assert revived.status == WorkerStatus.ACTIVE
    assert "w-1" in {w.worker_id for w in service.registry.list_workers(active_only=True)}


def test_fresh_registration_makes_restored_worker_usable_again(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_worker(_make_worker_info(status=WorkerStatus.ACTIVE))

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()
    assert service.registry.get_worker("w-1").status == WorkerStatus.OFFLINE

    service.register_worker_sync(WorkerRegistrationPayload(
        worker_id="w-1", endpoint_url="http://127.0.0.1:8001",
        ip_address="127.0.0.1", port=8001,
    ))

    assert service.registry.get_worker("w-1").status == WorkerStatus.ACTIVE


# ============================================================================
# 7-11. Fresh coordinator loads persisted workload records; terminal states
# are not re-driven; non-terminal states are.
# ============================================================================


def test_fresh_coordinator_loads_persisted_workload_records(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_workload(_make_record("task-1", WorkloadState.PLACED))

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()

    record = service.workload_registry.get_workload("task-1")
    assert record is not None
    assert record.state == WorkloadState.PLACED


@pytest.mark.parametrize("state", [WorkloadState.COMPLETED, WorkloadState.FAILED, WorkloadState.UNSCHEDULABLE])
def test_terminal_workload_states_are_not_re_driven(tmp_path: Path, state: WorkloadState):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_workload(_make_record("task-1", state))

    service = CoordinatorService(state_store=store)
    pending = service._restore_persisted_state()

    assert pending == []
    assert service.workload_registry.get_workload("task-1").state == state


@pytest.mark.parametrize(
    "state",
    [WorkloadState.SUBMITTED, WorkloadState.VALIDATING, WorkloadState.PLACING,
     WorkloadState.PLACED, WorkloadState.MIGRATING],
)
def test_non_terminal_workload_states_are_re_driven(tmp_path: Path, state: WorkloadState):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_workload(_make_record("task-1", state))

    service = CoordinatorService(state_store=store)
    pending = service._restore_persisted_state()

    assert pending == ["task-1"]


# ============================================================================
# 12/16. Recovered workload goes through the existing placement path, and
# is re-dispatched rather than assumed complete (at-least-once).
# ============================================================================


@pytest.mark.asyncio
async def test_recovered_workload_dispatches_through_existing_placement_path(tmp_path: Path):
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        result = WorkloadExecutionResult(
            workload_id="task-1", worker_id="w-1", success=True,
            output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    store = CoordinatorStateStore(tmp_path / "state.db")
    # Persisted as PLACED -- i.e. dispatch was in flight when the
    # coordinator crashed. The worker may have actually finished the work;
    # the coordinator has no way to know that.
    store.save_workload(_make_record("task-1", WorkloadState.PLACED))
    store.save_worker(_make_worker_info(status=WorkerStatus.ACTIVE))

    service = CoordinatorService(state_store=store)
    service.orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    pending = service._restore_persisted_state()
    assert pending == ["task-1"]
    # Restored worker starts OFFLINE/unverified...
    assert service.registry.get_worker("w-1").status == WorkerStatus.OFFLINE
    # ...and only becomes eligible once it proves liveness, exactly as a
    # real worker process would after reconnecting to the restarted
    # coordinator: a fresh heartbeat.
    service.registry.record_heartbeat("w-1", payload=HeartbeatPayload(worker_id="w-1"))
    assert service.registry.get_worker("w-1").status == WorkerStatus.ACTIVE

    await service.orchestrator._process_workload("task-1")

    assert call_count == 1, "recovery must dispatch again -- it cannot assume the prior execution completed"
    record = service.workload_registry.get_workload("task-1")
    assert record.state == WorkloadState.COMPLETED
    assert record.placement_decision is not None
    assert record.placement_decision.selected_worker_id == "w-1"


# ============================================================================
# 13. Recovery does not create a second, independent dispatch mechanism.
# ============================================================================


@pytest.mark.asyncio
async def test_redrive_delegates_to_existing_process_workload(tmp_path: Path, monkeypatch):
    store = CoordinatorStateStore(tmp_path / "state.db")
    service = CoordinatorService(state_store=store)

    calls = []

    async def fake_process_workload(workload_id: str) -> None:
        calls.append(workload_id)

    monkeypatch.setattr(service.orchestrator, "_process_workload", fake_process_workload)

    service._redrive_recovered_workloads(["task-1", "task-2"])
    await asyncio.sleep(0)  # let the fire-and-forget tasks run once

    assert sorted(calls) == ["task-1", "task-2"]


# ============================================================================
# 14. Recovery preserves WorkloadSpec and execution-result data.
# ============================================================================


def test_restore_preserves_spec_and_execution_result_data(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    output_hashes = {"e" * 64, "f" * 64}
    record = _make_record(
        "task-1", WorkloadState.COMPLETED,
        success=True, output_asset_hashes=output_hashes,
        execution_duration_seconds=42.5, stdout_snippet="render ok",
    )
    store.save_workload(record)

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()

    restored = service.workload_registry.get_workload("task-1")
    assert restored.spec.workload_id == "task-1"
    assert restored.spec.task_type == "test"
    assert restored.execution_result is not None
    assert restored.execution_result.output_asset_hashes == output_hashes
    assert restored.execution_result.execution_duration_seconds == 42.5
    assert restored.execution_result.stdout_snippet == "render ok"


# ============================================================================
# 15. No store configured preserves existing behavior.
# ============================================================================


@pytest.mark.asyncio
async def test_no_state_store_preserves_existing_start_stop_behavior():
    service = CoordinatorService(coordinator_id="no-store-coord", eviction_interval_seconds=0.05)
    assert service.state_store is None

    pending = service._restore_persisted_state()
    assert pending == []

    await service.start()
    try:
        assert service.registry.get_worker_count() == 0
        assert service.workload_registry.list_workloads() == []
    finally:
        await service.stop()


# ============================================================================
# 17. Documentation clearly reflects at-least-once semantics.
# ============================================================================


def test_redrive_documents_at_least_once_semantics():
    doc = CoordinatorService._redrive_recovered_workloads.__doc__ or ""
    assert "AT-LEAST-ONCE" in doc
    assert "exactly-once" in doc.lower()
