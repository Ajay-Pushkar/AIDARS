"""Phase 5.2B: WorkerRegistry/WorkloadRegistry optional write-through
persistence via CoordinatorStateStore.

Scope reminder: this only tests that the registries correctly call (or
correctly don't call) the store. CoordinatorService startup recovery,
workload re-drive, worker recovery, and job/M8 concerns are explicitly
out of scope and not exercised here.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from aidars.distributed.models import (
    HeartbeatPayload,
    PlacementDecision,
    WorkerInfo,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


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


class _RaisingStore:
    """A minimal stand-in that raises on every write, to prove failures
    aren't swallowed. Deliberately not a real CoordinatorStateStore
    subclass -- only save_worker/save_workload are exercised by the
    registries, so duck typing is sufficient and keeps the test focused."""

    def save_worker(self, worker_info):
        raise RuntimeError("simulated disk failure")

    def save_workload(self, record):
        raise RuntimeError("simulated disk failure")


# ============================================================================
# 1 & 13. No-store mode is unchanged / requires no SQLite
# ============================================================================


def test_worker_registry_without_store_behaves_exactly_as_before():
    registry = WorkerRegistry()  # no state_store arg -- must not touch SQLite at all
    info = _make_worker_info()

    result = registry.register_worker(info)
    assert result.worker_id == "w-1"
    assert registry.has_worker("w-1")

    added = registry.add_worker_hashes("w-1", {"a" * 64})
    assert added == 1
    assert registry.get_workers_for_hash("a" * 64) == {"w-1"}


def test_workload_registry_without_store_behaves_exactly_as_before():
    registry = WorkloadRegistry()  # no state_store arg
    spec = WorkloadSpec(workload_id="task-1", task_type="test")

    record = registry.add_workload(spec)
    assert record.state == WorkloadState.SUBMITTED

    assert registry.update_state("task-1", WorkloadState.PLACED) is True
    assert registry.get_workload("task-1").state == WorkloadState.PLACED


# ============================================================================
# 3. Worker registration persists
# ============================================================================


def test_worker_registration_persists_when_store_injected(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkerRegistry(state_store=store)

    registry.register_worker(_make_worker_info(inventory_hashes={"a" * 64}))

    loaded = store.load_workers()
    assert len(loaded) == 1
    assert loaded[0].worker_id == "w-1"
    assert loaded[0].inventory_hashes == {"a" * 64}


# ============================================================================
# 4. Worker heartbeat mutation persists
# ============================================================================


def test_worker_heartbeat_persists_updated_info(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkerRegistry(state_store=store)
    registry.register_worker(_make_worker_info())

    registry.record_heartbeat("w-1", payload=HeartbeatPayload(
        worker_id="w-1", used_bytes=777, active_transfers=3,
    ), current_time=1700000000.0)

    loaded = store.load_workers()[0]
    assert loaded.last_heartbeat_utc == 1700000000.0
    assert loaded.used_bytes == 777
    assert loaded.active_transfers == 3


def test_worker_heartbeat_with_inventory_delta_persists_final_state_once(tmp_path: Path):
    """The heartbeat path calls the *_locked inventory helpers internally;
    this proves the final persisted state reflects both the heartbeat
    fields and the delta in a single consistent snapshot (not a partial
    write from a nested/mid-lock persist)."""
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkerRegistry(state_store=store)
    registry.register_worker(_make_worker_info())

    registry.record_heartbeat("w-1", payload=HeartbeatPayload(
        worker_id="w-1", used_bytes=500,
        inventory_delta_added={"b" * 64},
    ))

    loaded = store.load_workers()[0]
    assert loaded.used_bytes == 500
    assert loaded.inventory_hashes == {"b" * 64}
    assert store.load_workers()[0].inventory_hashes == {"b" * 64}  # single row, not duplicated


# ============================================================================
# 5. Inventory changes persist (add / remove / sync)
# ============================================================================


def test_add_worker_hashes_persists(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkerRegistry(state_store=store)
    registry.register_worker(_make_worker_info())

    registry.add_worker_hashes("w-1", {"c" * 64})

    assert store.load_workers()[0].inventory_hashes == {"c" * 64}


def test_remove_worker_hashes_persists(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkerRegistry(state_store=store)
    registry.register_worker(_make_worker_info(inventory_hashes={"c" * 64, "d" * 64}))

    registry.remove_worker_hashes("w-1", {"c" * 64})

    assert store.load_workers()[0].inventory_hashes == {"d" * 64}


def test_sync_worker_inventory_persists(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkerRegistry(state_store=store)
    registry.register_worker(_make_worker_info(inventory_hashes={"c" * 64}))

    added, removed = registry.sync_worker_inventory("w-1", {"d" * 64})

    assert (added, removed) == (1, 1)
    assert store.load_workers()[0].inventory_hashes == {"d" * 64}


# ============================================================================
# 6. Worker removal/eviction: documented gap, NOT implemented this phase
# ============================================================================


def test_unregister_worker_removes_persisted_row(tmp_path: Path):
    """Phase 5.2C: CoordinatorStateStore.delete_worker() closed the gap
    documented in Phase 5.2B -- unregister_worker() now removes the
    persisted row, not just the in-memory entry."""
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkerRegistry(state_store=store)
    registry.register_worker(_make_worker_info())
    assert len(store.load_workers()) == 1

    registry.unregister_worker("w-1")

    assert registry.has_worker("w-1") is False  # gone in-memory
    assert len(store.load_workers()) == 0  # gone from the store too


# ============================================================================
# 7-9. Workload add / state changes / placement persist
# ============================================================================


def test_add_workload_persists(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkloadRegistry(state_store=store)

    registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test"))

    loaded = store.load_workloads()
    assert len(loaded) == 1
    assert loaded[0].spec.workload_id == "task-1"
    assert loaded[0].state == WorkloadState.SUBMITTED


def test_update_state_persists(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkloadRegistry(state_store=store)
    registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test"))

    registry.update_state("task-1", WorkloadState.PLACING)

    assert store.load_workloads()[0].state == WorkloadState.PLACING


def test_set_placement_persists(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkloadRegistry(state_store=store)
    registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test"))

    registry.set_placement("task-1", PlacementDecision(
        workload_id="task-1", selected_worker_id="w-1", placement_score=1.0,
        score_breakdown={}, missing_assets_on_worker=set(), execution_tier="lan",
    ))

    loaded = store.load_workloads()[0]
    assert loaded.placement_decision is not None
    assert loaded.placement_decision.selected_worker_id == "w-1"


# ============================================================================
# 10-12. set_result persistence + checkpoint/MIGRATING boundary preserved
# ============================================================================


def test_set_result_completed_persists(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkloadRegistry(state_store=store)
    registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test"))

    registry.set_result("task-1", WorkloadExecutionResult(
        workload_id="task-1", worker_id="w-1", success=True,
        output_asset_hashes={"e" * 64}, execution_duration_seconds=1.0,
    ))

    loaded = store.load_workloads()[0]
    assert loaded.state == WorkloadState.COMPLETED
    assert loaded.execution_result.output_asset_hashes == {"e" * 64}


def test_set_result_failed_persists(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkloadRegistry(state_store=store)
    registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test"))

    registry.set_result("task-1", WorkloadExecutionResult(
        workload_id="task-1", worker_id="w-1", success=False,
        output_asset_hashes=set(), execution_duration_seconds=0.1,
        error_message="boom",
    ))

    loaded = store.load_workloads()[0]
    assert loaded.state == WorkloadState.FAILED
    assert loaded.execution_result.error_message == "boom"


def test_migrating_state_persists_transition_but_not_via_set_result(tmp_path: Path):
    """Preserves the Phase 5.0 semantic boundary: update_state(MIGRATING)
    persists the state transition itself, but set_result() is never
    called for a checkpointed result (that decision lives in
    WorkloadOrchestrator, already covered by
    tests/unit/test_workload_result_persistence.py). Here we confirm the
    registry-level consequence: a MIGRATING record persists with state
    MIGRATING and execution_result still None."""
    store = CoordinatorStateStore(tmp_path / "state.db")
    registry = WorkloadRegistry(state_store=store)
    registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test"))

    registry.update_state("task-1", WorkloadState.MIGRATING)

    loaded = store.load_workloads()[0]
    assert loaded.state == WorkloadState.MIGRATING
    assert loaded.execution_result is None


# ============================================================================
# 14. Persistence failures are not silently swallowed
# ============================================================================


def test_worker_registry_persistence_failure_propagates():
    registry = WorkerRegistry(state_store=_RaisingStore())

    with pytest.raises(RuntimeError, match="simulated disk failure"):
        registry.register_worker(_make_worker_info())

    # The in-memory mutation already happened before the failed persist --
    # documented, not a rollback/transaction (see final report).
    assert registry.has_worker("w-1") is True


def test_workload_registry_persistence_failure_propagates():
    registry = WorkloadRegistry(state_store=_RaisingStore())

    with pytest.raises(RuntimeError, match="simulated disk failure"):
        registry.add_workload(WorkloadSpec(workload_id="task-1", task_type="test"))

    assert registry.get_workload("task-1") is not None
