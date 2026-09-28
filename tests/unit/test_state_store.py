"""Phase 5.2A: CoordinatorStateStore round-trip fidelity tests.

Isolated tests only -- no CoordinatorService, WorkerRegistry,
WorkloadRegistry, or WorkloadOrchestrator involved. This module has no
wiring to anything yet; these tests exercise the store entirely on its
own using temporary, per-test SQLite databases.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from aidars.distributed.models import (
    PlacementDecision,
    WorkerInfo,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadState


@pytest.fixture
def store(tmp_path: Path) -> CoordinatorStateStore:
    return CoordinatorStateStore(tmp_path / "coordinator_state.db")


def _make_worker_info(worker_id: str = "w-1", **overrides) -> WorkerInfo:
    defaults = dict(
        worker_id=worker_id,
        endpoint_url="http://127.0.0.1:8001",
        ip_address="127.0.0.1",
        port=8001,
        status=WorkerStatus.ACTIVE,
        capacity_bytes=4096,
        used_bytes=1024,
        inventory_hashes={"a" * 64, "b" * 64},
        last_heartbeat_utc=1700000000.123,
        tags={"role": "compute"},
        can_execute_workloads=True,
    )
    defaults.update(overrides)
    return WorkerInfo(**defaults)


def _make_spec(workload_id: str = "task-1", **overrides) -> WorkloadSpec:
    defaults = dict(
        workload_id=workload_id,
        task_type="blender_render",
        min_ram_bytes=2048,
        parameters={
            "input_path": "/scenes/scene.blend",
            "frame_start": 1,
            "frame_end": 125,
            "chunk_index": 0,
            "job_id": "future-m8-job-id",  # must survive untouched today
            "nested": {"a": [1, 2, {"b": True}], "c": None},
        },
    )
    defaults.update(overrides)
    return WorkloadSpec(**defaults)


# ============================================================================
# 1. Schema initialization
# ============================================================================


def test_schema_initializes_successfully(tmp_path: Path):
    db_path = tmp_path / "fresh.db"
    assert not db_path.exists()

    store = CoordinatorStateStore(db_path)

    assert db_path.exists()
    assert store.load_workers() == []
    assert store.load_workloads() == []


# ============================================================================
# 2. WorkerInfo round-trip
# ============================================================================


def test_worker_info_round_trip_preserves_all_relevant_fields(store: CoordinatorStateStore):
    info = _make_worker_info()

    store.save_worker(info)
    loaded = store.load_workers()

    assert len(loaded) == 1
    result = loaded[0]
    assert result.worker_id == info.worker_id
    assert result.endpoint_url == info.endpoint_url
    assert result.ip_address == info.ip_address
    assert result.port == info.port
    assert result.status == info.status
    assert result.capacity_bytes == info.capacity_bytes
    assert result.used_bytes == info.used_bytes
    assert result.inventory_hashes == info.inventory_hashes
    assert result.last_heartbeat_utc == info.last_heartbeat_utc
    assert result.tags == info.tags
    assert result.can_execute_workloads == info.can_execute_workloads


# ============================================================================
# 3-5. WorkloadRecord round-trip (spec only, +placement, +execution_result)
# ============================================================================


def test_workload_record_with_spec_only_round_trips(store: CoordinatorStateStore):
    spec = _make_spec()
    record = WorkloadRecord(spec)

    store.save_workload(record)
    loaded = store.load_workloads()

    assert len(loaded) == 1
    result = loaded[0]
    assert result.spec.workload_id == spec.workload_id
    assert result.spec.task_type == spec.task_type
    assert result.spec.parameters == spec.parameters
    assert result.state == WorkloadState.SUBMITTED
    assert result.placement_decision is None
    assert result.execution_result is None
    assert result.error_message is None
    assert result.completed_at is None


def test_workload_record_with_placement_decision_round_trips(store: CoordinatorStateStore):
    spec = _make_spec(workload_id="task-placed")
    record = WorkloadRecord(spec)
    record.state = WorkloadState.PLACED
    record.placement_decision = PlacementDecision(
        workload_id=spec.workload_id,
        selected_worker_id="w-1",
        placement_score=12.34,
        score_breakdown={"compute": 3.6, "memory": 8.0},
        missing_assets_on_worker={"c" * 64},
        execution_tier="lan",
    )

    store.save_workload(record)
    loaded = store.load_workloads()[0]

    assert loaded.state == WorkloadState.PLACED
    assert loaded.placement_decision is not None
    assert loaded.placement_decision.selected_worker_id == "w-1"
    assert loaded.placement_decision.placement_score == 12.34
    assert loaded.placement_decision.score_breakdown == {"compute": 3.6, "memory": 8.0}
    assert loaded.placement_decision.missing_assets_on_worker == {"c" * 64}
    assert loaded.placement_decision.execution_tier == "lan"


def test_workload_record_with_execution_result_round_trips(store: CoordinatorStateStore):
    spec = _make_spec(workload_id="task-completed")
    record = WorkloadRecord(spec)
    record.state = WorkloadState.COMPLETED
    record.completed_at = 1700000500.0
    record.execution_result = WorkloadExecutionResult(
        workload_id=spec.workload_id,
        worker_id="w-1",
        success=True,
        output_asset_hashes={"d" * 64, "e" * 64},
        execution_duration_seconds=42.5,
        stdout_snippet="render ok",
    )

    store.save_workload(record)
    loaded = store.load_workloads()[0]

    assert loaded.state == WorkloadState.COMPLETED
    assert loaded.execution_result is not None
    assert loaded.execution_result.success is True
    assert loaded.execution_result.output_asset_hashes == {"d" * 64, "e" * 64}
    assert loaded.execution_result.execution_duration_seconds == 42.5
    assert loaded.execution_result.stdout_snippet == "render ok"


# ============================================================================
# 6. output_asset_hashes survive exactly as a set
# ============================================================================


def test_output_asset_hashes_survive_as_a_set(store: CoordinatorStateStore):
    spec = _make_spec(workload_id="task-hashes")
    record = WorkloadRecord(spec)
    hashes = {"f" * 64, "1" * 64, "9" * 64}
    record.execution_result = WorkloadExecutionResult(
        workload_id=spec.workload_id, worker_id="w-1", success=True,
        output_asset_hashes=hashes, execution_duration_seconds=1.0,
    )

    store.save_workload(record)
    loaded = store.load_workloads()[0]

    assert isinstance(loaded.execution_result.output_asset_hashes, set)
    assert loaded.execution_result.output_asset_hashes == hashes


# ============================================================================
# 7. Failed execution result preserves error_message/stderr/stdout
# ============================================================================


def test_failed_execution_result_preserves_error_details(store: CoordinatorStateStore):
    spec = _make_spec(workload_id="task-failed")
    record = WorkloadRecord(spec)
    record.state = WorkloadState.FAILED
    record.error_message = "render crashed"
    record.execution_result = WorkloadExecutionResult(
        workload_id=spec.workload_id, worker_id="w-1", success=False,
        output_asset_hashes=set(), execution_duration_seconds=0.3,
        error_message="render crashed",
        stdout_snippet="starting...",
        stderr_snippet="Traceback (most recent call last): ...",
    )

    store.save_workload(record)
    loaded = store.load_workloads()[0]

    assert loaded.state == WorkloadState.FAILED
    assert loaded.error_message == "render crashed"
    assert loaded.execution_result.success is False
    assert loaded.execution_result.error_message == "render crashed"
    assert loaded.execution_result.stdout_snippet == "starting..."
    assert loaded.execution_result.stderr_snippet == "Traceback (most recent call last): ..."


# ============================================================================
# 8. Timestamps survive with acceptable precision
# ============================================================================


def test_timestamps_survive_round_trip(store: CoordinatorStateStore):
    spec = _make_spec(workload_id="task-timestamps")
    record = WorkloadRecord(spec)
    record.submitted_at = 1700000000.123456
    record.completed_at = 1700000042.654321

    store.save_workload(record)
    loaded = store.load_workloads()[0]

    assert loaded.submitted_at == pytest.approx(record.submitted_at, abs=1e-6)
    assert loaded.completed_at == pytest.approx(record.completed_at, abs=1e-6)


# ============================================================================
# 9. None optional fields remain None
# ============================================================================


def test_none_optional_fields_remain_none(store: CoordinatorStateStore):
    spec = _make_spec(workload_id="task-nones")
    record = WorkloadRecord(spec)  # placement_decision, execution_result, error_message, completed_at all None

    store.save_workload(record)
    loaded = store.load_workloads()[0]

    assert loaded.placement_decision is None
    assert loaded.execution_result is None
    assert loaded.error_message is None
    assert loaded.completed_at is None


# ============================================================================
# 10. Repeated save updates rather than duplicates
# ============================================================================


def test_repeated_save_worker_updates_not_duplicates(store: CoordinatorStateStore):
    info_v1 = _make_worker_info(used_bytes=100)
    info_v2 = _make_worker_info(used_bytes=999)

    store.save_worker(info_v1)
    store.save_worker(info_v2)

    loaded = store.load_workers()
    assert len(loaded) == 1
    assert loaded[0].used_bytes == 999


def test_repeated_save_workload_updates_not_duplicates(store: CoordinatorStateStore):
    spec = _make_spec(workload_id="task-upsert")
    record = WorkloadRecord(spec)
    store.save_workload(record)

    record.state = WorkloadState.COMPLETED
    record.execution_result = WorkloadExecutionResult(
        workload_id=spec.workload_id, worker_id="w-1", success=True,
        output_asset_hashes=set(), execution_duration_seconds=1.0,
    )
    store.save_workload(record)

    loaded = store.load_workloads()
    assert len(loaded) == 1
    assert loaded[0].state == WorkloadState.COMPLETED
    assert loaded[0].execution_result is not None


# ============================================================================
# 11. WorkloadSpec.parameters survives arbitrary nested JSON, incl. job_id
# ============================================================================


def test_workload_spec_parameters_survive_arbitrary_nested_json(store: CoordinatorStateStore):
    spec = _make_spec(workload_id="task-params")
    record = WorkloadRecord(spec)

    store.save_workload(record)
    loaded = store.load_workloads()[0]

    assert loaded.spec.parameters == spec.parameters
    assert loaded.spec.parameters["job_id"] == "future-m8-job-id"
    assert loaded.spec.parameters["nested"] == {"a": [1, 2, {"b": True}], "c": None}


# ============================================================================
# 12. Multiple workers/workloads stored and loaded
# ============================================================================


def test_multiple_workers_and_workloads_stored_and_loaded(store: CoordinatorStateStore):
    for i in range(3):
        store.save_worker(_make_worker_info(worker_id=f"w-{i}"))
    for i in range(4):
        store.save_workload(WorkloadRecord(_make_spec(workload_id=f"task-{i}")))

    workers = store.load_workers()
    workloads = store.load_workloads()

    assert {w.worker_id for w in workers} == {"w-0", "w-1", "w-2"}
    assert {r.spec.workload_id for r in workloads} == {"task-0", "task-1", "task-2", "task-3"}
