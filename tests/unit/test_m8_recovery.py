"""M8.10: coordinator recovery extended for Jobs/Artifacts.

Extends Phase 5.2C's recovery model (CoordinatorService._restore_persisted_state)
without touching its worker/workload semantics -- those are covered by
tests/unit/test_coordinator_recovery.py and are not re-tested here except
where a Job now sits alongside them.
"""
from __future__ import annotations

import time
from pathlib import Path

import httpx
import pytest

from aidars.distributed.artifact import ArtifactLifecycleState
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.job_registry import CompletionPolicy, JobState
from aidars.distributed.models import (
    HeartbeatPayload,
    WorkerInfo,
    WorkerResourceProfile,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadState


def _record(workload_id: str, job_id: str, state: WorkloadState, **result_kwargs) -> WorkloadRecord:
    spec = WorkloadSpec(workload_id=workload_id, job_id=job_id, task_type="test", min_ram_bytes=1024)
    record = WorkloadRecord(spec)
    record.state = state
    if result_kwargs:
        record.execution_result = WorkloadExecutionResult(
            workload_id=workload_id, worker_id="w-1", **result_kwargs,
        )
    return record


def _worker(worker_id="w-1"):
    return WorkerInfo(
        worker_id=worker_id, endpoint_url="http://127.0.0.1:8001", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.ACTIVE, capacity_bytes=999999999, used_bytes=0,
        resource_profile=WorkerResourceProfile(
            worker_id=worker_id,
            endpoint_url="http://127.0.0.1:8001",
            ip_address="127.0.0.1",
            cpu_cores_total=4,
            cpu_utilization_percent=0.0,
            ram_total_bytes=8 * 1024**3,
            ram_available_bytes=8 * 1024**3,
            gpu_available=False,
            vram_total_bytes=0,
            vram_available_bytes=0,
            timestamp_utc=time.time(),
        ),
    )


# ============================================================================
# Jobs are reconstructed correctly on restart
# ============================================================================


def test_fresh_coordinator_restores_job_membership(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_job(__import__(
        "aidars.distributed.job_registry", fromlist=["JobRecord"]
    ).JobRecord("job-1", {"c0", "c1"}, CompletionPolicy.ALL_REQUIRED))

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()

    job = service.job_registry.get_job("job-1")
    assert job is not None
    assert job.workload_ids == {"c0", "c1"}
    assert job.completion_policy == CompletionPolicy.ALL_REQUIRED


def test_job_aggregate_reconstructed_from_restored_workloads(tmp_path: Path):
    """M8.10: 'reconstruct Job aggregate' requires no separate step --
    once membership + workloads are both restored, get_aggregate() is
    already correct because it's derived, never stored."""
    store = CoordinatorStateStore(tmp_path / "state.db")
    from aidars.distributed.job_registry import JobRecord
    store.save_job(JobRecord("job-1", {"c0", "c1"}, CompletionPolicy.ALL_REQUIRED))
    store.save_workload(_record("c0", "job-1", WorkloadState.COMPLETED,
                                 success=True, output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0))
    store.save_workload(_record("c1", "job-1", WorkloadState.PLACED))

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()

    agg = service.job_registry.get_aggregate("job-1")
    assert agg.completed == 1
    assert agg.running == 1
    assert agg.state == JobState.RUNNING


# ============================================================================
# Artifact relationships survive restart
# ============================================================================


def test_artifact_relationships_survive_restart(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    from aidars.distributed.artifact import Artifact, ArtifactVerificationState
    store.save_artifact(Artifact(
        artifact_id="c0:" + "a" * 64, content_hash="a" * 64,
        producer_workload_id="c0", producer_job_id="job-1",
        created_at=1700000000.0, verification_state=ArtifactVerificationState.VERIFIED,
        lifecycle_state=ArtifactLifecycleState.AVAILABLE, storage_location="cas", size_bytes=42,
    ))

    service = CoordinatorService(state_store=store)
    service._restore_persisted_state()

    restored = service.artifact_registry.get_artifact("c0:" + "a" * 64)
    assert restored is not None
    assert restored.producer_job_id == "job-1"
    assert restored.size_bytes == 42
    assert restored.lifecycle_state == ArtifactLifecycleState.AVAILABLE


# ============================================================================
# Terminal workloads not rerun; non-terminal redriven (Job-aware end-to-end)
# ============================================================================


@pytest.mark.asyncio
async def test_recovered_job_terminal_workload_not_rerun(tmp_path: Path):
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="c0", worker_id="w-1", success=True,
            output_asset_hashes=set(), execution_duration_seconds=1.0,
        ).model_dump(mode="json"))

    store = CoordinatorStateStore(tmp_path / "state.db")
    from aidars.distributed.job_registry import JobRecord
    store.save_job(JobRecord("job-1", {"c0"}, CompletionPolicy.ALL_REQUIRED))
    store.save_workload(_record("c0", "job-1", WorkloadState.COMPLETED,
                                 success=True, output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0))

    service = CoordinatorService(state_store=store)
    service.orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    pending = service._restore_persisted_state()

    assert pending == []  # c0 is COMPLETED -- not queued for re-drive
    assert call_count == 0


@pytest.mark.asyncio
async def test_recovered_job_non_terminal_workload_redriven_through_existing_dispatcher(tmp_path: Path):
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="c0", worker_id="w-1", success=True,
            output_asset_hashes={"b" * 64}, execution_duration_seconds=1.0,
        ).model_dump(mode="json"))

    store = CoordinatorStateStore(tmp_path / "state.db")
    from aidars.distributed.job_registry import JobRecord
    store.save_job(JobRecord("job-1", {"c0"}, CompletionPolicy.ALL_REQUIRED))
    store.save_workload(_record("c0", "job-1", WorkloadState.PLACED))  # in-flight when it "crashed"
    store.save_worker(_worker())

    service = CoordinatorService(state_store=store)
    service.orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    pending = service._restore_persisted_state()
    assert pending == ["c0"]

    # Prove liveness the same way a real restarted worker would.
    service.registry.record_heartbeat("w-1", payload=HeartbeatPayload(
        worker_id="w-1", resource_profile=_worker().resource_profile,
    ))

    # Redrive via the exact existing dispatcher -- same call
    # _redrive_recovered_workloads() makes, not a parallel mechanism.
    await service.orchestrator._process_workload("c0")

    assert call_count == 1
    record = service.workload_registry.get_workload("c0")
    assert record.state == WorkloadState.COMPLETED

    agg = service.job_registry.get_aggregate("job-1")
    assert agg.state == JobState.COMPLETED
    assert agg.output_asset_hashes == {"b" * 64}


# ============================================================================
# At-least-once behavior remains intact (M8.11)
# ============================================================================


@pytest.mark.asyncio
async def test_at_least_once_semantics_still_documented_after_m8(tmp_path: Path):
    doc = CoordinatorService._redrive_recovered_workloads.__doc__ or ""
    assert "AT-LEAST-ONCE" in doc
    assert "exactly-once" in doc.lower()


@pytest.mark.asyncio
async def test_recovered_in_flight_workload_can_duplicate_compute_and_is_recorded_as_such(tmp_path: Path):
    """Simulates the exact crash scenario M8.11 describes: a workload was
    dispatched (PLACED) when the coordinator crashed; the worker may have
    already finished it, but the coordinator has no way to know and
    re-dispatches on recovery. Both the redrive and (if it happened) the
    original execution's artifacts would coexist -- this test proves the
    redrive path itself does not silently assume prior completion."""
    store = CoordinatorStateStore(tmp_path / "state.db")
    from aidars.distributed.job_registry import JobRecord
    store.save_job(JobRecord("job-1", {"c0"}, CompletionPolicy.ALL_REQUIRED))
    store.save_workload(_record("c0", "job-1", WorkloadState.PLACED))
    store.save_worker(_worker())

    dispatched_again = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal dispatched_again
        dispatched_again = True
        return httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="c0", worker_id="w-1", success=True,
            output_asset_hashes={"c" * 64}, execution_duration_seconds=1.0,
        ).model_dump(mode="json"))

    service = CoordinatorService(state_store=store)
    service.orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    pending = service._restore_persisted_state()
    service.registry.record_heartbeat("w-1", payload=HeartbeatPayload(
        worker_id="w-1", resource_profile=_worker().resource_profile,
    ))

    await service.orchestrator._process_workload(pending[0])

    assert dispatched_again is True  # re-dispatched, not assumed complete
    assert service.workload_registry.get_workload("c0").state == WorkloadState.COMPLETED


# ============================================================================
# No-store parity for the Job/Artifact layer
# ============================================================================


def test_no_state_store_job_artifact_layer_unaffected():
    service = CoordinatorService()
    assert service.state_store is None
    pending = service._restore_persisted_state()
    assert pending == []
    assert service.job_registry.list_jobs() == []
    assert service.artifact_registry.list_artifacts() == []
