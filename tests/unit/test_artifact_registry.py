"""M8.8/M8.9: Artifact model, provenance, lifecycle, and GC eligibility."""
from __future__ import annotations

import time
from pathlib import Path

import httpx
import pytest

from aidars.distributed.artifact import (
    ArtifactLifecycleState,
    ArtifactRegistry,
    ArtifactVerificationState,
    make_artifact_id,
)
from aidars.distributed.models import WorkerInfo, WorkerResourceProfile, WorkloadExecutionResult, WorkloadSpec
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRegistry

HASH_A = "a" * 64
HASH_B = "b" * 64


# ============================================================================
# Artifact creation / CAS identity reuse / metadata / producer relationships
# ============================================================================


def test_artifact_creation_records_content_identity_and_producer():
    ar = ArtifactRegistry()
    created = ar.record_artifacts("w1", "job-1", {HASH_A}, sizes={HASH_A: 4096})

    assert len(created) == 1
    artifact = created[0]
    assert artifact.content_hash == HASH_A  # reuses the exact CAS SHA-256, no second hash
    assert artifact.artifact_id == make_artifact_id("w1", HASH_A)
    assert artifact.producer_workload_id == "w1"
    assert artifact.producer_job_id == "job-1"
    assert artifact.size_bytes == 4096
    assert artifact.storage_location == "cas"


def test_artifact_verification_state_is_verified_on_creation():
    """Every hash reaching ArtifactRegistry already passed M8.6 output
    verification and CAS commit -- this registry records that fact, it
    doesn't re-verify."""
    ar = ArtifactRegistry()
    [artifact] = ar.record_artifacts("w1", None, {HASH_A})
    assert artifact.verification_state == ArtifactVerificationState.VERIFIED


def test_producer_job_id_is_optional_for_standalone_workloads():
    ar = ArtifactRegistry()
    [artifact] = ar.record_artifacts("w1", None, {HASH_A})
    assert artifact.producer_job_id is None


def test_duplicate_compute_produces_separate_provenance_rows_same_hash():
    """At-least-once execution (M8.11) can re-run a workload and re-produce
    byte-identical output under a *different* workload_id after recovery.
    Both provenance events must be recorded, not collapsed into one --
    CAS already dedupes the bytes; this registry tracks who produced
    them."""
    ar = ArtifactRegistry()
    ar.record_artifacts("w1", "job-1", {HASH_A})
    ar.record_artifacts("w1-retry", "job-1", {HASH_A})

    rows = ar.get_artifacts_for_hash(HASH_A)
    assert len(rows) == 2
    assert {r.producer_workload_id for r in rows} == {"w1", "w1-retry"}


def test_recording_same_workload_hash_pair_twice_is_idempotent():
    ar = ArtifactRegistry()
    ar.record_artifacts("w1", "job-1", {HASH_A})
    ar.record_artifacts("w1", "job-1", {HASH_A})  # no-op, not a second row

    assert len(ar.get_artifacts_for_hash(HASH_A)) == 1


# ============================================================================
# M8.5 integration: WorkloadOrchestrator records artifacts on COMPLETED
# ============================================================================


@pytest.mark.asyncio
async def test_orchestrator_records_artifacts_on_completed_result():
    registry = WorkerRegistry()
    wr = WorkloadRegistry()
    ar = ArtifactRegistry()
    orch = WorkloadOrchestrator(registry, wr, artifact_registry=ar)
    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        capacity_bytes=999999999, used_bytes=0,
        resource_profile=WorkerResourceProfile(
            timestamp_utc=time.time(), worker_id="w-1", endpoint_url="http://worker-1",
            ip_address="127.0.0.1", cpu_cores_total=4, cpu_utilization_percent=0.0,
            ram_total_bytes=16 * 1024**3, ram_available_bytes=16 * 1024**3,
        ),
    ))

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="task-1", worker_id="w-1", success=True,
            output_asset_hashes={HASH_A}, output_asset_sizes={HASH_A: 999},
            execution_duration_seconds=1.0,
        ).model_dump(mode="json"))

    orch.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    spec = WorkloadSpec(workload_id="task-1", job_id="job-9", task_type="test", min_ram_bytes=1024)
    wr.add_workload(spec)

    await orch._process_workload("task-1")

    artifacts = ar.list_artifacts()
    assert len(artifacts) == 1
    assert artifacts[0].content_hash == HASH_A
    assert artifacts[0].producer_workload_id == "task-1"
    assert artifacts[0].producer_job_id == "job-9"
    assert artifacts[0].size_bytes == 999


@pytest.mark.asyncio
async def test_orchestrator_without_artifact_registry_does_not_error():
    """Backward compatible: artifact_registry=None (default) must not
    break successful workload processing."""
    registry = WorkerRegistry()
    wr = WorkloadRegistry()
    orch = WorkloadOrchestrator(registry, wr)  # no artifact_registry
    registry.register_worker(WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        capacity_bytes=999999999, used_bytes=0,
        resource_profile=WorkerResourceProfile(
            timestamp_utc=time.time(), worker_id="w-1", endpoint_url="http://worker-1",
            ip_address="127.0.0.1", cpu_cores_total=4, cpu_utilization_percent=0.0,
            ram_total_bytes=16 * 1024**3, ram_available_bytes=16 * 1024**3,
        ),
    ))
    orch.http_client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="task-1", worker_id="w-1", success=True,
            output_asset_hashes={HASH_A}, execution_duration_seconds=1.0,
        ).model_dump(mode="json"))
    ))
    wr.add_workload(WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024))

    await orch._process_workload("task-1")  # must not raise

    assert wr.get_workload("task-1").state.value == "completed"


# ============================================================================
# Lifecycle transitions / GC eligibility / protection of referenced artifacts
# ============================================================================


def test_gc_eligibility_protects_artifacts_of_non_terminal_jobs():
    ar = ArtifactRegistry()
    ar.record_artifacts("w1", "job-active", {HASH_A})
    ar.record_artifacts("w2", "job-terminal", {HASH_B})

    eligible = ar.compute_gc_eligible(protected_job_ids={"job-active"})
    assert eligible == [make_artifact_id("w2", HASH_B)]


def test_gc_eligibility_protects_artifacts_with_no_job_context():
    ar = ArtifactRegistry()
    ar.record_artifacts("w1", None, {HASH_A})

    eligible = ar.compute_gc_eligible(protected_job_ids=set())
    assert eligible == []  # producer_job_id is None -> always protected


def test_gc_does_not_auto_run_anywhere():
    """No background sweep exists -- compute_gc_eligible() is purely a
    caller-invoked, on-demand computation. This test documents that
    artifacts stay AVAILABLE until something explicitly calls it."""
    ar = ArtifactRegistry()
    [artifact] = ar.record_artifacts("w1", "job-terminal", {HASH_A})
    assert artifact.lifecycle_state == ArtifactLifecycleState.AVAILABLE
    # No automatic transition happens merely from time passing or from
    # other registry calls.
    assert ar.get_artifact(artifact.artifact_id).lifecycle_state == ArtifactLifecycleState.AVAILABLE


def test_mark_gc_eligible_transitions_state_without_deleting():
    ar = ArtifactRegistry()
    [artifact] = ar.record_artifacts("w1", "job-terminal", {HASH_A})

    count = ar.mark_gc_eligible([artifact.artifact_id])

    assert count == 1
    assert ar.get_artifact(artifact.artifact_id).lifecycle_state == ArtifactLifecycleState.GC_ELIGIBLE
    # The row still exists -- "GC eligible" is metadata, not deletion.
    assert ar.get_artifact(artifact.artifact_id) is not None


def test_mark_deleted_requires_gc_eligible_first():
    """Cannot jump straight from AVAILABLE to DELETED -- must pass through
    GC_ELIGIBLE, preventing unsafe immediate deletion."""
    ar = ArtifactRegistry()
    [artifact] = ar.record_artifacts("w1", "job-terminal", {HASH_A})

    assert ar.mark_deleted(artifact.artifact_id) is False  # still AVAILABLE, refused
    assert ar.get_artifact(artifact.artifact_id).lifecycle_state == ArtifactLifecycleState.AVAILABLE

    ar.mark_gc_eligible([artifact.artifact_id])
    assert ar.mark_deleted(artifact.artifact_id) is True
    assert ar.get_artifact(artifact.artifact_id).lifecycle_state == ArtifactLifecycleState.DELETED


def test_referenced_artifact_never_appears_in_gc_eligible_list_even_after_other_artifacts_are_marked():
    ar = ArtifactRegistry()
    ar.record_artifacts("w1", "job-active", {HASH_A})
    ar.record_artifacts("w2", "job-terminal", {HASH_B})

    first_pass = ar.compute_gc_eligible(protected_job_ids={"job-active"})
    ar.mark_gc_eligible(first_pass)

    second_pass = ar.compute_gc_eligible(protected_job_ids={"job-active"})
    assert make_artifact_id("w1", HASH_A) not in second_pass  # still protected
    assert second_pass == []  # HASH_B's artifact is already GC_ELIGIBLE, not AVAILABLE, so it's not listed again


# ============================================================================
# Persistence round-trip
# ============================================================================


def test_artifact_persists_through_state_store(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    ar = ArtifactRegistry(state_store=store)

    ar.record_artifacts("w1", "job-1", {HASH_A}, sizes={HASH_A: 512})

    loaded = store.load_artifacts()
    assert len(loaded) == 1
    assert loaded[0].content_hash == HASH_A
    assert loaded[0].producer_workload_id == "w1"
    assert loaded[0].producer_job_id == "job-1"
    assert loaded[0].size_bytes == 512
    assert loaded[0].verification_state == ArtifactVerificationState.VERIFIED
    assert loaded[0].lifecycle_state == ArtifactLifecycleState.AVAILABLE


def test_artifact_lifecycle_transition_persists(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    ar = ArtifactRegistry(state_store=store)
    [artifact] = ar.record_artifacts("w1", "job-1", {HASH_A})

    ar.mark_gc_eligible([artifact.artifact_id])

    loaded = store.load_artifacts()
    assert loaded[0].lifecycle_state == ArtifactLifecycleState.GC_ELIGIBLE


def test_artifact_registry_without_store_requires_no_sqlite():
    ar = ArtifactRegistry()  # no state_store
    ar.record_artifacts("w1", "job-1", {HASH_A})
    assert len(ar.list_artifacts()) == 1
