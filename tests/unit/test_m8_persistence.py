"""M8 persistence: Job and Artifact round-trip through CoordinatorStateStore,
including a genuine restart simulation (a fresh CoordinatorStateStore
handle opened against the same on-disk SQLite file, not just reusing the
same Python object) -- the same rigor as tests/unit/test_state_store.py
applied to the two new M8 tables.
"""
from __future__ import annotations

from pathlib import Path

from aidars.distributed.artifact import (
    Artifact,
    ArtifactLifecycleState,
    ArtifactRegistry,
    ArtifactVerificationState,
)
from aidars.distributed.job_registry import CompletionPolicy, JobRecord, JobRegistry, JobState
from aidars.distributed.models import WorkloadExecutionResult, WorkloadSpec
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


# ============================================================================
# Job persistence
# ============================================================================


def test_job_round_trips_all_fields(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_job(JobRecord("job-1", {"c0", "c1", "c2"}, CompletionPolicy.THRESHOLD, threshold=2))

    [loaded] = store.load_jobs()
    assert loaded.job_id == "job-1"
    assert loaded.workload_ids == {"c0", "c1", "c2"}
    assert loaded.completion_policy == CompletionPolicy.THRESHOLD
    assert loaded.threshold == 2


def test_repeated_save_job_updates_not_duplicates(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_job(JobRecord("job-1", {"c0"}, CompletionPolicy.ALL_REQUIRED))
    store.save_job(JobRecord("job-1", {"c0", "c1"}, CompletionPolicy.ALL_REQUIRED))

    loaded = store.load_jobs()
    assert len(loaded) == 1
    assert loaded[0].workload_ids == {"c0", "c1"}


# ============================================================================
# Artifact persistence
# ============================================================================


def test_artifact_round_trips_all_fields(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_artifact(Artifact(
        artifact_id="c0:" + "a" * 64, content_hash="a" * 64,
        producer_workload_id="c0", producer_job_id="job-1",
        created_at=1700000000.5, verification_state=ArtifactVerificationState.VERIFIED,
        lifecycle_state=ArtifactLifecycleState.GC_ELIGIBLE, storage_location="cas", size_bytes=2048,
    ))

    [loaded] = store.load_artifacts()
    assert loaded.content_hash == "a" * 64
    assert loaded.producer_workload_id == "c0"
    assert loaded.producer_job_id == "job-1"
    assert loaded.created_at == 1700000000.5
    assert loaded.verification_state == ArtifactVerificationState.VERIFIED
    assert loaded.lifecycle_state == ArtifactLifecycleState.GC_ELIGIBLE
    assert loaded.storage_location == "cas"
    assert loaded.size_bytes == 2048


def test_artifact_with_no_job_context_round_trips_none(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    store.save_artifact(Artifact(
        artifact_id="c0:" + "b" * 64, content_hash="b" * 64,
        producer_workload_id="c0", producer_job_id=None,
        created_at=1700000000.0, verification_state=ArtifactVerificationState.VERIFIED,
        lifecycle_state=ArtifactLifecycleState.AVAILABLE, storage_location="cas",
    ))

    [loaded] = store.load_artifacts()
    assert loaded.producer_job_id is None
    assert loaded.size_bytes is None


# ============================================================================
# Genuine restart/reload: a fresh CoordinatorStateStore handle against the
# same file, not the same Python object.
# ============================================================================


def test_job_and_artifact_survive_a_real_store_handle_restart(tmp_path: Path):
    db_path = tmp_path / "state.db"

    store_before_restart = CoordinatorStateStore(db_path)
    store_before_restart.save_job(JobRecord("job-1", {"c0", "c1"}, CompletionPolicy.ALL_REQUIRED))
    store_before_restart.save_artifact(Artifact(
        artifact_id="c0:" + "a" * 64, content_hash="a" * 64,
        producer_workload_id="c0", producer_job_id="job-1",
        created_at=1700000000.0, verification_state=ArtifactVerificationState.VERIFIED,
        lifecycle_state=ArtifactLifecycleState.AVAILABLE, storage_location="cas", size_bytes=99,
    ))
    store_before_restart.close()

    # Simulates the coordinator process restarting: a brand new
    # CoordinatorStateStore instance opened against the same file.
    store_after_restart = CoordinatorStateStore(db_path)

    jobs = store_after_restart.load_jobs()
    artifacts = store_after_restart.load_artifacts()
    assert len(jobs) == 1 and jobs[0].job_id == "job-1"
    assert len(artifacts) == 1 and artifacts[0].content_hash == "a" * 64


# ============================================================================
# Membership reconstruction + derived state reconstruction
# ============================================================================


def test_membership_and_derived_state_reconstruct_correctly_after_restart(tmp_path: Path):
    db_path = tmp_path / "state.db"

    store_before = CoordinatorStateStore(db_path)
    store_before.save_job(JobRecord("job-1", {"c0", "c1"}, CompletionPolicy.ALL_REQUIRED))

    spec_c0 = WorkloadSpec(workload_id="c0", job_id="job-1", task_type="test", min_ram_bytes=1024)
    from aidars.distributed.workload_registry import WorkloadRecord
    record_c0 = WorkloadRecord(spec_c0)
    record_c0.state = WorkloadState.COMPLETED
    record_c0.execution_result = WorkloadExecutionResult(
        workload_id="c0", worker_id="w-1", success=True,
        output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0,
    )
    store_before.save_workload(record_c0)

    spec_c1 = WorkloadSpec(workload_id="c1", job_id="job-1", task_type="test", min_ram_bytes=1024)
    record_c1 = WorkloadRecord(spec_c1)
    record_c1.state = WorkloadState.COMPLETED
    record_c1.execution_result = WorkloadExecutionResult(
        workload_id="c1", worker_id="w-1", success=True,
        output_asset_hashes={"b" * 64}, execution_duration_seconds=1.0,
    )
    store_before.save_workload(record_c1)
    store_before.close()

    # Fresh registries + a fresh store handle -- genuine reconstruction,
    # not reuse of any in-memory state from before "restart".
    store_after = CoordinatorStateStore(db_path)
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    for record in store_after.load_workloads():
        wr.restore_workload(record)
    for job in store_after.load_jobs():
        jr.restore_job(job)

    assert jr.get_job("job-1").workload_ids == {"c0", "c1"}
    agg = jr.get_aggregate("job-1")
    assert agg.state == JobState.COMPLETED
    assert agg.completed == 2
    assert agg.output_asset_hashes == {"a" * 64, "b" * 64}


# ============================================================================
# No-store mode requires no SQLite
# ============================================================================


def test_job_registry_and_artifact_registry_no_store_mode_have_no_sqlite_dependency():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)  # no state_store
    ar = ArtifactRegistry()  # no state_store

    jr.create_job("job-1", {"w1"})
    ar.record_artifacts("w1", "job-1", {"a" * 64})

    assert jr.get_job("job-1") is not None
    assert len(ar.list_artifacts()) == 1
