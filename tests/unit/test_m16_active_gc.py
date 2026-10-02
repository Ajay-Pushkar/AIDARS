"""M16.1 Active Artifact Garbage Collection tests.

Validates that unreferenced artifacts are swept from SQLite safely
without introducing race conditions or breaking lifecycle observability.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
from unittest import mock

import pytest

from aidars.distributed.artifact import Artifact, ArtifactLifecycleState, ArtifactRegistry, ArtifactVerificationState
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.job_registry import CompletionPolicy, JobRegistry
from aidars.distributed.models import WorkloadSpec
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.mark.asyncio
async def test_active_gc_sweeps_terminal_jobs_iteratively(temp_db):
    """Artifacts from terminal jobs progress AVAILABLE -> GC_ELIGIBLE -> DELETED -> Purged over 3 GC iterations."""
    store = CoordinatorStateStore(temp_db, enforce_single_writer=False)
    workload_registry = WorkloadRegistry(state_store=store)
    job_registry = JobRegistry(workload_registry, state_store=store)
    artifact_registry = ArtifactRegistry(state_store=store)

    coordinator = CoordinatorService(
        artifact_gc_interval_seconds=0.01,
        state_store=store,
    )
    coordinator.workload_registry = workload_registry
    coordinator.job_registry = job_registry
    coordinator.artifact_registry = artifact_registry

    # Setup a completed job with an artifact
    workload_registry.add_workload(WorkloadSpec(workload_id="w-1", job_id="job-1", task_type="test"))
    job_registry.create_job("job-1", {"w-1"}, CompletionPolicy.ALL_REQUIRED)
    workload_registry.update_state("w-1", WorkloadState.COMPLETED)

    artifact_registry.record_artifacts(
        producer_workload_id="w-1",
        producer_job_id="job-1",
        content_hashes={"hash-1"},
    )

    art_id = "w-1:hash-1"
    art = artifact_registry.get_artifact(art_id)
    assert art is not None
    assert art.lifecycle_state == ArtifactLifecycleState.AVAILABLE

    # Start coordinator background tasks
    await coordinator.start()

    try:
        # Give the loop time to run 3+ iterations
        for _ in range(20):
            await asyncio.sleep(0.01)
            art = artifact_registry.get_artifact(art_id)
            if art is None:
                break

        art = artifact_registry.get_artifact(art_id)
        assert art is None, "Artifact was not purged from memory"

        # Verify SQLite is also purged
        loaded = store.load_artifacts()
        assert len(loaded) == 0, "Artifact was not purged from SQLite"

    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_active_gc_protects_non_terminal_jobs(temp_db):
    """Artifacts from non-terminal jobs are ignored by the sweep."""
    store = CoordinatorStateStore(temp_db, enforce_single_writer=False)
    workload_registry = WorkloadRegistry(state_store=store)
    job_registry = JobRegistry(workload_registry, state_store=store)
    artifact_registry = ArtifactRegistry(state_store=store)

    coordinator = CoordinatorService(
        artifact_gc_interval_seconds=0.01,
        state_store=store,
    )
    coordinator.workload_registry = workload_registry
    coordinator.job_registry = job_registry
    coordinator.artifact_registry = artifact_registry

    # Setup a RUNNING job with an artifact (e.g. part of a pipeline where w-1 finished but job-1 is running)
    workload_registry.add_workload(WorkloadSpec(workload_id="w-1", job_id="job-1", task_type="test"))
    workload_registry.add_workload(WorkloadSpec(workload_id="w-2", job_id="job-1", task_type="test"))
    job_registry.create_job("job-1", {"w-1", "w-2"}, CompletionPolicy.ALL_REQUIRED)
    workload_registry.update_state("w-1", WorkloadState.COMPLETED)
    workload_registry.update_state("w-2", WorkloadState.EXECUTING)

    artifact_registry.record_artifacts(
        producer_workload_id="w-1",
        producer_job_id="job-1",
        content_hashes={"hash-1"},
    )

    art_id = "w-1:hash-1"

    await coordinator.start()
    try:
        await asyncio.sleep(0.1)
        art = artifact_registry.get_artifact(art_id)
        assert art is not None
        assert art.lifecycle_state == ArtifactLifecycleState.AVAILABLE, "Artifact from running job must remain AVAILABLE"
    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_active_gc_protects_contextless_artifacts(temp_db):
    """Artifacts with producer_job_id is None are explicitly ignored."""
    store = CoordinatorStateStore(temp_db, enforce_single_writer=False)
    artifact_registry = ArtifactRegistry(state_store=store)

    coordinator = CoordinatorService(
        artifact_gc_interval_seconds=0.01,
        state_store=store,
    )
    coordinator.artifact_registry = artifact_registry

    artifact_registry.record_artifacts(
        producer_workload_id="w-1",
        producer_job_id=None,
        content_hashes={"hash-1"},
    )
    art_id = "w-1:hash-1"

    await coordinator.start()
    try:
        await asyncio.sleep(0.1)
        art = artifact_registry.get_artifact(art_id)
        assert art is not None
        assert art.lifecycle_state == ArtifactLifecycleState.AVAILABLE, "Contextless artifacts must remain AVAILABLE"
    finally:
        await coordinator.stop()


def test_purge_deleted_artifacts_sqlite_failure_leaves_memory_intact(temp_db):
    """SQLite deletion failure leaves the artifact recoverable/retryable."""
    store = CoordinatorStateStore(temp_db, enforce_single_writer=False)
    artifact_registry = ArtifactRegistry(state_store=store)

    # Manually setup a DELETED artifact
    artifact_registry.record_artifacts("w-1", "job-1", {"hash-1"})
    art_id = "w-1:hash-1"

    with artifact_registry._lock:
        artifact_registry._artifacts[art_id].lifecycle_state = ArtifactLifecycleState.DELETED

    # Mock the SQLite deletion to fail
    original_delete = store.delete_artifact

    def failing_delete(aid):
        raise sqlite3.OperationalError("Simulated database lock or IO error")

    store.delete_artifact = mock.Mock(side_effect=failing_delete)

    with pytest.raises(sqlite3.OperationalError):
        artifact_registry.purge_deleted_artifacts([art_id])

    # Verify the artifact is STILL in memory, so it can be retried next sweep
    art = artifact_registry.get_artifact(art_id)
    assert art is not None
    assert art.lifecycle_state == ArtifactLifecycleState.DELETED

    # Now allow it to succeed
    store.delete_artifact = original_delete
    count = artifact_registry.purge_deleted_artifacts([art_id])

    assert count == 1
    assert artifact_registry.get_artifact(art_id) is None
