"""Milestone 2 (M2) Unit Tests: Job-Scoped Adaptive Scheduling and Dynamic Blender Chunking.

Tests cover:
M2-01: test_job_scoped_worker_confinement
M2-02: test_unselected_worker_isolation
M2-03: test_unselected_worker_available_for_other_jobs
M2-04: test_explicit_worker_count_infeasibility
M2-05: test_explicit_worker_count_success
M2-06: test_recovery_dispatch_confinement
M2-07: test_all_selected_workers_temporarily_unavailable
M2-08: test_sizer_placement_consistency
M2-09: test_multiple_independent_jobs_different_groups
M2-10: test_database_migration_compatibility
M2-11: test_submission_idempotency_preserves_assignment
M2-12: test_repeated_submission_deterministic_selection
M2-13: test_heterogeneous_chunk_execution_in_group
M2-14: test_adaptive_chunk_planning_overhead_balance
M2-15: test_subset_exhaustion_produces_failed_state
M2-16: test_lightweight_cpu_blender_render_success
M2-17: test_exceeding_resource_requirements_rejected
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Set

import httpx
import pytest

from aidars.adapters.blender.adapter import BlenderAdapter
from aidars.adapters.blender.intelligence.dependency_graph import DependencyGraphBuilder
from aidars.adapters.blender.intelligence.engine import SceneIntelligenceEngine
from aidars.adapters.blender.strategies.frame_scheduler import ChunkingConfig, FrameScheduler
from aidars.distributed.attempt import AttemptRegistry
from aidars.distributed.job_registry import CompletionPolicy, JobRecord, JobRegistry
from aidars.distributed.models import (
    FailureCategory,
    JobSubmissionInfeasibleError,
    WorkerInfo,
    WorkerResourceProfile,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.sizing import ClusterSizer, SizingRequest
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


def _make_profile(
    worker_id: str,
    endpoint_url: str = "http://worker",
    ip_address: str = "127.0.0.1",
    cpu_cores: int = 4,
    ram_gb: float = 8.0,
    has_gpu: bool = False,
    vram_gb: float = 0.0,
    active_workloads: int = 0,
    max_concurrent: int = 4,
) -> WorkerResourceProfile:
    return WorkerResourceProfile(
        timestamp_utc=time.time(),
        worker_id=worker_id,
        endpoint_url=endpoint_url,
        ip_address=ip_address,
        cpu_cores_total=cpu_cores,
        cpu_utilization_percent=0.0,
        ram_total_bytes=int(ram_gb * 1024**3),
        ram_available_bytes=int(ram_gb * 1024**3),
        gpu_models=["RTX 3090"] if has_gpu else [],
        vram_total_bytes=int(vram_gb * 1024**3),
        vram_available_bytes=int(vram_gb * 1024**3),
        active_workloads=active_workloads,
        max_concurrent_workloads=max_concurrent,
    )


def _make_cluster_orchestrator(
    worker_specs: List[dict],
    mock_handler=None,
    max_attempts: int = 3,
    state_store: CoordinatorStateStore | None = None,
):
    worker_registry = WorkerRegistry(state_store=state_store)
    workload_registry = WorkloadRegistry(state_store=state_store)
    attempt_registry = AttemptRegistry(state_store=state_store)
    job_registry = JobRegistry(workload_registry, state_store=state_store)

    orchestrator = WorkloadOrchestrator(
        registry=worker_registry,
        workload_registry=workload_registry,
        attempt_registry=attempt_registry,
        job_registry=job_registry,
        max_attempts=max_attempts,
    )

    for spec in worker_specs:
        wid = spec["worker_id"]
        url = spec.get("endpoint_url", f"http://{wid}:8000")
        profile = _make_profile(
            worker_id=wid,
            endpoint_url=url,
            cpu_cores=spec.get("cpu_cores", 4),
            ram_gb=spec.get("ram_gb", 8.0),
            has_gpu=spec.get("has_gpu", False),
            vram_gb=spec.get("vram_gb", 0.0),
            active_workloads=spec.get("active_workloads", 0),
            max_concurrent=spec.get("max_concurrent", 4),
        )
        info = WorkerInfo(
            worker_id=wid,
            endpoint_url=url,
            ip_address="127.0.0.1",
            port=spec.get("port", 8000),
            status=spec.get("status", WorkerStatus.ACTIVE),
            capacity_bytes=spec.get("capacity_bytes", 1024**3),
            used_bytes=0,
            resource_profile=profile,
        )
        worker_registry.register_worker(info)

    if mock_handler is not None:
        orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    return orchestrator, worker_registry, workload_registry, job_registry, attempt_registry


# ============================================================================
# M2-01: Job-Scoped Worker Confinement
# ============================================================================

@pytest.mark.asyncio
async def test_job_scoped_worker_confinement():
    """3 workers available (w-1, w-2, w-3). Sizer/caller selects [w-1, w-2].
    6 chunks submitted. Assert all 6 chunks dispatch ONLY to w-1 and w-2.
    Worker w-3 receives 0 chunks.
    """
    dispatches: List[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        # Endpoint host identifies worker
        wid = request.url.host
        dispatches.append(wid)
        spec_data = request.read().decode("utf-8")
        import json
        payload = json.loads(spec_data)
        workload_id = payload.get("workload_id") or payload.get("spec", {}).get("workload_id", "t")
        res = WorkloadExecutionResult(
            workload_id=workload_id,
            worker_id=wid,
            success=True,
            output_asset_hashes={"out-hash"},
            execution_duration_seconds=0.1,
        )
        return httpx.Response(200, json=res.model_dump(mode="json"))

    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1"},
        {"worker_id": "w-2", "endpoint_url": "http://w-2"},
        {"worker_id": "w-3", "endpoint_url": "http://w-3"},
    ]
    orchestrator, _, workload_registry, job_registry, _ = _make_cluster_orchestrator(workers, handler)

    specs = [
        WorkloadSpec(workload_id=f"c-{i}", task_type="render", min_ram_bytes=1024)
        for i in range(6)
    ]
    job_id = await orchestrator.submit_job(specs, worker_count=2)
    job = job_registry.get_job(job_id)
    assert job is not None
    assert job.selected_worker_ids == ["w-1", "w-2"]
    assert job.spare_worker_ids == ["w-3"]

    for s in specs:
        await orchestrator._process_workload(s.workload_id)

    assert len(dispatches) == 6
    assert all(wid in ("w-1", "w-2") for wid in dispatches)
    assert "w-3" not in dispatches
    assert all(
        workload_registry.get_workload(s.workload_id).state == WorkloadState.COMPLETED
        for s in specs
    )


# ============================================================================
# M2-02: Unselected Worker Isolation (Candidate Diagnostics)
# ============================================================================

@pytest.mark.asyncio
async def test_unselected_worker_isolation():
    """Worker w-3 candidate explanation records eligible=False and
    rejection_reasons contains 'WORKER_NOT_IN_JOB_SELECTED_SET'.
    """
    def handler(request: httpx.Request) -> httpx.Response:
        wid = request.url.host
        res = WorkloadExecutionResult(
            workload_id="chunk-0",
            worker_id=wid,
            success=True,
            output_asset_hashes=set(),
            execution_duration_seconds=0.1,
        )
        return httpx.Response(200, json=res.model_dump(mode="json"))

    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1", "max_concurrent": 1},
        {"worker_id": "w-2", "endpoint_url": "http://w-2", "max_concurrent": 1},
        {"worker_id": "w-3", "endpoint_url": "http://w-3", "max_concurrent": 4},
    ]
    orchestrator, _, workload_registry, _, attempt_registry = _make_cluster_orchestrator(workers, handler)

    spec = WorkloadSpec(workload_id="chunk-0", task_type="render", min_ram_bytes=1024)
    await orchestrator.submit_job([spec], selected_worker_ids=["w-1", "w-2"])

    # Make w-1 and w-2 busy at capacity so placement records explanation
    att1 = attempt_registry.create_attempt("busy-1")
    attempt_registry.mark_assigned(att1.attempt_id, "w-1")
    att2 = attempt_registry.create_attempt("busy-2")
    attempt_registry.mark_assigned(att2.attempt_id, "w-2")

    await orchestrator._process_workload("chunk-0")
    r0 = workload_registry.get_workload("chunk-0")
    explanation = r0.placement_explanation
    assert "candidates" in explanation
    w3_cand = next((c for c in explanation["candidates"] if c["worker_id"] == "w-3"), None)
    assert w3_cand is not None
    assert w3_cand["eligible"] is False
    assert "WORKER_NOT_IN_JOB_SELECTED_SET" in w3_cand["rejection_reasons"]


# ============================================================================
# M2-03: Unselected Worker Available for Other Jobs
# ============================================================================

@pytest.mark.asyncio
async def test_unselected_worker_available_for_other_jobs():
    """Worker w-3 (unselected for Job 1) successfully accepts and executes
    chunks from independent Job 2.
    """
    dispatches: Dict[str, List[str]] = {"job-1": [], "job-2": []}

    def handler(request: httpx.Request) -> httpx.Response:
        wid = request.url.host
        import json
        payload = json.loads(request.read().decode("utf-8"))
        workload_id = payload.get("workload_id") or payload.get("spec", {}).get("workload_id", "t")
        job_tag = "job-1" if "j1" in workload_id else "job-2"
        dispatches[job_tag].append(wid)
        res = WorkloadExecutionResult(
            workload_id=workload_id,
            worker_id=wid,
            success=True,
            output_asset_hashes=set(),
            execution_duration_seconds=0.1,
        )
        return httpx.Response(200, json=res.model_dump(mode="json"))

    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1"},
        {"worker_id": "w-2", "endpoint_url": "http://w-2"},
        {"worker_id": "w-3", "endpoint_url": "http://w-3"},
    ]
    orchestrator, _, workload_registry, _, _ = _make_cluster_orchestrator(workers, handler)

    # Job 1 confines to w-1, w-2
    j1_specs = [WorkloadSpec(workload_id="j1-c0", task_type="render", min_ram_bytes=1024)]
    await orchestrator.submit_job(j1_specs, selected_worker_ids=["w-1", "w-2"])

    # Job 2 confines to w-3
    j2_specs = [WorkloadSpec(workload_id="j2-c0", task_type="render", min_ram_bytes=1024)]
    await orchestrator.submit_job(j2_specs, selected_worker_ids=["w-3"])

    await orchestrator._process_workload("j1-c0")
    await orchestrator._process_workload("j2-c0")

    assert dispatches["job-1"] == ["w-1"] or dispatches["job-1"] == ["w-2"]
    assert dispatches["job-2"] == ["w-3"]
    assert workload_registry.get_workload("j2-c0").state == WorkloadState.COMPLETED


# ============================================================================
# M2-04: Explicit Worker Count Infeasibility
# ============================================================================

@pytest.mark.asyncio
async def test_explicit_worker_count_infeasibility():
    """Caller requests worker_count=3. Only 2 workers eligible.
    Assert JobSubmissionInfeasibleError is raised. Assert job is NOT submitted.
    """
    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1", "ram_gb": 8.0},
        {"worker_id": "w-2", "endpoint_url": "http://w-2", "ram_gb": 8.0},
    ]
    orchestrator, _, workload_registry, job_registry, _ = _make_cluster_orchestrator(workers)

    specs = [WorkloadSpec(workload_id="c-0", task_type="render", min_ram_bytes=1024)]
    with pytest.raises(JobSubmissionInfeasibleError) as exc_info:
        await orchestrator.submit_job(specs, worker_count=3)

    assert "Explicit worker_count=3 requested, but only 2 eligible worker(s) available" in str(exc_info.value)
    assert len(job_registry.list_jobs()) == 0
    assert len(workload_registry.list_workloads()) == 0


# ============================================================================
# M2-05: Explicit Worker Count Success
# ============================================================================

@pytest.mark.asyncio
async def test_explicit_worker_count_success():
    """Caller requests worker_count=2. 3 workers eligible.
    Assert exactly 2 workers selected and stored.
    """
    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1"},
        {"worker_id": "w-2", "endpoint_url": "http://w-2"},
        {"worker_id": "w-3", "endpoint_url": "http://w-3"},
    ]
    orchestrator, _, _, job_registry, _ = _make_cluster_orchestrator(workers)

    specs = [WorkloadSpec(workload_id="c-0", task_type="render", min_ram_bytes=1024)]
    job_id = await orchestrator.submit_job(specs, worker_count=2)

    job = job_registry.get_job(job_id)
    assert job is not None
    assert len(job.selected_worker_ids) == 2
    assert len(job.spare_worker_ids) == 1
    assert set(job.selected_worker_ids + job.spare_worker_ids) == {"w-1", "w-2", "w-3"}


# ============================================================================
# M2-06: Recovery Dispatch Confinement
# ============================================================================

@pytest.mark.asyncio
async def test_recovery_dispatch_confinement(tmp_path: Path):
    """Coordinator restarts with pending chunks.
    Assert recovered JobRecord retains selected_worker_ids.
    Assert recovered chunks dispatch strictly within subset.
    """
    db_path = tmp_path / "coordinator.db"
    store = CoordinatorStateStore(db_path)

    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1"},
        {"worker_id": "w-2", "endpoint_url": "http://w-2"},
        {"worker_id": "w-3", "endpoint_url": "http://w-3"},
    ]
    orchestrator1, _, _, _, _ = _make_cluster_orchestrator(workers, state_store=store)

    specs = [
        WorkloadSpec(workload_id="c-rec-0", task_type="render", min_ram_bytes=1024),
        WorkloadSpec(workload_id="c-rec-1", task_type="render", min_ram_bytes=1024),
    ]
    job_id = await orchestrator1.submit_job(specs, selected_worker_ids=["w-1", "w-2"])

    # Simulate coordinator crash & recovery: close store1 lock before reopening store2
    store.close()
    dispatches: List[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        wid = request.url.host
        dispatches.append(wid)
        import json
        payload = json.loads(request.read().decode("utf-8"))
        workload_id = payload.get("workload_id") or payload.get("spec", {}).get("workload_id", "t")
        res = WorkloadExecutionResult(
            workload_id=workload_id,
            worker_id=wid,
            success=True,
            output_asset_hashes=set(),
            execution_duration_seconds=0.1,
        )
        return httpx.Response(200, json=res.model_dump(mode="json"))

    store2 = CoordinatorStateStore(db_path)
    try:
        recovered_jobs = store2.load_jobs()
        assert len(recovered_jobs) == 1
        rec_job = recovered_jobs[0]
        assert rec_job.job_id == job_id
        assert rec_job.selected_worker_ids == ["w-1", "w-2"]

        orchestrator2, _, workload_registry2, _, _ = _make_cluster_orchestrator(
            workers, mock_handler=handler, state_store=store2
        )
        # Recover workloads
        recovered_workloads = store2.load_workloads()
        for w in recovered_workloads:
            workload_registry2.restore_workload(w)

        for s in specs:
            await orchestrator2._process_workload(s.workload_id)

        assert len(dispatches) == 2
        assert all(wid in ("w-1", "w-2") for wid in dispatches)
        assert "w-3" not in dispatches
    finally:
        store2.close()


# ============================================================================
# M2-07: All Selected Workers Temporarily Unavailable
# ============================================================================

@pytest.mark.asyncio
async def test_all_selected_workers_temporarily_unavailable():
    """Selected workers w-1 and w-2 reach max_concurrency. Worker w-3 is idle.
    Assert chunks remain in SUBMITTED without dispatching to w-3.
    """
    dispatches = []

    def handler(request: httpx.Request) -> httpx.Response:
        dispatches.append(request.url.host)
        res = WorkloadExecutionResult(
            workload_id="c-0", worker_id=request.url.host, success=True,
            output_asset_hashes=set(), execution_duration_seconds=0.1,
        )
        return httpx.Response(200, json=res.model_dump(mode="json"))

    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1", "max_concurrent": 1},
        {"worker_id": "w-2", "endpoint_url": "http://w-2", "max_concurrent": 1},
        {"worker_id": "w-3", "endpoint_url": "http://w-3", "max_concurrent": 4},
    ]
    orchestrator, _, workload_registry, _, attempt_registry = _make_cluster_orchestrator(workers, handler)

    spec = WorkloadSpec(workload_id="c-0", task_type="render", min_ram_bytes=1024)
    await orchestrator.submit_job([spec], selected_worker_ids=["w-1", "w-2"])

    # Saturate w-1 and w-2
    att1 = attempt_registry.create_attempt("busy-1")
    attempt_registry.mark_assigned(att1.attempt_id, "w-1")
    att2 = attempt_registry.create_attempt("busy-2")
    attempt_registry.mark_assigned(att2.attempt_id, "w-2")

    await orchestrator._process_workload("c-0")

    assert len(dispatches) == 0
    record = workload_registry.get_workload("c-0")
    assert record.state == WorkloadState.SUBMITTED
    assert record.error_message == "Waiting for an eligible worker; placement will be reconsidered"
    assert len(attempt_registry.list_attempts_for_workload("c-0")) == 0


# ============================================================================
# M2-08: Sizer and Placement Consistency
# ============================================================================

def test_sizer_placement_consistency():
    """Verify every worker chosen by ClusterSizer passes PlacementEngine hard gate at dispatch time."""
    engine = PlacementEngine()
    sizer = ClusterSizer(placement_engine=engine)

    profiles = [
        _make_profile("w-1", ram_gb=16.0, cpu_cores=8),
        _make_profile("w-2", ram_gb=8.0, cpu_cores=4),
        _make_profile("w-3", ram_gb=2.0, cpu_cores=2),
    ]

    req = SizingRequest(
        request_id="req-1",
        task_type="blender_render",
        total_units=10,
        unit_workload_seconds=10.0,
        min_ram_bytes=4 * 1024**3,
        min_cpu_cores=4,
    )
    decision = sizer.size_cluster(req, profiles)
    assert decision.recommended_worker_count > 0

    probe = sizer.build_probe_spec(req, time.time())
    for wid in decision.selected_worker_ids:
        cand_profiles = [p for p in profiles if p.worker_id == wid]
        p_decision = engine.evaluate(probe, cand_profiles)
        assert p_decision is not None
        assert p_decision.selected_worker_id == wid


# ============================================================================
# M2-09: Multiple Independent Jobs Different Groups
# ============================================================================

@pytest.mark.asyncio
async def test_multiple_independent_jobs_different_groups():
    """Cluster has A, B, C, D. Job 1 selects [w-1, w-2]; Job 2 selects [w-3, w-4].
    Assert zero crossover.
    """
    dispatches = {"j1": [], "j2": []}

    def handler(request: httpx.Request) -> httpx.Response:
        wid = request.url.host
        import json
        payload = json.loads(request.read().decode("utf-8"))
        wid_req = payload.get("workload_id") or payload.get("spec", {}).get("workload_id", "t")
        tag = "j1" if "j1" in wid_req else "j2"
        dispatches[tag].append(wid)
        res = WorkloadExecutionResult(
            workload_id=wid_req, worker_id=wid, success=True,
            output_asset_hashes=set(), execution_duration_seconds=0.1,
        )
        return httpx.Response(200, json=res.model_dump(mode="json"))

    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1"},
        {"worker_id": "w-2", "endpoint_url": "http://w-2"},
        {"worker_id": "w-3", "endpoint_url": "http://w-3"},
        {"worker_id": "w-4", "endpoint_url": "http://w-4"},
    ]
    orchestrator, _, _, _, _ = _make_cluster_orchestrator(workers, handler)

    j1_specs = [WorkloadSpec(workload_id=f"j1-c{i}", task_type="t", min_ram_bytes=1024) for i in range(4)]
    j2_specs = [WorkloadSpec(workload_id=f"j2-c{i}", task_type="t", min_ram_bytes=1024) for i in range(4)]

    await orchestrator.submit_job(j1_specs, selected_worker_ids=["w-1", "w-2"])
    await orchestrator.submit_job(j2_specs, selected_worker_ids=["w-3", "w-4"])

    for s in j1_specs + j2_specs:
        await orchestrator._process_workload(s.workload_id)

    assert len(dispatches["j1"]) == 4
    assert len(dispatches["j2"]) == 4
    assert all(w in ("w-1", "w-2") for w in dispatches["j1"])
    assert all(w in ("w-3", "w-4") for w in dispatches["j2"])


# ============================================================================
# M2-10: Database Migration Compatibility
# ============================================================================

def test_database_migration_compatibility(tmp_path: Path):
    """Load SQLite DB without selected_worker_ids columns.
    Assert PRAGMA migration succeeds without data loss.
    """
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            workload_ids_json TEXT NOT NULL,
            completion_policy TEXT NOT NULL,
            threshold INTEGER,
            execution_shape TEXT NOT NULL DEFAULT 'task_split',
            required_worker_count INTEGER NOT NULL DEFAULT 1,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
    """)
    conn.execute(
        "INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("legacy-job", '["w1", "w2"]', "all_required", None, "task_split", 1, time.time(), time.time()),
    )
    conn.commit()
    conn.close()

    # Opening with CoordinatorStateStore triggers automatic schema migration
    store = CoordinatorStateStore(db_path)
    jobs = store.load_jobs()
    assert len(jobs) == 1
    assert jobs[0].job_id == "legacy-job"
    assert jobs[0].selected_worker_ids is None
    assert jobs[0].spare_worker_ids is None

    # Saving a new job with assignments works cleanly
    new_record = JobRecord(
        job_id="new-job",
        workload_ids={"w3"},
        selected_worker_ids=["w-1", "w-2"],
        spare_worker_ids=["w-3"],
    )
    store.save_job(new_record)

    loaded = {j.job_id: j for j in store.load_jobs()}
    assert loaded["new-job"].selected_worker_ids == ["w-1", "w-2"]
    assert loaded["new-job"].spare_worker_ids == ["w-3"]


# ============================================================================
# M2-11: Submission Idempotency Preserves Assignment
# ============================================================================

@pytest.mark.asyncio
async def test_submission_idempotency_preserves_assignment():
    """Resubmitting identical job_id returns existing JobRecord and preserves
    existing selected_worker_ids.
    """
    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1"},
        {"worker_id": "w-2", "endpoint_url": "http://w-2"},
    ]
    orchestrator, _, _, job_registry, _ = _make_cluster_orchestrator(workers)

    specs = [WorkloadSpec(workload_id="dup-c0", job_id="job-dup", task_type="t", min_ram_bytes=1024)]
    job_id_1 = await orchestrator.submit_job(specs, selected_worker_ids=["w-1"])
    assert job_id_1 == "job-dup"
    job_1 = job_registry.get_job(job_id_1)
    assert job_1.selected_worker_ids == ["w-1"]

    # Resubmit identical job with different worker_count parameter
    job_id_2 = await orchestrator.submit_job(specs, selected_worker_ids=["w-2"])
    assert job_id_2 == "job-dup"
    job_2 = job_registry.get_job(job_id_2)
    # Original selected_worker_ids remains authoritative
    assert job_2.selected_worker_ids == ["w-1"]


# ============================================================================
# M2-12: Repeated Submission Deterministic Selection
# ============================================================================

@pytest.mark.asyncio
async def test_repeated_submission_deterministic_selection():
    """Two independent jobs with identical requirements against identical telemetry
    select identical worker subsets.
    """
    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1", "ram_gb": 16.0},
        {"worker_id": "w-2", "endpoint_url": "http://w-2", "ram_gb": 16.0},
        {"worker_id": "w-3", "endpoint_url": "http://w-3", "ram_gb": 8.0},
    ]
    orchestrator, _, _, job_registry, _ = _make_cluster_orchestrator(workers)

    specs_a = [WorkloadSpec(workload_id="a-c0", task_type="t", min_ram_bytes=1024)]
    specs_b = [WorkloadSpec(workload_id="b-c0", task_type="t", min_ram_bytes=1024)]

    job_a = await orchestrator.submit_job(specs_a, worker_count=2)
    job_b = await orchestrator.submit_job(specs_b, worker_count=2)

    rec_a = job_registry.get_job(job_a)
    rec_b = job_registry.get_job(job_b)
    assert rec_a.selected_worker_ids == rec_b.selected_worker_ids == ["w-1", "w-2"]


# ============================================================================
# M2-13: Heterogeneous Chunk Execution in Group
# ============================================================================

@pytest.mark.asyncio
async def test_heterogeneous_chunk_execution_in_group():
    """In selected group [w-1, w-2], chunks execute across the group while w-3 executes 0."""
    dispatches = []

    def handler(request: httpx.Request) -> httpx.Response:
        wid = request.url.host
        dispatches.append(wid)
        import json
        payload = json.loads(request.read().decode("utf-8"))
        wid_req = payload.get("workload_id") or payload.get("spec", {}).get("workload_id", "t")
        res = WorkloadExecutionResult(
            workload_id=wid_req, worker_id=wid, success=True,
            output_asset_hashes=set(), execution_duration_seconds=0.05,
        )
        return httpx.Response(200, json=res.model_dump(mode="json"))

    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1"},
        {"worker_id": "w-2", "endpoint_url": "http://w-2"},
        {"worker_id": "w-3", "endpoint_url": "http://w-3"},
    ]
    orchestrator, _, _, _, _ = _make_cluster_orchestrator(workers, handler)

    specs = [WorkloadSpec(workload_id=f"het-c{i}", task_type="t", min_ram_bytes=1024) for i in range(6)]
    await orchestrator.submit_job(specs, selected_worker_ids=["w-1", "w-2"])

    for s in specs:
        await orchestrator._process_workload(s.workload_id)

    assert len(dispatches) == 6
    assert all(w in ("w-1", "w-2") for w in dispatches)
    assert "w-3" not in dispatches


# ============================================================================
# M2-14: Adaptive Chunk Planning Overhead Balance
# ============================================================================

def test_adaptive_chunk_planning_overhead_balance():
    """Test frame chunking algorithm: verify chunks meet minimum overhead duration
    and maintain complete coverage.
    """
    engine = SceneIntelligenceEngine()
    builder = DependencyGraphBuilder()
    snapshot = engine.analyze_scene_data({
        "metadata": {"frame_start": 1, "frame_end": 60, "fps": 24},
        "objects": [], "collections": [], "lights": [], "materials": [], "textures": [], "images": [],
    })
    graph = builder.build(snapshot)

    scheduler = FrameScheduler(
        config=ChunkingConfig(
            default_overhead_seconds=2.5,
            max_overhead_fraction=0.10,
            target_chunks_per_worker=3,
            fallback_frame_duration_seconds=5.0,
            min_frames_per_chunk=5,
        )
    )

    # 60 frames with 2 workers: candidate M = 6 chunks (10 frames each)
    # 10 frames * 5.0s = 50s work. Overhead ratio = 2.5 / (50 + 2.5) = 4.7% <= 10%
    plan = scheduler.schedule(
        snapshot, graph, {}, frame_start=1, frame_end=60, worker_count=2, adaptive=True
    )
    assert len(plan.chunks) == 6
    # Verify complete contiguous coverage
    covered = []
    for c in plan.chunks:
        assert c.frame_count >= 5
        covered.extend(range(c.frame_start, c.frame_end + 1))
    assert covered == list(range(1, 61))

    # Low frame count clamps to 1 chunk
    plan_small = scheduler.schedule(
        snapshot, graph, {}, frame_start=1, frame_end=3, worker_count=4, adaptive=True
    )
    assert len(plan_small.chunks) == 1
    assert plan_small.chunks[0].frame_start == 1
    assert plan_small.chunks[0].frame_end == 3


# ============================================================================
# M2-15: Subset Exhaustion Produces Failed State
# ============================================================================

@pytest.mark.asyncio
async def test_subset_exhaustion_produces_failed_state():
    """All selected workers fail. Assert workload transitions to FAILED with exhaustion error."""
    def handler(request: httpx.Request) -> httpx.Response:
        wid = request.url.host
        res = WorkloadExecutionResult(
            workload_id="c-fail", worker_id=wid, success=False,
            output_asset_hashes=set(), execution_duration_seconds=0.1,
            error_message="timeout", failure_category=FailureCategory.EXECUTION_TIMEOUT,
        )
        return httpx.Response(200, json=res.model_dump(mode="json"))

    workers = [
        {"worker_id": "w-1", "endpoint_url": "http://w-1"},
        {"worker_id": "w-2", "endpoint_url": "http://w-2"},  # unselected worker
    ]
    orchestrator, _, workload_registry, _, _ = _make_cluster_orchestrator(workers, handler, max_attempts=2)

    spec = WorkloadSpec(workload_id="c-fail", task_type="render", min_ram_bytes=1024)
    await orchestrator.submit_job([spec], selected_worker_ids=["w-1"])

    await orchestrator._process_workload("c-fail")

    record = workload_registry.get_workload("c-fail")
    assert record.state == WorkloadState.FAILED
    assert "All assigned workers for this job have been exhausted" in record.error_message


# ============================================================================
# M2-16: Lightweight CPU Blender Render Success
# ============================================================================

@pytest.mark.asyncio
async def test_lightweight_cpu_blender_render_success():
    """Lightweight scene on 1-core, 1 GiB RAM, CPU-only worker.
    Assert admitted and executed without 8GB/GPU rejection.
    """
    blend_path = os.path.join("tests", "data", "cube.blend")
    if not os.path.exists(blend_path):
        pytest.skip("cube.blend not present")

    adapter = BlenderAdapter()
    req = {
        "input_path": blend_path,
        "frame_start": 1,
        "frame_end": 1,
        "worker_count": 1,
        "requires_gpu": False,
    }
    specs = adapter.evaluate_request(req)
    assert len(specs) > 0
    spec = specs[0]
    assert spec.requires_gpu is False
    assert spec.min_cpu_cores == 1
    assert spec.min_ram_bytes <= 1024**3

    def handler(request: httpx.Request) -> httpx.Response:
        res = WorkloadExecutionResult(
            workload_id=spec.workload_id,
            worker_id="w-cpu",
            success=True,
            output_asset_hashes=set(),
            execution_duration_seconds=0.5,
        )
        return httpx.Response(200, json=res.model_dump(mode="json"))

    workers = [{
        "worker_id": "w-cpu",
        "endpoint_url": "http://w-cpu",
        "cpu_cores": 1,
        "ram_gb": 1.0,
        "has_gpu": False,
    }]
    orchestrator, _, workload_registry, _, _ = _make_cluster_orchestrator(workers, handler)

    await orchestrator.submit_job(specs, worker_count=1)
    await orchestrator._process_workload(spec.workload_id)

    record = workload_registry.get_workload(spec.workload_id)
    assert record.state == WorkloadState.COMPLETED


# ============================================================================
# M2-17: Exceeding Resource Requirements Rejected
# ============================================================================

def test_exceeding_resource_requirements_rejected():
    """Workload requiring 4 GiB RAM on 1 GiB worker.
    Assert correctly rejected with INSUFFICIENT_RAM.
    """
    engine = PlacementEngine()
    profile = _make_profile("w-small", ram_gb=1.0)
    spec = WorkloadSpec(
        workload_id="spec-heavy",
        task_type="blender_render",
        min_ram_bytes=4 * 1024**3,
    )
    decision = engine.evaluate(spec, [profile])
    assert decision is None
    explanation = engine.last_evaluation
    cand = next((c for c in explanation["candidates"] if c["worker_id"] == "w-small"), None)
    assert cand is not None
    assert cand["eligible"] is False
    assert "INSUFFICIENT_RAM" in cand["rejection_reasons"]
