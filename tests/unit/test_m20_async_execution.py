import asyncio
import sys
import time
import json
from pathlib import Path
from typing import Dict, Any

import httpx
import pytest

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.models import (
    ExecutionSpec,
    ExecutionSubmitRequest,
    ExecutionState,
    FailureCategory,
    RuntimeExecutionContext,
    WorkerInfo,
    WorkerStatus,
    WorkerResourceProfile,
    WorkloadExecutionRequest,
    WorkloadSpec,
)
from aidars.distributed.runtime import GenericSubprocessRuntime
from aidars.distributed.server import WorkerServer
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.attempt import AttemptRegistry, AttemptStatus
from aidars.distributed.execution import ExecutionManager

pytestmark = pytest.mark.asyncio

# ============================================================================
# 1-3. Worker-Side Async Execution & Idempotency
# ============================================================================

async def test_explicit_timeout_kills_child_process_and_reports_timeout(tmp_path: Path):
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    worker = DistributedWorker(worker_id="w-1", cas_adapter=cas)
    
    spec = WorkloadSpec(
        workload_id="task-timeout",
        task_type="test",
        execution_spec=ExecutionSpec(
            executable=sys.executable,
            args=["-c", "import time; time.sleep(10)"],
            timeout_seconds=0.1
        )
    )
    
    status = await worker.submit_execution("task-timeout#1", spec)
    assert status.state in (ExecutionState.ACCEPTED, ExecutionState.RUNNING)
    
    # Wait for completion
    while status.state in (ExecutionState.ACCEPTED, ExecutionState.RUNNING):
        await asyncio.sleep(0.05)
        status = await worker.get_execution("task-timeout#1")
        
    assert status.state == ExecutionState.FAILED
    assert status.result is not None
    assert status.result.failure_category == FailureCategory.EXECUTION_TIMEOUT
    
    # Verify child process is dead (the runtime task was cancelled and cleanup finished)
    record = worker._executions["task-timeout#1"]
    assert record.task.done()

async def test_no_timeout_ignores_estimated_duration(tmp_path: Path):
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    worker = DistributedWorker(worker_id="w-1", cas_adapter=cas)
    
    spec = WorkloadSpec(
        workload_id="task-no-timeout",
        task_type="test",
        estimated_duration_seconds=0.01, # Fallback would be 0.03s
        execution_spec=ExecutionSpec(
            executable=sys.executable,
            args=["-c", "import time; time.sleep(0.2)"],
            timeout_seconds=None
        )
    )
    
    status = await worker.submit_execution("task-no-timeout#1", spec)
    
    while status.state in (ExecutionState.ACCEPTED, ExecutionState.RUNNING):
        await asyncio.sleep(0.05)
        status = await worker.get_execution("task-no-timeout#1")
        
    assert status.state == ExecutionState.SUCCEEDED
    assert status.result is not None
    assert status.result.success is True

async def test_worker_submit_is_idempotent(tmp_path: Path):
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    worker = DistributedWorker(worker_id="w-1", cas_adapter=cas)
    
    spec = WorkloadSpec(
        workload_id="task-idempotent",
        task_type="test",
        execution_spec=ExecutionSpec(
            executable=sys.executable,
            args=["-c", "import time; time.sleep(0.2)"]
        )
    )
    
    status1 = await worker.submit_execution("task-idemp#1", spec)
    status2 = await worker.submit_execution("task-idemp#1", spec)
    
    assert status1.attempt_id == status2.attempt_id
    assert status1.state == status2.state
    # Only one task should be created
    assert len(worker._executions) == 1

# ============================================================================
# 4 & 13. API Route Tests
# ============================================================================

async def test_api_routes_and_404_409(tmp_path: Path):
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    worker = DistributedWorker(worker_id="w-1", cas_adapter=cas)
    server = WorkerServer(worker_id="w-1", cas_adapter=cas, distributed_worker=worker)
    
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://testserver") as client:
        # GET unknown
        resp = await client.get("/api/v1/executions/unknown-attempt")
        assert resp.status_code == 404
        
        # POST cancel unknown
        resp = await client.post("/api/v1/executions/unknown-attempt/cancel")
        assert resp.status_code == 404
        
        # Checkpoint route doesn't double prefix (should not 404 just for path, maybe 404 for workload)
        resp = await client.post("/api/v1/workloads/unknown-workload/checkpoint")
        assert resp.status_code == 404 # Route found, workload not found
        
        # Cancel workload route doesn't double prefix
        resp = await client.post("/api/v1/workloads/unknown-workload/cancel")
        assert resp.status_code == 404 # Route found, workload not found

        # Submit a job
        spec = WorkloadSpec(
            workload_id="task-routes",
            task_type="test",
            execution_spec=ExecutionSpec(
                executable=sys.executable,
                args=["-c", "import time; time.sleep(0.1)"]
            )
        )
        req = ExecutionSubmitRequest(attempt_id="task-routes#1", spec=spec)
        
        resp = await client.post("/api/v1/executions", json=req.model_dump(mode="json"))
        assert resp.status_code == 202
        
        # DELETE non-terminal -> 409
        resp = await client.delete("/api/v1/executions/task-routes%231")
        assert resp.status_code == 409
        
        # Wait for completion (poll)
        for _ in range(20):
            await asyncio.sleep(0.1)
            resp = await client.get("/api/v1/executions/task-routes%231")
            if resp.json()["state"] == "succeeded":
                break
                
        assert resp.json()["state"] == "succeeded"
        
        # DELETE terminal -> 204
        resp = await client.delete("/api/v1/executions/task-routes%231")
        assert resp.status_code == 204

# ============================================================================
# 5-11. Coordinator-Side Poll Logic
# ============================================================================

def _resource_profile(worker_id):
    return WorkerResourceProfile(
        timestamp_utc=time.time(), worker_id=worker_id, endpoint_url=f"http://{worker_id}",
        ip_address="127.0.0.1", cpu_cores_total=8, cpu_utilization_percent=0.0,
        ram_total_bytes=16 * 1024**3, ram_available_bytes=16 * 1024**3,
    )

def _make_orchestrator(handler):
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    attempt_registry = AttemptRegistry()
    orchestrator = WorkloadOrchestrator(
        registry=registry, workload_registry=workload_registry,
        attempt_registry=attempt_registry, execution_poll_interval_seconds=0.01
    )
    
    w1 = WorkerInfo(
        worker_id="w-1", endpoint_url="http://worker-1", ip_address="127.0.0.1", port=8001,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
        resource_profile=_resource_profile("w-1")
    )
    w2 = WorkerInfo(
        worker_id="w-2", endpoint_url="http://worker-2", ip_address="127.0.0.1", port=8002,
        status=WorkerStatus.ACTIVE, capacity_bytes=4096, used_bytes=0,
        resource_profile=_resource_profile("w-2")
    )
    registry.register_worker(w1)
    registry.register_worker(w2)
    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return orchestrator, workload_registry, attempt_registry

async def test_coordinator_long_execution_beyond_http_timeout():
    polls = 0
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        if request.method == "POST" and request.url.path == "/api/v1/executions":
            return httpx.Response(202, json={"attempt_id": "task-long#1", "workload_id": "task-long", "state": "accepted"})
        elif request.method == "GET":
            polls += 1
            if polls < 3:
                return httpx.Response(200, json={"attempt_id": "task-long#1", "workload_id": "task-long", "state": "running"})
            else:
                return httpx.Response(200, json={
                    "attempt_id": "task-long#1", "workload_id": "task-long", "state": "succeeded",
                    "result": {"workload_id": "task-long", "worker_id": "w-1", "success": True, "output_asset_hashes": [], "execution_duration_seconds": 120.0}
                })
        elif request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(404)

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-long", task_type="test"))
    
    await orchestrator._process_workload("task-long")
    
    assert polls >= 3
    wl = workload_registry.get_workload("task-long")
    assert wl.state == WorkloadState.COMPLETED

async def test_transient_poll_errors_do_not_fail_workload():
    polls = 0
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        if request.method == "POST" and request.url.path == "/api/v1/executions":
            return httpx.Response(202, json={"attempt_id": "task-transient#1", "workload_id": "task-transient", "state": "accepted"})
        elif request.method == "GET":
            polls += 1
            if polls == 1:
                # Transient network error
                raise httpx.ReadTimeout("Transient timeout")
            elif polls == 2:
                # Recovered, still running
                return httpx.Response(200, json={"attempt_id": "task-transient#1", "workload_id": "task-transient", "state": "running"})
            else:
                return httpx.Response(200, json={
                    "attempt_id": "task-transient#1", "workload_id": "task-transient", "state": "succeeded",
                    "result": {"workload_id": "task-transient", "worker_id": "w-1", "success": True, "output_asset_hashes": [], "execution_duration_seconds": 1.0}
                })
        elif request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(404)

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-transient", task_type="test"))
    
    await orchestrator._process_workload("task-transient")
    
    assert polls >= 3
    wl = workload_registry.get_workload("task-transient")
    assert wl.state == WorkloadState.COMPLETED
    attempts = attempt_registry.list_attempts_for_workload("task-transient")
    assert len(attempts) == 1 # No retry occurred

async def test_attempt_marked_lost_cancels_and_retries():
    polls = 0
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        if request.method == "POST" and request.url.path == "/api/v1/executions":
            return httpx.Response(202, json={"attempt_id": "task-lost#1", "workload_id": "task-lost", "state": "accepted"})
        elif request.method == "GET":
            polls += 1
            if polls == 2:
                # Simulate heartbeat eviction marking it LOST while we're polling
                attempt_registry.mark_lost("task-lost#1", "heartbeat")
            return httpx.Response(200, json={"attempt_id": "task-lost#1", "workload_id": "task-lost", "state": "running"})
        elif request.method == "POST" and request.url.path.endswith("/cancel"):
            return httpx.Response(200, json={"attempt_id": "task-lost#1", "workload_id": "task-lost", "state": "cancelled"})
        return httpx.Response(404)

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-lost", task_type="test"))
    
    # We must mock max_attempts = 1 otherwise it loops forever in mock
    orchestrator.max_attempts = 1
    
    await orchestrator._process_workload("task-lost")
    
    wl = workload_registry.get_workload("task-lost")
    assert wl.state == WorkloadState.FAILED
    attempts = attempt_registry.list_attempts_for_workload("task-lost")
    assert len(attempts) == 1
    assert attempts[0].failure_category == FailureCategory.WORKER_UNAVAILABLE

async def test_status_404_mid_run_retries():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/api/v1/executions":
            body = json.loads(request.content)
            if body["attempt_id"] == "task-404#1":
                return httpx.Response(202, json={"attempt_id": "task-404#1", "workload_id": "task-404", "state": "accepted"})
            else:
                return httpx.Response(200, json={
                    "attempt_id": body["attempt_id"], "workload_id": "task-404", "state": "succeeded",
                    "result": {"workload_id": "task-404", "worker_id": "w-2", "success": True, "output_asset_hashes": [], "execution_duration_seconds": 1.0}
                })
        elif request.method == "GET":
            # Mid-run, worker restarted and lost the record
            return httpx.Response(404)
        elif request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(404)

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-404", task_type="test"))
    
    await orchestrator._process_workload("task-404")
    
    wl = workload_registry.get_workload("task-404")
    assert wl.state == WorkloadState.COMPLETED
    attempts = attempt_registry.list_attempts_for_workload("task-404")
    assert len(attempts) == 2
    assert attempts[0].status == AttemptStatus.LOST
    assert attempts[0].failure_category == FailureCategory.WORKER_UNAVAILABLE
    assert attempts[1].status == AttemptStatus.SUCCEEDED

async def test_ambiguous_submit_resolution():
    post_count = 0
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal post_count
        if request.method == "POST" and request.url.path == "/api/v1/executions":
            post_count += 1
            if post_count == 1:
                # Ambiguous network failure during submit
                raise httpx.ReadTimeout("Timeout on submit")
            elif post_count == 2:
                # Should not be called if we successfully attach
                assert False, "Should not resubmit blindly!"
        elif request.method == "GET":
            # Resolving the ambiguity: we find it was accepted
            return httpx.Response(200, json={
                "attempt_id": "task-ambig#1", "workload_id": "task-ambig", "state": "succeeded",
                "result": {"workload_id": "task-ambig", "worker_id": "w-1", "success": True, "output_asset_hashes": [], "execution_duration_seconds": 1.0}
            })
        elif request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(404)

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-ambig", task_type="test"))
    
    await orchestrator._process_workload("task-ambig")
    
    wl = workload_registry.get_workload("task-ambig")
    assert wl.state == WorkloadState.COMPLETED
    assert post_count == 1

async def test_cancel_workload_during_polling():
    polls = 0
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        if request.method == "POST" and request.url.path == "/api/v1/executions":
            return httpx.Response(202, json={"attempt_id": "task-cancel#1", "workload_id": "task-cancel", "state": "accepted"})
        elif request.method == "GET":
            polls += 1
            if polls == 2:
                # Simulate a concurrent user cancellation
                orchestrator.workload_registry.update_state("task-cancel", WorkloadState.CANCELLED)
            return httpx.Response(200, json={"attempt_id": "task-cancel#1", "workload_id": "task-cancel", "state": "running"})
        elif request.method == "POST" and request.url.path.endswith("/cancel"):
            # The coordinator should cancel the attempt
            return httpx.Response(200, json={"attempt_id": "task-cancel#1", "workload_id": "task-cancel", "state": "cancelled"})
        elif request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(404)

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-cancel", task_type="test"))
    
    await orchestrator._process_workload("task-cancel")
    
    wl = workload_registry.get_workload("task-cancel")
    assert wl.state == WorkloadState.CANCELLED
    attempts = attempt_registry.list_attempts_for_workload("task-cancel")
    assert len(attempts) == 1
    assert attempts[0].status == AttemptStatus.FAILED
    assert "cancelled" in str(attempts[0].failure_reason).lower()

async def test_artifact_verification_failure_still_works():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/api/v1/executions":
            return httpx.Response(200, json={
                "attempt_id": "task-verif#1", "workload_id": "task-verif", "state": "failed",
                "result": {"workload_id": "task-verif", "worker_id": "w-1", "success": False, "output_asset_hashes": [], "execution_duration_seconds": 1.0, "failure_category": FailureCategory.ARTIFACT_VERIFICATION_FAILURE}
            })
        return httpx.Response(404)

    orchestrator, workload_registry, attempt_registry = _make_orchestrator(handler)
    workload_registry.add_workload(WorkloadSpec(workload_id="task-verif", task_type="test"))
    
    orchestrator.max_attempts = 1
    await orchestrator._process_workload("task-verif")
    
    wl = workload_registry.get_workload("task-verif")
    assert wl.state == WorkloadState.FAILED
    attempts = attempt_registry.list_attempts_for_workload("task-verif")
    assert attempts[0].failure_category == FailureCategory.ARTIFACT_VERIFICATION_FAILURE
