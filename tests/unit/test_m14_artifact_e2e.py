import asyncio
import json
import os
import time
from pathlib import Path

import httpx
import pytest
from httpx import ASGITransport

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import WorkloadExecutionResult, WorkloadSpec, WorkerStatus
from aidars.distributed.runtime import GenericSubprocessRuntime
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.workload_registry import WorkloadState


@pytest.mark.asyncio
async def test_m14_artifact_producing_workload_e2e(tmp_path: Path):
    """
    M14.1 Milestone: Prove that AIDAR can execute a workload that produces a 
    tangible output artifact and correctly records/verifies that artifact.
    """
    os.environ["AIDAR_INSECURE_MODE"] = "1"
    
    # 1. Setup Coordinator and Worker in-process
    coordinator = CoordinatorService(
        heartbeat_interval_seconds=1.0,
        heartbeat_timeout_seconds=5.0,
        eviction_interval_seconds=1.0,
    )
    
    worker_cas_dir = tmp_path / "worker-1-cas"
    worker_cas = LocalCASAdapter(cas_dir=worker_cas_dir)
    worker = DistributedWorker(
        worker_id="worker-1",
        cas_adapter=worker_cas,
        ip_address="127.0.0.1",
        port=8001,
        coordinator_url="http://coordinator"
    )
    
    # Wire worker to coordinator ASGI app
    transport = ASGITransport(app=coordinator.app)
    worker.client.http_client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")
    
    # Register worker
    await worker.register()
    
    # 3. Create the artifact-producing workload
    workload_id = "m14-artifact-test-001"
    command = "python -c \"with open('outputs/test_artifact.txt', 'w') as f: f.write('m14-verified-content')\""
    
    spec = WorkloadSpec(
        workload_id=workload_id,
        task_type="test",
        min_ram_bytes=1024,
        parameters={
            "command": command,
            "expected_output_count": 1
        }
    )
    
    # 4. Execute the workload directly on the worker's ExecutionManager
    # This runs the subprocess and CAS ingestion natively in the test loop
    result = await worker.execution_manager.execute_workload(
        spec, worker.worker_id, GenericSubprocessRuntime()
    )
    
    # 5. Wire orchestrator dispatch to return this real execution result
    def orchestrator_dispatch_mock(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=result.model_dump(mode="json"))

    coordinator.orchestrator.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(orchestrator_dispatch_mock)
    )
    
    # 6. Submit workload to coordinator and process it
    coordinator.workload_registry.add_workload(spec)
    await coordinator.orchestrator._process_workload(workload_id)
    
    # 7. Verify End-to-End state
    
    # - Workload should be COMPLETED
    record = coordinator.workload_registry.get_workload(workload_id)
    assert record.state == WorkloadState.COMPLETED, f"Workload failed: {record.error_message}"
    
    # - Result should be success
    assert record.execution_result.success is True
    
    # - Placement selected the active worker
    assert record.placement_decision.selected_worker_id == "worker-1"
    
    # - Output hashes captured
    output_hashes = record.execution_result.output_asset_hashes
    assert len(output_hashes) == 1
    artifact_hash = next(iter(output_hashes))
    
    # - Verify artifact is actually in the worker's CAS
    assert worker_cas.has_asset(artifact_hash) is True
    
    # - Verify artifact content
    with open(worker_cas.get_asset_path(artifact_hash), "r") as f:
        content = f.read()
        assert content == "m14-verified-content"
    
    # - Worker health is unaffected
    worker_info = coordinator.registry.get_worker("worker-1")
    assert worker_info.status == WorkerStatus.ACTIVE
