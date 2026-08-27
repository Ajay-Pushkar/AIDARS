import asyncio
import time
import pytest
from typing import Dict, Any

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.models import (
    WorkerResourceProfile,
    WorkloadSpec,
    WorkerStatus,
    WorkerInfo,
    WorkerMetrics,
    WorkerCapabilities
)
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.server import create_worker_app
from aidars.distributed.client import DistributedClient

@pytest.mark.asyncio
async def test_predictive_recovery_e2e(tmp_path):
    # Setup Coordinator
    coord_cas = LocalCASAdapter(str(tmp_path / "coord_cas"))
    coord = CoordinatorService(coordinator_id="coord-1", eviction_interval_seconds=1.0)
    coord._cas = coord_cas
    
    # We will simulate workers directly in the registry and mock execution
    worker_a_id = "worker-a-failing"
    worker_b_id = "worker-b-healthy"
    
    coord.registry.register_worker(
        WorkerInfo(
            worker_id=worker_a_id,
            endpoint_url="http://fake-a",
            ip_address="127.0.0.1",
            port=8001,
            capacity_bytes=1000,
            capabilities=WorkerCapabilities(gpu_available=False),
            inventory_hashes=set(),
            last_heartbeat_utc=time.time(),
            last_metrics=WorkerMetrics(
                cpu_percent=10.0,
                ram_used_bytes=100,
                active_transfers=0,
                used_bytes=0,
                available_bytes=1000
            )
        )
    )
    
    coord.registry.register_worker(
        WorkerInfo(
            worker_id=worker_b_id,
            endpoint_url="http://fake-b",
            ip_address="127.0.0.1",
            port=8002,
            capacity_bytes=1000,
            capabilities=WorkerCapabilities(gpu_available=False),
            inventory_hashes=set(),
            last_heartbeat_utc=time.time(),
            last_metrics=WorkerMetrics(
                cpu_percent=10.0,
                ram_used_bytes=100,
                active_transfers=0,
                used_bytes=0,
                available_bytes=1000
            )
        )
    )

    # 1. Start the coordinator loop
    await coord.start()
    
    # Submit workload
    spec = WorkloadSpec(
        workload_id="wl-long",
        task_type="simulation",
        min_cpu_cores=1,
        min_ram_bytes=100,
        estimated_duration_seconds=10.0,
        parameters={"command": "sleep 10"}
    )
    
    # Wait, we don't want to actually run HTTP servers. We want to test the orchestration.
    # We can just simulate the telemetry and policy.
    # Inject degrading telemetry for Worker A (Trigger ERRATIC)
    for i in range(100):
        coord.m7_memory.ingest_worker_metrics(
            worker_a_id,
            cpu_ratio=1.0,
            ram_ratio=1.0,
            latency=5.0,
            failed=True
        )
        coord.m7_memory.ingest_worker_metrics(
            worker_b_id,
            cpu_ratio=0.1, # CPU steady at 10% utilization
            ram_ratio=0.1,
            latency=5.0,
            failed=False
        )      
    # Wait for health loop to evaluate
    await asyncio.sleep(coord.eviction_interval_seconds + 0.5)
    
    # Assert Worker A was drained!
    worker_a_info = coord.registry.get_worker(worker_a_id)
    assert worker_a_info.status == WorkerStatus.DRAINING, "M7 should have detected risk and set worker to DRAINING"
    
    # Submit workload. It should go to Worker B!
    # Let's run the placement engine manually to see where it goes.
    profiles = []
    for info in coord.registry.list_workers(active_only=True): # active_only will STILL include it unless we filtered it, but we filtered in evaluate()
        profiles.append(WorkerResourceProfile(
            worker_id=info.worker_id,
            endpoint_url=info.endpoint_url,
            ip_address=info.ip_address,
            cpu_cores_total=8,
            cpu_utilization_percent=0.0,
            ram_total_bytes=1000,
            ram_available_bytes=1000,
            status=info.status
        ))
        
    decision = coord.orchestrator.placement_engine.evaluate(spec, profiles)
    assert decision.selected_worker_id == worker_b_id, "Workload must be placed on healthy worker B, avoiding draining worker A"
    
    await coord.stop()
