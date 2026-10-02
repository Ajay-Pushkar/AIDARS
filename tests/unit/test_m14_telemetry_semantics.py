import asyncio
import time
import pytest
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    WorkerRegistrationPayload,
    WorkerResourceProfile,
    HeartbeatPayload,
    WorkerMetrics,
    WorkerCapabilities,
    WorkerStatus
)

@pytest.mark.asyncio
async def test_healthy_worker_not_draining_from_double_inversion():
    """
    Reproduces the M14.1 condition where a healthy worker with low CPU and RAM
    was being falsely classified as DRAINING (overloaded) due to a telemetry 
    semantic mismatch (double inversion).
    """
    coordinator = CoordinatorService(
        heartbeat_interval_seconds=1.0,
        heartbeat_timeout_seconds=5.0,
        eviction_interval_seconds=0.5,
    )
    
    # We will invoke the health evaluation loop manually for the test
    # so we don't have to wait for background loops.
    
    worker_id = "test-worker-m14"
    capacity = 100 * 1024 * 1024 * 1024
    ram_total = 16 * 1024 * 1024 * 1024
    ram_available = int(ram_total * 0.86)  # 14% RAM used
    cpu_util = 0.3  # 0.3% CPU used
    
    # 1. Register worker
    reg_profile = WorkerResourceProfile(
        worker_id=worker_id,
        endpoint_url="http://127.0.0.1:8000",
        ip_address="127.0.0.1",
        cpu_cores_total=8,
        cpu_utilization_percent=cpu_util,
        ram_total_bytes=ram_total,
        ram_available_bytes=ram_available,
        active_workload_count=0,
    )
    
    reg_payload = WorkerRegistrationPayload(
        worker_id=worker_id,
        endpoint_url="http://127.0.0.1:8000",
        ip_address="127.0.0.1",
        port=8000,
        capacity_bytes=capacity,
        used_bytes=0,
        resource_profile=reg_profile,
        capabilities=WorkerCapabilities(),
    )
    
    coordinator.register_worker_sync(reg_payload)
    
    # 2. Send heartbeat to ingest telemetry
    # This simulates `TelemetryIngestor.on_worker_heartbeat` which calculates availability ratios
    # and populates the M7 temporal memory EMAs.
    heartbeat = HeartbeatPayload(
        worker_id=worker_id,
        metrics=WorkerMetrics(
            active_transfers=0,
            transfer_error_count=0
        ),
        resource_profile=reg_profile,
        active_transfers=0,
        used_bytes=0,
        available_bytes=capacity
    )
    
    # Manually ingest it as the router would
    coordinator.m7_ingestor.on_worker_heartbeat(
        worker_id=worker_id,
        cpu_utilization_percent=cpu_util,
        ram_total=ram_total,
        ram_available=ram_available,
        failed=False,
        latency_ms=10.0
    )
    
    # 3. Verify worker is ACTIVE before health evaluation
    worker = coordinator.registry.get_worker(worker_id)
    assert worker.status == WorkerStatus.ACTIVE
    
    # 4. Trigger M7 Health Evaluation (like the background loop does)
    # We use a coroutine mock approach or just call the logic directly since it's inside a while loop.
    # We can isolate the logic that does the evaluation:
    active_workers = list(coordinator.m7_memory.workers.values())
    
    from aidars.m7.contracts import WorkerFeatureVector
    from aidars.m7.behavior import BehaviorInferencer
    from aidars.m7.contracts import WorkerState as M7WorkerState
    
    for worker_state in active_workers:
        # The fixed logic in coordinator.py:
        features = WorkerFeatureVector(
            cpu_available_ratio=worker_state.cpu_utilization_ema.value,
            ram_available_ratio=worker_state.ram_utilization_ema.value,
            vram_available_ratio=1.0,
            has_gpu=0.0,
            active_workload_ratio=0.0,
            cache_locality_ratio=0.0,
            heartbeat_stability=1.0,
            recent_failure_rate=worker_state.failure_rate_ema.value,
            recent_latency_normalized=min(1.0, worker_state.latency_ema.value / 100.0),
            throughput_normalized=0.5
        )
        behavior = BehaviorInferencer.infer_worker_behavior(worker_state.worker_id, features)
        
        if behavior.state in (M7WorkerState.DEGRADED, M7WorkerState.ERRATIC, M7WorkerState.OVERLOADED):
            wid = worker_state.worker_id
            w_info = coordinator.registry.get_worker(wid)
            if w_info and w_info.status == WorkerStatus.ACTIVE:
                coordinator.registry._workers[wid].status = WorkerStatus.DRAINING
    
    # 5. Worker should still be ACTIVE, not DRAINING
    worker_after = coordinator.registry.get_worker(worker_id)
    assert worker_after.status == WorkerStatus.ACTIVE, f"Worker became {worker_after.status.value}, expected ACTIVE"
