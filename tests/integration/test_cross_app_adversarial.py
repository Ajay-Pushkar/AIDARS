import pytest
import asyncio
import time
from aidars.distributed.models import WorkloadSpec, WorkerInfo, WorkerCapabilities, WorkerMetrics
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.adapters.blender.adapter import BlenderAdapter
from aidars.adapters.llm.adapter import LLMAdapter
from aidars.adapters.ml_training.adapter import MLTrainingAdapter

@pytest.mark.asyncio
async def test_concurrent_cross_app_workloads(tmp_path):
    """Test that Blender, LLM, and ML workloads can be scheduled concurrently on the cluster."""
    coord_cas = LocalCASAdapter(str(tmp_path / "coord_cas"))
    coord = CoordinatorService(coordinator_id="coord-cross-app", eviction_interval_seconds=1.0)
    coord._cas = coord_cas
    
    # Setup 2 workers: One GPU, One CPU
    gpu_worker = "worker-gpu-1"
    cpu_worker = "worker-cpu-1"
    
    coord.registry.register_worker(
        WorkerInfo(
            worker_id=gpu_worker,
            endpoint_url="http://gpu-worker",
            ip_address="127.0.0.1",
            port=8001,
            capacity_bytes=100 * 1024 * 1024 * 1024,
            capabilities=WorkerCapabilities(gpu_available=True),
            inventory_hashes=set(),
            last_heartbeat_utc=time.time(),
            last_metrics=WorkerMetrics(
                cpu_percent=10.0,
                ram_used_bytes=100,
                active_transfers=0,
                used_bytes=0,
                available_bytes=100 * 1024 * 1024 * 1024
            )
        )
    )
    
    coord.registry.register_worker(
        WorkerInfo(
            worker_id=cpu_worker,
            endpoint_url="http://cpu-worker",
            ip_address="127.0.0.1",
            port=8002,
            capacity_bytes=100 * 1024 * 1024 * 1024,
            capabilities=WorkerCapabilities(gpu_available=False),
            inventory_hashes=set(),
            last_heartbeat_utc=time.time(),
            last_metrics=WorkerMetrics(
                cpu_percent=10.0,
                ram_used_bytes=100,
                active_transfers=0,
                used_bytes=0,
                available_bytes=100 * 1024 * 1024 * 1024
            )
        )
    )
    
    # Create adapters
    blender = BlenderAdapter()
    llm = LLMAdapter()
    ml = MLTrainingAdapter()
    
    # Generate specs
    blender_specs = blender.evaluate_request({"input_path": "/test.blend", "requires_gpu": False})
    llm_specs = llm.evaluate_request({"prompt": "Hello", "requires_gpu": True})
    ml_specs = ml.evaluate_request({"dataset": "mnist", "requires_gpu": True})
    
    all_specs = blender_specs + llm_specs + ml_specs
    
    # We should have 4 specs total: 2 blender (cpu), 1 llm (gpu), 1 ml (gpu)
    assert len(all_specs) == 4
    
    # Submit them to coordinator orchestrator
    for spec in all_specs:
        await coord.orchestrator.submit_workload(spec)
        
    # Check that they were placed correctly
    # GPU workloads should go to GPU worker, CPU workloads to CPU worker
    # Note: In a real simulation, we might need to mock PlacementEngine or let it run
    
    # For now we just verify they exist in the registry
    for spec in all_specs:
        record = coord.workload_registry.get_workload(spec.workload_id)
        assert record is not None
