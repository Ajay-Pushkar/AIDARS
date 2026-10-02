import asyncio
import time
import os
import shutil
import tempfile
import pytest
from httpx import AsyncClient
from aidars.distributed.models import WorkloadSpec, WorkerRegistrationPayload, WorkerCapabilities
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.state_store import CoordinatorStateStore
from .utils import BenchmarkRunner

os.environ["AIDAR_ADMIN_TOKENS"] = "test_token"

async def simulate_worker_heartbeats(worker_id, coord, stop_event):
    while not stop_event.is_set():
        # A real heartbeat goes through the API, but to simulate true concurrency and 
        # HTTP overhead on the coordinator, we should hit the API.
        # However, hitting the API from the same process might skew. 
        # But we'll do it using an AsyncClient.
        async with AsyncClient() as client:
            headers = {"Authorization": "Bearer test_token"}
            payload = {
                "cpu_utilization_percent": 10.0,
                "ram_total_bytes": 1024,
                "ram_available_bytes": 512,
                "disk_total_bytes": 1024,
                "disk_free_bytes": 512,
                "active_workload_count": 0,
                "sequence_number": 1
            }
            try:
                await client.post(f"http://127.0.0.1:8000/api/v1/workers/{worker_id}/heartbeat", 
                                  json=payload, headers=headers)
            except Exception:
                pass
        await asyncio.sleep(1.0) # 1 second heartbeat to aggressively saturate the loop

async def run_scale_test(num_workers: int, burst_size: int = 100):
    temp_dir = tempfile.mkdtemp()
    coord_db = os.path.join(temp_dir, "coordinator_state.db")
    
    state_store = CoordinatorStateStore(coord_db)
    coord = CoordinatorService(state_store=state_store)
    await coord.start()
    
    import uvicorn
    config = uvicorn.Config(app=coord.app, host="127.0.0.1", port=8000, log_level="error")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    
    await asyncio.sleep(1) # wait for server
    
    # Register workers
    print(f"\nRegistering {num_workers} workers...")
    for i in range(num_workers):
        wid = f"worker-{i}"
        coord.register_worker_sync(WorkerRegistrationPayload(
            worker_id=wid,
            endpoint_url=f"http://127.0.0.1:{8001+i}",
            ip_address="127.0.0.1",
            port=8001+i,
            hostname=wid,
            capacity_bytes=10000,
            used_bytes=0,
            capabilities=WorkerCapabilities(),
            inventory_hashes=[],
            tags={},
            can_execute_workloads=True
        ))
    
    await asyncio.sleep(1)
    
    stop_event = asyncio.Event()
    heartbeat_tasks = [
        asyncio.create_task(simulate_worker_heartbeats(f"worker-{i}", coord, stop_event))
        for i in range(num_workers)
    ]
    
    await asyncio.sleep(2) # Let heartbeats saturate the loop
    
    headers = {"Authorization": "Bearer test_token"}
    
    runner = BenchmarkRunner(f"Burst 100 (Workers: {num_workers})", iterations=burst_size, warmup=10)
    
    print(f"Running {burst_size} bursts for N={num_workers}...")
    async with AsyncClient() as client:
        # Warmup
        for _ in range(runner.warmup):
            spec = WorkloadSpec(
                workload_id=f"warmup-0-{_}",
                job_id=f"warmup-{_}",
                task_type="test",
                min_ram_bytes=1024,
                parameters={"command": "python -c \"pass\""}
            )
            await client.post("http://127.0.0.1:8000/api/v1/jobs/submit", 
                              json={"specs": [spec.model_dump(mode="json")]}, headers=headers)
        
        # Benchmark
        for i in range(runner.iterations):
            spec = WorkloadSpec(
                workload_id=f"burst-{num_workers}-{i}-0",
                job_id=f"burst-{num_workers}-{i}",
                task_type="test",
                min_ram_bytes=1024,
                parameters={"command": "python -c \"pass\""}
            )
            
            payload = {"specs": [spec.model_dump(mode="json")]}
            start = time.perf_counter()
            resp = await client.post("http://127.0.0.1:8000/api/v1/jobs/submit", json=payload, headers=headers)
            end = time.perf_counter()
            
            assert resp.status_code == 202
            runner.record_ms((end - start) * 1000.0)
            
    result = runner.calculate()
    result.print_report()
    
    stop_event.set()
    await asyncio.gather(*heartbeat_tasks, return_exceptions=True)
    
    server.should_exit = True
    await server_task
    await coord.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)
    return result

@pytest.mark.asyncio
@pytest.mark.parametrize("num_workers", [1, 10, 50, 100])
async def test_m15_3_coordinator_scale(num_workers):
    res = await run_scale_test(num_workers)
    assert res.p50_ms >= 0
