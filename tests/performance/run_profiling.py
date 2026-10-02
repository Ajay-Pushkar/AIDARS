import asyncio
import time
import uuid
import cProfile
import pstats
from httpx import AsyncClient
from aidars.distributed.models import WorkloadSpec, WorkerRegistrationPayload, WorkerCapabilities
from aidars.distributed.server import WorkerServer
from aidars.distributed.coordinator import CoordinatorService


import tempfile
import os
import shutil

os.environ["AIDAR_ADMIN_TOKENS"] = "test_token"

async def run_burst_benchmark():
    temp_dir = tempfile.mkdtemp()

    coord_db = os.path.join(temp_dir, "coordinator_state.db")
    cas_dir = os.path.join(temp_dir, "cas")
    os.makedirs(cas_dir)

    from aidars.distributed.state_store import CoordinatorStateStore
    state_store = CoordinatorStateStore(coord_db)
    coord = CoordinatorService(state_store=state_store)
    await coord.start()

    # We don't even need a HTTP server for the coordinator. We can call submit_job directly.
    # But wait, we want to measure API overhead too.
    # Let's start the FastAPI server in background
    import uvicorn
    config = uvicorn.Config(app=coord.app, host="127.0.0.1", port=8000, log_level="error")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    # Wait for server to start
    await asyncio.sleep(1)

    headers = {"Authorization": "Bearer test_token"}

    # Register 1 fake worker directly
    coord.register_worker_sync(WorkerRegistrationPayload(
        worker_id="worker-1",
        endpoint_url="http://127.0.0.1:8001",
        ip_address="127.0.0.1",
        port=8001,
        hostname="worker-1",
        capacity_bytes=10000,
        used_bytes=0,
        capabilities=WorkerCapabilities(),
        inventory_hashes=[],
        tags={},
        can_execute_workloads=True
    ))

    # Wait to ensure heartbeat/health loop processes it
    await asyncio.sleep(0.5)

    async with AsyncClient() as client:
        # Warmup
        payload = {"specs": [
            WorkloadSpec(
                workload_id="warmup-0",
                job_id="warmup",
                task_type="test",
                min_ram_bytes=1024,
                parameters={"command": "python -c \"pass\""}
            ).model_dump(mode="json")
        ]}
        resp = await client.post("http://127.0.0.1:8000/api/v1/jobs/submit", json=payload, headers=headers)
        assert resp.status_code == 202

        await asyncio.sleep(1)

        # 100 Burst Workloads
        print("Running burst submit...")
        start_time = time.perf_counter()

        # cProfile section
        pr = cProfile.Profile()
        pr.enable()

        # We will submit 100 sequentially as fast as possible
        for i in range(100):
            job_id = f"burst-{i}"
            spec = WorkloadSpec(
                workload_id=f"{job_id}-0",
                job_id=job_id,
                task_type="test",
                min_ram_bytes=1024,
                parameters={"command": "python -c \"pass\""}
            )
            payload = {"specs": [spec.model_dump(mode="json")]}
            resp = await client.post("http://127.0.0.1:8000/api/v1/jobs/submit", json=payload, headers=headers)
            assert resp.status_code == 202

        pr.disable()
        end_time = time.perf_counter()

        print(f"100 Burst submissions took {(end_time - start_time)*1000:.2f} ms")
        print(f"Average time per API call: {((end_time - start_time)*1000)/100:.2f} ms")

        pr.dump_stats("tests/performance/burst.prof")

        p = pstats.Stats(pr)
        p.strip_dirs().sort_stats('cumtime').print_stats(30)

    server.should_exit = True
    await server_task
    await coord.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    asyncio.run(run_burst_benchmark())
