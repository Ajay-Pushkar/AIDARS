import asyncio
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
from httpx import AsyncClient

from aidars.adapters.blender.adapter import BlenderAdapter
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.worker import DistributedWorker
import uvicorn

@pytest.fixture
def blender_executable():
    executable = shutil.which("blender")
    if not executable:
        pytest.skip("blender executable not found on PATH. Required for E2E distributed blender execution test.")
    return executable

@pytest.mark.asyncio
async def test_m16_4_distributed_blender_e2e(blender_executable):
    """
    M16.4 Distributed Blender E2E Closure Test
    Validates a complete lifecycle:
    1. Minimal .blend generation
    2. Input asset ingestion into a source node CAS
    3. Workload creation via BlenderAdapter
    4. Submission to Coordinator
    5. Dispatch to an execution Worker
    6. Input asset transfer over HTTP
    7. Real Blender rendering via GenericSubprocessRuntime
    8. Output harvesting and CAS ingestion
    9. Final verification of output PNG.
    """
    temp_dir = tempfile.mkdtemp()

    coord_server = None
    coord_server_task = None
    worker_source = None
    worker_exec = None

    try:
        os.environ["AIDAR_ADMIN_TOKENS"] = "admin_token"
        os.environ["AIDAR_WORKER_BOOTSTRAP_SECRET"] = "worker_secret"
        os.environ["AIDAR_INSECURE_MODE"] = "1"

        coord_db = os.path.join(temp_dir, "coordinator_state.db")
        state_store = CoordinatorStateStore(coord_db)

        coord = CoordinatorService(
            heartbeat_interval_seconds=1.0,
            heartbeat_timeout_seconds=5.0,
            state_store=state_store
        )
        await coord.start()

        config = uvicorn.Config(app=coord.app, host="127.0.0.1", port=8002, log_level="error")
        coord_server = uvicorn.Server(config)
        coord_server_task = asyncio.create_task(coord_server.serve())
        await asyncio.sleep(1)

        source_cas_dir = os.path.join(temp_dir, "source_cas")
        os.makedirs(source_cas_dir, exist_ok=True)
        worker_source = DistributedWorker(
            worker_id="worker-source",
            cas_dir=Path(source_cas_dir),
            ip_address="127.0.0.1",
            port=8003,
            coordinator_url="http://127.0.0.1:8002",
            bootstrap_secret="worker_secret",
            can_execute_workloads=False,
            heartbeat_interval_seconds=1.0
        )
        await worker_source.start()

        config_ws = uvicorn.Config(app=worker_source.server.app, host="127.0.0.1", port=8003, log_level="error")
        ws_server = uvicorn.Server(config_ws)
        ws_server_task = asyncio.create_task(ws_server.serve())

        exec_cas_dir = os.path.join(temp_dir, "exec_cas")
        os.makedirs(exec_cas_dir, exist_ok=True)
        worker_exec = DistributedWorker(
            worker_id="worker-exec",
            cas_dir=Path(exec_cas_dir),
            ip_address="127.0.0.1",
            port=8004,
            coordinator_url="http://127.0.0.1:8002",
            bootstrap_secret="worker_secret",
            can_execute_workloads=True,
            heartbeat_interval_seconds=1.0
        )
        await worker_exec.start()

        config_we = uvicorn.Config(app=worker_exec.server.app, host="127.0.0.1", port=8004, log_level="error")
        we_server = uvicorn.Server(config_we)
        we_server_task = asyncio.create_task(we_server.serve())

        await asyncio.sleep(3)

        async with AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                "http://127.0.0.1:8002/api/v1/workers",
                headers={"Authorization": "Bearer admin_token"}
            )
            assert resp.status_code == 200
            workers = resp.json()
            assert len(workers) >= 2

        blend_path = os.path.join(temp_dir, "minimal.blend")
        blend_posix = Path(blend_path).resolve().as_posix()
        create_script = f"""
import bpy
bpy.ops.wm.read_factory_settings(use_empty=True)
bpy.ops.mesh.primitive_cube_add()
camera_data = bpy.data.cameras.new(name='Camera')
camera_object = bpy.data.objects.new('Camera', camera_data)
bpy.context.scene.collection.objects.link(camera_object)
bpy.context.scene.camera = camera_object
camera_object.location = (0, -5, 0)
camera_object.rotation_euler = (1.5708, 0, 0)
bpy.context.scene.render.engine = 'CYCLES'
bpy.context.scene.render.resolution_x = 16
bpy.context.scene.render.resolution_y = 16
bpy.ops.wm.save_as_mainfile(filepath="{blend_posix}")
"""
        create_py = os.path.join(temp_dir, "create.py")
        with open(create_py, "w") as f:
            f.write(create_script)

        res = await asyncio.to_thread(
            subprocess.run,
            [blender_executable, "-b", "-P", create_py],
            capture_output=True,
            check=False
        )
        if res.returncode != 0:
            raise RuntimeError(f"Blender failed: {res.stdout.decode()} {res.stderr.decode()}")
        if not os.path.exists(blend_path):
            raise RuntimeError(f"Blender failed to save file. stdout:\n{res.stdout.decode()}\nstderr:\n{res.stderr.decode()}")

        with open(blend_path, "rb") as f:
            blend_data = f.read()

        input_hash = worker_source.cas.store_bytes(blend_data)
        assert os.path.exists(worker_source.cas.get_asset_path(input_hash))

        # Let the background heartbeat sync the new inventory to the coordinator
        await asyncio.sleep(2)
        await asyncio.sleep(1)

        adapter = BlenderAdapter()
        request_dict = {
            "input_path": blend_path,
            "frame_start": 1,
            "frame_end": 1,
            "worker_count": 1,
            "requires_gpu": False
        }
        specs = adapter.evaluate_request(request_dict)
        assert len(specs) == 1
        spec = specs[0]
        spec.min_ram_bytes = 1024 * 1024 * 1024  # Override 8GB default to allow test to run locally
        spec.min_cpu_cores = 1

        # Fix Blender's output path interpretation on Windows by using //../ (relative to .blend file)
        if "command" in spec.parameters:
            spec.parameters["command"] = spec.parameters["command"].replace("outputs/frame_####", "//../outputs/frame_####")

        assert input_hash in spec.input_asset_hashes

        async with AsyncClient(timeout=5.0) as client:
            headers = {"Authorization": "Bearer admin_token"}
            resp = await client.post(
                "http://127.0.0.1:8002/api/v1/jobs/submit",
                json={"specs": [spec.model_dump(mode="json")]},
                headers=headers
            )
            assert resp.status_code == 202, f"Failed to submit: {resp.text}"

        workload_id = spec.workload_id
        completed = False
        final_state = None

        for _ in range(30):
            async with AsyncClient(timeout=5.0) as client:
                headers = {"Authorization": "Bearer admin_token"}
                resp = await client.get(
                    f"http://127.0.0.1:8002/api/v1/workloads/{workload_id}",
                    headers=headers
                )
                if resp.status_code == 200:
                    data = resp.json()
                    final_state = data
                    state = data.get("state", "").upper()
                    if state == "COMPLETED":
                        completed = True
                        break
                    elif state in ("FAILED", "CANCELED"):
                        break
            await asyncio.sleep(1)

        assert completed, f"Workload did not complete successfully. Final state: {final_state}"

        assert final_state.get("placement", {}).get("selected_worker_id") == "worker-exec", "Workload was not assigned to the execution worker"

        assert os.path.exists(worker_exec.cas.get_asset_path(input_hash)), "Worker did not synchronize the input artifact to its CAS"

        exec_result = final_state.get("result", {})

        assert exec_result.get("transfer_duration_seconds", 0) > 0, "Transfer duration was not recorded"

        output_hashes = exec_result.get("output_asset_hashes", [])
        assert len(output_hashes) > 0, "No output artifacts harvested by worker"

        output_hash = output_hashes[0]
        output_path = worker_exec.cas.get_asset_path(output_hash)
        assert os.path.exists(output_path), "Worker did not ingest output artifact into its CAS"

        with open(output_path, "rb") as f:
            header = f.read(8)
            assert header == b"\x89PNG\r\n\x1a\n", "Rendered artifact is not a valid PNG signature"

    finally:
        if worker_source:
            await worker_source.stop()
        if worker_exec:
            await worker_exec.stop()
        if coord_server:
            coord_server.should_exit = True
            if coord_server_task:
                await coord_server_task
        if 'ws_server' in locals() and ws_server:
            ws_server.should_exit = True
            if 'ws_server_task' in locals() and ws_server_task:
                await ws_server_task
        if 'we_server' in locals() and we_server:
            we_server.should_exit = True
            if 'we_server_task' in locals() and we_server_task:
                await we_server_task

        shutil.rmtree(temp_dir, ignore_errors=True)
