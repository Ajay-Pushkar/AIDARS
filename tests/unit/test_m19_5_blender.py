import pytest
import os
import time
import asyncio
import tempfile
from aidars.adapters.blender.adapter import BlenderAdapter
from aidars.distributed.models import WorkloadSpec
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.worker import DistributedWorker, WorkerStatus
from aidars.distributed.execution import ExecutionManager
from aidars.core.assets.manager import AssetManager

@pytest.fixture
def test_blend_file():
    path = os.path.join("tests", "data", "cube.blend")
    if not os.path.exists(path):
        pytest.skip("Test requires cube.blend")
    return path

@pytest.mark.asyncio
async def test_m19_5_real_blender_execution(test_blend_file):
    # Setup Coordinator CAS (Mocking Master)
    coord_cas_dir = tempfile.mkdtemp()
    coord_cas = LocalCASAdapter(coord_cas_dir)
    coord_asset_manager = AssetManager(coord_cas)

    # BlenderAdapter uses coord_asset_manager to ingest the file
    adapter = BlenderAdapter(asset_manager=coord_asset_manager)
    
    request = {
        "input_path": test_blend_file,
        "frame_start": 1,
        "frame_end": 1,
        "worker_count": 1,
        "requires_gpu": False # Use CPU rendering to guarantee it runs on CI/Test env without GPU
    }
    
    assert adapter.validate(request)
    specs = adapter.evaluate_request(request)
    assert len(specs) > 0
    spec = specs[0]
    spec.estimated_duration_seconds = 30.0
    spec.execution_spec = adapter.build_execution_spec(spec)
    
    # Setup Worker
    worker_cas_dir = tempfile.mkdtemp()
    worker_cas = LocalCASAdapter(worker_cas_dir)
    
    # Manually copy the ingested asset to the worker CAS to simulate network transfer
    # In E2E this is done by single_flight_sync
    import shutil
    for h in spec.input_asset_hashes:
        source_path = coord_cas.get_asset_path(h)
        worker_cas.store_file(source_path, expected_sha256=h)
    
    worker = DistributedWorker(
        coordinator_url="http://localhost:dummy",
        port=0,
        cas_adapter=worker_cas
    )
    
    # We execute directly using the worker's execute_workload
    # This will stage assets, execute, and harvest outputs
    result = await worker.execute_workload(spec)
    
    assert result.success is True, f"Blender execution failed: {result.error_message}\nSTDOUT: {result.stdout_snippet}\nSTDERR: {result.stderr_snippet}"
    assert len(result.output_asset_hashes) == 1, "Expected exactly 1 output frame"
    
    # Verify the output is in the worker CAS
    out_hash = list(result.output_asset_hashes)[0]
    out_path = worker_cas.get_asset_path(out_hash)
    assert os.path.exists(out_path)
    assert os.path.getsize(out_path) > 0
