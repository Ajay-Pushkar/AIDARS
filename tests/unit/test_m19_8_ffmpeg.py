import pytest
import os
import shutil
import tempfile
import asyncio
from aidars.adapters.video_rendering.adapter import FFmpegAdapter
from aidars.distributed.models import WorkloadSpec
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.worker import DistributedWorker
from aidars.core.assets.manager import AssetManager

def is_ffmpeg_installed():
    return shutil.which("ffmpeg") is not None

@pytest.fixture
def mock_video_file():
    fd, path = tempfile.mkstemp(suffix=".mp4")
    with os.fdopen(fd, 'w') as f:
        f.write("mock_video_data")
    yield path
    os.remove(path)

@pytest.mark.asyncio
async def test_m19_8_ffmpeg_distributed_orchestration(mock_video_file):
    if not is_ffmpeg_installed():
        pytest.skip("ENVIRONMENT-GATED: ffmpeg not found")
        
    coord_cas_dir = tempfile.mkdtemp()
    coord_cas = LocalCASAdapter(coord_cas_dir)
    coord_asset_manager = AssetManager(coord_cas)
    
    adapter = FFmpegAdapter(asset_manager=coord_asset_manager)
    
    req = {
        "input_video_path": mock_video_file,
        "output_format": "mp4",
        "requires_gpu": False
    }
    
    assert adapter.validate(req)
    specs = adapter.evaluate_request(req)
    assert len(specs) == 1
    spec = specs[0]
    spec.execution_spec = adapter.build_execution_spec(spec)
    
    # Worker setup
    worker_cas_dir = tempfile.mkdtemp()
    worker_cas = LocalCASAdapter(worker_cas_dir)
    
    # Simulate single_flight_sync
    for h in spec.input_asset_hashes:
        source_path = coord_cas.get_asset_path(h)
        worker_cas.store_file(source_path, expected_sha256=h)
        
    worker = DistributedWorker(
        coordinator_url="http://localhost:dummy",
        port=0,
        cas_adapter=worker_cas
    )
    
    # Physical Execution Validation
    result = await worker.execute_workload(spec)
    
    assert result.success is True
    assert len(result.output_asset_hashes) == 1
