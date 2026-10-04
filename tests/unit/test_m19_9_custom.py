import pytest
import os
import tempfile
import asyncio
from aidars.adapters.custom.adapter import CustomScriptAdapter
from aidars.distributed.models import WorkloadSpec
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.worker import DistributedWorker
from aidars.core.assets.manager import AssetManager

@pytest.fixture
def custom_python_script():
    fd, path = tempfile.mkstemp(suffix=".py")
    script = """
import sys
import os

print("Running custom script...")
output_dir = "./outputs"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

with open(os.path.join(output_dir, "result.txt"), "w") as f:
    f.write("custom_success")
print("Done!")
"""
    with os.fdopen(fd, 'w') as f:
        f.write(script.strip())
    yield path
    os.remove(path)

@pytest.mark.asyncio
async def test_m19_9_custom_script_execution(custom_python_script):
    # Setup Coordinator CAS (Mocking Master)
    coord_cas_dir = tempfile.mkdtemp()
    coord_cas = LocalCASAdapter(coord_cas_dir)
    coord_asset_manager = AssetManager(coord_cas)

    adapter = CustomScriptAdapter(asset_manager=coord_asset_manager)
    
    request = {
        "script_path": custom_python_script,
        "args": ["--do-something"],
        "expected_output_count": 1
    }
    
    assert adapter.validate(request)
    specs = adapter.evaluate_request(request)
    assert len(specs) == 1
    spec = specs[0]
    spec.execution_spec = adapter.build_execution_spec(spec)
    
    # Setup Worker
    worker_cas_dir = tempfile.mkdtemp()
    worker_cas = LocalCASAdapter(worker_cas_dir)
    
    # Simulate single_flight_sync network transfer
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
    
    assert result.success is True, f"Custom script execution failed: {result.error_message}\nSTDOUT: {result.stdout_snippet}\nSTDERR: {result.stderr_snippet}"
    assert len(result.output_asset_hashes) == 1, "Expected exactly 1 output file"
    assert "Running custom script..." in result.stdout_snippet
    
    # Verify the output artifact exists in CAS
    out_hash = list(result.output_asset_hashes)[0]
    out_path = worker_cas.get_asset_path(out_hash)
    assert os.path.exists(out_path)
    with open(out_path, "r") as f:
        assert f.read() == "custom_success"
