import pytest
import os
import shutil
import tempfile
import asyncio
from aidars.adapters.llm.adapter import LLMAdapter
from aidars.distributed.models import WorkloadSpec
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.worker import DistributedWorker
from aidars.core.assets.manager import AssetManager

def is_llama_installed():
    return shutil.which("llama-cli") is not None

@pytest.fixture
def mock_model_file():
    fd, path = tempfile.mkstemp(suffix=".gguf")
    with os.fdopen(fd, 'w') as f:
        f.write("mock_model_data")
    yield path
    os.remove(path)

@pytest.mark.asyncio
async def test_m19_6_llm_distributed_orchestration(mock_model_file):
    if not is_llama_installed():
        pytest.skip("ENVIRONMENT-GATED: llama-cli not found")
        
    coord_cas_dir = tempfile.mkdtemp()
    coord_cas = LocalCASAdapter(coord_cas_dir)
    coord_asset_manager = AssetManager(coord_cas)
    
    adapter = LLMAdapter(asset_manager=coord_asset_manager)
    
    # 1. Submit multiple independent prompts (Distributed Orchestration)
    prompts = ["Tell me a joke", "What is AI?", "Translate hello"]
    all_specs = []
    
    for prompt in prompts:
        req = {
            "model_path": mock_model_file,
            "prompt": prompt,
            "max_tokens": 10,
            "requires_gpu": False
        }
        assert adapter.validate(req)
        specs = adapter.evaluate_request(req)
        spec = specs[0]
        spec.execution_spec = adapter.build_execution_spec(spec)
        all_specs.append(spec)
        
    assert len(all_specs) == 3
    # Check that job_ids and workload_ids are distinct for independent workloads
    job_ids = {s.job_id for s in all_specs}
    assert len(job_ids) == 3
    
    # 2. Worker setup
    worker_cas_dir = tempfile.mkdtemp()
    worker_cas = LocalCASAdapter(worker_cas_dir)
    
    # Simulate single_flight_sync for Worker A
    spec_a = all_specs[0]
    for h in spec_a.input_asset_hashes:
        source_path = coord_cas.get_asset_path(h)
        worker_cas.store_file(source_path, expected_sha256=h)
        
    worker_a = DistributedWorker(
        coordinator_url="http://localhost:dummy",
        port=0,
        cas_adapter=worker_cas
    )
    
    # 3. Physical Execution Validation
    result = await worker_a.execute_workload(spec_a)
    
    assert result.success is True
    assert len(result.output_asset_hashes) == 1
