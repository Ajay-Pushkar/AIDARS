import pytest
import os
import tempfile
import asyncio
from aidars.adapters.ml_training.adapter import MLTrainingAdapter
from aidars.distributed.models import WorkloadSpec
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.worker import DistributedWorker
from aidars.core.assets.manager import AssetManager

@pytest.fixture
def test_script_file():
    fd, path = tempfile.mkstemp(suffix=".py")
    script_content = """import argparse
import os
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
except ImportError:
    torch = None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    if not os.path.exists(args.dataset):
        raise FileNotFoundError(f"Dataset not found at {args.dataset}")
        
    if torch is None:
        print("PyTorch not installed. Faking training loop...")
        with open(args.output, "w") as f:
            f.write("fake_checkpoint")
        return
        
    model = nn.Sequential(nn.Linear(10, 5), nn.ReLU(), nn.Linear(5, 1))
    print(f"Training for {args.epochs} epochs on {args.dataset}...")
    optimizer = optim.SGD(model.parameters(), lr=0.01)
    criterion = nn.MSELoss()
    inputs = torch.randn(args.batch_size, 10)
    targets = torch.randn(args.batch_size, 1)
    for epoch in range(args.epochs):
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, targets)
        loss.backward()
        optimizer.step()
    torch.save(model.state_dict(), args.output)
    print(f"Saved checkpoint to {args.output}")

if __name__ == "__main__":
    main()
"""
    with os.fdopen(fd, 'w') as f:
        f.write(script_content)
    yield path
    os.remove(path)

@pytest.fixture
def test_dataset_file():
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, 'w') as f:
        f.write("dummy_data,label\n0.1,0\n0.2,1\n0.3,0\n")
    yield path
    os.remove(path)

@pytest.mark.asyncio
async def test_m19_7_real_ml_execution(test_script_file, test_dataset_file):
    # Setup Coordinator CAS (Mocking Master)
    coord_cas_dir = tempfile.mkdtemp()
    coord_cas = LocalCASAdapter(coord_cas_dir)
    coord_asset_manager = AssetManager(coord_cas)

    adapter = MLTrainingAdapter(asset_manager=coord_asset_manager)
    
    request = {
        "script_path": test_script_file,
        "dataset_path": test_dataset_file,
        "epochs": 2,
        "batch_size": 4,
        "requires_gpu": False # Force CPU for CI
    }
    
    assert adapter.validate(request)
    specs = adapter.evaluate_request(request)
    assert len(specs) > 0
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
    
    assert result.success is True, f"ML execution failed: {result.error_message}\nSTDOUT: {result.stdout_snippet}\nSTDERR: {result.stderr_snippet}"
    assert len(result.output_asset_hashes) == 1, "Expected exactly 1 output checkpoint"
    
    # Verify the output checkpoint exists in CAS
    out_hash = list(result.output_asset_hashes)[0]
    out_path = worker_cas.get_asset_path(out_hash)
    assert os.path.exists(out_path)
    assert os.path.getsize(out_path) > 0
