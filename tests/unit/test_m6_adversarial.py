import pytest
import asyncio
from typing import Set

from aidars.distributed.models import (
    WorkloadSpec,
    WorkerResourceProfile,
)
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.runtime import GenericSubprocessRuntime
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.singleflight import SingleFlight
from aidars.distributed.workload_registry import WorkloadRegistry
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.models import WorkerInfo

# ==========================================
# CAMPAIGN A: RESOURCE ADMISSION (6.15)
# ==========================================
def test_a1_resource_admission():
    engine = PlacementEngine()
    
    profiles = [
        WorkerResourceProfile(
            worker_id="WorkerA", endpoint_url="http://A", ip_address="1.1.1.1",
            cpu_cores_total=8, cpu_utilization_percent=0.0,
            ram_total_bytes=16 * 1024**3, ram_available_bytes=16 * 1024**3,
            gpu_available=True
        ),
        WorkerResourceProfile(
            worker_id="WorkerB", endpoint_url="http://B", ip_address="2.2.2.2",
            cpu_cores_total=4, cpu_utilization_percent=0.0,
            ram_total_bytes=8 * 1024**3, ram_available_bytes=8 * 1024**3,
            gpu_available=False
        )
    ]
    
    w1 = WorkloadSpec(workload_id="W1", task_type="test", min_cpu_cores=2, min_ram_bytes=4 * 1024**3, requires_gpu=False)
    w2 = WorkloadSpec(workload_id="W2", task_type="test", min_cpu_cores=6, min_ram_bytes=12 * 1024**3, requires_gpu=True)
    w3 = WorkloadSpec(workload_id="W3", task_type="test", min_cpu_cores=10, min_ram_bytes=32 * 1024**3, requires_gpu=True)
    
    # W1 -> A or B
    d1 = engine.evaluate(w1, profiles)
    assert d1 is not None and d1.selected_worker_id in ("WorkerA", "WorkerB")
    
    # W2 -> A only
    d2 = engine.evaluate(w2, profiles)
    assert d2 is not None and d2.selected_worker_id == "WorkerA"
    
    # W3 -> Rejected
    d3 = engine.evaluate(w3, profiles)
    assert d3 is None

# ==========================================
# CAMPAIGN B & C: EXECUTION & RECOVERY
# ==========================================
@pytest.mark.asyncio
async def test_c1_runtime_crash(tmp_path):
    cas_dir = tmp_path / "cas"
    cas = LocalCASAdapter(cas_dir=str(cas_dir))
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    spec = WorkloadSpec(
        workload_id="task-crash",
        task_type="test",
        parameters={"command": "python -c \"import sys; sys.exit(1)\""}
    )
    
    res = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())
    assert not res.success
    assert len(res.output_asset_hashes) == 0

@pytest.mark.asyncio
async def test_c2_runtime_timeout(tmp_path):
    cas_dir = tmp_path / "cas"
    cas = LocalCASAdapter(cas_dir=str(cas_dir))
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    spec = WorkloadSpec(
        workload_id="task-timeout",
        task_type="test",
        estimated_duration_seconds=0.1,
        parameters={"command": "python -c \"import time; time.sleep(10)\""}
    )
    
    res = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())
    assert not res.success
    assert "timed out" in (res.stderr_snippet or "").lower()

@pytest.mark.asyncio
async def test_c4_partial_output(tmp_path):
    cas_dir = tmp_path / "cas"
    cas = LocalCASAdapter(cas_dir=str(cas_dir))
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    # Writes part of a file then crashes
    spec = WorkloadSpec(
        workload_id="task-partial",
        task_type="test",
        parameters={"command": "python -c \"open('outputs/partial.dat', 'w').write('half'); import sys; sys.exit(1)\""}
    )
    
    res = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())
    assert not res.success
    assert len(res.output_asset_hashes) == 0

# ==========================================
# CAMPAIGN D: SINGLEFLIGHT TORTURE
# ==========================================
@pytest.mark.asyncio
async def test_d1_singleflight_100_way():
    sf = SingleFlight()
    counter = 0
    
    async def fetch():
        nonlocal counter
        counter += 1
        await asyncio.sleep(0.01)
        return "success"
    
    # 10 hashes, 100 callers each
    tasks = []
    for h_idx in range(10):
        for _ in range(100):
            tasks.append(asyncio.create_task(sf.run(f"hash-{h_idx}", fetch)))
            
    results = await asyncio.gather(*tasks)
    
    assert len(results) == 1000
    assert all(r == "success" for r in results)
    assert counter == 10  # Exactly 1 network fetch per hash

@pytest.mark.asyncio
async def test_d2_singleflight_failure_recovery():
    sf = SingleFlight()
    attempt = 0
    
    async def flaky_fetch():
        nonlocal attempt
        attempt += 1
        await asyncio.sleep(0.01)
        if attempt == 1:
            raise ValueError("Network failure")
        return "recovered"
    
    # First 10 callers fail together
    tasks1 = [asyncio.create_task(sf.run("fail-hash", flaky_fetch)) for _ in range(10)]
    results1 = await asyncio.gather(*tasks1, return_exceptions=True)
    assert all(isinstance(r, ValueError) for r in results1)
    assert attempt == 1
    
    # 11th caller retries and succeeds
    res = await sf.run("fail-hash", flaky_fetch)
    assert res == "recovered"
    assert attempt == 2

@pytest.mark.asyncio
async def test_c3_output_corruption(tmp_path):
    cas_dir = tmp_path / "cas"
    cas = LocalCASAdapter(cas_dir=str(cas_dir))
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    # We will simulate output tampering by manually altering the file in the staging CAS context
    # However, ExecutionManager uses `self.cas.store_bytes()`, which computes hash directly from memory.
    # To simulate corruption, we'll override store_bytes to simulate a hash mismatch.
    
    original_store = cas.store_bytes
    def malicious_store(data: bytes) -> str:
        # returns a fake hash that won't match validation when someone else reads it, 
        # or we just raise ValueError which CAS adapter does on corruption.
        raise ValueError("Invalid SHA-256 hash format")
        
    cas.store_bytes = malicious_store
    
    spec = WorkloadSpec(
        workload_id="task-tamper",
        task_type="test",
        parameters={"command": "python -c \"open('outputs/data.txt', 'w').write('valid data')\""}
    )
    
    try:
        res = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())
        assert not res.success
        assert "ingestion failed" in res.stderr_snippet.lower()
    finally:
        cas.store_bytes = original_store

@pytest.mark.asyncio
async def test_j1_concurrent_workloads(tmp_path):
    cas_dir = tmp_path / "cas"
    cas = LocalCASAdapter(cas_dir=str(cas_dir))
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    # Fire 10 parallel workloads that write distinct outputs
    async def run_workload(idx):
        spec = WorkloadSpec(
            workload_id=f"task-parallel-{idx}",
            task_type="test",
            parameters={"command": f"python -c \"open('outputs/out.txt', 'w').write('data {idx}')\""}
        )
        return await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())
        
    tasks = [asyncio.create_task(run_workload(i)) for i in range(10)]
    results = await asyncio.gather(*tasks)
    
    assert all(r.success for r in results)
    
    # Each must produce a distinct hash because the content is distinct
    hashes = set()
    for r in results:
        hashes.update(r.output_asset_hashes)
        
    assert len(hashes) == 10
