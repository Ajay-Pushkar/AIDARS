"""M6 Adaptive Computational Resource System tests."""

import pytest
import time
import asyncio
from pydantic import ValidationError

from aidars.distributed.models import (
    WorkloadSpec,
    WorkerResourceProfile,
    PlacementDecision,
)
from aidars.distributed.singleflight import SingleFlight
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.runtime import GenericSubprocessRuntime
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState
from aidars.distributed.cas_adapter import LocalCASAdapter

# 6.1 Contract Validation
def test_6_1_contract_validation():
    # Valid spec
    spec = WorkloadSpec(
        workload_id="task-123",
        task_type="test",
        input_asset_hashes={"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
    )
    assert spec.min_cpu_cores == 1

    # Invalid hash
    with pytest.raises(ValidationError):
        WorkloadSpec(workload_id="task-2", task_type="test", input_asset_hashes={"invalid-hash"})

    # Negative constraints
    with pytest.raises(ValidationError):
        WorkloadSpec(workload_id="task-3", task_type="test", min_cpu_cores=0)

# 6.3 Placement Hard Filters
def test_6_3_placement_hard_filters():
    engine = PlacementEngine()
    spec = WorkloadSpec(
        workload_id="task-1",
        task_type="test",
        min_ram_bytes=2048,
        requires_gpu=True,
    )

    profiles = [
        WorkerResourceProfile(
            worker_id="w1",
            endpoint_url="http://127.0.0.1:8001",
            ip_address="127.0.0.1",
            cpu_cores_total=4,
            cpu_utilization_percent=10.0,
            ram_total_bytes=4096,
            ram_available_bytes=1024, # Fails min_ram
            gpu_available=True,
        ),
        WorkerResourceProfile(
            worker_id="w2",
            endpoint_url="http://127.0.0.1:8002",
            ip_address="127.0.0.1",
            cpu_cores_total=4,
            cpu_utilization_percent=10.0,
            ram_total_bytes=4096,
            ram_available_bytes=4096,
            gpu_available=False, # Fails requires_gpu
        ),
    ]

    decision = engine.evaluate(spec, profiles)
    assert decision is None, "No worker should be eligible due to hard constraints"

# 6.4 Placement Multi-Score
def test_6_4_placement_multi_score():
    engine = PlacementEngine()
    spec = WorkloadSpec(
        workload_id="task-1",
        task_type="test",
        min_cpu_cores=1,
        min_ram_bytes=1024,
    )

    profiles = [
        WorkerResourceProfile(
            worker_id="w1",
            endpoint_url="http://127.0.0.1:8001",
            ip_address="127.0.0.1",
            cpu_cores_total=2,
            cpu_utilization_percent=90.0, # High load
            ram_total_bytes=2048,
            ram_available_bytes=2048,
            gpu_available=False,
        ),
        WorkerResourceProfile(
            worker_id="w2",
            endpoint_url="http://127.0.0.1:8002",
            ip_address="127.0.0.1",
            cpu_cores_total=4,
            cpu_utilization_percent=10.0, # Low load
            ram_total_bytes=4096,
            ram_available_bytes=4096,
            gpu_available=False,
        ),
    ]

    decision = engine.evaluate(spec, profiles)
    assert decision is not None
    assert decision.selected_worker_id == "w2", "Should pick w2 due to compute headroom"

# 6.7 SingleFlight Stress
@pytest.mark.asyncio
async def test_6_7_singleflight_stress():
    sf = SingleFlight()
    execution_count = 0

    async def _operation():
        nonlocal execution_count
        execution_count += 1
        await asyncio.sleep(0.1)
        return "success"

    # Fire 100 concurrent requests for the same key
    tasks = [asyncio.create_task(sf.run("asset-123", _operation)) for _ in range(100)]
    results = await asyncio.gather(*tasks)

    # All should return success
    assert all(r == "success" for r in results)
    # But the operation should have only executed exactly once
    assert execution_count == 1

# 6.5 Data Locality Bias
def test_6_5_data_locality_bias():
    engine = PlacementEngine()
    h1 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    h2 = "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3" + "000000000000000000000000" # 64 chars
    spec = WorkloadSpec(
        workload_id="task-1",
        task_type="test",
        min_ram_bytes=4096,
        input_asset_hashes={h1, h2}
    )
    # w1 has identical compute to w2 but has local cache
    profiles = [
        WorkerResourceProfile(
            worker_id="w1", endpoint_url="http://w1", ip_address="1.1.1.1",
            cpu_cores_total=4, cpu_utilization_percent=0,
            ram_total_bytes=4096, ram_available_bytes=4096,
            local_cached_hashes={h1, h2}
        ),
        WorkerResourceProfile(
            worker_id="w2", endpoint_url="http://w2", ip_address="2.2.2.2",
            cpu_cores_total=4, cpu_utilization_percent=0,
            ram_total_bytes=4096, ram_available_bytes=4096,
            local_cached_hashes=set()
        ),
    ]
    decision = engine.evaluate(spec, profiles)
    assert decision.selected_worker_id == "w1"
    assert decision.score_breakdown["locality"] == 1.0

# 6.9 Runtime Failure
@pytest.mark.asyncio
async def test_6_9_runtime_failure(tmp_path):
    cas_dir = tmp_path / "cas"
    cas = LocalCASAdapter(cas_dir=str(cas_dir))
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    spec = WorkloadSpec(
        workload_id="task-fail",
        task_type="test",
        parameters={"command": "exit 1"} # Force non-zero exit
    )
    
    res = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())
    assert res.success is False
    assert len(res.output_asset_hashes) == 0

# 6.10 Execution Timeout
@pytest.mark.asyncio
async def test_6_10_execution_timeout(tmp_path):
    cas_dir = tmp_path / "cas"
    cas = LocalCASAdapter(cas_dir=str(cas_dir))
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    spec = WorkloadSpec(
        workload_id="task-timeout",
        task_type="test",
        estimated_duration_seconds=0.1, # Short timeout (0.3s hard timeout)
        parameters={"command": "python -c \"import time; time.sleep(1)\""}
    )
    
    res = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())
    assert res.success is False
    assert res.stderr_snippet is not None
    assert "timed out" in res.stderr_snippet.lower()

# 6.13 Task Idempotency (Registry)
def test_6_13_task_idempotency():
    reg = WorkloadRegistry()
    spec = WorkloadSpec(workload_id="task-dup", task_type="test")
    
    r1 = reg.add_workload(spec)
    r2 = reg.add_workload(spec)
    
    assert r1 is r2
    assert len(reg.list_workloads()) == 1
