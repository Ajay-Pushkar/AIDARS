import asyncio
import sys
import time
from pathlib import Path

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.models import (
    ExecutionSpec,
    ExecutionSubmitRequest,
    ExecutionState,
    FailureCategory,
    WorkloadSpec,
)
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.server import create_worker_app

async def main():
    tmp_path = Path("m20_physical_test")
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / "cas").mkdir(exist_ok=True)
    
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    worker = DistributedWorker(worker_id="w-1", cas_adapter=cas)
    
    print("\n--- TEST 1: No timeout (> 60s workload) ---")
    spec_no_timeout = WorkloadSpec(
        workload_id="task-long",
        task_type="test",
        estimated_duration_seconds=0.1,  # Intentional low estimate
        execution_spec=ExecutionSpec(
            executable=sys.executable,
            args=["-c", "import time; print('starting'); time.sleep(62); print('done')"],
            timeout_seconds=None
        )
    )
    
    status = await worker.submit_execution("task-long#1", spec_no_timeout)
    print(f"Submitted task-long: {status.state.value}")
    
    start_time = time.time()
    last_print = start_time
    while status.state in (ExecutionState.ACCEPTED, ExecutionState.RUNNING):
        await asyncio.sleep(1.0)
        status = await worker.get_execution("task-long#1")
        now = time.time()
        if now - last_print >= 10.0:
            print(f"  still running... elapsed: {now - start_time:.1f}s")
            last_print = now
            
    end_time = time.time()
    print(f"Finished task-long: {status.state.value} in {end_time - start_time:.1f}s")
    assert status.state == ExecutionState.SUCCEEDED
    
    print("\n--- TEST 2: Explicit timeout (10s limit, 60s sleep) ---")
    spec_timeout = WorkloadSpec(
        workload_id="task-explicit",
        task_type="test",
        execution_spec=ExecutionSpec(
            executable=sys.executable,
            args=["-c", "import time; time.sleep(60)"],
            timeout_seconds=10.0
        )
    )
    
    status = await worker.submit_execution("task-explicit#1", spec_timeout)
    print(f"Submitted task-explicit: {status.state.value}")
    
    start_time = time.time()
    while status.state in (ExecutionState.ACCEPTED, ExecutionState.RUNNING):
        await asyncio.sleep(1.0)
        status = await worker.get_execution("task-explicit#1")
        
    end_time = time.time()
    print(f"Finished task-explicit: {status.state.value} in {end_time - start_time:.1f}s")
    assert status.state == ExecutionState.FAILED
    assert status.result.failure_category == FailureCategory.EXECUTION_TIMEOUT
    
    print("\nALL PHYSICAL VALIDATIONS PASSED.")

if __name__ == "__main__":
    asyncio.run(main())
