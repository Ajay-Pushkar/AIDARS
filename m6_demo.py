"""M6 Adaptive Computational Resource System - End-to-End Demonstration."""

import asyncio
import os
import shutil
import tempfile
import time
from pathlib import Path

from aidars.distributed.models import WorkloadSpec
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.cas_adapter import LocalCASAdapter
import uvicorn
import httpx


async def run_m6_demo():
    print("=== M6 E2E Demo ===")
    
    # 1. Setup CAS and Workloads directory
    demo_dir = Path(tempfile.mkdtemp(prefix="aidar-m6-demo-"))
    cas_dir = demo_dir / "cas"
    
    try:
        # Start coordinator
        print("1. Starting Coordinator...")
        coord = CoordinatorService(coordinator_id="coord-demo", heartbeat_interval_seconds=1.0)
        await coord.start()
        
        # Start worker
        print("2. Starting Worker...")
        worker = DistributedWorker(
            worker_id="worker-demo",
            cas_dir=cas_dir,
            ip_address="127.0.0.1",
            port=8001,
            coordinator_url="http://127.0.0.1:8000"
        )
        # Register synchronously for the mock (bypassing http server)
        coord.register_worker_sync(worker.get_worker_info())
        
        # 3. Submit Workload to Coordinator
        print("3. Submitting Workload...")
        spec = WorkloadSpec(
            workload_id="demo-workload",
            task_type="echo_test",
            parameters={"command": "echo 'Hello from M6 Adaptive Compute!' > outputs/hello.txt"},
            min_cpu_cores=1,
            min_ram_bytes=1024,
            estimated_duration_seconds=5.0
        )
        
        workload_id = await coord.orchestrator.submit_workload(spec)
        print(f"Workload submitted: {workload_id}")
        
        # Give coordinator time to place it
        await asyncio.sleep(0.5)
        
        # Check placement
        record = coord.workload_registry.get_workload(workload_id)
        print(f"Workload state: {record.state.value}")
        if record.placement_decision:
            print(f"Placed on worker: {record.placement_decision.selected_worker_id}")
            
            # 4. Execute on Worker
            print("4. Executing Workload on Worker...")
            result = await worker.execute_workload(spec)
            
            print(f"Execution Success: {result.success}")
            print(f"Duration: {result.execution_duration_seconds:.2f}s")
            if result.stdout_snippet:
                print(f"Stdout:\n{result.stdout_snippet}")
            if result.stderr_snippet:
                print(f"Stderr:\n{result.stderr_snippet}")
                
            print(f"Generated Asset Hashes: {result.output_asset_hashes}")
            
            # Verify asset is in CAS
            for h in result.output_asset_hashes:
                assert worker.cas.has_asset(h)
                with open(worker.cas.get_asset_path(h), "r") as f:
                    content = f.read().strip()
                print(f"Verified CAS content for {h}: {content}")
        
    finally:
        await coord.stop()
        shutil.rmtree(demo_dir, ignore_errors=True)

if __name__ == "__main__":
    asyncio.run(run_m6_demo())
