import asyncio
from aidars.distributed.models import WorkloadSpec, WorkerInfo
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.cas_adapter import LocalCASAdapter
import time

async def run_coordinator():
    print("Starting PC-A Coordinator...")
    
    registry = WorkerRegistry()
    # Replace the IP with actual PC-B LAN IP (e.g. 192.168.1.10)
    WORKER_B_IP = "127.0.0.1"
    
    print(f"Registering PC-B at {WORKER_B_IP}:8001")
    registry.register_worker(WorkerInfo(
        worker_id="Worker-B",
        endpoint_url=f"http://{WORKER_B_IP}:8001",
        ip_address=WORKER_B_IP,
        port=8001,
        cpu_cores_total=8,
        capacity_bytes=10*1024**3,
        used_bytes=0,
        last_heartbeat_utc=time.time()
    ))
    
    # We need a WorkloadRegistry and Orchestrator
    from aidars.distributed.workload_registry import WorkloadRegistry
    workload_reg = WorkloadRegistry()
    orch = WorkloadOrchestrator(registry, workload_reg)
    
    # Create the payload that Worker-B will need
    cas = LocalCASAdapter("storage_coord/cas")
    asset_hash = cas.store_bytes(b"print('Hello from PC-A via physical LAN!')")
    print(f"Created script asset locally on PC-A CAS: {asset_hash}")
    
    spec = WorkloadSpec(
        workload_id="lan-task-01",
        task_type="python_script",
        parameters={"script_hash": asset_hash},
        min_cpu_cores=1
    )
    
    print("Submitting workload to orchestrator...")
    await orch.submit_workload(spec)
    
    # Wait for completion
    for _ in range(10):
        await asyncio.sleep(1)
        record = workload_reg.get_workload("lan-task-01")
        print(f"State: {record.state.name}")
        if record.state.name in ("COMPLETED", "FAILED"):
            break
            
    print(f"Final State: {record.state.name}")
    if record.state.name == "FAILED":
        print(f"Error: {record.error_message}")
        
if __name__ == "__main__":
    asyncio.run(run_coordinator())
