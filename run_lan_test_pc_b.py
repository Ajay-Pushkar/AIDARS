import uvicorn
import asyncio
from aidars.distributed.server import create_worker_app
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.cas_adapter import LocalCASAdapter

def start_worker_b():
    print("Starting Worker B (Execution Node) on 0.0.0.0:8001")
    
    # Needs a dedicated CAS directory for this worker
    cas = LocalCASAdapter(cas_dir="storage_worker_b/cas")
    
    worker = DistributedWorker(
        worker_id="Worker-B",
        ip_address="0.0.0.0",
        port=8001,
        endpoint_url="http://127.0.0.1:8001", # Will be overriden or user can change to actual IP
        cas_adapter=cas
    )
    
    # We use worker.server.app which was linked in M6
    app = worker.server.app
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")

if __name__ == "__main__":
    start_worker_b()
