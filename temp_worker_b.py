
import uvicorn
import os
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.cas_adapter import LocalCASAdapter
ip = os.environ.get("WORKER_B_IP", "0.0.0.0")
cas = LocalCASAdapter(cas_dir="storage_worker_b/cas")
worker = DistributedWorker(worker_id="Worker-B", ip_address=ip, port=8001, cas_adapter=cas)
uvicorn.run(worker.server.app, host=ip, port=8001, log_level="warning")
