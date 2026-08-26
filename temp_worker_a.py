
import uvicorn
from aidars.distributed.worker import DistributedWorker
from aidars.distributed.cas_adapter import LocalCASAdapter
cas = LocalCASAdapter(cas_dir="storage_worker_a/cas")
worker = DistributedWorker(worker_id="Worker-A", ip_address="127.0.0.1", port=8002, cas_adapter=cas)
uvicorn.run(worker.server.app, host="127.0.0.1", port=8002, log_level="warning")
