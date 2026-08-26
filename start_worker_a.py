import asyncio
import uvicorn
from pathlib import Path

from aidars.distributed.worker import DistributedWorker

async def main():
    worker = DistributedWorker(
        worker_id="worker-a",
        ip_address="127.0.0.2",
        port=8001,
        coordinator_url="http://127.0.0.2:8000",
        cas_dir=Path(r"C:\AIDAR-M5\worker-a\cas"),
    )

    print("[AIDAR TEST] Starting Worker A...")
    print("[AIDAR TEST] Worker ID: worker-a")
    print("[AIDAR TEST] Coordinator: http://127.0.0.2:8000")
    print(r"[AIDAR TEST] CAS: C:\AIDAR-M5\worker-a\cas")

    await worker.start()

    print("[AIDAR TEST] Worker.start() completed")
    print("[AIDAR TEST] Starting WorkerServer on 127.0.0.2:8001")

    config = uvicorn.Config(
        app=worker.server.app,
        host="127.0.0.2",
        port=8001,
        log_level="info",
    )

    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
