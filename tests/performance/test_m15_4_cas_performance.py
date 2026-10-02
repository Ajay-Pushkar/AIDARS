import asyncio
import os
import shutil
import tempfile
import time
import pytest
from pathlib import Path

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.runtime import RuntimeAdapter
from aidars.distributed.models import WorkloadSpec, WorkloadExecutionResult

async def event_loop_probe(stop_event: asyncio.Event, intervals_ms: list):
    """Probes the event loop latency. Every 10ms it wakes up and records the actual delay."""
    while not stop_event.is_set():
        start = time.perf_counter()
        await asyncio.sleep(0.01) # Sleep 10ms
        end = time.perf_counter()
        delay_ms = (end - start - 0.01) * 1000.0
        intervals_ms.append(delay_ms)

def create_dummy_file(path: str, size_mb: int):
    size_bytes = size_mb * 1024 * 1024
    chunk_size = 1024 * 1024
    with open(path, "wb") as f:
        data = os.urandom(min(chunk_size, size_bytes))
        written = 0
        while written < size_bytes:
            to_write = min(chunk_size, size_bytes - written)
            f.write(data[:to_write])
            written += to_write

class WriterRuntime(RuntimeAdapter):
    def __init__(self, size_mb: int):
        self.size_mb = size_mb
        self.supports_checkpointing = False

    async def execute(self, spec, workdir):
        return await self.execute_with_context(spec, workdir, None)

    async def execute_with_context(self, spec, workdir, context):
        outputs_dir = os.path.join(workdir, "outputs")
        os.makedirs(outputs_dir, exist_ok=True)
        dummy_file = os.path.join(outputs_dir, "output.bin")
        await asyncio.to_thread(create_dummy_file, dummy_file, self.size_mb)
        return True, "ok", "ok"

    async def checkpoint(self):
        pass

@pytest.mark.asyncio
@pytest.mark.parametrize("size_mb", [10, 100, 500])
async def test_cas_ingestion_baseline(size_mb):
    temp_dir = tempfile.mkdtemp()
    cas_dir = os.path.join(temp_dir, "cas")
    workloads_dir = os.path.join(temp_dir, "workloads")
    cas = LocalCASAdapter(cas_dir=cas_dir)
    exec_manager = ExecutionManager(cas_adapter=cas, workloads_dir=workloads_dir)
    
    stop_event = asyncio.Event()
    probe_delays = []
    probe_task = asyncio.create_task(event_loop_probe(stop_event, probe_delays))
    
    # Yield control to let probe start
    await asyncio.sleep(0.1)
    
    print(f"Starting {size_mb}MB execution & ingestion...")
    start = time.perf_counter()
    
    spec = WorkloadSpec(
        workload_id="test-wl",
        job_id="test-job",
        task_type="test",
        min_ram_bytes=100,
        parameters={}
    )
    
    res = await exec_manager.execute_workload(spec, worker_id="w1", runtime=WriterRuntime(size_mb))
    
    end = time.perf_counter()
    duration_ms = (end - start) * 1000.0
    
    stop_event.set()
    await probe_task
    
    max_delay = max(probe_delays) if probe_delays else 0
    p50_delay = sorted(probe_delays)[len(probe_delays)//2] if probe_delays else 0
    print(f"\n[CAS INGESTION {size_mb} MB]")
    print(f"  Total Duration: {duration_ms:.2f} ms")
    print(f"  Ingestion Duration: {res.output_ingestion_duration_seconds * 1000.0:.2f} ms")
    print(f"  Event Loop Max Delay: {max_delay:.2f} ms")
    print(f"  Event Loop p50 Delay: {p50_delay:.2f} ms")
    
    assert res.success
    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.mark.asyncio
@pytest.mark.parametrize("size_mb", [10, 100, 500])
async def test_cas_staging_baseline(size_mb):
    temp_dir = tempfile.mkdtemp()
    cas_dir = os.path.join(temp_dir, "cas")
    cas = LocalCASAdapter(cas_dir=cas_dir)
    
    dummy_file = os.path.join(temp_dir, "input.bin")
    create_dummy_file(dummy_file, size_mb)
    
    h = cas.store_file(dummy_file)
    
    dest_path = os.path.join(temp_dir, "staged.bin")
    asset_path = cas.get_asset_path(h)
    
    stop_event = asyncio.Event()
    probe_delays = []
    probe_task = asyncio.create_task(event_loop_probe(stop_event, probe_delays))
    
    await asyncio.sleep(0.1)
    
    start = time.perf_counter()
    
    try:
        os.link(asset_path, dest_path)
    except OSError:
        shutil.copy2(asset_path, dest_path)
        
    end = time.perf_counter()
    duration_ms = (end - start) * 1000.0
    
    stop_event.set()
    await probe_task
    
    max_delay = max(probe_delays) if probe_delays else 0
    
    print(f"\n[CAS STAGING {size_mb} MB]")
    print(f"  Duration: {duration_ms:.2f} ms")
    print(f"  Event Loop Max Delay: {max_delay:.2f} ms")
    
    shutil.rmtree(temp_dir, ignore_errors=True)
