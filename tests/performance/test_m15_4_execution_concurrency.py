import asyncio
import os
import shutil
import tempfile
import time
import pytest

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.runtime import RuntimeAdapter
from aidars.distributed.models import WorkloadSpec

class DummyRuntime(RuntimeAdapter):
    def __init__(self, delay_sec: float = 0.1):
        self.delay_sec = delay_sec
        self.supports_checkpointing = False

    async def execute(self, spec, workdir):
        return await self.execute_with_context(spec, workdir, None)

    async def execute_with_context(self, spec, workdir, context):
        await asyncio.sleep(self.delay_sec)
        return True, "ok", "ok"

    async def checkpoint(self):
        pass

async def event_loop_probe(stop_event: asyncio.Event, intervals_ms: list):
    while not stop_event.is_set():
        start = time.perf_counter()
        await asyncio.sleep(0.01)
        end = time.perf_counter()
        delay_ms = (end - start - 0.01) * 1000.0
        intervals_ms.append(delay_ms)

@pytest.mark.asyncio
async def test_execution_manager_concurrency():
    temp_dir = tempfile.mkdtemp()
    cas_dir = os.path.join(temp_dir, "cas")
    workloads_dir = os.path.join(temp_dir, "workloads")
    cas = LocalCASAdapter(cas_dir=cas_dir)
    exec_manager = ExecutionManager(cas_adapter=cas, workloads_dir=workloads_dir)
    
    # We want to measure the overhead of submitting 100 concurrent workloads.
    # The dummy runtime takes 100ms. If execution dispatch is purely async and parallel,
    # the total time should be close to 100ms + minimal overhead.
    num_tasks = 100
    runtime = DummyRuntime(delay_sec=0.1)
    
    stop_event = asyncio.Event()
    probe_delays = []
    probe_task = asyncio.create_task(event_loop_probe(stop_event, probe_delays))
    
    specs = [
        WorkloadSpec(
            workload_id=f"wl-{i}",
            job_id="job",
            task_type="test",
            min_ram_bytes=100,
            parameters={}
        ) for i in range(num_tasks)
    ]
    
    print(f"\nSubmitting {num_tasks} concurrent workloads to ExecutionManager...")
    
    await asyncio.sleep(0.1)
    
    start = time.perf_counter()
    
    tasks = []
    for spec in specs:
        tasks.append(
            asyncio.create_task(
                exec_manager.execute_workload(spec, worker_id="w1", runtime=runtime)
            )
        )
    
    results = await asyncio.gather(*tasks)
    end = time.perf_counter()
    
    duration_ms = (end - start) * 1000.0
    
    stop_event.set()
    await probe_task
    
    max_delay = max(probe_delays) if probe_delays else 0
    p50_delay = sorted(probe_delays)[len(probe_delays)//2] if probe_delays else 0
    
    print(f"\n[EXECUTION CONCURRENCY N={num_tasks}]")
    print(f"  Duration: {duration_ms:.2f} ms")
    print(f"  Event Loop Max Delay: {max_delay:.2f} ms")
    print(f"  Event Loop p50 Delay: {p50_delay:.2f} ms")
    
    assert all(r.success for r in results)
    shutil.rmtree(temp_dir, ignore_errors=True)
