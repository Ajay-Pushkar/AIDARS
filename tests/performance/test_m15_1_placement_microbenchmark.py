import pytest
import time
from aidars.distributed.models import WorkloadSpec, WorkerResourceProfile
from aidars.distributed.placement import PlacementEngine
from .utils import BenchmarkRunner

def generate_fake_workers(n: int) -> dict:
    workers = {}
    for i in range(n):
        workers[f"worker-{i}"] = WorkerResourceProfile(
            worker_id=f"worker-{i}",
            endpoint_url=f"http://172.29.0.{11+i}:8001",
            ip_address=f"172.29.0.{11+i}",
            cpu_cores=16,
            cpu_cores_total=16,
            cpu_utilization_percent=10.0,
            ram_bytes=32_000_000_000,
            ram_total_bytes=32_000_000_000,
            ram_available_bytes=30_000_000_000,
            has_gpu=True,
            vram_bytes=16_000_000_000,
            active_workload_count=i % 3,  # Slight variation
            supported_capabilities=["test"],
            status="active"
        )
    return workers

@pytest.mark.parametrize("num_workers", [1, 10, 50, 100])
def test_placement_engine_microbenchmark(num_workers):
    engine = PlacementEngine()
    workers = generate_fake_workers(num_workers)
    spec = WorkloadSpec(
        workload_id="bench-workload",
        job_id="bench-job",
        task_type="test",
        min_ram_bytes=1024,
        parameters={}
    )
    
    runner = BenchmarkRunner(f"PlacementEngine (N={num_workers} workers)", iterations=100, warmup=10)
    
    # Warmup
    for _ in range(runner.warmup):
        engine.evaluate(spec, list(workers.values()))
        
    # Benchmark
    for _ in range(runner.iterations):
        start = time.perf_counter()
        engine.evaluate(spec, list(workers.values()))
        end = time.perf_counter()
        runner.record_ms((end - start) * 1000.0)
        
    result = runner.calculate()
    result.print_report()
    
    # Simple assertion just to ensure the test passes
    assert result.p50_ms >= 0
