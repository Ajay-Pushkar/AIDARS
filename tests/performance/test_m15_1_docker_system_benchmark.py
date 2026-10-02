import pytest
import time
import httpx
from aidars.distributed.models import WorkloadSpec
from .utils import BenchmarkRunner

@pytest.mark.parametrize("scalable_docker_cluster", [1, 3, 5], indirect=True)
def test_docker_system_benchmark(scalable_docker_cluster):
    """
    E2E System Benchmark running against an isolated Docker cluster.
    Measures API Submission overhead and E2E dispatch-to-completion time.
    """
    project_name = scalable_docker_cluster  # Just for logging if needed
    
    headers = {"Authorization": "Bearer test_token"}
    
    # 1. Benchmark Job Submission Overhead
    submit_runner = BenchmarkRunner(f"API Job Submission (Cluster: {project_name})", iterations=10, warmup=2)
    
    for i in range(submit_runner.warmup + submit_runner.iterations):
        job_id = f"bench-submit-{i}"
        workload_id = f"{job_id}-0"
        
        spec = WorkloadSpec(
            workload_id=workload_id,
            job_id=job_id,
            task_type="test",
            min_ram_bytes=1024,
            parameters={
                "command": "python -c \"import time; time.sleep(0.1)\""
            }
        )
        
        payload = {"specs": [spec.model_dump(mode="json")]}
        
        if i >= submit_runner.warmup:
            start = time.perf_counter()
            
        resp = httpx.post("http://127.0.0.1:8000/api/v1/jobs/submit", json=payload, headers=headers)
        assert resp.status_code == 202
        
        if i >= submit_runner.warmup:
            end = time.perf_counter()
            submit_runner.record_ms((end - start) * 1000.0)
            
    submit_result = submit_runner.calculate()
    submit_result.print_report()
    
    # Wait for the cluster to finish processing those jobs to avoid cross-talk
    time.sleep(5)
    
    # 2. Benchmark E2E Latency
    e2e_runner = BenchmarkRunner(f"E2E Latency (Cluster: {project_name})", iterations=5, warmup=1)
    
    for i in range(e2e_runner.warmup + e2e_runner.iterations):
        job_id = f"bench-e2e-{i}"
        workload_id = f"{job_id}-0"
        
        spec = WorkloadSpec(
            workload_id=workload_id,
            job_id=job_id,
            task_type="test",
            min_ram_bytes=1024,
            parameters={
                "command": "python -c \"import time; time.sleep(0.01)\"" # 10ms payload
            }
        )
        
        payload = {"specs": [spec.model_dump(mode="json")]}
        
        if i >= e2e_runner.warmup:
            start = time.perf_counter()
            
        resp = httpx.post("http://127.0.0.1:8000/api/v1/jobs/submit", json=payload, headers=headers)
        assert resp.status_code == 202
        
        # Poll for completion
        while True:
            status_resp = httpx.get(f"http://127.0.0.1:8000/api/v1/jobs/{job_id}", headers=headers)
            assert status_resp.status_code == 200
            data = status_resp.json()
            if data["state"] in ["completed", "failed"]:
                break
            time.sleep(0.05)
            
        if i >= e2e_runner.warmup:
            end = time.perf_counter()
            e2e_runner.record_ms((end - start) * 1000.0)
            
    e2e_result = e2e_runner.calculate()
    e2e_result.print_report()
    
    assert submit_result.p50_ms >= 0
    assert e2e_result.p50_ms >= 0
