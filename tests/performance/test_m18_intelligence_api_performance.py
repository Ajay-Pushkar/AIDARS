import pytest
import time
from fastapi.testclient import TestClient

from aidars.distributed.auth import CredentialStore
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadState
from aidars.distributed.models import WorkloadSpec
from aidars.distributed.attempt import AttemptRecord, AttemptStatus
from aidars.distributed.models import WorkloadExecutionResult
from .utils import BenchmarkRunner

def test_intelligence_api_performance():
    # Setup coordinator and intelligence with fake data
    service = CoordinatorService(credential_store=CredentialStore(admin_tokens={"admin-test"}, insecure_mode=False))

    # Generate 100 workloads and 300 attempts
    records = []
    attempts = []
    now = time.time()
    for i in range(100):
        wid = f"bench-task-{i}"
        w = WorkloadRecord(WorkloadSpec(workload_id=wid, task_type="render"))
        w.state = WorkloadState.COMPLETED
        w.submitted_at = now - 30
        w.completed_at = now
        records.append(w)
        service.workload_registry.add_workload(w.spec)

        for a_num in range(3):
            duration = 10.0 + (i % 5)
            a = AttemptRecord(
                attempt_id=f"{wid}#{a_num}",
                workload_id=wid,
                attempt_number=a_num,
                status=AttemptStatus.SUCCEEDED,
                worker_id=f"worker-{i%10}",
                queued_at=now - 20,
                assigned_at=now - 15,
                started_at=now - 10,
                finished_at=now,
                execution_result=WorkloadExecutionResult(
                    workload_id=wid, worker_id=f"worker-{i%10}",
                    success=True, output_asset_hashes=set(),
                    execution_duration_seconds=duration
                )
            )
            attempts.append(a)

    service.m12_intelligence.rebuild_history(records, attempts)

    client = TestClient(service.app)
    headers = {"Authorization": "Bearer admin-test"}

    runner = BenchmarkRunner("Intelligence API - /workloads/{id}", iterations=100, warmup=10)
    for _ in range(runner.warmup):
        client.get("/api/v1/intelligence/workloads/bench-task-50", headers=headers)

    for _ in range(runner.iterations):
        start = time.perf_counter()
        resp = client.get("/api/v1/intelligence/workloads/bench-task-50", headers=headers)
        end = time.perf_counter()
        assert resp.status_code == 200
        runner.record_ms((end - start) * 1000.0)

    result_workload = runner.calculate()
    result_workload.print_report()

    runner_cap = BenchmarkRunner("Intelligence API - /capacity", iterations=100, warmup=10)
    for _ in range(runner_cap.warmup):
        client.get("/api/v1/intelligence/capacity", headers=headers)

    for _ in range(runner_cap.iterations):
        start = time.perf_counter()
        resp = client.get("/api/v1/intelligence/capacity", headers=headers)
        end = time.perf_counter()
        assert resp.status_code == 200
        runner_cap.record_ms((end - start) * 1000.0)

    result_cap = runner_cap.calculate()
    result_cap.print_report()
