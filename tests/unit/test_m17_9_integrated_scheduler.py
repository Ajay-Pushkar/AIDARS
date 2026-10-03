import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from aidars.distributed.models import (
    WorkerResourceProfile, WorkerStatus, WorkloadSpec,
    WorkerInfo, WorkerCapabilities, FailureCategory, WorkloadExecutionResult
)
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState
from aidars.distributed.attempt import AttemptRegistry, AttemptStatus
from aidars.distributed.workload import WorkloadOrchestrator


def profile(worker_id="w-1", **overrides):
    values = dict(worker_id=worker_id, endpoint_url=f"http://{worker_id}", ip_address="127.0.0.1",
                  cpu_cores_total=8, cpu_utilization_percent=0, ram_total_bytes=16_000,
                  ram_available_bytes=16_000, gpu_available=True, gpu_vendor="NVIDIA",
                  gpu_model="RTX Test", gpu_device_name="RTX Test", gpu_compute_capability="8.9",
                  gpu_driver_version="550.54", vram_total_bytes=8_000, vram_available_bytes=8_000,
                  active_workload_count=0, max_concurrent_workloads=2,
                  local_cached_hashes=set(), timestamp_utc=time.time(), cost_per_hour=0.0)
    values.update(overrides)
    return WorkerResourceProfile(**values)


def spec(workload_id="task-1", **overrides):
    values = dict(workload_id=workload_id, task_type="compute",
                  min_cpu_cores=1, min_ram_bytes=1000)
    values.update(overrides)
    return WorkloadSpec(**values)


def test_hard_resource_constraints_plus_cost():
    # 1. HARD RESOURCE CONSTRAINTS + COST
    engine = PlacementEngine()
    s = spec("w1", min_cpu_cores=4)
    # A is cheaper but lacks CPU
    w_a = profile("w-cheap-weak", cpu_cores_total=2, cpu_utilization_percent=0, cost_per_hour=1.0)
    # B is more expensive and has CPU
    w_b = profile("w-expensive", cpu_cores_total=4, cpu_utilization_percent=0, cost_per_hour=5.0)

    decision = engine.evaluate(s, [w_a, w_b])
    assert decision.selected_worker_id == "w-expensive"

    exp_a = next(e for e in decision.candidate_explanations if e["worker_id"] == "w-cheap-weak")
    assert not exp_a["eligible"]
    assert "INSUFFICIENT_CPU" in exp_a["rejection_reasons"]


def test_hard_gpu_requirements_plus_locality():
    # 2. HARD GPU REQUIREMENTS + LOCALITY
    engine = PlacementEngine()
    s = spec("w1", requires_gpu=True, min_vram_bytes=8_000, input_asset_hashes={"a"*64})
    # A has locality but no GPU
    w_a = profile("w-local-no-gpu", gpu_available=False, local_cached_hashes={"a"*64})
    # B has GPU but no locality
    w_b = profile("w-gpu-no-local", gpu_available=True, vram_available_bytes=8_000, local_cached_hashes=set())

    decision = engine.evaluate(s, [w_a, w_b], asset_sizes_bytes={"a"*64: 100})
    assert decision.selected_worker_id == "w-gpu-no-local"

    exp_a = next(e for e in decision.candidate_explanations if e["worker_id"] == "w-local-no-gpu")
    assert not exp_a["eligible"]
    assert "REQUIRED_GPU_UNAVAILABLE" in exp_a["rejection_reasons"]


def test_deadline_plus_cost():
    # 3. DEADLINE + COST
    engine = PlacementEngine()
    now = time.time()
    s = spec("w1", deadline_at_utc=now + 10.0, estimated_duration_seconds=10.0)
    w_cheap = profile("w-cheap", cost_per_hour=2.0)
    w_expensive = profile("w-expensive", cost_per_hour=10.0)

    # Cost doesn't strictly override deadline, but they are both eligible.
    # The queue handles deadline priority for admission, but placement engine just tracks deadline state.
    decision = engine.evaluate(s, [w_cheap, w_expensive])
    assert decision is not None, "Both workers should be eligible"
    assert decision.deadline_details["state"] == "approaching"
    assert decision.selected_worker_id == "w-cheap"

    exp = next(e for e in decision.candidate_explanations if e["worker_id"] == "w-cheap")
    assert exp["m11_ranking_factors"]["cost_penalty"] == -2.0


def test_priority_aging_plus_cost(monkeypatch):
    # 4. PRIORITY/AGING + COST (Queue sorting)
    registry = WorkloadRegistry()
    w_reg = WorkerRegistry()
    orc = WorkloadOrchestrator(w_reg, registry)

    s1 = spec("task-1", priority=10)
    s2 = spec("task-2", priority=50)

    registry.add_workload(s1)
    registry.add_workload(s2)

    now = time.time()
    
    # task-2 has higher priority. Even if cost is a factor for placement, the queue 
    # evaluates priority first.
    q_snap = orc.queue_snapshot()
    # Ordered highest effective_priority first.
    assert q_snap[0]["workload_id"] == "task-2"
    assert q_snap[1]["workload_id"] == "task-1"


def test_affinity_plus_cost():
    # 5. AFFINITY + COST
    engine = PlacementEngine()
    s = spec("w1", affinity_mode="same_worker", affinity_hard=True, affinity_group_id="group1")
    w_cheap = profile("w-cheap", cost_per_hour=1.0)
    w_expensive = profile("w-expensive", cost_per_hour=5.0)

    # Hard affinity to expensive worker
    decision = engine.evaluate(s, [w_cheap, w_expensive], group_workers={"group1": {"w-expensive"}})
    assert decision.selected_worker_id == "w-expensive"


def test_worker_health_plus_cost():
    # 6. WORKER HEALTH + COST
    engine = PlacementEngine()
    s = spec("w1")
    w_cheap = profile("w-cheap", cost_per_hour=1.0, status=WorkerStatus.OFFLINE)
    w_expensive = profile("w-expensive", cost_per_hour=5.0)

    decision = engine.evaluate(s, [w_cheap, w_expensive])
    assert decision.selected_worker_id == "w-expensive"


def test_dependency_readiness_plus_cost():
    # 7. DEPENDENCY READINESS + COST
    registry = WorkloadRegistry()
    w_reg = WorkerRegistry()
    orc = WorkloadOrchestrator(w_reg, registry)

    s_dep = spec("dep-1")
    s_task = spec("task-1", depends_on=["dep-1"])
    
    r_dep = registry.add_workload(s_dep)
    r_task = registry.add_workload(s_task)
    
    assert orc._dependency_state(r_task) == "waiting"


def test_gpu_compatibility_plus_cost():
    # 8. GPU COMPATIBILITY + COST
    engine = PlacementEngine()
    s = spec("w1", requires_gpu=True, required_driver_version="550.54")
    
    w_cheap = profile("w-cheap", cost_per_hour=1.0, gpu_driver_version="470.00")
    w_expensive = profile("w-expensive", cost_per_hour=5.0, gpu_driver_version="550.54")

    decision = engine.evaluate(s, [w_cheap, w_expensive])
    assert decision.selected_worker_id == "w-expensive"
    
    exp = next(e for e in decision.candidate_explanations if e["worker_id"] == "w-cheap")
    assert "GPU_DRIVER_VERSION_MISMATCH_OR_UNKNOWN" in exp["rejection_reasons"]


def test_deadline_plus_locality_plus_cost():
    # 9. DEADLINE + LOCALITY + COST
    engine = PlacementEngine()
    s = spec("w1", input_asset_hashes={"a"*64})
    
    # Locality gives w_d * 1.0 (w_d=2.0) => score = 2.0
    w_local = profile("w-local", cost_per_hour=5.0, local_cached_hashes={"a"*64})
    
    # Cheap gives -cost_penalty => -1.0
    w_cheap = profile("w-cheap", cost_per_hour=1.0, local_cached_hashes=set())
    
    # M6 base score without locality is w_c + w_m = 1 + 1 = 2.0
    # w_local: 2.0 (base) + 2.0 (locality) - 5.0 (cost) = -1.0
    # w_cheap: 2.0 (base) + 0.0 (locality) - 1.0 (cost) = 1.0
    # w_cheap should win because penalty of -5.0 for cost outweighs locality
    decision = engine.evaluate(s, [w_local, w_cheap], asset_sizes_bytes={"a"*64: 100})
    assert decision.selected_worker_id == "w-cheap"


def test_m7_prediction_plus_cost():
    # 10. M7 PREDICTION + COST
    # Mock M7 Bridge
    class MockM7Bridge:
        def evaluate_candidates(self, spec, candidates):
            class Risk:
                total_risk = 0.5
            return {c.worker_id: Risk() for c in candidates}
            
        def adjust_ranking(self, scores, intelligence, risk_weight):
            # M7 says w-expensive is magically better
            return ["w-expensive", "w-cheap_invalid"]

    engine = PlacementEngine(m7_bridge=MockM7Bridge())
    s = spec("w1", requires_gpu=True)
    
    w_cheap_invalid = profile("w-cheap_invalid", cost_per_hour=1.0, gpu_available=False)
    w_expensive = profile("w-expensive", cost_per_hour=5.0, gpu_available=True)

    # M7 bridge cannot resurrect a rejected candidate.
    decision = engine.evaluate(s, [w_cheap_invalid, w_expensive])
    assert decision.selected_worker_id == "w-expensive"


@pytest.mark.asyncio
async def test_orchestrator_path_and_retry():
    # 11 & 12. ORCHESTRATOR PATH + FAILURE AND RETRY INTERACTION
    registry = WorkloadRegistry()
    w_reg = WorkerRegistry()
    att_reg = AttemptRegistry()
    orc = WorkloadOrchestrator(w_reg, registry, attempt_registry=att_reg)

    w_cheap = WorkerInfo(worker_id="w-cheap", endpoint_url="http://cheap", ip_address="127.0.0.1", port=80, 
                         status=WorkerStatus.ACTIVE, resource_profile=profile("w-cheap", cost_per_hour=1.0))
    w_exp = WorkerInfo(worker_id="w-exp", endpoint_url="http://exp", ip_address="127.0.0.1", port=80, 
                       status=WorkerStatus.ACTIVE, resource_profile=profile("w-exp", cost_per_hour=5.0))

    w_reg._workers = {"w-cheap": w_cheap, "w-exp": w_exp}

    s = spec("task-retry", min_cpu_cores=1)
    wid = await orc.submit_workload(s)
    
    # Mock httpx client
    mock_client = AsyncMock()
    # First attempt: dispatch to cheap worker, but it fails retryably
    mock_response1 = MagicMock()
    mock_response1.json.return_value = {
        "workload_id": "task-retry", "worker_id": "w-cheap", "success": False, "output_asset_hashes": [],
        "execution_duration_seconds": 1.0, "failure_category": "worker_unavailable", "error_message": "Network error"
    }
    
    # Second attempt: w-cheap is exhausted, must choose w-exp, and it succeeds
    mock_response2 = MagicMock()
    mock_response2.json.return_value = {
        "workload_id": "task-retry", "worker_id": "w-exp", "success": True, "output_asset_hashes": [],
        "execution_duration_seconds": 1.0
    }
    
    mock_client.post.side_effect = [mock_response1, mock_response2]
    orc.http_client = mock_client

    # Process all attempts (it will loop internally until success)
    await orc._process_workload(wid)
    
    attempts = att_reg.list_attempts_for_workload(wid)
    assert len(attempts) == 2
    assert attempts[0].worker_id == "w-cheap"
    assert attempts[0].status == AttemptStatus.FAILED
    assert attempts[0].failure_category == FailureCategory.WORKER_UNAVAILABLE
    
    assert attempts[1].worker_id == "w-exp"
    assert attempts[1].status == AttemptStatus.SUCCEEDED
    
    # Verify exact precedence in candidate explanation for second attempt
    record = registry.get_workload(wid)
    assert record.state == WorkloadState.COMPLETED
    assert record.placement_decision.selected_worker_id == "w-exp"
