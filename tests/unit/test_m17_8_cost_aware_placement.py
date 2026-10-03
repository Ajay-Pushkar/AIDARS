import pytest
import time
from aidars.distributed.models import WorkloadSpec, WorkerResourceProfile, WorkerStatus
from aidars.distributed.placement import PlacementEngine

def create_worker(worker_id, cpu=4, ram=8*1024**3, cost=0.0, **kwargs):
    w = WorkerResourceProfile(
        worker_id=worker_id,
        endpoint_url=f"http://{worker_id}",
        ip_address="127.0.0.1",
        cpu_cores_total=cpu,
        cpu_utilization_percent=0.0,
        ram_total_bytes=ram,
        ram_available_bytes=ram,
        active_workload_count=0,
        max_concurrent_workloads=10,
        status=WorkerStatus.ACTIVE,
        timestamp_utc=time.time(),
        cost_per_hour=cost,
    )
    for k, v in kwargs.items():
        setattr(w, k, v)
    return w

def create_spec(workload_id, cpu=1, ram=1024**3, **kwargs):
    spec = WorkloadSpec(
        workload_id=workload_id,
        task_type="test",
        min_cpu_cores=cpu,
        min_ram_bytes=ram,
    )
    for k, v in kwargs.items():
        setattr(spec, k, v)
    return spec

def test_cheaper_eligible_worker_preferred():
    # A. Cheaper eligible worker preferred over more expensive eligible worker.
    engine = PlacementEngine()
    spec = create_spec("w1")
    w_cheap = create_worker("cheap", cost=1.0)
    w_expensive = create_worker("expensive", cost=5.0)
    
    decision = engine.evaluate(spec, [w_cheap, w_expensive])
    assert decision.selected_worker_id == "cheap"

    # I. Candidate explanation contains the cost reasoning.
    cheap_exp = next(e for e in decision.candidate_explanations if e["worker_id"] == "cheap")
    exp_exp = next(e for e in decision.candidate_explanations if e["worker_id"] == "expensive")
    
    assert cheap_exp["m11_ranking_factors"]["cost_penalty"] == -1.0
    assert exp_exp["m11_ranking_factors"]["cost_penalty"] == -5.0

def test_cheaper_worker_rejected_by_hard_constraint():
    # B. Cheaper worker rejected by a hard constraint: expensive eligible worker MUST win.
    engine = PlacementEngine()
    spec = create_spec("w1", cpu=4)
    w_cheap_weak = create_worker("cheap", cpu=2, cost=1.0) # Fails CPU constraint
    w_expensive = create_worker("expensive", cpu=4, cost=5.0)
    
    decision = engine.evaluate(spec, [w_cheap_weak, w_expensive])
    assert decision.selected_worker_id == "expensive"
    
    cheap_exp = next(e for e in decision.candidate_explanations if e["worker_id"] == "cheap")
    assert not cheap_exp["eligible"]
    assert "INSUFFICIENT_CPU" in cheap_exp["rejection_reasons"]

def test_cost_must_not_override_gpu_requirements():
    # C. Cost must not override GPU requirements.
    engine = PlacementEngine()
    spec = create_spec("w1", requires_gpu=True, min_vram_bytes=8*1024**3)
    w_cheap_no_gpu = create_worker("cheap", cost=1.0, gpu_available=False)
    w_expensive_gpu = create_worker("expensive", cost=5.0, gpu_available=True, vram_total_bytes=16*1024**3, vram_available_bytes=16*1024**3)
    
    decision = engine.evaluate(spec, [w_cheap_no_gpu, w_expensive_gpu])
    assert decision.selected_worker_id == "expensive"

def test_cost_must_not_override_affinity():
    # D. Cost must not override affinity/anti-affinity.
    engine = PlacementEngine()
    spec = create_spec("w1", affinity_mode="same_tag", affinity_tag_key="env", affinity_hard=True, affinity_group_id="g1")
    w_cheap = create_worker("cheap", cost=1.0)
    w_expensive = create_worker("expensive", cost=5.0)
    
    # Prior group had tag env: prod
    group_workers = {"g1": {"prior_worker"}}
    worker_tags = {"cheap": {"env": "dev"}, "expensive": {"env": "prod"}, "prior_worker": {"env": "prod"}}
    
    decision = engine.evaluate(spec, [w_cheap, w_expensive], worker_tags=worker_tags, group_workers=group_workers)
    assert decision.selected_worker_id == "expensive"

def test_cost_must_not_override_worker_health():
    # E. Cost must not override worker health/liveness.
    engine = PlacementEngine()
    spec = create_spec("w1")
    w_cheap_offline = create_worker("cheap", cost=1.0, status=WorkerStatus.OFFLINE)
    w_expensive = create_worker("expensive", cost=5.0)
    
    decision = engine.evaluate(spec, [w_cheap_offline, w_expensive])
    assert decision.selected_worker_id == "expensive"

def test_equal_cost_deterministic_selection():
    # G. Equal-cost workers retain deterministic selection (tie-breaker by worker_id).
    engine = PlacementEngine()
    spec = create_spec("w1")
    # Same exact specs
    w_a = create_worker("b_worker", cost=2.0)
    w_b = create_worker("a_worker", cost=2.0)
    
    decision = engine.evaluate(spec, [w_a, w_b])
    # a_worker should be selected due to lexicographical tie-break
    assert decision.selected_worker_id == "a_worker"

def test_backward_compatible_no_cost():
    # H. Existing placement behavior remains backward compatible when cost info is absent/default.
    engine = PlacementEngine()
    spec = create_spec("w1")
    w_a = create_worker("a_worker")
    w_b = create_worker("b_worker")
    
    decision = engine.evaluate(spec, [w_a, w_b])
    assert decision.selected_worker_id == "a_worker"
    exp = next(e for e in decision.candidate_explanations if e["worker_id"] == "a_worker")
    assert "cost_penalty" not in exp.get("m11_ranking_factors", {})

def test_hard_cost_constraint():
    # Additional: test max_cost_per_hour
    engine = PlacementEngine()
    spec = create_spec("w1", max_cost_per_hour=4.0)
    w_expensive = create_worker("expensive", cost=5.0)
    
    decision = engine.evaluate(spec, [w_expensive])
    assert decision is None
    
    exp = next(e for e in engine.last_evaluation["candidates"] if e["worker_id"] == "expensive")
    assert not exp["eligible"]
    assert "COST_EXCEEDS_BUDGET" in exp["rejection_reasons"]
