"""Adversarial testing for the M7 Intelligence Layer."""

from aidars.m7.controller import M7OrchestratorBridge
from aidars.m7.telemetry import TelemetryMemory
from aidars.distributed.models import WorkloadSpec, WorkerResourceProfile

def test_adversarial_memory_poisoning_isolation():
    """Test that a worker with a poisoned memory history is correctly ranked at the bottom."""
    memory = TelemetryMemory()
    bridge = M7OrchestratorBridge(memory)
    
    workload = WorkloadSpec(
        workload_id="wl-adv",
        task_type="m6-lan-test",
        min_cpu_cores=4,
        min_ram_bytes=4000,
        requires_gpu=False,
        estimated_duration_seconds=10.0
    )
    
    candidates = [
        WorkerResourceProfile(
            worker_id="w-normal",
            endpoint_url="http://normal",
            ip_address="127.0.0.1",
            cpu_cores_total=8,
            cpu_utilization_percent=0.0,
            ram_total_bytes=16000,
            ram_available_bytes=16000,
            gpu_available=False
        ),
        WorkerResourceProfile(
            worker_id="w-poisoned",
            endpoint_url="http://poison",
            ip_address="127.0.0.2",
            cpu_cores_total=8,
            cpu_utilization_percent=0.0,
            ram_total_bytes=16000,
            ram_available_bytes=16000,
            gpu_available=False
        )
    ]
    
    # Both workers appear completely idle and healthy in their static profiles.
    # However, w-poisoned has a horrific temporal history.
    for _ in range(10):
        memory.ingest_worker_metrics("w-poisoned", cpu_ratio=1.0, ram_ratio=1.0, latency=5000.0, failed=True)
        memory.ingest_worker_metrics("w-normal", cpu_ratio=0.1, ram_ratio=0.1, latency=5.0, failed=False)
        
    scores = bridge.evaluate_candidates(workload, candidates)
    
    risk_normal = scores["w-normal"]
    risk_poisoned = scores["w-poisoned"]
    
    # Intelligence must detect the poisoning and assign a massive risk
    assert risk_poisoned.p_failure > 0.8
    assert risk_poisoned.total_risk > risk_normal.total_risk * 2.0
    
    # If M6 initially ranked w-poisoned higher (because it seemed identical statically),
    # M7 must adjust the ranking to put w-normal first.
    adjusted = bridge.adjust_ranking(["w-poisoned", "w-normal"], scores, risk_weight=1.0)
    assert adjusted == ["w-normal", "w-poisoned"]

def test_adversarial_oracle_denial():
    """M7.14 No-Oracle Test: M7 must not artificially know things it hasn't observed."""
    memory = TelemetryMemory()
    bridge = M7OrchestratorBridge(memory)
    
    workload = WorkloadSpec(
        workload_id="wl-new",
        task_type="m6-lan-test",
        min_cpu_cores=4,
        min_ram_bytes=4000,
        requires_gpu=False,
        estimated_duration_seconds=10.0
    )
    
    candidates = [
        WorkerResourceProfile(
            worker_id="w-blank",
            endpoint_url="http://blank",
            ip_address="127.0.0.1",
            cpu_cores_total=8,
            cpu_utilization_percent=0.0,
            ram_total_bytes=16000,
            ram_available_bytes=16000,
            gpu_available=False
        )
    ]
    
    # No telemetry is ingested. M7 is completely blind about history.
    scores = bridge.evaluate_candidates(workload, candidates)
    
    risk_blank = scores["w-blank"]
    
    # Because M7 has no oracle, it must assign a high uncertainty (u_prediction)
    assert risk_blank.u_prediction >= 0.4
