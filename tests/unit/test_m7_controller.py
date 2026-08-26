"""Tests for M7 Orchestrator Bridge."""

from aidars.m7.controller import M7OrchestratorBridge
from aidars.m7.telemetry import TelemetryMemory
from aidars.distributed.models import WorkloadSpec, WorkerResourceProfile
from aidars.m7.risk import RiskScore

def test_evaluate_candidates():
    """Test full pipeline of evaluating candidates."""
    memory = TelemetryMemory()
    bridge = M7OrchestratorBridge(memory)
    
    workload = WorkloadSpec(
        workload_id="wl-1",
        task_type="m6-lan-test",
        min_cpu_cores=4,
        min_ram_bytes=4000,
        requires_gpu=False,
        estimated_duration_seconds=10.0
    )
    
    candidates = [
        WorkerResourceProfile(
            worker_id="w-safe",
            endpoint_url="http://safe",
            ip_address="127.0.0.1",
            cpu_cores_total=8,
            cpu_utilization_percent=0.0,
            ram_total_bytes=16000,
            ram_available_bytes=16000,
            gpu_available=False
        ),
        WorkerResourceProfile(
            worker_id="w-risky",
            endpoint_url="http://risky",
            ip_address="127.0.0.2",
            cpu_cores_total=8,
            cpu_utilization_percent=99.0, # Highly loaded
            ram_total_bytes=16000,
            ram_available_bytes=1000,     # Low RAM
            gpu_available=False
        )
    ]
    
    # Pre-populate some erratic history for w-risky
    memory.ingest_worker_metrics("w-risky", cpu_ratio=0.99, ram_ratio=0.1, latency=500.0, failed=True)
    memory.ingest_worker_metrics("w-safe", cpu_ratio=0.0, ram_ratio=1.0, latency=10.0, failed=False)
    
    scores = bridge.evaluate_candidates(workload, candidates)
    
    assert "w-safe" in scores
    assert "w-risky" in scores
    
    assert scores["w-safe"].total_risk < scores["w-risky"].total_risk

def test_adjust_ranking():
    """Test soft penalty adjustment."""
    bridge = M7OrchestratorBridge(TelemetryMemory())
    
    original_ranking = ["w-risky", "w-safe"]
    
    intelligence = {
        "w-risky": RiskScore(0.9, 0.5, 0.9, 0.9, 0.9),  # Extremely risky
        "w-safe": RiskScore(0.0, 0.1, 0.0, 0.0, 0.05)   # Very safe
    }
    
    adjusted = bridge.adjust_ranking(original_ranking, intelligence, risk_weight=1.0)
    
    # Safe worker should jump ahead due to high risk penalty on risky worker
    assert adjusted == ["w-safe", "w-risky"]
