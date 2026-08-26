"""Tests for M7 Contracts (Features, Behavior, Prediction, Risk, Anomaly)."""

from aidars.m7.features import WorkerFeatureVector, WorkloadFeatureVector
from aidars.m7.behavior import WorkerBehavior, WorkerState, WorkloadBehavior, WorkloadState
from aidars.m7.prediction import PredictionResult
from aidars.m7.risk import RiskScore
from aidars.m7.anomaly import AnomalyScore

def test_worker_feature_vector():
    """Test WorkerFeatureVector serialization."""
    vec = WorkerFeatureVector(
        cpu_available_ratio=0.8,
        ram_available_ratio=0.6,
        vram_available_ratio=1.0,
        has_gpu=1.0,
        active_workload_ratio=0.2,
        cache_locality_ratio=0.5,
        heartbeat_stability=0.99,
        recent_failure_rate=0.01,
        recent_latency_normalized=0.1,
        throughput_normalized=0.7
    )
    fv = vec.to_vector()
    assert len(fv.values) == 10
    assert fv.values[0] == 0.8
    assert fv.values[3] == 1.0

def test_workload_feature_vector():
    """Test WorkloadFeatureVector serialization."""
    vec = WorkloadFeatureVector(
        required_cpu_ratio=0.5,
        required_ram_ratio=0.2,
        required_vram_ratio=0.0,
        requires_gpu=0.0,
        dependency_count_normalized=0.1,
        dependency_bytes_normalized=0.05,
        priority_normalized=0.8,
        estimated_duration_normalized=0.3
    )
    fv = vec.to_vector()
    assert len(fv.values) == 8
    assert fv.values[0] == 0.5
    assert fv.values[3] == 0.0

def test_behavior_contracts():
    """Test behavior dataclasses."""
    wkr = WorkerBehavior("Worker-A", WorkerState.STABLE, confidence=0.95)
    assert wkr.state == "stable"
    
    wl = WorkloadBehavior("wl-1", WorkloadState.CPU_BOUND, confidence=0.8)
    assert wl.state == "cpu_bound"

def test_risk_calculation():
    """Test RiskScore calculation logic."""
    risk = RiskScore.calculate(
        p_failure=0.5,
        u_prediction=0.2,
        p_resource_exhaustion=0.1,
        p_deadline_miss=0.0
    )
    # 0.4 * 0.5 + 0.2 * 0.2 + 0.3 * 0.1 + 0.1 * 0.0 = 0.20 + 0.04 + 0.03 = 0.27
    assert abs(risk.total_risk - 0.27) < 1e-6

def test_anomaly_significance():
    """Test anomaly severity threshold."""
    anomaly_minor = AnomalyScore(0.1, 0.1, 1.0, severity=0.2)
    assert not anomaly_minor.is_significant
    
    anomaly_major = AnomalyScore(0.9, 0.9, 0.9, severity=0.85)
    assert anomaly_major.is_significant
