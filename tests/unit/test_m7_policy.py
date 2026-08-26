"""Tests for M7 Adaptive Policy Engine."""

from aidars.m7.policy import AdaptivePolicyEngine
from aidars.m7.telemetry import WorkerTemporalState

def test_evaluate_environment_healthy():
    """Test policy generation for a healthy environment."""
    w1 = WorkerTemporalState("w-1")
    w1.latency_ema.value = 50.0
    w1.failure_rate_ema.value = 0.01
    
    w2 = WorkerTemporalState("w-2")
    w2.latency_ema.value = 40.0
    w2.failure_rate_ema.value = 0.05
    
    weights = AdaptivePolicyEngine.evaluate_environment([w1, w2])
    
    # Should be close to base weights
    assert abs(weights.compute_weight - 0.35) < 1e-2
    assert abs(weights.locality_weight - 0.20) < 1e-2
    assert abs(weights.risk_weight - 0.15) < 1e-2

def test_evaluate_environment_degraded_network():
    """Test policy generation when network latency is high."""
    w1 = WorkerTemporalState("w-1")
    w1.latency_ema.value = 300.0
    w1.failure_rate_ema.value = 0.01
    
    weights = AdaptivePolicyEngine.evaluate_environment([w1])
    
    # Locality should be prioritized over compute
    assert weights.locality_weight > weights.compute_weight
    assert weights.locality_weight > 0.3

def test_evaluate_environment_high_failure():
    """Test policy generation when cluster is highly unstable."""
    w1 = WorkerTemporalState("w-1")
    w1.latency_ema.value = 50.0
    w1.failure_rate_ema.value = 0.20  # 20% failure rate
    
    weights = AdaptivePolicyEngine.evaluate_environment([w1])
    
    # Risk should dominate
    assert weights.risk_weight > weights.compute_weight
    assert weights.risk_weight > 0.3
