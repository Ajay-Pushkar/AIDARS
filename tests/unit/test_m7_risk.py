"""Tests for M7 Risk Engine."""

from aidars.m7.risk import PlacementRiskEvaluator, RiskScore
from aidars.m7.prediction import PredictionResult

def test_evaluate_low_risk():
    """Test evaluating a perfectly safe prediction."""
    pred = PredictionResult(
        predicted_duration_seconds=10.0,
        predicted_ram_peak_bytes=1000,
        predicted_vram_peak_bytes=0,
        failure_probability=0.0,
        confidence=1.0,
        explanation=None
    )
    
    risk = PlacementRiskEvaluator.evaluate(pred, max_ram_available=10000, deadline_seconds=100.0)
    
    assert risk.p_failure == 0.0
    assert risk.u_prediction == 0.0
    assert risk.p_resource_exhaustion == 0.0
    assert risk.p_deadline_miss == 0.0
    assert risk.total_risk == 0.0

def test_evaluate_high_risk():
    """Test evaluating a dangerous prediction."""
    pred = PredictionResult(
        predicted_duration_seconds=110.0,  # Past deadline
        predicted_ram_peak_bytes=9500,     # >90% of RAM
        predicted_vram_peak_bytes=0,
        failure_probability=0.8,
        confidence=0.5,
        explanation=None
    )
    
    risk = PlacementRiskEvaluator.evaluate(pred, max_ram_available=10000, deadline_seconds=100.0)
    
    assert risk.p_failure == 0.8
    assert risk.u_prediction == 0.5
    assert risk.p_resource_exhaustion == 0.8
    assert risk.p_deadline_miss == 1.0
    
    # Total risk calculation:
    # 0.4 * 0.8 + 0.2 * 0.5 + 0.3 * 0.8 + 0.1 * 1.0 = 0.32 + 0.1 + 0.24 + 0.1 = 0.76
    assert abs(risk.total_risk - 0.76) < 1e-6
