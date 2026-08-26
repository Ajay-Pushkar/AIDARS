"""Tests for M7 Anomaly Detector."""

from aidars.m7.anomaly import AnomalyDetector
from aidars.m7.prediction import PredictionResult

def test_detect_no_anomaly():
    """Test when observed matches predicted closely."""
    pred = PredictionResult(
        predicted_duration_seconds=10.0,
        predicted_ram_peak_bytes=1000,
        predicted_vram_peak_bytes=0,
        failure_probability=0.0,
        confidence=1.0,
        explanation=None
    )
    
    score = AnomalyDetector.detect(pred, observed_duration_seconds=10.5, observed_ram_peak=1050)
    assert score.deviation_magnitude == 0.0
    assert not score.is_significant

def test_detect_significant_anomaly():
    """Test when observed is vastly different from predicted."""
    pred = PredictionResult(
        predicted_duration_seconds=10.0,
        predicted_ram_peak_bytes=1000,
        predicted_vram_peak_bytes=0,
        failure_probability=0.0,
        confidence=1.0,
        explanation=None
    )
    
    # Took 5x longer, used 3x more RAM
    score = AnomalyDetector.detect(pred, observed_duration_seconds=50.0, observed_ram_peak=3000, persistence_history=0.8)
    
    assert score.deviation_magnitude > 0.8
    assert score.severity > 0.8
    assert score.is_significant
