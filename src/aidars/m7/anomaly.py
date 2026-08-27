"""M7 Intelligence: Anomaly Detection Contracts.

Defines the structure for outlier detection, which compares observed behavior
against expected predictions to identify significant deviations.
"""
from aidars.m7.contracts import AnomalyScore

class AnomalyDetector:
    """Detects deviations between predicted and observed execution."""
    
    @staticmethod
    def detect(expected: 'PredictionResult', observed_duration_seconds: float, observed_ram_peak: int, persistence_history: float = 0.0) -> AnomalyScore:
        """Calculates an anomaly score by comparing observed metrics against predictions."""
        
        # 1. Calculate duration deviation
        duration_ratio = 1.0
        if expected.predicted_duration_seconds > 0:
            duration_ratio = observed_duration_seconds / expected.predicted_duration_seconds
        
        duration_deviation = 0.0
        if duration_ratio > 1.5:  # 50% slower than expected
            duration_deviation = min(1.0, (duration_ratio - 1.5) / 2.0)
            
        # 2. Calculate RAM deviation
        ram_ratio = 1.0
        if expected.predicted_ram_peak_bytes > 0:
            ram_ratio = observed_ram_peak / expected.predicted_ram_peak_bytes
            
        ram_deviation = 0.0
        if ram_ratio > 1.2:  # 20% more RAM than expected
            ram_deviation = min(1.0, (ram_ratio - 1.2) / 1.0)
            
        # 3. Overall magnitude
        deviation_magnitude = max(duration_deviation, ram_deviation)
        
        # 4. Persistence
        # In a real temporal system, persistence_history would be an EMA of past deviation_magnitudes
        # Here we just smooth the current deviation into the history to get the new persistence
        persistence = (0.3 * deviation_magnitude) + (0.7 * persistence_history)
        
        # 5. Confidence
        # We are more confident if the original prediction was confident
        confidence = expected.confidence
        
        # 6. Severity = f(magnitude, persistence, confidence)
        # A massive one-off deviation (persistence low) has medium severity
        # A medium deviation that persists (persistence high) has high severity
        severity = (deviation_magnitude * 0.4) + (persistence * 0.6)
        severity *= confidence
        
        return AnomalyScore(
            deviation_magnitude=deviation_magnitude,
            persistence=persistence,
            confidence=confidence,
            severity=min(1.0, max(0.0, severity))
        )
