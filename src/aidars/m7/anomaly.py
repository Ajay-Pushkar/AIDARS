"""M7 Intelligence: Anomaly Detection Contracts.

Defines the structure for outlier detection, which compares observed behavior
against expected predictions to identify significant deviations.
"""
from dataclasses import dataclass

@dataclass
class AnomalyScore:
    """Represents a detected deviation from expected behavior."""
    
    # The magnitude of the deviation [0.0, 1.0]
    deviation_magnitude: float
    
    # Is this a persistent deviation or a one-off spike? [0.0 = one-off, 1.0 = highly persistent]
    persistence: float
    
    # Confidence in this anomaly [0.0, 1.0]
    confidence: float
    
    # Combined anomaly severity score [0.0, 1.0]
    # typically f(deviation_magnitude, persistence, confidence)
    severity: float

    @property
    def is_significant(self) -> bool:
        """Returns True if the anomaly requires a behavioral state change."""
        return self.severity > 0.75  # Example threshold
