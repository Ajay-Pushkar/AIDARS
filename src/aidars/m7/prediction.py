"""M7 Intelligence: Prediction Contracts.

Defines the output structure of the M7 prediction engine, exposing
estimated outcomes and their associated uncertainty.
"""
from dataclasses import dataclass
from typing import Optional

@dataclass
class PredictionResult:
    """The statistical prediction for a specific workload on a specific worker."""
    
    # Predicted outcomes
    predicted_duration_seconds: float
    predicted_ram_peak_bytes: int
    predicted_vram_peak_bytes: int
    failure_probability: float  # [0.0, 1.0]
    
    # Uncertainty/Confidence (1.0 = absolute certainty, 0.0 = total guess)
    confidence: float
    
    # Optional explanation (for interpretability, e.g., "Historically fast but highly volatile")
    explanation: Optional[str] = None
