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

class PerformancePredictor:
    """Predicts workload outcomes on a candidate worker using a statistical model."""
    
    @staticmethod
    def predict(
        worker_features: 'WorkerFeatureVector',
        workload_features: 'WorkloadFeatureVector',
        worker_behavior: 'WorkerBehavior',
        workload_behavior: 'WorkloadBehavior',
        base_duration: float = 10.0,
        base_ram: int = 1024 * 1024 * 1024
    ) -> PredictionResult:
        """Calculates prediction using explicit feature weights and behavioral inferences."""
        
        # 1. Base Failure Probability
        p_fail = worker_features.recent_failure_rate
        
        # 2. Resource Exhaustion Risks
        if workload_behavior.state == 'memory_bound' and worker_features.ram_available_ratio < 0.2:
            p_fail = min(1.0, p_fail + 0.3)
        if workload_behavior.state == 'gpu_bound' and worker_features.has_gpu < 0.5:
            p_fail = min(1.0, p_fail + 0.9)  # Fatal
            
        # 3. Behavioral Modifiers on Failure
        if worker_behavior.state == 'erratic':
            p_fail = min(1.0, p_fail + 0.4)
        elif worker_behavior.state == 'overloaded':
            p_fail = min(1.0, p_fail + 0.5)
            
        # 4. Duration Prediction
        # Scale duration based on resource availability
        duration_multiplier = 1.0
        if workload_behavior.state == 'cpu_bound':
            # If CPU is highly available, it goes faster, if constrained, much slower
            cpu_factor = max(0.1, worker_features.cpu_available_ratio)
            duration_multiplier = 1.0 / cpu_factor
        elif workload_behavior.state == 'io_bound':
            # If assets are already cached locally, it goes much faster
            duration_multiplier = 1.0 - (worker_features.cache_locality_ratio * 0.5)
            
        pred_duration = base_duration * duration_multiplier
        
        # 5. Peak RAM prediction
        # Scale up slightly if memory bound to be safe
        ram_multiplier = 1.2 if workload_behavior.state == 'memory_bound' else 1.0
        pred_ram = int(base_ram * ram_multiplier)
        
        # 6. Confidence Calculation
        # Confidence drops if worker is erratic, or if heartbeat is unstable
        confidence = worker_behavior.confidence * workload_behavior.confidence
        confidence = confidence * worker_features.heartbeat_stability
        if worker_behavior.state == 'unknown' or workload_behavior.state == 'unknown':
            confidence *= 0.5
            
        explanation = f"P(fail)={p_fail:.2f}. Worker is {worker_behavior.state.value}."
        
        return PredictionResult(
            predicted_duration_seconds=pred_duration,
            predicted_ram_peak_bytes=pred_ram,
            predicted_vram_peak_bytes=0,
            failure_probability=p_fail,
            confidence=min(1.0, max(0.0, confidence)),
            explanation=explanation
        )
