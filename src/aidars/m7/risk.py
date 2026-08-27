"""M7 Intelligence: Risk Contracts.

Defines the risk metric used to influence M6 placement rankings without
overriding M6 hard constraints.
"""
from aidars.m7.contracts import RiskScore

class PlacementRiskEvaluator:
    """Evaluates the risk of placing a workload based on its predicted outcome."""
    
    @staticmethod
    def evaluate(prediction: 'PredictionResult', max_ram_available: int, deadline_seconds: Optional[float] = None) -> RiskScore:
        """Converts a prediction result into a formalized RiskScore."""
        
        # 1. Failure Risk
        p_fail = prediction.failure_probability
        
        # 2. Uncertainty (1.0 - confidence)
        u_pred = 1.0 - prediction.confidence
        
        # 3. Resource Exhaustion Risk
        p_exhaust = 0.0
        if max_ram_available > 0:
            ram_ratio = prediction.predicted_ram_peak_bytes / max_ram_available
            if ram_ratio > 0.9:
                p_exhaust = 0.8
            elif ram_ratio > 0.7:
                p_exhaust = 0.4
                
        # 4. Deadline Miss Risk
        p_deadline = 0.0
        if deadline_seconds and deadline_seconds > 0:
            if prediction.predicted_duration_seconds > deadline_seconds:
                p_deadline = 1.0
            elif prediction.predicted_duration_seconds > (deadline_seconds * 0.8):
                p_deadline = 0.5
                
        return RiskScore.calculate(
            p_failure=p_fail,
            u_prediction=u_pred,
            p_resource_exhaustion=p_exhaust,
            p_deadline_miss=p_deadline
        )
