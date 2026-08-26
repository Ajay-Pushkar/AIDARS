"""M7 Intelligence: Risk Contracts.

Defines the risk metric used to influence M6 placement rankings without
overriding M6 hard constraints.
"""
from dataclasses import dataclass

@dataclass
class RiskScore:
    """The evaluated risk of placing a specific workload on a specific worker."""
    
    # Normalized component probabilities [0.0, 1.0]
    p_failure: float
    u_prediction: float  # Uncertainty
    p_resource_exhaustion: float
    p_deadline_miss: float
    
    # Combined risk score [0.0, 1.0]
    total_risk: float

    @classmethod
    def calculate(
        cls,
        p_failure: float,
        u_prediction: float,
        p_resource_exhaustion: float,
        p_deadline_miss: float,
        alpha: float = 0.4,
        beta: float = 0.2,
        gamma: float = 0.3,
        delta: float = 0.1,
    ) -> "RiskScore":
        """Calculates total risk using weighted components."""
        total = (
            alpha * p_failure +
            beta * u_prediction +
            gamma * p_resource_exhaustion +
            delta * p_deadline_miss
        )
        return cls(
            p_failure=p_failure,
            u_prediction=u_prediction,
            p_resource_exhaustion=p_resource_exhaustion,
            p_deadline_miss=p_deadline_miss,
            total_risk=min(1.0, max(0.0, total))
        )
