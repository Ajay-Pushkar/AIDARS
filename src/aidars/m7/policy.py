"""M7 Intelligence: Adaptive Policy Contracts.

Defines global adaptive strategies that dynamically shift weights based on
environmental conditions (e.g., network degradation).
"""
from aidars.m7.contracts import PolicyWeights

class AdaptivePolicyEngine:
    """Dynamically adjusts placement weights based on global environmental conditions."""
    
    @staticmethod
    def evaluate_environment(active_workers: List['WorkerTemporalState']) -> PolicyWeights:
        """Evaluates global telemetry to recommend a placement policy."""
        
        if not active_workers:
            return PolicyWeights()  # Default
            
        avg_latency = sum(w.latency_ema.value for w in active_workers) / len(active_workers)
        avg_failure = sum(w.failure_rate_ema.value for w in active_workers) / len(active_workers)
        
        # Base policy
        weights = PolicyWeights()
        
        # 1. Network Degradation
        # If latency is exceptionally high (e.g., > 200ms), prefer locality to avoid transfers
        if avg_latency > 200.0:
            weights.locality_weight = 0.40
            weights.network_weight = 0.10
            weights.compute_weight = 0.20
            
        # 2. High Global Instability
        # If average failure rate across the cluster is > 10%
        if avg_failure > 0.10:
            weights.risk_weight = 0.40
            # Steal from compute and queue
            weights.compute_weight = max(0.05, weights.compute_weight - 0.15)
            weights.queue_weight = max(0.05, weights.queue_weight - 0.10)
            
        # Ensure they sum to 1.0
        total = weights.compute_weight + weights.locality_weight + weights.network_weight + weights.risk_weight + weights.queue_weight
        
        weights.compute_weight /= total
        weights.locality_weight /= total
        weights.network_weight /= total
        weights.risk_weight /= total
        weights.queue_weight /= total
        
        return weights
