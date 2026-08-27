"""M7 Intelligence: Orchestration Controller.

The integration point between M6 distributed execution and M7 intelligence.
It acts as a non-mutating bridge that observes, predicts, and recommends.
"""
from typing import Dict, List, Optional
from aidars.distributed.models import WorkloadSpec, WorkerResourceProfile
from aidars.m7.telemetry import TelemetryMemory
from aidars.m7.features import FeatureExtractor
from aidars.m7.behavior import BehaviorInferencer
from aidars.m7.prediction import PerformancePredictor
from aidars.m7.risk import PlacementRiskEvaluator, RiskScore
from aidars.m7.anomaly import AnomalyDetector
from aidars.m7.policy import AdaptivePolicyEngine

class M7OrchestratorBridge:
    """The intelligence bridge between M6 execution and M7 understanding.
    
    Provides risk assessments and policy recommendations without mutating
    M6 placement or execution state directly.
    """
    
    def __init__(self, telemetry_memory: TelemetryMemory):
        self.memory = telemetry_memory
        
    def evaluate_candidates(
        self,
        workload: WorkloadSpec,
        candidates: List[WorkerResourceProfile],
        network_max_ram: int = 32 * 1024 * 1024 * 1024,
        network_max_cpu: int = 64
    ) -> Dict[str, RiskScore]:
        """Provides a risk score for each valid M6 candidate."""
        
        # 1. Extract workload features
        wl_features = FeatureExtractor.extract_workload_features(workload, network_max_ram, network_max_cpu)
        
        # 2. Infer workload behavior
        wl_behavior = BehaviorInferencer.infer_workload_behavior(workload.workload_id, wl_features)
        
        risk_scores: Dict[str, RiskScore] = {}
        
        for candidate in candidates:
            # 3. Retrieve temporal memory
            temporal = self.memory.get_worker_state(candidate.worker_id)
            
            # 4. Extract worker features
            w_features = FeatureExtractor.extract_worker_features(candidate, temporal, workload)
            
            # 5. Infer worker behavior
            w_behavior = BehaviorInferencer.infer_worker_behavior(candidate.worker_id, w_features)
            
            # 6. Predict outcome
            wl_temporal = self.memory.get_workload_state(workload.task_type)
            prediction = PerformancePredictor.predict(w_features, wl_features, w_behavior, wl_behavior, worker_temporal=temporal, workload_temporal=wl_temporal)
            
            # 7. Evaluate risk
            risk = PlacementRiskEvaluator.evaluate(
                prediction, 
                max_ram_available=candidate.ram_available_bytes, 
                deadline_seconds=workload.estimated_duration_seconds * 1.5  # 50% buffer as soft deadline
            )
            
            risk_scores[candidate.worker_id] = risk
            
        return risk_scores
        
    def adjust_ranking(self, original_scores: Dict[str, float], intelligence: Dict[str, RiskScore], risk_weight: float = 0.5) -> List[str]:
        """Adjusts the M6 candidate ranking using M7 risk scores as a soft penalty."""
        
        if not intelligence:
            # Sort by original score descending
            return sorted(original_scores.keys(), key=lambda w_id: original_scores[w_id], reverse=True)
            
        # Extract cluster anomaly state to drive policy weights
        active_workers = list(self.memory.workers.values())
        policy = AdaptivePolicyEngine.evaluate_environment(active_workers)
        
        # Override the base risk weight with the policy's risk_weight factor
        effective_lambda = risk_weight * policy.risk_weight
        
        final_scores = {}
        for worker_id, m6_score in original_scores.items():
            risk = intelligence.get(worker_id)
            if risk:
                # S_final = S_M6 - lambda * R_M7
                final_scores[worker_id] = m6_score - (effective_lambda * risk.total_risk)
            else:
                final_scores[worker_id] = m6_score
                
        # Sort by final score descending
        return sorted(final_scores.keys(), key=lambda w_id: final_scores[w_id], reverse=True)
