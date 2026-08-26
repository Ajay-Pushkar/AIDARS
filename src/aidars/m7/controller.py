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
            w_features = FeatureExtractor.extract_worker_features(candidate, temporal, network_max_ram)
            
            # 5. Infer worker behavior
            w_behavior = BehaviorInferencer.infer_worker_behavior(candidate.worker_id, w_features)
            
            # 6. Predict outcome
            prediction = PerformancePredictor.predict(w_features, wl_features, w_behavior, wl_behavior)
            
            # 7. Evaluate risk
            risk = PlacementRiskEvaluator.evaluate(
                prediction, 
                max_ram_available=candidate.ram_available_bytes, 
                deadline_seconds=workload.estimated_duration_seconds * 1.5  # 50% buffer as soft deadline
            )
            
            risk_scores[candidate.worker_id] = risk
            
        return risk_scores
        
    def adjust_ranking(self, original_ranking: List[str], intelligence: Dict[str, RiskScore], risk_weight: float = 0.2) -> List[str]:
        """Adjusts the M6 candidate ranking using M7 risk scores as a soft penalty."""
        
        # If no intelligence, return original
        if not intelligence:
            return original_ranking
            
        def get_adjusted_score(worker_id: str, original_index: int) -> float:
            # Base score: closer to 0 is better (rank)
            base_score = float(original_index)
            
            # Risk penalty: [0.0, 1.0] scaled by the length of the list to be meaningful
            risk = intelligence.get(worker_id)
            risk_penalty = 0.0
            if risk:
                # E.g. max penalty moves it down `len(original_ranking) * risk_weight` spots
                risk_penalty = risk.total_risk * len(original_ranking) * risk_weight * 5.0 
                
            return base_score + risk_penalty
            
        # Re-sort candidates based on adjusted score
        return sorted(original_ranking, key=lambda w_id: get_adjusted_score(w_id, original_ranking.index(w_id)))
