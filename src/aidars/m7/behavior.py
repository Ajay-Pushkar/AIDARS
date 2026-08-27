"""M7 Intelligence: Behavioral State Contracts.

Defines the explicit behavioral classifications (inferences) for workers
and workloads, along with confidence scores.
"""
from aidars.m7.contracts import WorkerState, WorkloadState, WorkerBehavior, WorkloadBehavior

class BehaviorInferencer:
    """Infers explicit behavioral states based on feature vectors."""
    
    @staticmethod
    def infer_worker_behavior(worker_id: str, features: 'WorkerFeatureVector') -> WorkerBehavior:
        """Probabilistically classifies a worker's behavioral state."""
        
        # 1. Overloaded Check
        if features.cpu_available_ratio < 0.15 and features.ram_available_ratio < 0.15:
            conf = 1.0 - (max(features.cpu_available_ratio, features.ram_available_ratio) / 0.15)
            return WorkerBehavior(worker_id, WorkerState.OVERLOADED, confidence=min(1.0, max(0.5, conf)))
            
        # 2. Erratic / High Failure Rate Check
        if features.recent_failure_rate > 0.3:
            conf = min(1.0, features.recent_failure_rate * 2.0)
            return WorkerBehavior(worker_id, WorkerState.ERRATIC, confidence=conf)
            
        # 3. Degraded (Poor heartbeat or high latency)
        if features.heartbeat_stability < 0.8:
            conf = 1.0 - features.heartbeat_stability
            return WorkerBehavior(worker_id, WorkerState.DEGRADED, confidence=conf)
            
        if features.recent_latency_normalized > 0.7:
            conf = features.recent_latency_normalized
            return WorkerBehavior(worker_id, WorkerState.DEGRADED, confidence=conf)
            
        # 4. Default: Stable
        # Confidence is high if heartbeat is perfect and failure rate is 0
        conf = features.heartbeat_stability * (1.0 - features.recent_failure_rate)
        return WorkerBehavior(worker_id, WorkerState.STABLE, confidence=min(1.0, max(0.1, conf)))

    @staticmethod
    def infer_workload_behavior(workload_id: str, features: 'WorkloadFeatureVector') -> WorkloadBehavior:
        """Probabilistically classifies a workload's behavior footprint."""
        
        # If it explicitly needs a GPU, it's overwhelmingly GPU bound in our context
        if features.requires_gpu > 0.5:
            return WorkloadBehavior(workload_id, WorkloadState.GPU_BOUND, confidence=0.9)
            
        # Heavy IO / Network Dependency
        if features.dependency_count_normalized > 0.5 or features.dependency_bytes_normalized > 0.5:
            conf = max(features.dependency_count_normalized, features.dependency_bytes_normalized)
            return WorkloadBehavior(workload_id, WorkloadState.IO_BOUND, confidence=min(1.0, conf))
            
        # CPU vs Memory Dominance
        # If CPU requirement ratio is strictly double the memory requirement ratio
        if features.required_cpu_ratio > (features.required_ram_ratio * 1.5) and features.required_cpu_ratio > 0.1:
            conf = min(1.0, (features.required_cpu_ratio / (features.required_ram_ratio or 0.01)) * 0.2)
            return WorkloadBehavior(workload_id, WorkloadState.CPU_BOUND, confidence=max(0.5, conf))
            
        if features.required_ram_ratio > (features.required_cpu_ratio * 1.5) and features.required_ram_ratio > 0.1:
            conf = min(1.0, (features.required_ram_ratio / (features.required_cpu_ratio or 0.01)) * 0.2)
            return WorkloadBehavior(workload_id, WorkloadState.MEMORY_BOUND, confidence=max(0.5, conf))
            
        # Default: Mixed
        # Confidence increases as all metrics trend toward the middle
        return WorkloadBehavior(workload_id, WorkloadState.MIXED, confidence=0.6)
