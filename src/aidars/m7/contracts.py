"""M7 Intelligence: Core Contracts.

This module defines the central vocabulary (data structures) for the M7
intelligence layer, ensuring all sub-components speak the same language.
"""
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

@dataclass
class FeatureVector:
    """Base numerical vector for M7 statistical/ML models."""
    values: List[float]

@dataclass
class WorkerFeatureVector:
    """Normalized numerical representation of a Worker's state and history."""
    cpu_available_ratio: float
    ram_available_ratio: float
    vram_available_ratio: float
    has_gpu: float
    active_workload_ratio: float
    cache_locality_ratio: float
    heartbeat_stability: float
    recent_failure_rate: float
    recent_latency_normalized: float
    throughput_normalized: float

    def to_vector(self) -> FeatureVector:
        return FeatureVector([
            self.cpu_available_ratio,
            self.ram_available_ratio,
            self.vram_available_ratio,
            self.has_gpu,
            self.active_workload_ratio,
            self.cache_locality_ratio,
            self.heartbeat_stability,
            self.recent_failure_rate,
            self.recent_latency_normalized,
            self.throughput_normalized,
        ])

@dataclass
class WorkloadFeatureVector:
    """Normalized numerical representation of a Workload's footprint."""
    required_cpu_ratio: float
    required_ram_ratio: float
    required_vram_ratio: float
    requires_gpu: float
    dependency_count_normalized: float
    dependency_bytes_normalized: float
    priority_normalized: float
    estimated_duration_normalized: float

    def to_vector(self) -> FeatureVector:
        return FeatureVector([
            self.required_cpu_ratio,
            self.required_ram_ratio,
            self.required_vram_ratio,
            self.requires_gpu,
            self.dependency_count_normalized,
            self.dependency_bytes_normalized,
            self.priority_normalized,
            self.estimated_duration_normalized,
        ])

class WorkerState(str, Enum):
    """Behavioral classifications for a worker node."""
    STABLE = "stable"
    DEGRADED = "degraded"
    ERRATIC = "erratic"
    OVERLOADED = "overloaded"
    RECOVERING = "recovering"
    UNKNOWN = "unknown"

class WorkloadState(str, Enum):
    """Behavioral classifications for a workload's resource footprint."""
    CPU_BOUND = "cpu_bound"
    MEMORY_BOUND = "memory_bound"
    GPU_BOUND = "gpu_bound"
    IO_BOUND = "io_bound"
    NETWORK_BOUND = "network_bound"
    MIXED = "mixed"
    UNKNOWN = "unknown"

@dataclass
class WorkerBehavior:
    """Inferred behavior state for a worker with confidence."""
    worker_id: str
    state: WorkerState
    confidence: float

@dataclass
class WorkloadBehavior:
    """Inferred behavior state for a workload with confidence."""
    workload_id: str
    state: WorkloadState
    confidence: float

@dataclass
class PredictionResult:
    """The statistical prediction for a specific workload on a specific worker."""
    predicted_duration_seconds: float
    predicted_ram_peak_bytes: int
    predicted_vram_peak_bytes: float
    failure_probability: float
    confidence: float
    explanation: Optional[str] = None

@dataclass
class RiskScore:
    """The evaluated risk of placing a specific workload on a specific worker."""
    p_failure: float
    u_prediction: float
    p_resource_exhaustion: float
    p_deadline_miss: float
    total_risk: float

    @classmethod
    def calculate(
        cls,
        p_failure: float,
        u_prediction: float,
        p_resource_exhaustion: float,
        p_deadline_miss: float
    ) -> 'RiskScore':
        total = (
            (p_failure * 0.4) +
            (u_prediction * 0.2) +
            (p_resource_exhaustion * 0.3) +
            (p_deadline_miss * 0.1)
        )
        return cls(
            p_failure=p_failure,
            u_prediction=u_prediction,
            p_resource_exhaustion=p_resource_exhaustion,
            p_deadline_miss=p_deadline_miss,
            total_risk=min(1.0, max(0.0, total))
        )

@dataclass
class AnomalyScore:
    """Represents a detected deviation from expected behavior."""
    deviation_magnitude: float
    persistence: float
    confidence: float
    severity: float

    @property
    def is_significant(self) -> bool:
        """Returns True if the anomaly requires a behavioral state change."""
        return self.severity > 0.75

@dataclass
class PolicyWeights:
    """Adaptive weighting recommendations for the M6 Placement Engine."""
    compute_weight: float = 0.35
    locality_weight: float = 0.20
    network_weight: float = 0.15
    risk_weight: float = 0.15
    queue_weight: float = 0.15
