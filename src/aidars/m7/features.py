"""M7 Intelligence: Feature Engineering Contracts.

Defines the normalized numerical representations (Feature Vectors) that the
M7 intelligence layer uses to model the distributed environment.
"""
from dataclasses import dataclass
from typing import List

@dataclass
class FeatureVector:
    """Base numerical vector for M7 statistical/ML models."""
    values: List[float]

@dataclass
class WorkerFeatureVector:
    """Normalized numerical representation of a Worker's state and history."""
    
    # Current resource availability [0.0, 1.0]
    cpu_available_ratio: float
    ram_available_ratio: float
    vram_available_ratio: float
    
    # Binary capability [0.0 or 1.0]
    has_gpu: float
    
    # Load and contention
    active_workload_ratio: float
    cache_locality_ratio: float  # How many required assets are already on disk
    
    # Historical telemetry [0.0, 1.0]
    heartbeat_stability: float
    recent_failure_rate: float
    recent_latency_normalized: float
    throughput_normalized: float

    def to_vector(self) -> FeatureVector:
        """Flatten into an ordered numerical vector."""
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
    """Normalized numerical representation of a Workload's requirements."""
    
    # Normalized requirements [0.0, 1.0] (relative to network max capability)
    required_cpu_ratio: float
    required_ram_ratio: float
    required_vram_ratio: float
    
    # Binary requirements [0.0 or 1.0]
    requires_gpu: float
    
    # Scale and constraints
    dependency_count_normalized: float
    dependency_bytes_normalized: float
    priority_normalized: float
    
    # Extracted from WorkloadSpec estimations
    estimated_duration_normalized: float

    def to_vector(self) -> FeatureVector:
        """Flatten into an ordered numerical vector."""
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
