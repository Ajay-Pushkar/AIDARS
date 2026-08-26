"""M7 Intelligence: Behavioral State Contracts.

Defines the explicit behavioral classifications (inferences) for workers
and workloads, along with confidence scores.
"""
from dataclasses import dataclass
from enum import Enum

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
    confidence: float  # [0.0, 1.0]

@dataclass
class WorkloadBehavior:
    """Inferred behavior state for a workload with confidence."""
    workload_id: str
    state: WorkloadState
    confidence: float  # [0.0, 1.0]
