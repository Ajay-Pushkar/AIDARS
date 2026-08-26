"""M7 Intelligence: Temporal Memory & Observation.

Aggregates point-in-time metrics from the distributed layer into temporal
windows using Exponential Moving Averages (EMA) to understand baseline behavior,
current trends, and sudden shifts.
"""
from dataclasses import dataclass, field
from typing import Dict, Optional
import time

@dataclass
class EMA:
    """Exponential Moving Average tracker for a single metric."""
    value: float = 0.0
    alpha: float = 0.2  # Smoothing factor. Higher = favors recent data.
    initialized: bool = False

    def update(self, new_value: float) -> float:
        if not self.initialized:
            self.value = new_value
            self.initialized = True
        else:
            self.value = (self.alpha * new_value) + ((1.0 - self.alpha) * self.value)
        return self.value

@dataclass
class WorkerTemporalState:
    """Maintains the historical and current behavior of a single worker."""
    worker_id: str
    
    # EMAs for continuous metrics
    cpu_utilization_ema: EMA = field(default_factory=lambda: EMA(alpha=0.15))
    ram_utilization_ema: EMA = field(default_factory=lambda: EMA(alpha=0.15))
    latency_ema: EMA = field(default_factory=lambda: EMA(alpha=0.3))  # Faster reaction
    
    # Event counters over a sliding time window could be implemented here
    # For now, we track rates via EMA (1.0 for event, 0.0 for non-event)
    failure_rate_ema: EMA = field(default_factory=lambda: EMA(alpha=0.1))
    
    last_updated_utc: float = 0.0

    def get_trend(self, metric: str, current_value: float) -> float:
        """Returns the ratio of current value vs historical baseline.
        > 1.0 means metric is spiking relative to history.
        < 1.0 means metric is dropping.
        """
        ema = getattr(self, f"{metric}_ema")
        if not ema.initialized or ema.value == 0:
            return 1.0
        return current_value / ema.value

@dataclass
class WorkloadTemporalState:
    """Maintains historical execution characteristics for a workload type."""
    workload_type: str  # e.g., 'm6-lan-test' or the generic spec signature
    
    duration_ema: EMA = field(default_factory=lambda: EMA(alpha=0.2))
    ram_peak_ema: EMA = field(default_factory=lambda: EMA(alpha=0.2))
    failure_rate_ema: EMA = field(default_factory=lambda: EMA(alpha=0.1))

    last_executed_utc: float = 0.0

class TelemetryMemory:
    """Central temporal memory engine for M7.
    
    Provides the answers to:
    - What happened? (Long term baselines)
    - What is happening? (Current values)
    - What is changing? (Trends: current vs baseline)
    """
    def __init__(self):
        self.workers: Dict[str, WorkerTemporalState] = {}
        self.workloads: Dict[str, WorkloadTemporalState] = {}

    def ingest_worker_metrics(self, worker_id: str, cpu_ratio: float, ram_ratio: float, latency: float, failed: bool = False) -> None:
        """Ingest a real-time observation of a worker's health/metrics."""
        if worker_id not in self.workers:
            self.workers[worker_id] = WorkerTemporalState(worker_id=worker_id)
            
        state = self.workers[worker_id]
        state.cpu_utilization_ema.update(cpu_ratio)
        state.ram_utilization_ema.update(ram_ratio)
        state.latency_ema.update(latency)
        state.failure_rate_ema.update(1.0 if failed else 0.0)
        state.last_updated_utc = time.time()

    def ingest_workload_result(self, workload_type: str, duration: float, ram_peak: float, failed: bool) -> None:
        """Ingest the execution result of a completed or failed workload."""
        if workload_type not in self.workloads:
            self.workloads[workload_type] = WorkloadTemporalState(workload_type=workload_type)
            
        state = self.workloads[workload_type]
        if not failed:
            # Only update duration and ram on success
            state.duration_ema.update(duration)
            state.ram_peak_ema.update(ram_peak)
            
        state.failure_rate_ema.update(1.0 if failed else 0.0)
        state.last_executed_utc = time.time()
        
    def get_worker_state(self, worker_id: str) -> Optional[WorkerTemporalState]:
        return self.workers.get(worker_id)
        
    def get_workload_state(self, workload_type: str) -> Optional[WorkloadTemporalState]:
        return self.workloads.get(workload_type)
