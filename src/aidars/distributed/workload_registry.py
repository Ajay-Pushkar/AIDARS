"""Workload state registry.

Maintains the state of all submitted workloads, enabling querying and lifecycle management.
"""

import threading
import time
from enum import Enum
from typing import Dict, List, Optional

from aidars.distributed.models import (
    PlacementDecision,
    WorkloadExecutionResult,
    WorkloadSpec,
)


class WorkloadState(str, Enum):
    SUBMITTED = "submitted"
    VALIDATING = "validating"
    PLACING = "placing"
    PLACED = "placed"
    SYNCING_ASSETS = "syncing_assets"
    READY = "ready"
    EXECUTING = "executing"
    INGESTING = "ingesting"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    UNSCHEDULABLE = "unschedulable"


class WorkloadRecord:
    """A record of a workload's lifecycle and current state."""

    def __init__(self, spec: WorkloadSpec) -> None:
        self.spec = spec
        self.state = WorkloadState.SUBMITTED
        self.submitted_at = time.time()
        self.completed_at: Optional[float] = None
        self.placement_decision: Optional[PlacementDecision] = None
        self.execution_result: Optional[WorkloadExecutionResult] = None
        self.error_message: Optional[str] = None


class WorkloadRegistry:
    """Thread-safe in-memory registry of workloads."""

    def __init__(self) -> None:
        self._workloads: Dict[str, WorkloadRecord] = {}
        self._lock = threading.RLock()

    def add_workload(self, spec: WorkloadSpec) -> WorkloadRecord:
        with self._lock:
            if spec.workload_id in self._workloads:
                return self._workloads[spec.workload_id]
            record = WorkloadRecord(spec)
            self._workloads[spec.workload_id] = record
            return record

    def get_workload(self, workload_id: str) -> Optional[WorkloadRecord]:
        with self._lock:
            return self._workloads.get(workload_id)

    def update_state(self, workload_id: str, state: WorkloadState, error_message: Optional[str] = None) -> bool:
        with self._lock:
            record = self._workloads.get(workload_id)
            if not record:
                return False
            record.state = state
            if error_message is not None:
                record.error_message = error_message
            if state in (WorkloadState.COMPLETED, WorkloadState.FAILED, WorkloadState.TIMEOUT, WorkloadState.UNSCHEDULABLE):
                if not record.completed_at:
                    record.completed_at = time.time()
            return True

    def set_placement(self, workload_id: str, decision: PlacementDecision) -> bool:
        with self._lock:
            record = self._workloads.get(workload_id)
            if not record:
                return False
            record.placement_decision = decision
            return True

    def set_result(self, workload_id: str, result: WorkloadExecutionResult) -> bool:
        with self._lock:
            record = self._workloads.get(workload_id)
            if not record:
                return False
            record.execution_result = result
            if result.success:
                record.state = WorkloadState.COMPLETED
            else:
                record.state = WorkloadState.FAILED
            record.completed_at = time.time()
            return True

    def list_workloads(self) -> List[WorkloadRecord]:
        with self._lock:
            return list(self._workloads.values())
