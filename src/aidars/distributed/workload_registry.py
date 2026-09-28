"""Workload state registry.

Maintains the state of all submitted workloads, enabling querying and lifecycle management.
"""

import copy
import threading
import time
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional

from aidars.distributed.models import (
    PlacementDecision,
    WorkloadExecutionResult,
    WorkloadSpec,
)

if TYPE_CHECKING:
    # state_store.py imports WorkloadRecord/WorkloadState from this module,
    # so a top-level import here would be circular. Only needed for the
    # type hint below.
    from aidars.distributed.state_store import CoordinatorStateStore


class WorkloadState(str, Enum):
    SUBMITTED = "submitted"
    VALIDATING = "validating"
    PLACING = "placing"
    PLACED = "placed"
    SYNCING_ASSETS = "syncing_assets"
    READY = "ready"
    EXECUTING = "executing"
    MIGRATING = "migrating"
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

    def __init__(self, state_store: Optional["CoordinatorStateStore"] = None) -> None:
        self._workloads: Dict[str, WorkloadRecord] = {}
        self._lock = threading.RLock()

        # Optional write-through persistence (Phase 5.2B). None (the
        # default) preserves pure in-memory behavior exactly as before --
        # no SQLite dependency unless a store is explicitly injected.
        self._state_store = state_store

    def _persist_workload(self, snapshot: Optional[WorkloadRecord]) -> None:
        """Write a workload snapshot to the state store, if one is configured.

        MUST be called with self._lock already released -- this performs
        synchronous disk I/O and must never run inside the registry's
        critical section. `snapshot` must be a deepcopy taken while the
        lock was held: WorkloadRecord is a plain mutable object (not a
        Pydantic model), so persisting the live reference after releasing
        the lock could race with a concurrent mutation of the same record
        and serialize a torn/inconsistent state. Persistence failures are
        not swallowed -- they propagate to the caller of the mutating
        method that triggered this write, since the in-memory state has
        already changed and must not be silently reported as durable when
        it isn't.
        """
        if self._state_store is None or snapshot is None:
            return
        self._state_store.save_workload(snapshot)

    def add_workload(self, spec: WorkloadSpec) -> WorkloadRecord:
        with self._lock:
            if spec.workload_id in self._workloads:
                return self._workloads[spec.workload_id]
            record = WorkloadRecord(spec)
            self._workloads[spec.workload_id] = record
            snapshot = copy.deepcopy(record)
        self._persist_workload(snapshot)
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
            snapshot = copy.deepcopy(record)
        self._persist_workload(snapshot)
        return True

    def set_placement(self, workload_id: str, decision: PlacementDecision) -> bool:
        with self._lock:
            record = self._workloads.get(workload_id)
            if not record:
                return False
            record.placement_decision = decision
            snapshot = copy.deepcopy(record)
        self._persist_workload(snapshot)
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
            snapshot = copy.deepcopy(record)
        self._persist_workload(snapshot)
        return True

    def list_workloads(self) -> List[WorkloadRecord]:
        with self._lock:
            return list(self._workloads.values())
