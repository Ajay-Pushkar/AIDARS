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


# Phase 5.2C: states a recovered workload must NOT be re-driven from,
# per the coordinator-recovery spec. Deliberately narrower than the
# `update_state()` completed_at-setting tuple below (which also includes
# TIMEOUT for an unrelated reason): TIMEOUT is not assigned anywhere in
# live code today, so it is left out here rather than assumed reachable.
TERMINAL_WORKLOAD_STATES = frozenset({
    WorkloadState.COMPLETED,
    WorkloadState.FAILED,
    WorkloadState.UNSCHEDULABLE,
})


class WorkloadIdConflictError(ValueError):
    """Raised by WorkloadRegistry.add_workload() when workload_id already
    refers to a DIFFERENT spec.

    Subclasses ValueError so any existing/future caller that already
    catches ValueError for input-validation purposes (e.g. the
    CoordinatorService REST layer's existing convention) catches this
    too without changes; callers that want to distinguish "this specific
    ID is taken by something else" from a generic validation error can
    catch WorkloadIdConflictError first.

    workload_id is externally meaningful and persisted -- silently
    replacing or silently dropping a submission under an existing ID
    would mean the wrong WorkloadSpec gets dispatched/reported under
    that ID. An identical resubmission (same spec, byte-for-byte) is NOT
    a conflict -- see add_workload()'s docstring.
    """

    def __init__(self, workload_id: str, existing_spec: WorkloadSpec, incoming_spec: WorkloadSpec) -> None:
        self.workload_id = workload_id
        self.existing_spec = existing_spec
        self.incoming_spec = incoming_spec
        super().__init__(
            f"workload_id '{workload_id}' already exists with a different spec "
            f"(job_id={existing_spec.job_id!r}, task_type={existing_spec.task_type!r}); "
            f"refusing to silently replace it or dispatch the new submission under "
            f"the same ID."
        )


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
        """Register a new workload, or, if workload_id already exists,
        either no-op (identical resubmission) or reject (conflicting
        submission).

        Identity contract: workload_id is the sole identity key. If it
        already exists:
          - and the incoming spec is equal (==) to the existing one --
            i.e. a byte-for-byte identical resubmission, such as a client
            retrying after a dropped response -- this is idempotent: the
            existing record is returned unchanged, exactly as before this
            check existed (WorkloadSpec has no server-assigned mutable
            fields, so equality here means "the caller is asking for the
            exact same thing again").
          - and the incoming spec differs in any field -- this is a
            genuine identity collision: raises WorkloadIdConflictError
            rather than silently keeping the old spec (previous behavior)
            or silently overwriting it. Never dispatch a workload under
            the wrong spec.
        """
        with self._lock:
            existing = self._workloads.get(spec.workload_id)
            if existing is not None:
                if existing.spec == spec:
                    return existing
                raise WorkloadIdConflictError(spec.workload_id, existing.spec, spec)
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

    def restore_workload(self, record: WorkloadRecord) -> None:
        """Insert a fully-formed WorkloadRecord directly into the registry,
        bypassing add_workload()'s default-initialization (which always
        creates a fresh SUBMITTED record and would discard the restored
        state/placement/execution_result).

        Phase 5.2C only: used by CoordinatorService startup recovery to
        load records reconstructed from CoordinatorStateStore.load_workloads().
        Does not persist -- the record's source IS the persisted store, so
        writing it straight back would be a redundant no-op at best.
        """
        with self._lock:
            self._workloads[record.spec.workload_id] = record
