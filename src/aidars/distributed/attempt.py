"""M10.1: persisted execution-attempt model.

A logical Workload (WorkloadRecord in workload_registry.py) keeps a
STABLE workload_id across retries -- it is never re-minted. Each actual
try at executing it is a separate, numbered Attempt:

    Job -> Workload -> Attempt(1), Attempt(2), Attempt(3), ... -> ExecutionResult -> Artifact

WorkloadRecord.execution_result remains exactly what it was before M10
(the M8-era "latest/final result" field, read unchanged by
JobRegistry.get_aggregate() and the existing GET /workloads/{id} API).
AttemptRecord is a new, additive, purely historical ledger sitting
alongside it -- it does not replace or duplicate WorkloadRecord, and
nothing about the pre-M10 workload read/aggregation path changes.

Persisted through the same CoordinatorStateStore/SQLite/WAL pattern as
every other registry in this package (see state_store.py's `attempts`
table) -- this is not a second persistence mechanism.
"""
from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional

from aidars.distributed.models import FailureCategory, WorkloadExecutionResult

if TYPE_CHECKING:
    from aidars.distributed.state_store import CoordinatorStateStore


class AttemptStatus(str, Enum):
    """Lifecycle status of one execution attempt.

    No CANCELLED member: there is no cancel operation anywhere in this
    codebase (same discipline already applied to WorkloadState and
    JobState's unreachable members -- see their module docstrings).

    LOST is distinct from FAILED: FAILED means the attempt produced an
    explicit, observed failure outcome (a WorkloadExecutionResult with
    success=False, or a dispatch-time exception). LOST means the
    coordinator's owning worker disappeared (heartbeat timeout/eviction)
    while this attempt was RUNNING and the coordinator has NO observed
    outcome at all -- the work may have actually completed on the worker
    before it vanished. This is the M10.6 "worker completes work but
    coordinator doesn't observe completion" case: a LOST attempt is
    still classified as retryable (worker unavailability), and recovery
    creates a fresh attempt for the same workload_id, which is why
    at-least-once (never exactly-once) execution remains the documented
    invariant.
    """

    QUEUED = "queued"
    ASSIGNED = "assigned"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LOST = "lost"


# Attempt statuses that mean "this attempt is done trying" -- no further
# transition will ever be applied to it.
TERMINAL_ATTEMPT_STATUSES = frozenset({
    AttemptStatus.SUCCEEDED, AttemptStatus.FAILED, AttemptStatus.LOST,
})


def make_attempt_id(workload_id: str, attempt_number: int) -> str:
    return f"{workload_id}#{attempt_number}"


@dataclass
class AttemptRecord:
    """One execution attempt of a logical Workload."""

    attempt_id: str
    workload_id: str
    attempt_number: int
    status: AttemptStatus = AttemptStatus.QUEUED
    worker_id: Optional[str] = None

    # M10.11 observability timeline (coordinator-side; worker-side phase
    # durations live on execution_result -- see models.WorkloadExecutionResult).
    queued_at: float = field(default_factory=time.time)
    assigned_at: Optional[float] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None

    failure_category: Optional[FailureCategory] = None
    failure_reason: Optional[str] = None
    execution_result: Optional[WorkloadExecutionResult] = None

    # M10.7 checkpoint metadata -- lives on the Attempt (the natural owner
    # of "did THIS specific attempt produce a checkpoint"), not a separate
    # CheckpointRegistry. See checkpoint.py for validation.
    checkpoint_hash: Optional[str] = None
    checkpoint_runtime_type: Optional[str] = None
    checkpoint_format_version: Optional[int] = None

    @property
    def queued_duration_seconds(self) -> Optional[float]:
        """Time this attempt spent waiting for a placement decision."""
        if self.assigned_at is None:
            return None
        return max(0.0, self.assigned_at - self.queued_at)

    @property
    def total_duration_seconds(self) -> Optional[float]:
        """Wall-clock time from attempt creation to its terminal outcome."""
        if self.finished_at is None:
            return None
        return max(0.0, self.finished_at - self.queued_at)

    def to_summary_dict(self) -> Dict[str, object]:
        """Compact, API-safe summary (Part 17: expose attempt/retry/timing
        state through the existing workload API surface)."""
        return {
            "attempt_id": self.attempt_id,
            "attempt_number": self.attempt_number,
            "status": self.status.value,
            "worker_id": self.worker_id,
            "queued_at": self.queued_at,
            "assigned_at": self.assigned_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "queued_duration_seconds": self.queued_duration_seconds,
            "total_duration_seconds": self.total_duration_seconds,
            "failure_category": self.failure_category.value if self.failure_category else None,
            "failure_reason": self.failure_reason,
            "was_checkpointed": self.checkpoint_hash is not None,
            "checkpoint_hash": self.checkpoint_hash,
        }


class AttemptRegistry:
    """Thread-safe in-memory registry of Attempts, mirroring the exact
    JobRegistry/ArtifactRegistry pattern already established in this
    package: in-memory dict + optional write-through CoordinatorStateStore
    + a restore_*() method for startup recovery."""

    def __init__(self, state_store: Optional["CoordinatorStateStore"] = None) -> None:
        self._lock = threading.RLock()
        self._attempts: Dict[str, AttemptRecord] = {}
        self._by_workload: Dict[str, List[str]] = {}  # workload_id -> [attempt_id, ...] in creation order
        self._state_store = state_store

    def _persist(self, snapshot: Optional[AttemptRecord]) -> None:
        if self._state_store is None or snapshot is None:
            return
        self._state_store.save_attempt(snapshot)

    def create_attempt(self, workload_id: str, worker_id: Optional[str] = None) -> AttemptRecord:
        """Create the next-numbered attempt for workload_id. attempt_number
        starts at 1 and is stable/monotonic per workload_id -- the
        workload_id itself never changes across retries."""
        with self._lock:
            existing_ids = self._by_workload.setdefault(workload_id, [])
            attempt_number = len(existing_ids) + 1
            attempt_id = make_attempt_id(workload_id, attempt_number)
            record = AttemptRecord(
                attempt_id=attempt_id,
                workload_id=workload_id,
                attempt_number=attempt_number,
                worker_id=worker_id,
            )
            self._attempts[attempt_id] = record
            existing_ids.append(attempt_id)
            snapshot = copy.deepcopy(record)
        self._persist(snapshot)
        return record

    def get_attempt(self, attempt_id: str) -> Optional[AttemptRecord]:
        with self._lock:
            return self._attempts.get(attempt_id)

    def list_attempts_for_workload(self, workload_id: str) -> List[AttemptRecord]:
        with self._lock:
            ids = self._by_workload.get(workload_id, [])
            return [self._attempts[i] for i in ids if i in self._attempts]

    def get_latest_attempt(self, workload_id: str) -> Optional[AttemptRecord]:
        attempts = self.list_attempts_for_workload(workload_id)
        return attempts[-1] if attempts else None

    def count_attempts(self, workload_id: str) -> int:
        with self._lock:
            return len(self._by_workload.get(workload_id, []))

    def _update(self, attempt_id: str, mutator) -> Optional[AttemptRecord]:
        with self._lock:
            record = self._attempts.get(attempt_id)
            if record is None:
                return None
            mutator(record)
            snapshot = copy.deepcopy(record)
        self._persist(snapshot)
        return snapshot

    def mark_assigned(self, attempt_id: str, worker_id: str) -> Optional[AttemptRecord]:
        def _mut(r: AttemptRecord) -> None:
            r.status = AttemptStatus.ASSIGNED
            r.worker_id = worker_id
            r.assigned_at = time.time()
        return self._update(attempt_id, _mut)

    def mark_running(self, attempt_id: str) -> Optional[AttemptRecord]:
        def _mut(r: AttemptRecord) -> None:
            r.status = AttemptStatus.RUNNING
            r.started_at = time.time()
        return self._update(attempt_id, _mut)

    def mark_succeeded(self, attempt_id: str, result: WorkloadExecutionResult) -> Optional[AttemptRecord]:
        def _mut(r: AttemptRecord) -> None:
            r.status = AttemptStatus.SUCCEEDED
            r.finished_at = time.time()
            r.execution_result = result
            if result.was_checkpointed and result.checkpoint_hash:
                r.checkpoint_hash = result.checkpoint_hash
                r.checkpoint_runtime_type = result.checkpoint_runtime_type
                r.checkpoint_format_version = result.checkpoint_format_version
        return self._update(attempt_id, _mut)

    def mark_failed(
        self,
        attempt_id: str,
        failure_category: Optional[FailureCategory],
        failure_reason: Optional[str],
        result: Optional[WorkloadExecutionResult] = None,
    ) -> Optional[AttemptRecord]:
        def _mut(r: AttemptRecord) -> None:
            r.status = AttemptStatus.FAILED
            r.finished_at = time.time()
            r.failure_category = failure_category
            r.failure_reason = failure_reason
            if result is not None:
                r.execution_result = result
        return self._update(attempt_id, _mut)

    def mark_lost(self, attempt_id: str, reason: str) -> Optional[AttemptRecord]:
        """M10.6: the owning worker disappeared while this attempt was
        RUNNING with no observed outcome. See AttemptStatus.LOST docstring."""
        def _mut(r: AttemptRecord) -> None:
            r.status = AttemptStatus.LOST
            r.finished_at = time.time()
            r.failure_category = FailureCategory.WORKER_UNAVAILABLE
            r.failure_reason = reason
        return self._update(attempt_id, _mut)

    def list_running_attempts_for_worker(self, worker_id: str) -> List[AttemptRecord]:
        """Used by M10.6 worker-failure recovery to find attempts that
        need reclassifying when a worker is evicted."""
        with self._lock:
            return [
                r for r in self._attempts.values()
                if r.worker_id == worker_id and r.status in (AttemptStatus.ASSIGNED, AttemptStatus.RUNNING)
            ]

    def restore_attempt(self, record: AttemptRecord) -> None:
        """Insert a fully-formed AttemptRecord directly (loaded from
        CoordinatorStateStore.load_attempts()). Does not persist -- the
        record's source IS the persisted store."""
        with self._lock:
            self._attempts[record.attempt_id] = record
            self._by_workload.setdefault(record.workload_id, [])
            if record.attempt_id not in self._by_workload[record.workload_id]:
                self._by_workload[record.workload_id].append(record.attempt_id)
                self._by_workload[record.workload_id].sort(
                    key=lambda aid: self._attempts[aid].attempt_number
                )
