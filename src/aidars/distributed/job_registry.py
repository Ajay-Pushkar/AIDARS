"""M8: Job-level orchestration state.

A Job is a durable grouping of one or more WorkloadSpecs submitted
together (e.g. every frame-range chunk of one Blender render request).
JobRegistry owns membership and completion policy only -- it never
duplicates placement, execution results, or resource requirements, which
remain exclusively WorkloadRegistry's responsibility.

Job STATE is deliberately never stored as an independent field. It is
always derived, on demand, from the actual WorkloadRecord facts of its
member workload_ids (via WorkloadRegistry). This makes "Job disagrees
with WorkloadRegistry" structurally impossible, rather than something to
be prevented by discipline.
"""
from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from aidars.distributed.workload_registry import (
    TERMINAL_WORKLOAD_STATES,
    WorkloadRegistry,
    WorkloadState,
)
from aidars.distributed.models import ExecutionShape

if TYPE_CHECKING:
    from aidars.distributed.state_store import CoordinatorStateStore


class CompletionPolicy(str, Enum):
    """How a Job's member workload outcomes roll up into a Job outcome."""

    ALL_REQUIRED = "all_required"   # every member workload must COMPLETE (default for rendering)
    ANY_SUCCESS = "any_success"     # at least one member workload must COMPLETE
    THRESHOLD = "threshold"         # at least `threshold` member workloads must COMPLETE
    BEST_EFFORT = "best_effort"     # never FAILED merely for partial completion; PARTIALLY_COMPLETED instead


class JobState(str, Enum):
    """Derived (never independently stored) Job-level state.

    CANCELLED is kept for PRD-conceptual completeness but is never
    assigned by any code path in this milestone -- there is no cancel
    operation anywhere in the system yet, so introducing a live
    transition into it would be inventing a feature the PRD does not
    ask M8 to build. Same discipline already applied to
    WorkloadState's unreachable members.
    """

    SUBMITTED = "submitted"
    RUNNING = "running"
    PARTIALLY_COMPLETED = "partially_completed"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Workload states this module buckets as "the workload is done trying".
_FAILED_WORKLOAD_STATES = frozenset({WorkloadState.FAILED, WorkloadState.UNSCHEDULABLE})
# Every WorkloadState.* live code actually assigns (see workload_registry.TERMINAL_WORKLOAD_STATES
# and workload.py) minus the ones already covered above/below -- kept explicit rather than
# "everything not COMPLETED/FAILED/UNSCHEDULABLE" so a newly-reachable state added later must be
# deliberately classified here, not silently swept into a bucket.
_PENDING_WORKLOAD_STATES = frozenset({
    WorkloadState.SUBMITTED, WorkloadState.VALIDATING, WorkloadState.PLACING,
})
_RUNNING_WORKLOAD_STATES = frozenset({
    WorkloadState.PLACED, WorkloadState.MIGRATING,
})


@dataclass
class JobAggregate:
    """A point-in-time derived summary of a Job's member workloads."""

    job_id: str
    total: int
    completed: int
    failed: int
    pending: int
    running: int
    state: JobState
    output_asset_hashes: Set[str] = field(default_factory=set)

    @property
    def is_terminal(self) -> bool:
        return self.state in (JobState.COMPLETED, JobState.FAILED, JobState.PARTIALLY_COMPLETED)


class JobRecord:
    """Durable Job identity, membership, and completion policy.

    Everything on this record is genuinely durable fact (what was
    submitted, when, under what policy) -- never a cached/derived value.
    Derived state lives only in JobAggregate, computed fresh by
    JobRegistry.get_aggregate().
    """

    def __init__(
        self,
        job_id: str,
        workload_ids: Set[str],
        completion_policy: CompletionPolicy = CompletionPolicy.ALL_REQUIRED,
        threshold: Optional[int] = None,
        execution_shape: ExecutionShape = ExecutionShape.TASK_SPLIT,
        required_worker_count: int = 1,
    ) -> None:
        if completion_policy == CompletionPolicy.THRESHOLD and not threshold:
            raise ValueError("THRESHOLD completion policy requires a positive threshold")
        if execution_shape == ExecutionShape.DISTRIBUTED_NATIVE:
            if len(workload_ids) != 1 or required_worker_count < 2 or required_worker_count > 64:
                raise ValueError("DISTRIBUTED_NATIVE requires one workload and 2 to 64 workers")
        elif required_worker_count != 1:
            raise ValueError("required_worker_count is only valid for DISTRIBUTED_NATIVE")
        elif execution_shape == ExecutionShape.SINGLE_MACHINE and len(workload_ids) != 1:
            raise ValueError("SINGLE_MACHINE requires exactly one workload")
        self.job_id = job_id
        self.workload_ids: Set[str] = set(workload_ids)
        self.completion_policy = completion_policy
        self.threshold = threshold
        self.execution_shape = execution_shape
        self.required_worker_count = required_worker_count
        self.created_at = time.time()


class JobRegistry:
    """Thread-safe in-memory registry of Jobs, backed by an injected WorkloadRegistry
    for state derivation and an optional CoordinatorStateStore for durability."""

    def __init__(
        self,
        workload_registry: WorkloadRegistry,
        state_store: Optional["CoordinatorStateStore"] = None,
    ) -> None:
        self._workload_registry = workload_registry
        self._lock = threading.RLock()
        self._jobs: Dict[str, JobRecord] = {}

        # Optional write-through persistence, same pattern as
        # WorkerRegistry/WorkloadRegistry (Phase 5.2B). None preserves
        # pure in-memory behavior -- no SQLite dependency unless injected.
        self._state_store = state_store

    def _persist_job(self, snapshot: Optional[JobRecord]) -> None:
        if self._state_store is None or snapshot is None:
            return
        self._state_store.save_job(snapshot)

    def create_job(
        self,
        job_id: str,
        workload_ids: Set[str],
        completion_policy: CompletionPolicy = CompletionPolicy.ALL_REQUIRED,
        threshold: Optional[int] = None,
        execution_shape: ExecutionShape = ExecutionShape.TASK_SPLIT,
        required_worker_count: int = 1,
    ) -> JobRecord:
        """Create (or, if job_id already exists, return the existing) Job record.

        Idempotent by job_id, matching WorkloadRegistry.add_workload()'s
        existing-id-is-a-no-op convention.
        """
        with self._lock:
            if job_id in self._jobs:
                return self._jobs[job_id]
            record = JobRecord(job_id, workload_ids, completion_policy, threshold,
                               execution_shape, required_worker_count)
            self._jobs[job_id] = record
            snapshot = copy.deepcopy(record)
        self._persist_job(snapshot)
        return record

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> List[JobRecord]:
        with self._lock:
            return list(self._jobs.values())

    def restore_job(self, record: JobRecord) -> None:
        """Insert a fully-formed JobRecord directly (e.g. loaded from
        CoordinatorStateStore.load_jobs()), bypassing create_job()'s
        idempotent-no-op path. Does not persist -- the record's source IS
        the persisted store. Phase-5.2C-recovery-equivalent for Jobs."""
        with self._lock:
            self._jobs[record.job_id] = record

    def get_aggregate(self, job_id: str) -> Optional[JobAggregate]:
        """Derive a Job's current state purely from its member
        WorkloadRecords. Never reads or writes any independently-stored
        Job state -- this IS the Job state, computed fresh every call."""
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            return None

        total = 0
        completed = 0
        failed = 0
        pending = 0
        running = 0
        output_hashes: Set[str] = set()

        for workload_id in job.workload_ids:
            record = self._workload_registry.get_workload(workload_id)
            if record is None:
                # Membership references a workload_id this WorkloadRegistry
                # doesn't (yet) know about -- e.g. mid-recovery before
                # workloads are restored. Count it as pending rather than
                # silently dropping it from `total`.
                total += 1
                pending += 1
                continue

            total += 1
            state = record.state
            if state == WorkloadState.COMPLETED:
                completed += 1
                if record.execution_result is not None:
                    output_hashes |= record.execution_result.output_asset_hashes
            elif state in _FAILED_WORKLOAD_STATES:
                failed += 1
            elif state in _RUNNING_WORKLOAD_STATES:
                running += 1
            elif state in _PENDING_WORKLOAD_STATES:
                pending += 1
            else:
                # A WorkloadState not in any known bucket (e.g. one of the
                # enum members live code never actually assigns). Treat
                # conservatively as still-pending rather than silently
                # mis-bucketing it as done.
                pending += 1

        state = self._derive_state(job, total, completed, failed, pending, running)
        return JobAggregate(
            job_id=job_id, total=total, completed=completed, failed=failed,
            pending=pending, running=running, state=state,
            output_asset_hashes=output_hashes,
        )

    @staticmethod
    def _derive_state(
        job: JobRecord, total: int, completed: int, failed: int, pending: int, running: int,
    ) -> JobState:
        if total == 0:
            return JobState.SUBMITTED

        still_active = pending + running
        if still_active > 0:
            if completed == 0 and failed == 0 and running == 0:
                return JobState.SUBMITTED
            return JobState.RUNNING

        # Nothing pending or running left -- every member workload is terminal.
        policy = job.completion_policy
        if policy == CompletionPolicy.ALL_REQUIRED:
            return JobState.COMPLETED if failed == 0 else JobState.FAILED
        if policy == CompletionPolicy.ANY_SUCCESS:
            return JobState.COMPLETED if completed > 0 else JobState.FAILED
        if policy == CompletionPolicy.THRESHOLD:
            return JobState.COMPLETED if completed >= (job.threshold or 0) else JobState.FAILED
        if policy == CompletionPolicy.BEST_EFFORT:
            if completed == total:
                return JobState.COMPLETED
            if completed > 0:
                return JobState.PARTIALLY_COMPLETED
            return JobState.FAILED
        # Unreachable given CompletionPolicy is a closed enum, but avoid a
        # silent None return if a new policy value is ever added without
        # updating this method.
        raise ValueError(f"Unhandled completion policy: {policy!r}")
