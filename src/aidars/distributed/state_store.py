"""Phase 5.2A: Coordinator state persistence foundation.

Isolated SQLite persistence for the coordinator's currently in-memory
state (WorkerInfo, WorkloadRecord). Follows the same conventions already
established in src/aidars/cache/index.py: WAL mode, a busy timeout,
parameterized SQL, one table per entity, and JSON blobs for nested
Pydantic models rather than a column per nested field.

This module is deliberately NOT wired into CoordinatorService,
WorkerRegistry, WorkloadRegistry, or WorkloadOrchestrator. It has no
opinion on when to persist or how to recover -- it only knows how to
save and load the two entities faithfully. Wiring, startup recovery, and
job/M8 concerns are later, separately-scoped phases.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, List, Optional, TypeVar, Union

try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:  # pragma: no cover - fcntl is POSIX-only
    fcntl = None  # type: ignore[assignment]
    _HAS_FCNTL = False

try:
    import msvcrt
    _HAS_MSVCRT = True
except ImportError:  # pragma: no cover - msvcrt is Windows-only
    msvcrt = None  # type: ignore[assignment]
    _HAS_MSVCRT = False

# Bug fix (post-M10): the single-writer lock previously only had an
# fcntl (POSIX) implementation. `_HAS_FCNTL` is False on Windows, so
# `_acquire_single_writer_lock()` was never even called there (see its
# call site in __init__) -- a second CoordinatorStateStore on the same
# path silently succeeded instead of raising CoordinatorAlreadyRunningError.
# `_HAS_LOCK_PRIMITIVE` is the single flag both __init__ and close() now
# branch on, backed by whichever of fcntl/msvcrt is actually available.
_HAS_LOCK_PRIMITIVE = _HAS_FCNTL or _HAS_MSVCRT

from aidars.distributed.artifact import (
    Artifact,
    ArtifactLifecycleState,
    ArtifactVerificationState,
)
from aidars.distributed.attempt import AttemptRecord, AttemptStatus
from aidars.distributed.job_registry import CompletionPolicy, JobRecord
from aidars.distributed.models import (
    FailureCategory,
    PlacementDecision,
    WorkerInfo,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadState

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class CoordinatorAlreadyRunningError(RuntimeError):
    """M10.15: raised when a second CoordinatorService process tries to
    open the same SQLite state file another process already holds an
    exclusive lock on.

    This is NOT distributed consensus/HA -- it is the one concrete,
    honest safety mechanism the M10 PRD asks for in lieu of real HA: a
    single coordinator remains the only valid configuration for this
    architecture (see docs/M10_ARCHITECTURE.md's HA readiness audit), so
    accidentally starting a second one against the same state file must
    fail loudly instead of silently corrupting or racing on shared
    state.
    """


# Sidecar lock file suffix. Kept as a byte-identical companion path next
# to the SQLite file itself rather than reusing SQLite's own locking,
# because SQLite's own busy_timeout/WAL locking is designed to arbitrate
# many short-lived transactions from the SAME process, not to detect "a
# second whole coordinator process is running" at startup.
_LOCK_FILE_SUFFIX = ".coordinator.lock"


class CoordinatorStateStore:
    """SQLite-backed persistence for WorkerInfo and WorkloadRecord.

    Round-trip fidelity is the only requirement: what goes in via
    save_worker()/save_workload() must come back unchanged (modulo
    floating-point timestamp precision) via load_workers()/load_workloads().
    """

    def __init__(self, db_path: Union[str, Path], enforce_single_writer: bool = True) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._lock_fd = None
        if enforce_single_writer and _HAS_LOCK_PRIMITIVE:
            self._acquire_single_writer_lock()

        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            timeout=10.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _acquire_single_writer_lock(self) -> None:
        """M10.15: exclusive, non-blocking advisory lock on a sidecar
        file next to db_path. Fails FAST and LOUD (raises
        CoordinatorAlreadyRunningError) rather than blocking, because a
        second coordinator process starting up should be told
        immediately, not hang waiting for the first one to exit.

        Cross-platform: fcntl.flock on POSIX, msvcrt.locking on Windows
        (the two OS-level advisory/mandatory byte-range lock primitives
        the standard library actually offers -- no third-party
        dependency added). The lock file is opened "r+b" over content
        that's ensured to exist (creating a fresh 1-byte file only if
        one doesn't already exist) rather than "w", specifically so a
        contending second process's open() call never truncates the
        first process's still-locked file out from under it -- "w" mode
        truncates on every open, which on Windows can race with (and
        potentially corrupt the visibility of) an active msvcrt.locking()
        byte-range lock held by another handle.
        """
        lock_path = self.db_path.parent / (self.db_path.name + _LOCK_FILE_SUFFIX)
        if not lock_path.exists():
            lock_path.write_bytes(b"\0")
        fd = open(lock_path, "r+b")
        try:
            if _HAS_FCNTL:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                fd.seek(0)
                msvcrt.locking(fd.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            fd.close()
            raise CoordinatorAlreadyRunningError(
                f"Another coordinator process already holds the lock for state "
                f"file {self.db_path} (lock file: {lock_path}). Running two "
                f"coordinators against the same SQLite state file is unsafe and "
                f"not supported -- see docs/M10_ARCHITECTURE.md's HA readiness "
                f"audit for why, and what a real multi-coordinator deployment "
                f"would require."
            ) from exc
        self._lock_fd = fd

    def _release_single_writer_lock(self, fd) -> None:
        """Cross-platform counterpart to _acquire_single_writer_lock().
        Best-effort: an error unlocking is never allowed to prevent the
        fd from being closed (close() releases any OS-held lock anyway
        on both platforms; this just does it explicitly/promptly first
        so a `close()`-then-immediately-reopen-same-path in the SAME
        process, as several tests do, doesn't depend on close() alone)."""
        try:
            if _HAS_FCNTL:
                fcntl.flock(fd, fcntl.LOCK_UN)
            elif _HAS_MSVCRT:
                fd.seek(0)
                msvcrt.locking(fd.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass

    def __del__(self) -> None:
        self.close()

    @staticmethod
    def _safe_reconstruct(
        rows: List[sqlite3.Row],
        converter: Callable[[sqlite3.Row], _T],
        entity_name: str,
        pk_column: str,
    ) -> List[_T]:
        """M9: recovery-abuse protection. A persisted row can be malformed
        (corrupted JSON, a value that no longer matches its Pydantic
        schema, an enum value that isn't valid) whether through disk
        corruption, a bug in an older version, or deliberate tampering.
        Reconstruction is therefore per-row and explicit: a row that
        fails to deserialize is logged (loudly, with its primary key, but
        never with the raw row content, which could contain sensitive
        workload/job metadata) and excluded from the returned list --
        never silently accepted as-is, and never allowed to abort loading
        every OTHER valid row in the table (a single corrupted row must
        not be a denial-of-service vector against the rest of coordinator
        startup). Catching the broad `Exception` type here is deliberate,
        not a bare `except:` -- deserialization can fail via
        pydantic.ValidationError, json.JSONDecodeError, KeyError, or
        ValueError (invalid enum value), and the whole point of this
        method is defending against arbitrary malformed input, so every
        failure mode must be caught, explicitly logged, and skipped.
        """
        results: List[_T] = []
        for row in rows:
            try:
                results.append(converter(row))
            except Exception as exc:
                pk_value = row[pk_column] if pk_column in row.keys() else "<unknown>"
                logger.error(
                    "Skipping malformed persisted %s row (%s=%r): %s: %s",
                    entity_name, pk_column, pk_value, type(exc).__name__, exc,
                )
        return results

    def close(self) -> None:
        """Deterministic, idempotent release of both resources this
        store may hold: the SQLite connection and (M10.15) the
        single-writer lock file handle. Safe to call multiple times
        (e.g. an explicit close() followed by __del__ finding nothing
        left to do) and safe to call on a partially-constructed instance
        (e.g. __init__ raised CoordinatorAlreadyRunningError before
        self._conn was ever assigned) -- every attribute access below is
        defensive via getattr() for exactly that reason.
        """
        if getattr(self, "_conn", None) is not None:
            self._conn.close()
            self._conn = None
        lock_fd = getattr(self, "_lock_fd", None)
        if lock_fd is not None:
            self._release_single_writer_lock(lock_fd)
            lock_fd.close()
            self._lock_fd = None

    def _init_schema(self) -> None:
        """Initialize schema and configure WAL mode pragmas."""
        with self._lock:
            if self._conn is None:
                return
            cursor = self._conn.cursor()
            cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute("PRAGMA busy_timeout = 10000;")
            cursor.execute("PRAGMA synchronous = NORMAL;")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    info_json TEXT NOT NULL,
                    last_heartbeat_utc REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_workers_last_heartbeat
                ON workers(last_heartbeat_utc);
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS workloads (
                    workload_id TEXT PRIMARY KEY,
                    spec_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    placement_json TEXT,
                    placement_explanation_json TEXT,
                    execution_result_json TEXT,
                    error_message TEXT,
                    submitted_at REAL NOT NULL,
                    completed_at REAL,
                    updated_at REAL NOT NULL
                );
            """)
            workload_columns = {row[1] for row in cursor.execute("PRAGMA table_info(workloads)").fetchall()}
            if "placement_explanation_json" not in workload_columns:
                cursor.execute("ALTER TABLE workloads ADD COLUMN placement_explanation_json TEXT")
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_workloads_state
                ON workloads(state);
            """)

            # M8: Job identity/membership/completion-policy is durable;
            # Job STATE is deliberately not a column here -- it is always
            # derived from workloads at read time (see job_registry.py).
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    workload_ids_json TEXT NOT NULL,
                    completion_policy TEXT NOT NULL,
                    threshold INTEGER,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
            """)

            # M8.8/M8.9: Artifact provenance + lifecycle metadata only --
            # never a second copy of the CAS bytes or a second hash scheme.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    content_hash TEXT NOT NULL,
                    producer_workload_id TEXT NOT NULL,
                    producer_job_id TEXT,
                    created_at REAL NOT NULL,
                    verification_state TEXT NOT NULL,
                    lifecycle_state TEXT NOT NULL,
                    storage_location TEXT NOT NULL,
                    size_bytes INTEGER,
                    updated_at REAL NOT NULL
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_artifacts_content_hash
                ON artifacts(content_hash);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_artifacts_producer_job
                ON artifacts(producer_job_id);
            """)

            # M10.1: one row per execution Attempt. workload_id is NOT the
            # primary key (a workload has many attempts, numbered by
            # attempt_number) -- see attempt.py's module docstring for the
            # Job -> Workload -> Attempt -> ExecutionResult -> Artifact
            # relationship this preserves across a coordinator restart.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS attempts (
                    attempt_id TEXT PRIMARY KEY,
                    workload_id TEXT NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    worker_id TEXT,
                    queued_at REAL NOT NULL,
                    assigned_at REAL,
                    started_at REAL,
                    finished_at REAL,
                    failure_category TEXT,
                    failure_reason TEXT,
                    execution_result_json TEXT,
                    checkpoint_hash TEXT,
                    checkpoint_runtime_type TEXT,
                    checkpoint_format_version INTEGER,
                    updated_at REAL NOT NULL
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_attempts_workload_id
                ON attempts(workload_id);
            """)
            self._conn.commit()

    # ------------------------------------------------------------------ #
    # Workers
    # ------------------------------------------------------------------ #

    def save_worker(self, worker_info: WorkerInfo) -> None:
        """Insert or update the persisted record for a worker."""
        with self._lock:
            if self._conn is None:
                return
            now = time.time()
            self._conn.execute("""
                INSERT INTO workers (worker_id, info_json, last_heartbeat_utc, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    info_json = excluded.info_json,
                    last_heartbeat_utc = excluded.last_heartbeat_utc,
                    updated_at = excluded.updated_at;
            """, (
                worker_info.worker_id,
                worker_info.model_dump_json(),
                worker_info.last_heartbeat_utc,
                now,
            ))
            self._conn.commit()

    def delete_worker(self, worker_id: str) -> None:
        """Remove a worker's persisted row, if any (e.g. on unregister/eviction)."""
        with self._lock:
            if self._conn is None:
                return
            self._conn.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))
            self._conn.commit()

    def load_workers(self) -> List[WorkerInfo]:
        """Load every persisted worker as a fully-reconstructed WorkerInfo."""
        with self._lock:
            if self._conn is None:
                return []
            rows = self._conn.execute("SELECT worker_id, info_json FROM workers").fetchall()
            return self._safe_reconstruct(
                rows, lambda row: WorkerInfo.model_validate_json(row["info_json"]),
                entity_name="worker", pk_column="worker_id",
            )

    # ------------------------------------------------------------------ #
    # Workloads
    # ------------------------------------------------------------------ #

    def save_workload(self, record: WorkloadRecord) -> None:
        """Insert or update the persisted record for a workload."""
        with self._lock:
            if self._conn is None:
                return
            now = time.time()
            placement_json = (
                record.placement_decision.model_dump_json()
                if record.placement_decision is not None
                else None
            )
            placement_explanation_json = json.dumps(record.placement_explanation) if record.placement_explanation else None
            execution_result_json = (
                record.execution_result.model_dump_json()
                if record.execution_result is not None
                else None
            )
            self._conn.execute("""
                INSERT INTO workloads (
                    workload_id, spec_json, state, placement_json, placement_explanation_json,
                    execution_result_json, error_message, submitted_at,
                    completed_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workload_id) DO UPDATE SET
                    spec_json = excluded.spec_json,
                    state = excluded.state,
                    placement_json = excluded.placement_json,
                    placement_explanation_json = excluded.placement_explanation_json,
                    execution_result_json = excluded.execution_result_json,
                    error_message = excluded.error_message,
                    submitted_at = excluded.submitted_at,
                    completed_at = excluded.completed_at,
                    updated_at = excluded.updated_at;
            """, (
                record.spec.workload_id,
                record.spec.model_dump_json(),
                record.state.value,
                placement_json,
                placement_explanation_json,
                execution_result_json,
                record.error_message,
                record.submitted_at,
                record.completed_at,
                now,
            ))
            self._conn.commit()

    def load_workloads(self) -> List[WorkloadRecord]:
        """Load every persisted workload as a fully-reconstructed WorkloadRecord."""
        with self._lock:
            if self._conn is None:
                return []
            rows = self._conn.execute("SELECT * FROM workloads").fetchall()
            return self._safe_reconstruct(
                rows, self._row_to_workload_record,
                entity_name="workload", pk_column="workload_id",
            )

    @staticmethod
    def _row_to_workload_record(row: sqlite3.Row) -> WorkloadRecord:
        spec = WorkloadSpec.model_validate_json(row["spec_json"])
        record = WorkloadRecord(spec)
        record.state = WorkloadState(row["state"])
        record.submitted_at = row["submitted_at"]
        record.completed_at = row["completed_at"]
        record.error_message = row["error_message"]
        record.placement_decision = (
            PlacementDecision.model_validate_json(row["placement_json"])
            if row["placement_json"] is not None
            else None
        )
        record.placement_explanation = (
            json.loads(row["placement_explanation_json"])
            if "placement_explanation_json" in row.keys() and row["placement_explanation_json"]
            else {}
        )
        record.execution_result = (
            WorkloadExecutionResult.model_validate_json(row["execution_result_json"])
            if row["execution_result_json"] is not None
            else None
        )
        return record

    # ------------------------------------------------------------------ #
    # Jobs (M8)
    # ------------------------------------------------------------------ #

    def save_job(self, record: JobRecord) -> None:
        """Insert or update the persisted record for a Job. Only durable
        identity/membership/policy fields are written -- Job state is
        never persisted (see module/class docstrings)."""
        with self._lock:
            if self._conn is None:
                return
            now = time.time()
            self._conn.execute("""
                INSERT INTO jobs (
                    job_id, workload_ids_json, completion_policy, threshold,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    workload_ids_json = excluded.workload_ids_json,
                    completion_policy = excluded.completion_policy,
                    threshold = excluded.threshold,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at;
            """, (
                record.job_id,
                json.dumps(sorted(record.workload_ids)),
                record.completion_policy.value,
                record.threshold,
                record.created_at,
                now,
            ))
            self._conn.commit()

    def load_jobs(self) -> List[JobRecord]:
        """Load every persisted Job as a fully-reconstructed JobRecord."""
        with self._lock:
            if self._conn is None:
                return []
            rows = self._conn.execute("SELECT * FROM jobs").fetchall()
            return self._safe_reconstruct(
                rows, self._row_to_job_record,
                entity_name="job", pk_column="job_id",
            )

    @staticmethod
    def _row_to_job_record(row: sqlite3.Row) -> JobRecord:
        workload_ids = set(json.loads(row["workload_ids_json"]))
        record = JobRecord(
            job_id=row["job_id"],
            workload_ids=workload_ids,
            completion_policy=CompletionPolicy(row["completion_policy"]),
            threshold=row["threshold"],
        )
        record.created_at = row["created_at"]
        return record

    # ------------------------------------------------------------------ #
    # Artifacts (M8.8/M8.9)
    # ------------------------------------------------------------------ #

    def save_artifact(self, artifact: Artifact) -> None:
        """Insert or update the persisted record for an Artifact."""
        with self._lock:
            if self._conn is None:
                return
            now = time.time()
            self._conn.execute("""
                INSERT INTO artifacts (
                    artifact_id, content_hash, producer_workload_id,
                    producer_job_id, created_at, verification_state,
                    lifecycle_state, storage_location, size_bytes, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(artifact_id) DO UPDATE SET
                    content_hash = excluded.content_hash,
                    producer_workload_id = excluded.producer_workload_id,
                    producer_job_id = excluded.producer_job_id,
                    created_at = excluded.created_at,
                    verification_state = excluded.verification_state,
                    lifecycle_state = excluded.lifecycle_state,
                    storage_location = excluded.storage_location,
                    size_bytes = excluded.size_bytes,
                    updated_at = excluded.updated_at;
            """, (
                artifact.artifact_id,
                artifact.content_hash,
                artifact.producer_workload_id,
                artifact.producer_job_id,
                artifact.created_at,
                artifact.verification_state.value,
                artifact.lifecycle_state.value,
                artifact.storage_location,
                artifact.size_bytes,
                now,
            ))
            self._conn.commit()

    def load_artifacts(self) -> List[Artifact]:
        """Load every persisted Artifact."""
        with self._lock:
            if self._conn is None:
                return []
            rows = self._conn.execute("SELECT * FROM artifacts").fetchall()
            return self._safe_reconstruct(
                rows, self._row_to_artifact,
                entity_name="artifact", pk_column="artifact_id",
            )

    @staticmethod
    def _row_to_artifact(row: sqlite3.Row) -> Artifact:
        return Artifact(
            artifact_id=row["artifact_id"],
            content_hash=row["content_hash"],
            producer_workload_id=row["producer_workload_id"],
            producer_job_id=row["producer_job_id"],
            created_at=row["created_at"],
            verification_state=ArtifactVerificationState(row["verification_state"]),
            lifecycle_state=ArtifactLifecycleState(row["lifecycle_state"]),
            storage_location=row["storage_location"],
            size_bytes=row["size_bytes"],
        )

    # ------------------------------------------------------------------ #
    # Attempts (M10.1/M10.18)
    # ------------------------------------------------------------------ #

    def save_attempt(self, attempt: AttemptRecord) -> None:
        """Insert or update the persisted record for an execution Attempt."""
        with self._lock:
            if self._conn is None:
                return
            now = time.time()
            execution_result_json = (
                attempt.execution_result.model_dump_json()
                if attempt.execution_result is not None
                else None
            )
            self._conn.execute("""
                INSERT INTO attempts (
                    attempt_id, workload_id, attempt_number, status, worker_id,
                    queued_at, assigned_at, started_at, finished_at,
                    failure_category, failure_reason, execution_result_json,
                    checkpoint_hash, checkpoint_runtime_type, checkpoint_format_version,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(attempt_id) DO UPDATE SET
                    status = excluded.status,
                    worker_id = excluded.worker_id,
                    assigned_at = excluded.assigned_at,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    failure_category = excluded.failure_category,
                    failure_reason = excluded.failure_reason,
                    execution_result_json = excluded.execution_result_json,
                    checkpoint_hash = excluded.checkpoint_hash,
                    checkpoint_runtime_type = excluded.checkpoint_runtime_type,
                    checkpoint_format_version = excluded.checkpoint_format_version,
                    updated_at = excluded.updated_at;
            """, (
                attempt.attempt_id,
                attempt.workload_id,
                attempt.attempt_number,
                attempt.status.value,
                attempt.worker_id,
                attempt.queued_at,
                attempt.assigned_at,
                attempt.started_at,
                attempt.finished_at,
                attempt.failure_category.value if attempt.failure_category else None,
                attempt.failure_reason,
                execution_result_json,
                attempt.checkpoint_hash,
                attempt.checkpoint_runtime_type,
                attempt.checkpoint_format_version,
                now,
            ))
            self._conn.commit()

    def load_attempts(self) -> List[AttemptRecord]:
        """Load every persisted Attempt."""
        with self._lock:
            if self._conn is None:
                return []
            rows = self._conn.execute("SELECT * FROM attempts").fetchall()
            return self._safe_reconstruct(
                rows, self._row_to_attempt,
                entity_name="attempt", pk_column="attempt_id",
            )

    @staticmethod
    def _row_to_attempt(row: sqlite3.Row) -> AttemptRecord:
        return AttemptRecord(
            attempt_id=row["attempt_id"],
            workload_id=row["workload_id"],
            attempt_number=row["attempt_number"],
            status=AttemptStatus(row["status"]),
            worker_id=row["worker_id"],
            queued_at=row["queued_at"],
            assigned_at=row["assigned_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            failure_category=(
                FailureCategory(row["failure_category"]) if row["failure_category"] is not None else None
            ),
            failure_reason=row["failure_reason"],
            execution_result=(
                WorkloadExecutionResult.model_validate_json(row["execution_result_json"])
                if row["execution_result_json"] is not None
                else None
            ),
            checkpoint_hash=row["checkpoint_hash"],
            checkpoint_runtime_type=row["checkpoint_runtime_type"],
            checkpoint_format_version=row["checkpoint_format_version"],
        )
