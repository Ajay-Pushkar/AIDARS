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

import sqlite3
import threading
import time
from pathlib import Path
from typing import List, Optional, Union

from aidars.distributed.models import (
    PlacementDecision,
    WorkerInfo,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadState


class CoordinatorStateStore:
    """SQLite-backed persistence for WorkerInfo and WorkloadRecord.

    Round-trip fidelity is the only requirement: what goes in via
    save_worker()/save_workload() must come back unchanged (modulo
    floating-point timestamp precision) via load_workers()/load_workloads().
    """

    def __init__(self, db_path: Union[str, Path]) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            timeout=10.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

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
                    execution_result_json TEXT,
                    error_message TEXT,
                    submitted_at REAL NOT NULL,
                    completed_at REAL,
                    updated_at REAL NOT NULL
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_workloads_state
                ON workloads(state);
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
            rows = self._conn.execute("SELECT info_json FROM workers").fetchall()
            return [WorkerInfo.model_validate_json(row["info_json"]) for row in rows]

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
            execution_result_json = (
                record.execution_result.model_dump_json()
                if record.execution_result is not None
                else None
            )
            self._conn.execute("""
                INSERT INTO workloads (
                    workload_id, spec_json, state, placement_json,
                    execution_result_json, error_message, submitted_at,
                    completed_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workload_id) DO UPDATE SET
                    spec_json = excluded.spec_json,
                    state = excluded.state,
                    placement_json = excluded.placement_json,
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
            return [self._row_to_workload_record(row) for row in rows]

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
        record.execution_result = (
            WorkloadExecutionResult.model_validate_json(row["execution_result_json"])
            if row["execution_result_json"] is not None
            else None
        )
        return record
