"""Milestone 5 SQLite-backed Metadata Index.

Maintains metadata records, last_accessed_at timestamps, and LRU queries
for cached content-addressed assets using SQLite in WAL mode.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from aidars.cache.models import CacheEntry


class SQLiteMetadataIndex:
    """Authoritative SQLite-backed metadata index for cached objects."""

    def __init__(self, cache_root: Path | str, db_name: str = "index.db") -> None:
        p = Path(cache_root).resolve()
        if p.suffix == ".db" or p.name.endswith(".db"):
            self.db_path = p
            self.metadata_dir = self.db_path.parent
            self.cache_root = self.metadata_dir.parent
        else:
            self.cache_root = p
            self.metadata_dir = self.cache_root / "metadata"
            self.db_path = self.metadata_dir / db_name

        self.metadata_dir.mkdir(parents=True, exist_ok=True)

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

    def _init_schema(self) -> None:
        """Initialize schema and configure WAL mode pragmas."""
        with self._lock:
            if self._conn is None:
                return
            cursor = self._conn.cursor()
            cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute("PRAGMA busy_timeout = 10000;")
            cursor.execute("PRAGMA synchronous = NORMAL;")
            cursor.execute("PRAGMA foreign_keys = ON;")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS entries (
                    sha256 TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    asset_type TEXT NOT NULL DEFAULT 'unknown',
                    original_name TEXT NOT NULL DEFAULT '',
                    source_path TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    last_accessed_at REAL NOT NULL,
                    access_count INTEGER NOT NULL DEFAULT 1,
                    state TEXT NOT NULL DEFAULT 'valid',
                    relative_path TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_entries_last_accessed_at
                ON entries(last_accessed_at);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_entries_size_bytes
                ON entries(size_bytes);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_entries_state
                ON entries(state);
            """)

    def _row_to_entry(self, row: sqlite3.Row) -> CacheEntry:
        """Convert SQLite row to CacheEntry dataclass."""
        try:
            meta = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
        except (ValueError, TypeError):
            meta = {}

        return CacheEntry(
            sha256=row["sha256"],
            size_bytes=row["size_bytes"],
            asset_type=row["asset_type"],
            original_name=row["original_name"],
            source_path=row["source_path"],
            created_at=row["created_at"],
            last_accessed_at=row["last_accessed_at"],
            access_count=row["access_count"],
            state=row["state"],
            relative_path=row["relative_path"],
            metadata=meta,
        )

    def put(self, entry: CacheEntry) -> None:
        """Insert or update an authoritative cache entry."""
        meta_json = json.dumps(entry.metadata, ensure_ascii=False)
        with self._lock:
            if self._conn is None:
                return
            self._conn.execute("""
                INSERT INTO entries (
                    sha256, size_bytes, asset_type, original_name, source_path,
                    created_at, last_accessed_at, access_count, state,
                    relative_path, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sha256) DO UPDATE SET
                    size_bytes = excluded.size_bytes,
                    asset_type = excluded.asset_type,
                    original_name = excluded.original_name,
                    source_path = excluded.source_path,
                    last_accessed_at = excluded.last_accessed_at,
                    access_count = entries.access_count + 1,
                    state = excluded.state,
                    relative_path = excluded.relative_path,
                    metadata_json = excluded.metadata_json;
            """, (
                entry.sha256.lower(),
                entry.size_bytes,
                entry.asset_type,
                entry.original_name,
                entry.source_path,
                entry.created_at,
                entry.last_accessed_at,
                entry.access_count,
                entry.state,
                entry.relative_path,
                meta_json,
            ))
            self._conn.commit()

    def get(self, sha256: str) -> Optional[CacheEntry]:
        """Retrieve cache entry by SHA-256 hash, or None if not found."""
        with self._lock:
            if self._conn is None:
                return None
            cursor = self._conn.execute(
                "SELECT * FROM entries WHERE sha256 = ? LIMIT 1",
                (sha256.lower(),),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self._row_to_entry(row)

    def contains(self, sha256: str) -> bool:
        """Check if an entry with the given SHA-256 exists in index."""
        with self._lock:
            if self._conn is None:
                return False
            cursor = self._conn.execute(
                "SELECT 1 FROM entries WHERE sha256 = ? LIMIT 1",
                (sha256.lower(),),
            )
            return cursor.fetchone() is not None

    def touch(self, sha256: str, accessed_at: Optional[float] = None) -> bool:
        """Update last_accessed_at timestamp and increment access_count."""
        ts = accessed_at if accessed_at is not None else time.time()
        with self._lock:
            if self._conn is None:
                return False
            cursor = self._conn.execute("""
                UPDATE entries
                SET last_accessed_at = ?, access_count = access_count + 1
                WHERE sha256 = ?
            """, (ts, sha256.lower()))
            self._conn.commit()
            return cursor.rowcount > 0

    def touch_batch(self, hashes: Iterable[str], accessed_at: Optional[float] = None) -> int:
        """Batch touch multiple hashes for access tracking."""
        ts = accessed_at if accessed_at is not None else time.time()
        hash_list = [(ts, h.lower()) for h in hashes]
        if not hash_list:
            return 0
        with self._lock:
            if self._conn is None:
                return 0
            cursor = self._conn.executemany("""
                UPDATE entries
                SET last_accessed_at = ?, access_count = access_count + 1
                WHERE sha256 = ?
            """, hash_list)
            self._conn.commit()
            return cursor.rowcount

    def remove(self, sha256: str) -> bool:
        """Delete entry from index by hash. Returns True if deleted."""
        with self._lock:
            if self._conn is None:
                return False
            cursor = self._conn.execute(
                "DELETE FROM entries WHERE sha256 = ?",
                (sha256.lower(),),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def update_status(self, sha256: str, status: str) -> bool:
        """Update the state of an entry with strict FSM transitions."""
        with self._lock:
            if self._conn is None:
                return False
                
            cursor = self._conn.execute("SELECT state FROM entries WHERE sha256 = ?", (sha256.lower(),))
            row = cursor.fetchone()
            if not row:
                return False
                
            current_state = row[0]
            
            # Enforce valid transitions based on CacheState
            valid_transitions = {
                "absent": {"transferring", "verifying"},
                "transferring": {"verifying", "absent"},
                "verifying": {"valid", "corrupted", "absent"},
                "valid": {"evicting", "corrupted", "absent", "verifying"},
                "corrupted": {"absent", "verifying"},
                "evicting": {"absent", "valid"}
            }
            
            allowed = valid_transitions.get(current_state, set())
            if status not in allowed and current_state != status:
                raise ValueError(f"Invalid state transition from {current_state} to {status}")

            cursor = self._conn.execute(
                "UPDATE entries SET state = ? WHERE sha256 = ?",
                (status, sha256.lower()),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def mark_corrupted(self, sha256: str) -> bool:
        """Mark an entry status as corrupted."""
        return self.update_status(sha256, "corrupted")

    def get_lru_candidates(self, limit: Optional[int] = None) -> List[CacheEntry]:
        """Return entries ordered by last_accessed_at ascending (oldest first)."""
        with self._lock:
            if self._conn is None:
                return []
            query = "SELECT * FROM entries ORDER BY last_accessed_at ASC, sha256 ASC"
            params: tuple = ()
            if limit is not None and limit > 0:
                query += " LIMIT ?"
                params = (limit,)
            cursor = self._conn.execute(query, params)
            return [self._row_to_entry(row) for row in cursor.fetchall()]

    def get_lru_entries(self, limit: Optional[int] = None) -> List[CacheEntry]:
        """Alias for get_lru_candidates."""
        return self.get_lru_candidates(limit=limit)

    def get_total_size(self) -> int:
        """Return sum of size_bytes for all indexed entries."""
        with self._lock:
            if self._conn is None:
                return 0
            cursor = self._conn.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM entries")
            row = cursor.fetchone()
            return int(row[0]) if row and row[0] is not None else 0

    def get_count(self) -> int:
        """Return total count of indexed entries."""
        with self._lock:
            if self._conn is None:
                return 0
            cursor = self._conn.execute("SELECT COUNT(*) FROM entries")
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    def get_all_hashes(self) -> Set[str]:
        """Return set of all SHA-256 hashes in index."""
        with self._lock:
            if self._conn is None:
                return set()
            cursor = self._conn.execute("SELECT sha256 FROM entries")
            return {row[0] for row in cursor.fetchall()}

    def get_verified_hashes(self, required_hashes: Optional[Iterable[str]] = None) -> Set[str]:
        """Return set of SHA-256 hashes in index with 'valid' status. If required_hashes is provided, filters to only those."""
        with self._lock:
            if self._conn is None:
                return set()
            
            if required_hashes is not None:
                req_list = list(required_hashes)
                if not req_list:
                    return set()
                # Batch in 999 to respect SQLite variable limits
                results = set()
                for i in range(0, len(req_list), 900):
                    batch = req_list[i:i+900]
                    placeholders = ",".join("?" for _ in batch)
                    cursor = self._conn.execute(
                        f"SELECT sha256 FROM entries WHERE state = 'valid' AND sha256 IN ({placeholders})",
                        batch
                    )
                    results.update(row[0] for row in cursor.fetchall())
                return results
            else:
                cursor = self._conn.execute("SELECT sha256 FROM entries WHERE state = 'valid'")
                return {row[0] for row in cursor.fetchall()}

    def get_all_entries(self) -> List[CacheEntry]:
        """Return all entries in index."""
        with self._lock:
            if self._conn is None:
                return []
            cursor = self._conn.execute("SELECT * FROM entries ORDER BY created_at DESC")
            return [self._row_to_entry(row) for row in cursor.fetchall()]

    def get_entries_by_hashes(self, hashes: Iterable[str]) -> Dict[str, CacheEntry]:
        """Retrieve entries for a collection of hashes in batch."""
        unique_hashes = list({h.lower() for h in hashes})
        if not unique_hashes:
            return {}

        result: Dict[str, CacheEntry] = {}
        chunk_size = 500
        with self._lock:
            if self._conn is None:
                return {}
            for i in range(0, len(unique_hashes), chunk_size):
                chunk = unique_hashes[i:i + chunk_size]
                placeholders = ", ".join("?" for _ in chunk)
                cursor = self._conn.execute(
                    f"SELECT * FROM entries WHERE sha256 IN ({placeholders})",
                    chunk,
                )
                for row in cursor.fetchall():
                    entry = self._row_to_entry(row)
                    result[entry.sha256] = entry
        return result

    def close(self) -> None:
        """Close SQLite database connection cleanly."""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

    def __enter__(self) -> SQLiteMetadataIndex:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

