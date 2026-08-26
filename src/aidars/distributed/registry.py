"""In-memory thread-safe WorkerRegistry with inverted SHA-256 index, health tracking, and eviction."""
from __future__ import annotations

import copy
import logging
import math
import re
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from aidars.distributed.models import (
    HeartbeatPayload,
    WorkerInfo,
    WorkerStatus,
    validate_sha256_hex,
)

logger = logging.getLogger(__name__)

_WORKER_ID_REGEX = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")


def _validate_worker_id(worker_id: str) -> str:
    if not isinstance(worker_id, str):
        raise ValueError("worker_id must be a string")
    cleaned = worker_id.strip()
    if not _WORKER_ID_REGEX.match(cleaned):
        raise ValueError(f"Invalid worker_id format: '{worker_id}'")
    return cleaned


class WorkerHealthStatus(str, Enum):
    """Health classification for worker penalty tracking."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    SUSPECT = "suspect"
    DEAD = "dead"


@dataclass
class WorkerHealthRecord:
    """Detailed health and penalty metrics for a worker."""

    worker_id: str
    health_status: WorkerHealthStatus = WorkerHealthStatus.HEALTHY
    penalty_score: float = 0.0
    consecutive_failures: int = 0
    total_transfers_served: int = 0
    total_bytes_served: int = 0
    total_corruptions: int = 0
    last_success_utc: float = 0.0
    last_failure_utc: float = 0.0
    last_decay_utc: float = field(default_factory=time.time)


@dataclass
class ClusterStats:
    """Summary statistics of all active workers and indexed assets."""

    total_workers: int
    healthy_workers: int
    degraded_workers: int
    suspect_workers: int
    total_capacity_bytes: int
    total_used_bytes: int
    total_unique_hashes: int
    average_rtt_ms: float


class WorkerRegistry:
    """Thread-safe in-memory control plane registry for AIDAR workers.

    Maintains active node state, dual inverted hash index for O(1) asset lookups
    and O(K) eviction, heartbeat tracking with dead worker purging, and adaptive
    health penalty scoring.
    """

    def __init__(
        self,
        heartbeat_timeout_seconds: float = 15.0,
        degraded_threshold: float = 3.0,
        suspect_threshold: float = 10.0,
        penalty_half_life_seconds: float = 300.0,
    ) -> None:
        self._heartbeat_timeout = float(heartbeat_timeout_seconds)
        self._degraded_threshold = float(degraded_threshold)
        self._suspect_threshold = float(suspect_threshold)
        self._penalty_half_life = float(penalty_half_life_seconds)

        self._lock = threading.RLock()
        self._workers: Dict[str, WorkerInfo] = {}
        self._hash_index: Dict[str, Set[str]] = {}  # sha256 -> Set[worker_id]
        self._worker_hashes: Dict[str, Set[str]] = {}  # worker_id -> Set[sha256]
        self._health_records: Dict[str, WorkerHealthRecord] = {}

    # ========================================================================
    # Worker Registration & Lifecycle
    # ========================================================================

    def register_worker(self, info: WorkerInfo) -> WorkerInfo:
        """Register or update a worker node.

        Thread-safe. Replaces prior inventory if already registered and populates
        the inverted hash index.
        """
        wid = _validate_worker_id(info.worker_id)

        with self._lock:
            now = time.time()
            worker_copy = info.model_copy(deep=True)
            if worker_copy.last_heartbeat_utc <= 0:
                worker_copy.last_heartbeat_utc = now

            # If worker was already registered, prune old index entries first
            if wid in self._worker_hashes:
                self._remove_worker_from_index(wid)

            # Store worker info
            self._workers[wid] = worker_copy
            self._worker_hashes[wid] = set()

            # Populate inverted index with validated hashes
            normalized_hashes: Set[str] = set()
            for raw_h in worker_copy.inventory_hashes:
                try:
                    norm_h = validate_sha256_hex(raw_h)
                    normalized_hashes.add(norm_h)
                    if norm_h not in self._hash_index:
                        self._hash_index[norm_h] = set()
                    self._hash_index[norm_h].add(wid)
                except ValueError as exc:
                    logger.warning("Worker %s provided invalid hash %r: %s", wid, raw_h, exc)

            self._worker_hashes[wid] = normalized_hashes
            worker_copy.inventory_hashes = normalized_hashes

            # Initialize or update health record
            if wid not in self._health_records:
                self._health_records[wid] = WorkerHealthRecord(
                    worker_id=wid,
                    health_status=WorkerHealthStatus.HEALTHY,
                    last_decay_utc=now,
                )
            else:
                record = self._health_records[wid]
                if record.health_status == WorkerHealthStatus.DEAD:
                    record.health_status = WorkerHealthStatus.HEALTHY
                    record.consecutive_failures = 0

            logger.info("Worker registered: %s (%d hashes indexed)", wid, len(normalized_hashes))
            return worker_copy.model_copy(deep=True)

    def unregister_worker(self, worker_id: str) -> Optional[WorkerInfo]:
        """Unregister a worker and prune its assets from the inverted index.

        Runs in O(K) time where K is the number of assets held by this worker.
        """
        with self._lock:
            worker = self._workers.pop(worker_id, None)
            if not worker:
                return None

            self._remove_worker_from_index(worker_id)

            if worker_id in self._health_records:
                self._health_records[worker_id].health_status = WorkerHealthStatus.DEAD

            logger.info("Worker unregistered: %s", worker_id)
            return worker.model_copy(deep=True)

    def _remove_worker_from_index(self, worker_id: str) -> None:
        """Internal helper to prune worker from inverted hash index. Assumes lock held."""
        hashes = self._worker_hashes.pop(worker_id, set())
        for h in hashes:
            worker_set = self._hash_index.get(h)
            if worker_set:
                worker_set.discard(worker_id)
                if not worker_set:
                    del self._hash_index[h]

    def get_worker(self, worker_id: str, copy: bool = True) -> Optional[WorkerInfo]:
        """Retrieve worker info, returning a defensive deep copy by default, or internal reference if copy=False."""
        with self._lock:
            worker = self._workers.get(worker_id)
            if not worker:
                return None
            return worker.model_copy(deep=True) if copy else worker

    def get_workers_map(self) -> Dict[str, WorkerInfo]:
        """Return a snapshot dictionary mapping worker_id -> WorkerInfo internal references under a single lock read."""
        with self._lock:
            return dict(self._workers)

    def get_candidates_for_hash(self, sha256_hex: str) -> List[WorkerInfo]:
        """Return internal WorkerInfo references for candidate ranking in O(1) time without deepcopy."""
        try:
            norm_h = validate_sha256_hex(sha256_hex)
        except ValueError:
            return []
        with self._lock:
            worker_ids = self._hash_index.get(norm_h)
            if not worker_ids:
                return []
            candidates: List[WorkerInfo] = []
            for wid in worker_ids:
                w = self._workers.get(wid)
                if w:
                    candidates.append(w)
            return candidates

    def list_workers(self, active_only: bool = True) -> List[WorkerInfo]:
        """List registered workers.

        If active_only is True, excludes workers marked OFFLINE, SUSPECT, or DEAD.
        """
        with self._lock:
            result: List[WorkerInfo] = []
            for wid, worker in self._workers.items():
                if active_only:
                    if worker.status == WorkerStatus.OFFLINE:
                        continue
                    health = self._health_records.get(wid)
                    if health and health.health_status in (
                        WorkerHealthStatus.SUSPECT,
                        WorkerHealthStatus.DEAD,
                    ):
                        continue
                result.append(worker.model_copy(deep=True))
            return result

    def has_worker(self, worker_id: str) -> bool:
        """Check if a worker is currently registered."""
        with self._lock:
            return worker_id in self._workers

    def get_worker_count(self) -> int:
        """Return total number of registered workers."""
        with self._lock:
            return len(self._workers)

    # ========================================================================
    # Inverted Hash Index Operations
    # ========================================================================

    def get_workers_for_hash(self, sha256_hex: str) -> Set[str]:
        """Return defensive copy of worker IDs possessing the given hash in O(1) time."""
        try:
            norm_h = validate_sha256_hex(sha256_hex)
        except ValueError:
            return set()
        with self._lock:
            return set(self._hash_index.get(norm_h, set()))

    def locate_hashes(self, sha256_hashes: Iterable[str]) -> Dict[str, Set[str]]:
        """Batch query returning map of SHA-256 -> Set of worker IDs."""
        with self._lock:
            result: Dict[str, Set[str]] = {}
            for raw_h in sha256_hashes:
                try:
                    norm_h = validate_sha256_hex(raw_h)
                    if norm_h in self._hash_index:
                        result[norm_h] = set(self._hash_index[norm_h])
                except ValueError:
                    continue
            return result

    def add_worker_hashes(self, worker_id: str, hashes: Iterable[str]) -> int:
        """Add new hashes to a worker's inventory in the registry.

        Returns count of newly indexed hashes.
        """
        with self._lock:
            if worker_id not in self._workers:
                return 0

            added_count = 0
            worker = self._workers[worker_id]
            worker_hashes = self._worker_hashes.setdefault(worker_id, set())

            for raw_h in hashes:
                try:
                    norm_h = validate_sha256_hex(raw_h)
                    if norm_h not in worker_hashes:
                        worker_hashes.add(norm_h)
                        if norm_h not in self._hash_index:
                            self._hash_index[norm_h] = set()
                        self._hash_index[norm_h].add(worker_id)
                        added_count += 1
                except ValueError as exc:
                    logger.warning("Invalid hash skipped for worker %s: %s", worker_id, exc)

            worker.inventory_hashes = set(worker_hashes)
            return added_count

    def remove_worker_hashes(self, worker_id: str, hashes: Iterable[str]) -> int:
        """Remove hashes from a worker's inventory in the registry.

        Returns count of removed hashes.
        """
        with self._lock:
            if worker_id not in self._workers:
                return 0

            removed_count = 0
            worker = self._workers[worker_id]
            worker_hashes = self._worker_hashes.get(worker_id, set())

            for raw_h in hashes:
                try:
                    norm_h = validate_sha256_hex(raw_h)
                    if norm_h in worker_hashes:
                        worker_hashes.discard(norm_h)
                        worker_set = self._hash_index.get(norm_h)
                        if worker_set:
                            worker_set.discard(worker_id)
                            if not worker_set:
                                del self._hash_index[norm_h]
                        removed_count += 1
                except ValueError:
                    continue

            worker.inventory_hashes = set(worker_hashes)
            return removed_count

    def sync_worker_inventory(self, worker_id: str, inventory: Set[str]) -> Tuple[int, int]:
        """Atomically synchronize a worker's inventory with a new target set.

        Returns (added_count, removed_count).
        """
        with self._lock:
            if worker_id not in self._workers:
                return (0, 0)

            normalized_target: Set[str] = set()
            for h in inventory:
                try:
                    normalized_target.add(validate_sha256_hex(h))
                except ValueError:
                    continue

            current_hashes = self._worker_hashes.get(worker_id, set())
            to_add = normalized_target - current_hashes
            to_remove = current_hashes - normalized_target

            added = self.add_worker_hashes(worker_id, to_add)
            removed = self.remove_worker_hashes(worker_id, to_remove)
            return (added, removed)

    def get_all_indexed_hashes(self) -> Set[str]:
        """Return defensive set of all unique hashes indexed across the cluster."""
        with self._lock:
            return set(self._hash_index.keys())

    def get_hash_count(self) -> int:
        """Return count of unique hashes indexed across the cluster."""
        with self._lock:
            return len(self._hash_index)

    # ========================================================================
    # Heartbeat & Eviction
    # ========================================================================

    def record_heartbeat(
        self,
        worker_id: str,
        payload: Optional[HeartbeatPayload] = None,
        current_time: Optional[float] = None,
    ) -> bool:
        """Record a heartbeat timestamp and update metrics/inventory deltas."""
        with self._lock:
            worker = self._workers.get(worker_id)
            if not worker:
                return False

            now = current_time if current_time is not None else time.time()
            worker.last_heartbeat_utc = now
            worker.consecutive_heartbeat_failures = 0

            if payload is not None:
                if payload.metrics is not None:
                    worker.last_metrics = payload.metrics
                if payload.used_bytes is not None:
                    worker.used_bytes = payload.used_bytes
                worker.active_transfers = payload.active_transfers

                # Process inventory deltas if present
                if payload.inventory_delta_added:
                    self.add_worker_hashes(worker_id, payload.inventory_delta_added)
                if payload.inventory_delta_removed:
                    self.remove_worker_hashes(worker_id, payload.inventory_delta_removed)

            health = self._health_records.get(worker_id)
            if health and health.health_status == WorkerHealthStatus.DEAD:
                health.health_status = WorkerHealthStatus.HEALTHY

            return True

    def evict_expired_workers(
        self,
        timeout_seconds: Optional[float] = None,
        current_time: Optional[float] = None,
    ) -> List[str]:
        """Identify and unregister workers whose heartbeat has expired.

        Purges their assets from the inverted hash index.
        Returns list of evicted worker IDs.
        """
        threshold = timeout_seconds if timeout_seconds is not None else self._heartbeat_timeout
        now = current_time if current_time is not None else time.time()
        cutoff = now - threshold

        with self._lock:
            expired_ids = [
                wid
                for wid, w in self._workers.items()
                if w.last_heartbeat_utc < cutoff
            ]

            evicted: List[str] = []
            for wid in expired_ids:
                logger.warning("Evicting expired worker %s (last heartbeat: %.1fs ago)", wid, now - self._workers[wid].last_heartbeat_utc)
                self.unregister_worker(wid)
                evicted.append(wid)

            return evicted

    # ========================================================================
    # Health & Penalty Scoring
    # ========================================================================

    def record_success(self, worker_id: str, bytes_transferred: int = 0) -> None:
        """Record a successful transfer from a worker, decreasing penalty score."""
        with self._lock:
            record = self._health_records.get(worker_id)
            if not record:
                return
            now = time.time()
            record.total_transfers_served += 1
            record.total_bytes_served += max(0, bytes_transferred)
            record.consecutive_failures = 0
            record.last_success_utc = now
            record.penalty_score = max(0.0, record.penalty_score - 0.5)

            # Update WorkerInfo penalty
            worker = self._workers.get(worker_id)
            if worker:
                worker.penalty_score = record.penalty_score

            if record.penalty_score < self._degraded_threshold:
                if record.health_status == WorkerHealthStatus.DEGRADED:
                    record.health_status = WorkerHealthStatus.HEALTHY
                    if worker:
                        worker.status = WorkerStatus.ACTIVE

    def record_failure(self, worker_id: str, reason: str = "", penalty: float = 1.0) -> None:
        """Record a transfer failure or timeout for a worker, applying penalty points."""
        with self._lock:
            record = self._health_records.get(worker_id)
            if not record:
                return
            now = time.time()
            record.consecutive_failures += 1
            record.penalty_score += max(0.0, penalty)
            record.last_failure_utc = now

            worker = self._workers.get(worker_id)
            if worker:
                worker.penalty_score = record.penalty_score

            if record.penalty_score >= self._suspect_threshold:
                record.health_status = WorkerHealthStatus.SUSPECT
                if worker:
                    worker.status = WorkerStatus.UNHEALTHY
            elif record.penalty_score >= self._degraded_threshold:
                record.health_status = WorkerHealthStatus.DEGRADED
                if worker:
                    worker.status = WorkerStatus.DEGRADED

            logger.warning(
                "Worker failure recorded for %s: penalty=%.1f, reason=%r, status=%s",
                worker_id,
                record.penalty_score,
                reason,
                record.health_status.value,
            )

    def record_corruption(self, worker_id: str, sha256_hex: str, penalty: float = 5.0) -> None:
        """Record a SHA-256 corruption event, heavily penalizing node and pruning bad hash."""
        with self._lock:
            record = self._health_records.get(worker_id)
            if not record:
                return
            record.total_corruptions += 1
            self.record_failure(worker_id, reason=f"Corrupt data for {sha256_hex[:8]}", penalty=penalty)

            # Prune corrupted hash from worker's advertised inventory
            try:
                norm_h = validate_sha256_hex(sha256_hex)
                self.remove_worker_hashes(worker_id, [norm_h])
            except ValueError:
                pass

    def decay_penalties(self, current_time: Optional[float] = None) -> None:
        """Apply exponential half-life decay to all worker penalty scores."""
        now = current_time if current_time is not None else time.time()
        with self._lock:
            for wid, record in self._health_records.items():
                dt = now - record.last_decay_utc
                if dt <= 0:
                    continue
                # Exponential decay formula: score * 2^(-dt / half_life)
                decay_factor = math.exp(-(dt * math.log(2.0)) / self._penalty_half_life)
                record.penalty_score *= decay_factor
                record.last_decay_utc = now

                worker = self._workers.get(wid)
                if worker:
                    worker.penalty_score = round(record.penalty_score, 4)

                if record.penalty_score < self._degraded_threshold and record.health_status == WorkerHealthStatus.DEGRADED:
                    record.health_status = WorkerHealthStatus.HEALTHY
                    if worker:
                        worker.status = WorkerStatus.ACTIVE
                elif record.penalty_score < self._suspect_threshold and record.health_status == WorkerHealthStatus.SUSPECT:
                    record.health_status = WorkerHealthStatus.DEGRADED
                    if worker:
                        worker.status = WorkerStatus.DEGRADED

    def get_health_record(self, worker_id: str) -> Optional[WorkerHealthRecord]:
        """Retrieve a copy of the health record for a worker."""
        with self._lock:
            rec = self._health_records.get(worker_id)
            return copy.deepcopy(rec) if rec else None

    def get_worker_health_multiplier(self, worker_id: str) -> float:
        """Calculate score multiplier based on worker health penalty."""
        with self._lock:
            rec = self._health_records.get(worker_id)
            if not rec:
                return 1.0
            if rec.health_status in (WorkerHealthStatus.SUSPECT, WorkerHealthStatus.DEAD):
                return 0.0
            return 1.0 / (1.0 + rec.penalty_score)

    # ========================================================================
    # Cluster Stats & Telemetry
    # ========================================================================

    def get_cluster_stats(self) -> ClusterStats:
        """Compute aggregated cluster health, capacity, and inventory metrics."""
        with self._lock:
            total_w = len(self._workers)
            healthy = 0
            degraded = 0
            suspect = 0
            cap = 0
            used = 0
            rtt_sum = 0.0

            for wid, w in self._workers.items():
                cap += w.capacity_bytes
                used += w.used_bytes
                rtt_sum += w.estimated_rtt_ms
                rec = self._health_records.get(wid)
                if rec:
                    if rec.health_status == WorkerHealthStatus.HEALTHY:
                        healthy += 1
                    elif rec.health_status == WorkerHealthStatus.DEGRADED:
                        degraded += 1
                    elif rec.health_status == WorkerHealthStatus.SUSPECT:
                        suspect += 1
                else:
                    healthy += 1

            avg_rtt = round(rtt_sum / max(total_w, 1), 3)

            return ClusterStats(
                total_workers=total_w,
                healthy_workers=healthy,
                degraded_workers=degraded,
                suspect_workers=suspect,
                total_capacity_bytes=cap,
                total_used_bytes=used,
                total_unique_hashes=len(self._hash_index),
                average_rtt_ms=avg_rtt,
            )

    def clear(self) -> None:
        """Reset registry state completely."""
        with self._lock:
            self._workers.clear()
            self._hash_index.clear()
            self._worker_hashes.clear()
            self._health_records.clear()
