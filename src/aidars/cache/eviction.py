"""Milestone 5 LRU Eviction Engine.

Enforces cache quotas by sorting entries by last_accessed_at ASC,
unlinking physical files, and removing index records with Windows file locking resilience.
"""
from __future__ import annotations

import logging
from typing import Optional

from aidars.cache.index import SQLiteMetadataIndex
from aidars.cache.models import CacheQuotaExceededError
from aidars.cache.storage import SplitHashStorage

logger = logging.getLogger(__name__)


class LRUEvictor:
    """LRU cache quota eviction manager."""

    def __init__(
        self,
        storage: SplitHashStorage,
        index: SQLiteMetadataIndex,
        max_cache_size_bytes: Optional[int] = None,
    ) -> None:
        self.storage = storage
        self.index = index
        self.max_cache_size_bytes = max_cache_size_bytes

    def evict_to_free(self, target_bytes_to_free: int) -> int:
        """Evict oldest entries according to LRU policy until target bytes are freed.

        Handles Windows file locking (PermissionError) gracefully by skipping
        currently locked files and continuing to the next candidate.

        Returns total bytes freed.
        """
        if target_bytes_to_free <= 0:
            return 0

        freed_bytes = 0
        batch_size = 100
        
        while freed_bytes < target_bytes_to_free:
            candidates = self.index.get_lru_candidates(limit=batch_size)
            if not candidates:
                break
                
            progress_made = False
            for entry in candidates:
                if freed_bytes >= target_bytes_to_free:
                    break
    
                sha256 = entry.sha256
                entry_size = entry.size_bytes
                original_state = getattr(entry, "state", "valid")
                
                # FSM Transition: VALID -> EVICTING
                try:
                    self.index.update_status(sha256, "evicting")
                except ValueError:
                    # Invalid transition (e.g. already transferring or verifying) - skip
                    continue
    
                # Attempt to delete file from disk first
                try:
                    self.storage.delete(sha256)
                except Exception as e:
                    # If deletion failed due to Windows file lock (PermissionError) or other OS lock,
                    # restore previous state and push it down the LRU queue
                    self.index.update_status(sha256, original_state)
                    logger.warning("Failed to delete candidate %s during eviction: %s", sha256, e)
                    self.index.touch(sha256)
                    continue
    
                # File successfully removed or not on disk; remove from SQLite index
                self.index.remove(sha256)
                freed_bytes += entry_size
                progress_made = True
                
            # If we couldn't evict anything in a batch (e.g. all locked), prevent infinite loop
            if not progress_made:
                break
                
        return freed_bytes

    def enforce_quota(self, incoming_bytes: int = 0) -> int:
        """Enforce maximum cache size quota before ingesting incoming bytes.

        If incoming_bytes alone exceeds max_cache_size_bytes, raises CacheQuotaExceededError.
        Returns total bytes freed during eviction.
        """
        if self.max_cache_size_bytes is None or self.max_cache_size_bytes <= 0:
            return 0

        if incoming_bytes > self.max_cache_size_bytes:
            raise CacheQuotaExceededError(
                f"Incoming object size ({incoming_bytes} bytes) exceeds maximum cache quota "
                f"({self.max_cache_size_bytes} bytes)."
            )

        current_size = self.index.get_total_size()
        required_capacity = current_size + incoming_bytes

        if required_capacity <= self.max_cache_size_bytes:
            return 0

        target_to_free = required_capacity - self.max_cache_size_bytes
        freed = self.evict_to_free(target_to_free)

        # Check if quota requirement is now satisfied
        new_size = self.index.get_total_size()
        if new_size + incoming_bytes > self.max_cache_size_bytes:
            raise CacheQuotaExceededError(
                f"Unable to free sufficient space for {incoming_bytes} bytes. "
                f"Target free: {target_to_free} bytes, Freed: {freed} bytes, Current size: {new_size} bytes, "
                f"Max quota: {self.max_cache_size_bytes} bytes."
            )

        return freed
