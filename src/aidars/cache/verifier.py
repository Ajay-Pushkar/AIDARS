"""Milestone 5 Integrity & Verification Subsystem.

Provides fast metadata validation, deep chunked SHA-256 digest validation,
and automated self-healing/eviction of corrupted cache records.
"""
from __future__ import annotations

import logging
from typing import List, Optional
from pathlib import Path
from contextlib import nullcontext
from aidars.cache.index import SQLiteMetadataIndex
from aidars.cache.models import VerificationReport
from aidars.cache.storage import SplitHashStorage
from aidars.cache.lock import ProcessLock

logger = logging.getLogger(__name__)


class IntegrityVerifier:
    """Integrity verifier and self-healing scrubber for cached objects."""

    def __init__(
        self,
        storage: SplitHashStorage,
        index: SQLiteMetadataIndex,
        lock_path: Optional[Path | str] = None,
    ) -> None:
        self.storage = storage
        self.index = index
        self.lock_path = Path(lock_path) if lock_path else None

    def verify_entry(
        self,
        sha256: str,
        deep_check: bool = True,
        auto_evict: bool = False,
    ) -> bool:
        """Verify integrity of a single cache entry.

        Performs fast file presence & size checks, and optional deep SHA-256 stream check.
        If corrupted or absent and auto_evict=True, deletes bad files and removes DB record.
        """
        lock_ctx = ProcessLock(self.lock_path) if self.lock_path else nullcontext()
        with lock_ctx:
            return self._verify_entry_locked(sha256, deep_check=deep_check, auto_evict=auto_evict)

    def _verify_entry_locked(
        self,
        sha256: str,
        deep_check: bool = True,
        auto_evict: bool = False,
    ) -> bool:
        norm_hash = sha256.lower()
        entry = self.index.get(norm_hash)
        if entry is None:
            return False
            
        self.index.update_status(norm_hash, "verifying")

        # 1. Existence check on disk
        if not self.storage.exists(norm_hash):
            if auto_evict:
                self.index.remove(norm_hash)
            else:
                self.index.update_status(norm_hash, "absent")
            return False

        # 2. Fast size check
        disk_path = self.storage.get_absolute_path(norm_hash)
        try:
            disk_size = disk_path.stat().st_size
        except OSError:
            if auto_evict:
                self.index.remove(norm_hash)
            else:
                self.index.update_status(norm_hash, "absent")
            return False

        if disk_size != entry.size_bytes:
            if auto_evict:
                try:
                    self.storage.delete(norm_hash)
                except Exception:
                    pass
                self.index.remove(norm_hash)
            else:
                self.index.update_status(norm_hash, "corrupted")
            return False

        # 3. Deep SHA-256 stream check
        if deep_check:
            try:
                computed_hash, stream_bytes = self.storage.compute_file_sha256(disk_path)
                if computed_hash != norm_hash or stream_bytes != entry.size_bytes:
                    if auto_evict:
                        try:
                            self.storage.delete(norm_hash)
                        except Exception:
                            pass
                        self.index.remove(norm_hash)
                    else:
                        self.index.update_status(norm_hash, "corrupted")
                    return False
            except Exception:
                if auto_evict:
                    try:
                        self.storage.delete(norm_hash)
                    except Exception:
                        pass
                    self.index.remove(norm_hash)
                else:
                    self.index.update_status(norm_hash, "corrupted")
                return False

        # All checks passed
        self.index.update_status(norm_hash, "valid")
        return True

    def verify_all(
        self,
        deep_check: bool = True,
        auto_evict: bool = True,
    ) -> VerificationReport:
        """Batch integrity scrubber across all entries in the cache index.

        Automatically cleanses and self-heals corrupted records when auto_evict is True.
        
        WARNING: Performance cost is O(N) where N is total cache size. This loads all
        entries and deep-scans all files if deep_check=True. Intended for background
        maintenance scrubbers, not high-frequency operations.
        """
        lock_ctx = ProcessLock(self.lock_path) if self.lock_path else nullcontext()
        with lock_ctx:
            entries = self.index.get_all_entries()
            verified_count = 0
            corrupted_count = 0
            missing_count = 0
            corrupted_hashes: List[str] = []
            missing_hashes: List[str] = []

            for entry in entries:
                sha256 = entry.sha256

                # Disk existence check
                if not self.storage.exists(sha256):
                    missing_count += 1
                    missing_hashes.append(sha256)
                    if auto_evict:
                        self.index.remove(sha256)
                    else:
                        self.index.update_status(sha256, "absent")
                    continue

                disk_path = self.storage.get_absolute_path(sha256)
                is_valid = True

                try:
                    disk_size = disk_path.stat().st_size
                    if disk_size != entry.size_bytes:
                        is_valid = False
                    elif deep_check:
                        computed_hash, stream_bytes = self.storage.compute_file_sha256(disk_path)
                        if computed_hash != sha256 or stream_bytes != entry.size_bytes:
                            is_valid = False
                except Exception:
                    is_valid = False

                if not is_valid:
                    corrupted_count += 1
                    corrupted_hashes.append(sha256)
                    if auto_evict:
                        try:
                            self.storage.delete(sha256)
                        except Exception:
                            pass
                        self.index.remove(sha256)
                    else:
                        self.index.update_status(sha256, "corrupted")
                else:
                    verified_count += 1
                    self.index.update_status(sha256, "valid")

            is_healthy = (corrupted_count == 0 and missing_count == 0)
            return VerificationReport(
                verified_count=verified_count,
                corrupted_count=corrupted_count,
                missing_count=missing_count,
                corrupted_hashes=corrupted_hashes,
                missing_hashes=missing_hashes,
                is_healthy=is_healthy,
            )
