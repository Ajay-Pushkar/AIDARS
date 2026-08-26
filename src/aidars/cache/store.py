"""Milestone 5 Concrete DiskCacheStore Facade.

Integrates SplitHashStorage, SQLiteMetadataIndex, HitMissResolver,
LRUEvictor, and IntegrityVerifier into a unified, high-performance facade.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterable, Iterator, Optional

from aidars.cache.base import CacheStore
from aidars.cache.eviction import LRUEvictor
from aidars.cache.index import SQLiteMetadataIndex
from aidars.cache.models import (
    CacheEntry,
    CacheEntryNotFoundError,
    CacheState,
    CacheStorageError,
    ResolutionResult,
    VerificationReport,
    CacheState,
)
from aidars.cache.resolver import HitMissResolver
from aidars.cache.storage import DEFAULT_CHUNK_SIZE, SplitHashStorage
from aidars.cache.verifier import IntegrityVerifier
from aidars.cache.lock import ProcessLock


class DiskCacheStore(CacheStore):
    """High-performance local content-addressed disk cache store."""

    def __init__(
        self,
        cache_root: Path | str,
        max_cache_size_bytes: Optional[int] = None,
        max_size_bytes: Optional[int] = None,
    ) -> None:
        quota = max_size_bytes if max_size_bytes is not None else max_cache_size_bytes
        self.cache_root = Path(cache_root).resolve()
        self.max_cache_size_bytes = quota
        self.max_size_bytes = quota

        self.storage = SplitHashStorage(self.cache_root)
        self.index = SQLiteMetadataIndex(self.cache_root)
        self._index = self.index
        self.resolver = HitMissResolver()
        self.evictor = LRUEvictor(self.storage, self.index, max_cache_size_bytes=quota)
        self.verifier = IntegrityVerifier(self.storage, self.index, lock_path=self.cache_root / "cache.lock")

    def __del__(self) -> None:
        self.close()

    def __contains__(self, sha256: str) -> bool:
        return self.contains(sha256)

    def is_cache_valid(self, sha256: str) -> bool:
        """The authoritative definition of cache validity: exists in index, verified, and physically exists."""
        norm_hash = self.storage.validate_hash(sha256)
        entry = self.index.get(norm_hash)
        if not entry:
            return False
        if entry.state != CacheState.VALID:
            return False
        if not self.storage.exists(norm_hash):
            return False
        return self.storage.get_size(norm_hash) == entry.size_bytes

    def contains(self, sha256: str) -> bool:
        """Check if asset exists in both index and physical storage and is verified."""
        return self.is_cache_valid(sha256)

    def get_path(self, sha256: str) -> Optional[Path]:
        """Return path to cached object and update access timestamp."""
        norm_hash = self.storage.validate_hash(sha256)
        if not self.is_cache_valid(norm_hash):
            return None
        self.index.touch(norm_hash)
        return self.storage.get_absolute_path(norm_hash)

    def get_stream(self, sha256: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Iterator[bytes]:
        """Stream chunks of cached bytes, raising CacheEntryNotFoundError if absent."""
        norm_hash = self.storage.validate_hash(sha256)
        if not self.is_cache_valid(norm_hash):
            raise CacheEntryNotFoundError(f"Asset {sha256} not found or corrupted.")
        self.index.touch(norm_hash)
        return self.storage.read_stream(norm_hash, chunk_size=chunk_size)

    def get_bytes(self, sha256: str) -> Optional[bytes]:
        """Return full byte contents or None if not found."""
        norm_hash = self.storage.validate_hash(sha256)
        if not self.is_cache_valid(norm_hash):
            return None
        self.index.touch(norm_hash)
        return self.storage.read_bytes(norm_hash)

    def put_bytes(
        self,
        data: bytes,
        sha256: Optional[str] = None,
        original_name: str = "",
        asset_type: str = "unknown",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CacheEntry:
        """Store in-memory bytes into cache."""
        import hashlib
        from aidars.cache.models import HashMismatchError
        size_bytes = len(data)
        
        # 1. Calculate actual identity
        actual_hash = hashlib.sha256(data).hexdigest().lower()
        
        # 2. Verify expected identity
        if sha256 is not None:
            norm_expected = self.storage.validate_hash(sha256)
            if actual_hash != norm_expected:
                raise HashMismatchError(
                    f"Computed SHA-256 ({actual_hash}) does not match expected ({norm_expected})"
                )
                
        with ProcessLock(self.cache_root / "cache.lock"):
            # 3. Check duplicate
            if self.is_cache_valid(actual_hash):
                self.index.touch(actual_hash)
                return self.index.get(actual_hash)
                
            self.evictor.enforce_quota(incoming_bytes=size_bytes)
            dest_path, written_size, computed_hash = self.storage.write_bytes(
                data,
                expected_sha256=sha256,
                expected_size=size_bytes,
            )
            rel_path = str(self.storage.get_relative_path(computed_hash)).replace("\\", "/")
            entry = CacheEntry(
                sha256=computed_hash,
                size_bytes=written_size,
                asset_type=asset_type,
                original_name=original_name,
                source_path="",
                relative_path=rel_path,
                metadata=metadata or {},
            )
            self.index.put(entry)
        return entry

    def put_file(
        self,
        file_path: Path | str,
        sha256: Optional[str] = None,
        original_name: str = "",
        asset_type: str = "unknown",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CacheEntry:
        """Ingest an existing local file into cache atomically.
        
        Args:
            sha256: Expected content identity. sha256 identifies the desired content. 
                    It does not guarantee validation of the supplied source file when 
                    the content already exists in the cache.
        """
        src_path = Path(file_path).resolve()
        if not src_path.is_file():
            raise CacheStorageError(f"Source file not found: {src_path}")

        file_size = src_path.stat().st_size
        
        class DuplicateFound(Exception):
            def __init__(self, entry):
                self.entry = entry

        def on_validated(size: int, computed_hash: str) -> None:
            if self.is_cache_valid(computed_hash):
                self.index.touch(computed_hash)
                raise DuplicateFound(self.index.get(computed_hash))
            self.evictor.enforce_quota(incoming_bytes=size)

        with ProcessLock(self.cache_root / "cache.lock"):
            if sha256 and self.is_cache_valid(sha256):
                self.index.touch(sha256)
                return self.index.get(sha256)
                
            try:
                dest_path, written_size, computed_hash = self.storage.write_file(
                    src_path,
                    expected_sha256=sha256,
                    expected_size=file_size,
                    on_validated=on_validated,
                )
            except DuplicateFound as e:
                return e.entry

            rel_path = str(self.storage.get_relative_path(computed_hash)).replace("\\", "/")
            entry = CacheEntry(
                sha256=computed_hash,
                size_bytes=written_size,
                asset_type=asset_type,
                original_name=original_name or src_path.name,
                source_path=str(src_path),
                relative_path=rel_path,
                metadata=metadata or {},
            )
            self.index.put(entry)
        return entry

    def put_stream(
        self,
        stream: BinaryIO,
        size_bytes: Optional[int] = None,
        sha256: Optional[str] = None,
        original_name: str = "",
        asset_type: str = "unknown",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CacheEntry:
        """Store stream with memory-bounded chunking."""
        class DuplicateFound(Exception):
            def __init__(self, entry):
                self.entry = entry

        def on_validated(size: int, computed_hash: str) -> None:
            if self.is_cache_valid(computed_hash):
                self.index.touch(computed_hash)
                raise DuplicateFound(self.index.get(computed_hash))
            self.evictor.enforce_quota(incoming_bytes=size)

        with ProcessLock(self.cache_root / "cache.lock"):
            if sha256 and self.is_cache_valid(sha256):
                self.index.touch(sha256)
                return self.index.get(sha256)
                
            try:
                dest_path, written_size, computed_hash = self.storage.write_stream(
                    stream,
                    expected_sha256=sha256,
                    expected_size=size_bytes,
                    max_bytes=self.evictor.max_cache_size_bytes,
                    on_validated=on_validated,
                )
            except DuplicateFound as e:
                return e.entry
                
            rel_path = str(self.storage.get_relative_path(computed_hash)).replace("\\", "/")
            entry = CacheEntry(
                sha256=computed_hash,
                size_bytes=written_size,
                asset_type=asset_type,
                original_name=original_name,
                source_path="",
                relative_path=rel_path,
                metadata=metadata or {},
            )
            self.index.put(entry)
        return entry

    def verify(self, sha256: str, deep_check: bool = True) -> bool:
        """Verify integrity of a specific cached entry."""
        return self.verifier.verify_entry(sha256, deep_check=deep_check)

    def verify_all(self, auto_evict: bool = True) -> VerificationReport:
        """Run batch integrity verification across all cached entries.
        
        WARNING: This operation performs a deep SHA-256 scan on the entire cache.
        Do not place this on the critical path of a render request. It should only
        be used by periodic background maintenance processes.
        """
        return self.verifier.verify_all(deep_check=True, auto_evict=auto_evict)

    def remove(self, sha256: str) -> bool:
        """Delete asset from disk and metadata index."""
        norm_hash = self.storage.validate_hash(sha256)
        with ProcessLock(self.cache_root / "cache.lock"):
            disk_deleted = self.storage.delete(norm_hash)
            index_deleted = self.index.remove(norm_hash)
            return disk_deleted or index_deleted

    def evict_lru(self, target_bytes_to_free: int) -> int:
        """Evict LRU entries to free target bytes."""
        with ProcessLock(self.cache_root / "cache.lock"):
            return self.evictor.evict_to_free(target_bytes_to_free)

    def resolve_hashes(
        self,
        required_hashes: Iterable[str],
        hash_sizes: Optional[Dict[str, int]] = None,
    ) -> ResolutionResult:
        """Resolve cache hits/misses for required hashes in O(A log N) time where N is cache size.
        
        Uses O(A) indexed SQLite lookups combined with fast O(1) physical filesystem existence checks.
        """
        req_list = list(required_hashes)
        
        # 1. Fetch metadata in one batch lookup (O(A))
        cached_entries = self.index.get_entries_by_hashes(req_list)
        
        # 2. Filter down to required hashes that physically exist and match size
        cached_hashes = set()
        actual_sizes = {
            h.lower(): size
            for h, size in (hash_sizes or {}).items()
        }
        
        for h in req_list:
            norm_hash = h.lower()
            entry = cached_entries.get(norm_hash)
            
            if not entry:
                continue
            if entry.state != CacheState.VALID:
                continue
            if not self.storage.exists(norm_hash):
                continue
            if self.storage.get_size(norm_hash) != entry.size_bytes:
                continue
                
            cached_hashes.add(norm_hash)
            actual_sizes[norm_hash] = entry.size_bytes
            
        return self.resolver.resolve_hashes(
            required_hashes=[h.lower() for h in req_list],
            cached_hashes=cached_hashes,
            hash_sizes=actual_sizes,
        )

    def reconcile(self, verify_hashes: bool = False) -> Dict[str, int]:
        """Index <-> filesystem reconciliation.
        
        Finds dangling index records (missing files) and orphan files (no index).
        If verify_hashes is True, it also verifies sizes and hashes.
        
        WARNING: This loads all database hashes and physical disk hashes into memory.
        This is an expensive O(N) maintenance operation designed for startup or 
        periodic manual repair. Do not run this on the critical path of a render.
        """
        with ProcessLock(self.cache_root / "cache.lock"):
            report = {
                "dangling_removed": 0,
                "orphans_removed": 0,
                "corrupted_removed": 0,
                "verified": 0,
            }
            
            indexed_entries = self.index.get_all_entries()
            indexed_hashes = {e.sha256 for e in indexed_entries}
            physical_hashes = self.storage.get_all_hashes()
            
            # 1. Dangling index records (index entry, no physical file)
            dangling = indexed_hashes - physical_hashes
            for h in dangling:
                self.index.remove(h)
                report["dangling_removed"] += 1
                
            # 2. Orphan physical files (physical file, no index entry)
            orphans = physical_hashes - indexed_hashes
            for h in orphans:
                self.storage.delete(h)
                report["orphans_removed"] += 1
                
            # 3. Size and hash verification (optional full scan)
            if verify_hashes:
                for entry in indexed_entries:
                    h = entry.sha256
                    if h in dangling:
                        continue
                    
                    # Check size first
                    actual_size = self.storage.get_size(h)
                    if actual_size != entry.size_bytes:
                        self.index.remove(h)
                        self.storage.delete(h)
                        report["corrupted_removed"] += 1
                        continue
                        
                    # Check full hash
                    try:
                        is_valid = self.verifier._verify_entry_locked(h, deep_check=True)
                        if not is_valid:
                            self.index.remove(h)
                            self.storage.delete(h)
                            report["corrupted_removed"] += 1
                        else:
                            report["verified"] += 1
                    except Exception:
                        self.index.remove(h)
                        self.storage.delete(h)
                        report["corrupted_removed"] += 1
            
            return report


    def resolve_plan(self, plan: Any) -> ResolutionResult:
        """Resolve required assets from an M4 PackagePlan or duck-typed plan."""
        from aidars.cache.resolver import HitMissResolver
        hash_sizes, req_hashes = HitMissResolver._extract_plan_assets(plan)
        return self.resolve_hashes(req_hashes, hash_sizes=hash_sizes)

    def get_stats(self) -> Dict[str, Any]:
        """Return comprehensive cache statistics."""
        total_size = self.index.get_total_size()
        total_count = self.index.get_count()
        max_quota = self.max_cache_size_bytes
        utilization = round((total_size / max_quota) * 100, 2) if (max_quota and max_quota > 0) else 0.0

        return {
            "cache_root": str(self.cache_root),
            "entry_count": total_count,
            "total_entries": total_count,
            "total_bytes": total_size,
            "total_size_bytes": total_size,
            "max_cache_size_bytes": max_quota,
            "max_size_bytes": max_quota,
            "utilization_percent": utilization,
        }

    def close(self) -> None:
        """Close open database connections."""
        if hasattr(self, "index") and self.index is not None:
            try:
                self.index.close()
            except Exception:
                pass


    def __enter__(self) -> DiskCacheStore:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

