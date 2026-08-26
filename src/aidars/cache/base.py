"""Milestone 5 CacheStore Abstract Base Class.

Defines the formal public interface contract for all AIDAR cache implementations.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterable, Iterator, Optional

from aidars.cache.models import CacheEntry, ResolutionResult, VerificationReport


class CacheStore(ABC):
    """Abstract interface defining all public cache operations."""

    @abstractmethod
    def contains(self, sha256: str) -> bool:
        """Check if an asset with the given SHA-256 hash exists in cache."""
        pass

    @abstractmethod
    def get_path(self, sha256: str) -> Optional[Path]:
        """Return absolute path to cached object if present, else None."""
        pass

    @abstractmethod
    def get_stream(self, sha256: str, chunk_size: int = 65536) -> Iterator[bytes]:
        """Stream chunks of bytes for the given cached asset."""
        pass

    @abstractmethod
    def get_bytes(self, sha256: str) -> Optional[bytes]:
        """Read and return full byte contents of cached asset, or None if not found."""
        pass

    @abstractmethod
    def put_bytes(
        self,
        data: bytes,
        sha256: Optional[str] = None,
        original_name: str = "",
        asset_type: str = "unknown",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CacheEntry:
        """Store in-memory bytes into cache, indexed by SHA-256."""
        pass

    @abstractmethod
    def put_file(
        self,
        file_path: Path | str,
        sha256: Optional[str] = None,
        original_name: str = "",
        asset_type: str = "unknown",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CacheEntry:
        """Ingest an existing local file into cache atomically."""
        pass

    @abstractmethod
    def put_stream(
        self,
        stream: BinaryIO,
        size_bytes: Optional[int] = None,
        sha256: Optional[str] = None,
        original_name: str = "",
        asset_type: str = "unknown",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CacheEntry:
        """Store a binary stream into cache with memory-bounded chunking."""
        pass

    @abstractmethod
    def verify(self, sha256: str, deep_check: bool = True) -> bool:
        """Verify integrity of a specific cached entry."""
        pass

    @abstractmethod
    def verify_all(self, auto_evict: bool = True) -> VerificationReport:
        """Run batch integrity verification across all cached entries."""
        pass

    @abstractmethod
    def remove(self, sha256: str) -> bool:
        """Delete an asset from disk storage and metadata index."""
        pass

    @abstractmethod
    def evict_lru(self, target_bytes_to_free: int) -> int:
        """Evict oldest entries according to LRU policy to free target bytes."""
        pass

    @abstractmethod
    def resolve_hashes(
        self,
        required_hashes: Iterable[str],
        hash_sizes: Optional[Dict[str, int]] = None,
    ) -> ResolutionResult:
        """Compute set-difference hit/miss resolution for a collection of hashes."""
        pass

    @abstractmethod
    def resolve_plan(self, plan: Any) -> ResolutionResult:
        """Resolve required assets from an M4 PackagePlan or duck-typed plan."""
        pass

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Return cache health, capacity, and usage statistics."""
        pass

    def close(self) -> None:
        """Close any open resources, database connections, or file handles."""
        pass

    def __enter__(self) -> CacheStore:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


