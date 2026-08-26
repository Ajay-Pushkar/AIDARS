"""Milestone 5 Data Models and Exceptions for Content-Addressed Asset Cache.

Defines the core data structures and domain exceptions used across the cache subsystem.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

# Standard 64-character lowercase hex string for SHA-256
SHA256_HEX_PATTERN = re.compile(r"^[0-9a-f]{64}$")

class CacheState(str, Enum):
    """Rigorous state machine for cache entries to prevent races during distributed transfer."""
    ABSENT = "absent"
    TRANSFERRING = "transferring"
    VERIFYING = "verifying"
    VALID = "valid"
    CORRUPTED = "corrupted"
    EVICTING = "evicting"


class CacheError(Exception):
    """Base exception for all cache subsystem errors."""
    pass


class InvalidHashError(CacheError):
    """Raised when an invalid hash format or non-SHA256 digest is provided."""
    pass


class HashMismatchError(CacheError):
    """Raised when content does not match expected SHA-256 hash."""
    pass


class CacheQuotaExceededError(CacheError):
    """Raised when cache quota is exceeded and cannot be satisfied by eviction."""
    pass


class CacheStorageError(CacheError):
    """Raised on filesystem, I/O, or storage integrity errors."""
    pass


class CacheEntryNotFoundError(CacheError):
    """Raised when a requested cache entry does not exist in index or storage."""
    pass


@dataclass(slots=True)
class CacheEntry:
    """Authoritative metadata record for a cached content-addressed asset."""

    sha256: str
    size_bytes: int
    asset_type: str = "unknown"
    original_name: str = ""
    source_path: str = ""
    created_at: float = field(default_factory=time.time)
    last_accessed_at: float = field(default_factory=time.time)
    access_count: int = 1
    state: CacheState = CacheState.VALID
    relative_path: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize entry to a dictionary."""
        return {
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "asset_type": self.asset_type,
            "original_name": self.original_name,
            "source_path": self.source_path,
            "created_at": self.created_at,
            "last_accessed_at": self.last_accessed_at,
            "access_count": self.access_count,
            "state": self.state.value if isinstance(self.state, CacheState) else self.state,
            "relative_path": self.relative_path,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CacheEntry:
        """Instantiate a CacheEntry from a dictionary."""
        return cls(
            sha256=str(data["sha256"]),
            size_bytes=int(data["size_bytes"]),
            asset_type=str(data.get("asset_type", "unknown")),
            original_name=str(data.get("original_name", "")),
            source_path=str(data.get("source_path", "")),
            created_at=float(data.get("created_at", time.time())),
            last_accessed_at=float(data.get("last_accessed_at", time.time())),
            access_count=int(data.get("access_count", 1)),
            state=CacheState(str(data.get("state", data.get("verification_status", "valid")))),
            relative_path=str(data.get("relative_path", "")),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(slots=True)
class ResolutionResult:
    """Result of resolving requested asset hashes against the local cache."""

    hits: Set[str]
    misses: Set[str]
    total_requested_bytes: int
    hit_bytes: int
    miss_bytes: int
    byte_hit_ratio: float
    network_saved_bytes: int

    def to_dict(self) -> Dict[str, Any]:
        """Serialize resolution result to a dictionary."""
        return {
            "hits": sorted(self.hits),
            "misses": sorted(self.misses),
            "total_requested_bytes": self.total_requested_bytes,
            "hit_bytes": self.hit_bytes,
            "miss_bytes": self.miss_bytes,
            "byte_hit_ratio": round(self.byte_hit_ratio, 4),
            "network_saved_bytes": self.network_saved_bytes,
        }


@dataclass(slots=True)
class VerificationReport:
    """Summary report of integrity verification across cache entries."""

    verified_count: int
    corrupted_count: int
    missing_count: int
    corrupted_hashes: List[str] = field(default_factory=list)
    missing_hashes: List[str] = field(default_factory=list)
    is_healthy: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Serialize report to a dictionary."""
        return {
            "verified_count": self.verified_count,
            "corrupted_count": self.corrupted_count,
            "missing_count": self.missing_count,
            "corrupted_hashes": list(self.corrupted_hashes),
            "missing_hashes": list(self.missing_hashes),
            "is_healthy": self.is_healthy,
        }
