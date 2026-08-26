"""AIDAR Content-Addressed Local Cache Subsystem (Milestone 5).

Provides high-performance, cryptographic SHA-256 content-addressed asset caching,
SQLite-backed indexing, O(A) hit/miss set difference, and LRU quota eviction.
"""
from __future__ import annotations

from aidars.cache.base import CacheStore
from aidars.cache.eviction import LRUEvictor
from aidars.cache.index import SQLiteMetadataIndex
from aidars.cache.models import (
    CacheEntry,
    CacheEntryNotFoundError,
    CacheError,
    CacheQuotaExceededError,
    CacheStorageError,
    HashMismatchError,
    InvalidHashError,
    ResolutionResult,
    VerificationReport,
)
from aidars.cache.resolver import HitMissResolver
from aidars.cache.storage import DEFAULT_CHUNK_SIZE, SplitHashStorage
from aidars.cache.store import DiskCacheStore
from aidars.cache.verifier import IntegrityVerifier

__all__ = [
    "CacheStore",
    "DiskCacheStore",
    "SplitHashStorage",
    "SQLiteMetadataIndex",
    "HitMissResolver",
    "LRUEvictor",
    "IntegrityVerifier",
    "CacheEntry",
    "ResolutionResult",
    "VerificationReport",
    "CacheError",
    "InvalidHashError",
    "HashMismatchError",
    "CacheQuotaExceededError",
    "CacheStorageError",
    "CacheEntryNotFoundError",
    "DEFAULT_CHUNK_SIZE",
]

