"""Comprehensive 4-Tier Test Suite for Milestone 5 Core: Local Content-Addressed Asset Cache.

Covers:
- Tier 1: Feature Coverage (Storage CAS, SQLite Index, Resolver O(A) set difference, LRU Eviction, Chunked Streaming, Integrity Verification).
- Tier 2: Boundary & Corner cases (0-byte file, huge files, quota=0, exact fit, special chars, empty sets).
- Tier 3: Cross-Feature Combinations (Put->Evict->Resolve, Put->Corrupt->Verify->Evict, concurrent ingest+eviction, dedup+evict, plan resolution).
- Tier 4: Real-World Workload Scenarios (M4 scene distribution cold/warm starts, byte_hit_ratio & network_saved validation, texture stream churn, multi-camera dedup).
"""
from __future__ import annotations

import hashlib
import io
import math
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.cache import (
    CacheEntry,
    CacheError,
    CacheQuotaExceededError,
    CacheStore,
    DiskCacheStore,
    HashMismatchError,
    HitMissResolver,
    IntegrityVerifier,
    InvalidHashError,
    LRUEvictor,
    ResolutionResult,
    SplitHashStorage,
    SQLiteMetadataIndex,
    VerificationReport,
)


def make_temp_dir() -> tempfile.TemporaryDirectory:
    """Create TemporaryDirectory with ignore_cleanup_errors=True on Python 3.10+."""
    if sys.version_info >= (3, 10):
        return tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    return tempfile.TemporaryDirectory()


def compute_sha256(data: bytes) -> str:
    """Authoritative SHA-256 calculation."""
    return hashlib.sha256(data).hexdigest()


class Tier1FeatureStorageCASTests(unittest.TestCase):
    """Tier 1: Feature 1 - Content-Addressed Storage & Split-Hash Mechanics."""

    def test_storage_split_hash_path_structure(self) -> None:
        """Verify objects are stored using the split-hash 2-level directory structure objects/<h[:2]>/<h[2:]>."""
        with make_temp_dir() as tmp_dir:
            storage = SplitHashStorage(tmp_dir)
            data = b"split_hash_structural_verification_data_123"
            h = compute_sha256(data)
            stored_h, size, path = storage.put_bytes(data, h)

            self.assertEqual(stored_h, h)
            self.assertEqual(size, len(data))
            expected_rel_path = Path("objects") / h[:2] / h[2:]
            self.assertTrue(str(path).endswith(str(expected_rel_path)))
            self.assertTrue(path.exists())
            self.assertEqual(path.read_bytes(), data)

    def test_storage_put_bytes_and_get_bytes(self) -> None:
        """Verify storing bytes returns valid entry and retrieving by SHA-256 returns identical bytes."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            payload = b"test_storage_put_bytes_roundtrip_payload"
            h = compute_sha256(payload)

            entry = store.put_bytes(payload, original_name="test.dat", asset_type="raw")
            self.assertEqual(entry.sha256, h)
            self.assertEqual(entry.size_bytes, len(payload))
            self.assertTrue(store.contains(h))

            retrieved = store.get_bytes(h)
            self.assertEqual(retrieved, payload)

    def test_storage_put_file_atomic_staging(self) -> None:
        """Verify put_file ingests via tmp staging directory and leaves no orphan tmp files."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            src_file = Path(tmp_dir) / "source_asset.bin"
            payload = b"atomic_staging_payload_" * 200
            src_file.write_bytes(payload)
            h = compute_sha256(payload)

            entry = store.put_file(src_file, original_name="source_asset.bin", asset_type="binary")
            self.assertEqual(entry.sha256, h)
            self.assertEqual(entry.size_bytes, len(payload))

            tmp_dir_path = Path(tmp_dir) / "tmp"
            if tmp_dir_path.exists():
                tmp_files = list(tmp_dir_path.iterdir())
                self.assertEqual(len(tmp_files), 0, "Staging directory must be empty after atomic ingestion")

            target_path = store.get_path(h)
            self.assertIsNotNone(target_path)
            self.assertTrue(target_path.exists())
            self.assertEqual(target_path.read_bytes(), payload)

    def test_storage_put_stream_chunked(self) -> None:
        """Verify ingesting a binary stream writes data chunk-by-chunk without errors."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"streaming_chunked_input_bytes_" * 500
            h = compute_sha256(data)
            stream = io.BytesIO(data)

            entry = store.put_stream(stream, size_bytes=len(data), sha256=h, original_name="stream.bin")
            self.assertEqual(entry.sha256, h)
            self.assertEqual(entry.size_bytes, len(data))
            self.assertEqual(store.get_bytes(h), data)

    def test_storage_deduplication(self) -> None:
        """Verify identical content ingested multiple times deduplicates to single physical asset."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"deduplication_target_content_data"
            h = compute_sha256(data)

            entry1 = store.put_bytes(data, original_name="file_a.png", asset_type="texture")
            entry2 = store.put_bytes(data, original_name="file_b.png", asset_type="texture")

            self.assertEqual(entry1.sha256, entry2.sha256)
            self.assertEqual(store.get_path(h), store.get_path(h))
            stats = store.get_stats()
            self.assertEqual(stats["entry_count"], 1)
            self.assertEqual(stats["total_bytes"], len(data))

    def test_storage_remove_and_contains(self) -> None:
        """Verify remove deletes physical file and contains returns False."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"content_to_be_removed_soon"
            h = compute_sha256(data)

            store.put_bytes(data)
            self.assertTrue(store.contains(h))
            self.assertIn(h, store)

            removed = store.remove(h)
            self.assertTrue(removed)
            self.assertFalse(store.contains(h))
            self.assertNotIn(h, store)
            self.assertIsNone(store.get_bytes(h))
            self.assertIsNone(store.get_path(h))



class Tier1FeatureSQLiteIndexTests(unittest.TestCase):
    """Tier 1: Feature 2 - SQLite Metadata Index & WAL Mode."""

    def test_index_wal_mode_and_pragmas(self) -> None:
        """Verify the SQLite metadata index enables WAL mode and busy timeout."""
        with make_temp_dir() as tmp_dir:
            index_path = Path(tmp_dir) / "metadata" / "index.db"
            with SQLiteMetadataIndex(index_path) as index:
                conn = sqlite3.connect(str(index_path))
                cur = conn.cursor()
                cur.execute("PRAGMA journal_mode;")
                journal_mode = cur.fetchone()[0]
                self.assertEqual(journal_mode.lower(), "wal")
                cur.close()
                conn.close()

    def test_index_crud_operations(self) -> None:
        """Verify Create, Read, Update, Delete of CacheEntry records."""
        with make_temp_dir() as tmp_dir:
            index_path = Path(tmp_dir) / "metadata" / "index.db"
            with SQLiteMetadataIndex(index_path) as index:
                h = compute_sha256(b"crud_test_entry")
                entry = CacheEntry(
                    sha256=h,
                    size_bytes=1024,
                    asset_type="mesh",
                    original_name="hero.obj",
                    source_path="/models/hero.obj",
                    access_count=1,
                    state="valid",
                    relative_path=f"objects/{h[:2]}/{h[2:]}",
                    metadata={"poly_count": 5000},
                )

                index.put(entry)
                retrieved = index.get(h)
                self.assertIsNotNone(retrieved)
                self.assertEqual(retrieved.sha256, h)
                self.assertEqual(retrieved.size_bytes, 1024)
                self.assertEqual(retrieved.asset_type, "mesh")
                self.assertEqual(retrieved.original_name, "hero.obj")
                self.assertEqual(retrieved.source_path, "/models/hero.obj")
                self.assertEqual(retrieved.metadata.get("poly_count"), 5000)

                # Delete
                self.assertTrue(index.remove(h))
                self.assertIsNone(index.get(h))

    def test_index_touch_updates_access_time_and_count(self) -> None:
        """Verify touch updates last_accessed_at and increments access_count."""
        with make_temp_dir() as tmp_dir:
            index_path = Path(tmp_dir) / "metadata" / "index.db"
            with SQLiteMetadataIndex(index_path) as index:
                h = compute_sha256(b"touch_test_entry")
                t0 = time.time() - 10.0
                entry = CacheEntry(
                    sha256=h,
                    size_bytes=512,
                    created_at=t0,
                    last_accessed_at=t0,
                    access_count=1,
                )
                index.put(entry)

                index.touch(h)
                updated = index.get(h)
                self.assertIsNotNone(updated)
                self.assertGreater(updated.last_accessed_at, t0)
                self.assertEqual(updated.access_count, 2)

    def test_index_lru_ordering_query(self) -> None:
        """Verify querying LRU entries returns items ordered by last_accessed_at ASC."""
        with make_temp_dir() as tmp_dir:
            index_path = Path(tmp_dir) / "metadata" / "index.db"
            with SQLiteMetadataIndex(index_path) as index:
                now = time.time()
                entries = []
                for i in range(5):
                    h = compute_sha256(f"lru_entry_{i}".encode("utf-8"))
                    e = CacheEntry(
                        sha256=h,
                        size_bytes=100,
                        last_accessed_at=now + i * 10,
                    )
                    index.put(e)
                    entries.append(e)

                lru_list = index.get_lru_entries()
                self.assertEqual(len(lru_list), 5)
                self.assertEqual([e.sha256 for e in lru_list], [e.sha256 for e in entries])

    def test_index_total_size_and_count_tracking(self) -> None:
        """Verify accurate aggregate count and total size tracking."""
        with make_temp_dir() as tmp_dir:
            index_path = Path(tmp_dir) / "metadata" / "index.db"
            with SQLiteMetadataIndex(index_path) as index:
                self.assertEqual(index.get_count(), 0)
                self.assertEqual(index.get_total_size(), 0)

                for i in range(3):
                    h = compute_sha256(f"agg_entry_{i}".encode("utf-8"))
                    index.put(CacheEntry(sha256=h, size_bytes=(i + 1) * 100))

                self.assertEqual(index.get_count(), 3)
                self.assertEqual(index.get_total_size(), 100 + 200 + 300)

    def test_index_mark_corrupted(self) -> None:
        """Verify marking an entry as corrupted updates state."""
        with make_temp_dir() as tmp_dir:
            index_path = Path(tmp_dir) / "metadata" / "index.db"
            with SQLiteMetadataIndex(index_path) as index:
                h = compute_sha256(b"corrupt_mark_data")
                index.put(CacheEntry(sha256=h, size_bytes=100, state="valid"))

                index.mark_corrupted(h)
                entry = index.get(h)
                self.assertIsNotNone(entry)
                self.assertEqual(entry.state, "corrupted")


class Tier1FeatureHitMissResolverTests(unittest.TestCase):
    """Tier 1: Feature 3 - O(A) Set-Difference Resolver & Metrics."""

    def test_resolver_pure_hits(self) -> None:
        """When all requested hashes exist in cache, resolver reports 0 misses, 1.0 byte_hit_ratio."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            h1 = compute_sha256(b"asset_1_pure_hit")
            h2 = compute_sha256(b"asset_2_pure_hit")
            store.put_bytes(b"asset_1_pure_hit")
            store.put_bytes(b"asset_2_pure_hit")

            res = store.resolve_hashes([h1, h2])
            self.assertEqual(res.hits, {h1, h2})
            self.assertEqual(res.misses, set())
            self.assertEqual(res.total_requested_bytes, len(b"asset_1_pure_hit") + len(b"asset_2_pure_hit"))
            self.assertEqual(res.hit_bytes, res.total_requested_bytes)
            self.assertEqual(res.miss_bytes, 0)
            self.assertEqual(res.byte_hit_ratio, 1.0)
            self.assertEqual(res.network_saved_bytes, res.total_requested_bytes)

    def test_resolver_pure_misses(self) -> None:
        """When no requested hashes exist in cache, resolver reports 100% misses, 0.0 byte_hit_ratio."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            h1 = compute_sha256(b"missing_1")
            h2 = compute_sha256(b"missing_2")

            hash_sizes = {h1: 500, h2: 300}
            res = store.resolve_hashes([h1, h2], hash_sizes=hash_sizes)
            self.assertEqual(res.hits, set())
            self.assertEqual(res.misses, {h1, h2})
            self.assertEqual(res.total_requested_bytes, 800)
            self.assertEqual(res.hit_bytes, 0)
            self.assertEqual(res.miss_bytes, 800)
            self.assertEqual(res.byte_hit_ratio, 0.0)
            self.assertEqual(res.network_saved_bytes, 0)

    def test_resolver_partial_hit_miss_split(self) -> None:
        """Verify accurate set difference when some assets are cached and others are missing."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            cached_data = b"cached_asset_100_bytes_" * 4
            h_cached = compute_sha256(cached_data)
            store.put_bytes(cached_data)

            h_missing = compute_sha256(b"missing_asset_data")
            hash_sizes = {h_cached: len(cached_data), h_missing: 60}

            res = store.resolve_hashes([h_cached, h_missing], hash_sizes=hash_sizes)
            self.assertEqual(res.hits, {h_cached})
            self.assertEqual(res.misses, {h_missing})
            self.assertEqual(res.hit_bytes, len(cached_data))
            self.assertEqual(res.miss_bytes, 60)
            expected_ratio = len(cached_data) / (len(cached_data) + 60)
            self.assertAlmostEqual(res.byte_hit_ratio, expected_ratio)
            self.assertEqual(res.network_saved_bytes, len(cached_data))

    def test_resolver_byte_metrics_calculation(self) -> None:
        """Verify mathematical integrity of byte_hit_ratio = hit_bytes / total_bytes."""
        cached_hashes = {"hash_a", "hash_b"}
        required = ["hash_a", "hash_b", "hash_c", "hash_d"]
        hash_sizes = {"hash_a": 100, "hash_b": 200, "hash_c": 300, "hash_d": 400}

        resolver = HitMissResolver()
        res = resolver.resolve_hashes(cached_hashes, required, hash_sizes)

        self.assertEqual(res.total_requested_bytes, 1000)
        self.assertEqual(res.hit_bytes, 300)
        self.assertEqual(res.miss_bytes, 700)
        self.assertAlmostEqual(res.byte_hit_ratio, 0.3)
        self.assertEqual(res.network_saved_bytes, 300)

    def test_resolver_duck_typed_package_plan_object(self) -> None:
        """Verify resolver accepts duck-typed M4 PackagePlan object with .assets list."""
        @dataclass
        class MockAsset:
            sha256: str
            size_bytes: int
            name: str

        @dataclass
        class MockPackagePlan:
            plan_id: str
            assets: List[MockAsset]

        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data_1 = b"mock_asset_1"
            h1 = compute_sha256(data_1)
            store.put_bytes(data_1)

            h2 = compute_sha256(b"mock_asset_2")

            plan = MockPackagePlan(
                plan_id="plan_001",
                assets=[
                    MockAsset(sha256=h1, size_bytes=len(data_1), name="tex1.png"),
                    MockAsset(sha256=h2, size_bytes=500, name="tex2.png"),
                ],
            )

            res = store.resolve_plan(plan)
            self.assertEqual(res.hits, {h1})
            self.assertEqual(res.misses, {h2})
            self.assertEqual(res.hit_bytes, len(data_1))
            self.assertEqual(res.miss_bytes, 500)

    def test_resolver_duck_typed_package_plan_dict(self) -> None:
        """Verify resolver accepts dictionary with 'assets' list."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            h1 = compute_sha256(b"plan_dict_1")
            store.put_bytes(b"plan_dict_1")
            h2 = compute_sha256(b"plan_dict_2")

            plan_dict = {
                "id": "dict_plan",
                "assets": [
                    {"sha256": h1, "size_bytes": len(b"plan_dict_1")},
                    {"sha256": h2, "size_bytes": 800},
                ],
            }

            res = store.resolve_plan(plan_dict)
            self.assertEqual(res.hits, {h1})
            self.assertEqual(res.misses, {h2})
            self.assertEqual(res.hit_bytes, len(b"plan_dict_1"))
            self.assertEqual(res.miss_bytes, 800)



class Tier1FeatureLRUEvictionTests(unittest.TestCase):
    """Tier 1: Feature 4 - LRU Eviction & Quota Enforcement."""

    def test_lru_eviction_under_quota_no_op(self) -> None:
        """When cache total bytes is below quota, no items are evicted."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir, max_size_bytes=1024 * 1024) as store:
            data = b"under_quota_data"
            store.put_bytes(data)

            freed = store.evict_lru(0)
            self.assertEqual(freed, 0)
            self.assertEqual(store.get_stats()["entry_count"], 1)

    def test_lru_eviction_strictly_evicts_oldest_accessed(self) -> None:
        """Eviction strictly evicts the least-recently accessed entry first."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            d1 = b"oldest_entry_data"
            d2 = b"middle_entry_data"
            d3 = b"newest_entry_data"

            h1 = compute_sha256(d1)
            h2 = compute_sha256(d2)
            h3 = compute_sha256(d3)

            store.put_bytes(d1)
            time.sleep(0.02)
            store.put_bytes(d2)
            time.sleep(0.02)
            store.put_bytes(d3)

            # Evict enough to free 1 item (len(d1))
            freed = store.evict_lru(len(d1))
            self.assertGreaterEqual(freed, len(d1))

            self.assertFalse(store.contains(h1), "Oldest entry h1 must be evicted")
            self.assertTrue(store.contains(h2), "Middle entry h2 must be retained")
            self.assertTrue(store.contains(h3), "Newest entry h3 must be retained")

    def test_lru_eviction_updates_both_disk_and_index(self) -> None:
        """Eviction unlinks physical file and deletes database metadata entry."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"eviction_cleanup_test_data"
            h = compute_sha256(data)
            entry = store.put_bytes(data)
            path = store.get_path(h)
            self.assertTrue(path.exists())

            store.evict_lru(len(data))
            self.assertFalse(path.exists(), "Physical file must be unlinked on eviction")
            self.assertFalse(store.contains(h), "Index entry must be deleted on eviction")

    def test_lru_eviction_target_bytes_to_free(self) -> None:
        """evict_lru(target_bytes) frees at least the target number of bytes."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            hashes = []
            for i in range(10):
                d = f"payload_{i}".encode("utf-8") * 50  # ~500 bytes each
                entry = store.put_bytes(d)
                hashes.append(entry.sha256)
                time.sleep(0.01)

            total_before = store.get_stats()["total_bytes"]
            target_free = 1200
            freed = store.evict_lru(target_free)

            self.assertGreaterEqual(freed, target_free)
            total_after = store.get_stats()["total_bytes"]
            self.assertEqual(total_before - total_after, freed)

    def test_lru_eviction_access_touch_prevents_eviction(self) -> None:
        """Touching an older entry makes it recently used, protecting it from immediate eviction."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            d1 = b"first_asset"
            d2 = b"second_asset"
            h1 = compute_sha256(d1)
            h2 = compute_sha256(d2)

            store.put_bytes(d1)
            time.sleep(0.02)
            store.put_bytes(d2)
            time.sleep(0.02)

            # Touch h1 by getting its bytes
            store.get_bytes(h1)

            # Now h2 is older than h1; evicting 1 item should evict h2!
            store.evict_lru(len(d2))
            self.assertTrue(store.contains(h1), "Touched item h1 must be preserved")
            self.assertFalse(store.contains(h2), "Untouched item h2 must be evicted")

    def test_lru_eviction_automatic_quota_enforcement_on_put(self) -> None:
        """When max_size_bytes is set, putting new assets automatically evicts LRU items."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir, max_size_bytes=1000) as store:
            d1 = b"a" * 400
            d2 = b"b" * 400
            d3 = b"c" * 400

            h1 = compute_sha256(d1)
            h2 = compute_sha256(d2)
            h3 = compute_sha256(d3)

            store.put_bytes(d1)
            time.sleep(0.01)
            store.put_bytes(d2)
            time.sleep(0.01)

            # Adding d3 (400 bytes) will exceed 1000 bytes (400+400+400=1200 > 1000), so d1 must be auto-evicted
            store.put_bytes(d3)

            self.assertLessEqual(store.get_stats()["total_bytes"], 1000)
            self.assertFalse(store.contains(h1), "Oldest item d1 must be evicted")
            self.assertTrue(store.contains(d2_hash := h2))
            self.assertTrue(store.contains(d3_hash := h3))


class Tier1FeatureChunkedStreamingTests(unittest.TestCase):
    """Tier 1: Feature 5 - Chunked Transfer & Bounded Memory."""

    def test_chunked_streaming_read(self) -> None:
        """Verify get_stream yields chunks of the requested chunk size."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"0123456789" * 1000  # 10,000 bytes
            h = compute_sha256(data)
            store.put_bytes(data)

            chunks = list(store.get_stream(h, chunk_size=1024))
            self.assertEqual(len(chunks), math.ceil(10000 / 1024))
            self.assertEqual(b"".join(chunks), data)

    def test_chunked_streaming_write(self) -> None:
        """Verify put_stream streams data into storage in chunks."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"stream_chunk_write_" * 800  # ~15 KB
            h = compute_sha256(data)
            stream = io.BytesIO(data)

            entry = store.put_stream(stream, size_bytes=len(data), sha256=h)
            self.assertEqual(entry.sha256, h)
            self.assertEqual(entry.size_bytes, len(data))
            self.assertEqual(store.get_bytes(h), data)

    def test_chunked_streaming_empty_file(self) -> None:
        """Streaming an empty (0-byte) file handles EOF cleanly."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            empty_data = b""
            h = compute_sha256(empty_data)
            store.put_bytes(empty_data)

            chunks = list(store.get_stream(h))
            self.assertEqual(b"".join(chunks), b"")

    def test_chunked_streaming_non_standard_chunk_size(self) -> None:
        """Streaming with small or non-power-of-2 chunk sizes functions correctly."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"abcdefghijklmnopqrstuvwxyz" * 10
            h = compute_sha256(data)
            store.put_bytes(data)

            for chunk_sz in [1, 7, 13, 100]:
                chunks = list(store.get_stream(h, chunk_size=chunk_sz))
                self.assertEqual(b"".join(chunks), data)

    def test_chunked_streaming_roundtrip_data_integrity(self) -> None:
        """Large payload roundtrip via streaming preserves byte-for-byte fidelity."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = os.urandom(256 * 1024)  # 256 KiB
            h = compute_sha256(data)

            store.put_stream(io.BytesIO(data), size_bytes=len(data), sha256=h)
            out_stream = store.get_stream(h, chunk_size=32768)
            retrieved = b"".join(out_stream)
            self.assertEqual(compute_sha256(retrieved), h)

    def test_chunked_streaming_generator_consumption(self) -> None:
        """Verify stream generator can be partially or incrementally consumed."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"chunk1_data_" + b"chunk2_data_" + b"chunk3_data_"
            h = compute_sha256(data)
            store.put_bytes(data)

            gen = store.get_stream(h, chunk_size=12)
            chunk1 = next(gen)
            self.assertEqual(chunk1, b"chunk1_data_")
            chunk2 = next(gen)
            self.assertEqual(chunk2, b"chunk2_data_")


class Tier1FeatureIntegrityVerificationTests(unittest.TestCase):
    """Tier 1: Feature 6 - Integrity Verification & Self-Healing."""

    def test_verify_valid_entry(self) -> None:
        """Valid entry passes fast and deep integrity verification."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"valid_unmodified_content"
            h = compute_sha256(data)
            store.put_bytes(data)

            self.assertTrue(store.verify(h, deep_check=False))
            self.assertTrue(store.verify(h, deep_check=True))

    def test_verify_corrupted_file_detected(self) -> None:
        """Bit-flipped or modified file on disk fails deep integrity verification."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"original_pure_asset_data"
            h = compute_sha256(data)
            store.put_bytes(data)

            # Corrupt the physical file
            path = store.get_path(h)
            path.write_bytes(b"corrupted_tampered_data")

            self.assertFalse(store.verify(h, deep_check=True))

    def test_verify_missing_file_detected(self) -> None:
        """Entry present in index but missing on disk fails verification."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"soon_to_be_missing_on_disk"
            h = compute_sha256(data)
            store.put_bytes(data)

            # Unlink physical file directly
            path = store.get_path(h)
            path.unlink()

            self.assertFalse(store.verify(h, deep_check=False))
            self.assertFalse(store.verify(h, deep_check=True))

    def test_verify_all_batch_scrub(self) -> None:
        """verify_all returns structured report with healthy, corrupted, and missing counts."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            h_valid = compute_sha256(b"valid_asset")
            h_corrupt = compute_sha256(b"corrupt_asset")
            h_missing = compute_sha256(b"missing_asset")

            store.put_bytes(b"valid_asset")
            store.put_bytes(b"corrupt_asset")
            store.put_bytes(b"missing_asset")

            # Corrupt one, delete one
            store.get_path(h_corrupt).write_bytes(b"tampered")
            store.get_path(h_missing).unlink()

            report = store.verify_all(auto_evict=False)
            self.assertFalse(report.is_healthy)
            self.assertEqual(report.verified_count, 1)
            self.assertEqual(report.corrupted_count, 1)
            self.assertEqual(report.missing_count, 1)
            self.assertIn(h_corrupt, report.corrupted_hashes)
            self.assertIn(h_missing, report.missing_hashes)

    def test_verify_all_auto_eviction_self_healing(self) -> None:
        """verify_all(auto_evict=True) cleans up corrupted & missing entries and leaves healthy cache."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            h_valid = compute_sha256(b"valid_asset")
            h_corrupt = compute_sha256(b"corrupt_asset")

            store.put_bytes(b"valid_asset")
            store.put_bytes(b"corrupt_asset")
            store.get_path(h_corrupt).write_bytes(b"tampered")

            report = store.verify_all(auto_evict=True)
            self.assertEqual(report.corrupted_count, 1)

            # Corrupted entry must be purged from cache
            self.assertFalse(store.contains(h_corrupt))
            self.assertTrue(store.contains(h_valid))

            # Second scrub should be 100% healthy
            report2 = store.verify_all(auto_evict=True)
            self.assertTrue(report2.is_healthy)
            self.assertEqual(report2.verified_count, 1)
            self.assertEqual(report2.corrupted_count, 0)

    def test_verify_corrupted_entry_status_updated(self) -> None:
        """When deep verify fails, state is recorded as corrupted in index."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"integrity_status_tracking"
            h = compute_sha256(data)
            store.put_bytes(data)

            store.get_path(h).write_bytes(b"tampered_data")
            store.verify(h, deep_check=True)

            stats = store.get_stats()
            # If not auto-evicted, status is reflected
            if store.contains(h):
                index_entry = store._index.get(h)
                self.assertEqual(index_entry.state, "corrupted")



class Tier2BoundaryCornerTests(unittest.TestCase):
    """Tier 2: Boundary & Corner Cases."""

    def test_boundary_zero_byte_asset(self) -> None:
        """Empty 0-byte asset (SHA-256 e3b0c44...) can be ingested, resolved, and retrieved."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            empty_data = b""
            empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

            entry = store.put_bytes(empty_data)
            self.assertEqual(entry.sha256, empty_hash)
            self.assertEqual(entry.size_bytes, 0)
            self.assertTrue(store.contains(empty_hash))
            self.assertEqual(store.get_bytes(empty_hash), b"")

            res = store.resolve_hashes([empty_hash])
            self.assertEqual(res.hits, {empty_hash})
            self.assertEqual(res.total_requested_bytes, 0)
            self.assertEqual(res.byte_hit_ratio, 1.0)

    def test_boundary_empty_resolution_request(self) -> None:
        """Empty resolution request returns empty sets and 1.0 byte_hit_ratio."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            res = store.resolve_hashes([])
            self.assertEqual(res.hits, set())
            self.assertEqual(res.misses, set())
            self.assertEqual(res.total_requested_bytes, 0)
            self.assertEqual(res.hit_bytes, 0)
            self.assertEqual(res.miss_bytes, 0)
            self.assertEqual(res.byte_hit_ratio, 1.0)
            self.assertEqual(res.network_saved_bytes, 0)

    def test_boundary_quota_equal_to_exact_file_size(self) -> None:
        """Cache quota exactly equal to file size allows storing that file."""
        with make_temp_dir() as tmp_dir:
            payload = b"exact_100_bytes_quota_boundary_test_asset_" + b"0" * 58
            self.assertEqual(len(payload), 100)
            with DiskCacheStore(tmp_dir, max_size_bytes=100) as store:
                entry = store.put_bytes(payload)
                self.assertEqual(entry.size_bytes, 100)
                self.assertEqual(store.get_stats()["total_bytes"], 100)

    def test_boundary_quota_zero_or_negative(self) -> None:
        """Quota of 0 indicates unbounded cache size."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir, max_size_bytes=0) as store:
            for i in range(10):
                store.put_bytes(f"unbounded_{i}".encode("utf-8") * 100)
            self.assertEqual(store.get_stats()["entry_count"], 10)

    def test_boundary_invalid_sha256_format_rejected(self) -> None:
        """Invalid SHA-256 strings (uppercase, wrong length, non-hex) are rejected."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            invalid_hashes = [
                "short_hash",
                "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",  # uppercase
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85g",  # non-hex 'g'
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855aa",  # too long
                "",
                "../../etc/shadow",
            ]
            for bad_h in invalid_hashes:
                with self.assertRaises((InvalidHashError, ValueError, CacheError)):
                    store.get_path(bad_h)

    def test_boundary_hash_mismatch_on_put_rejected(self) -> None:
        """Providing a SHA-256 that does not match actual bytes raises HashMismatchError."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            fake_hash = "0" * 64
            data = b"actual_data"

            with self.assertRaises((HashMismatchError, ValueError, CacheError)):
                store.put_bytes(data, sha256=fake_hash)

    def test_boundary_asset_with_special_characters_in_name(self) -> None:
        """Original name containing unicode, emoji, spaces, and path separators is handled safely."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"special_char_asset"
            weird_name = "models/subfolder/hero_🦸‍♂️_v2.0 (copy) #1 [final].obj"

            entry = store.put_bytes(data, original_name=weird_name, asset_type="model")
            self.assertEqual(entry.original_name, weird_name)
            retrieved = store._index.get(entry.sha256)
            self.assertEqual(retrieved.original_name, weird_name)

    def test_boundary_single_byte_asset(self) -> None:
        """Single 1-byte asset is stored and retrieved with correct size."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"X"
            entry = store.put_bytes(data)
            self.assertEqual(entry.size_bytes, 1)
            self.assertEqual(store.get_bytes(entry.sha256), b"X")

    def test_boundary_large_asset_size_metadata(self) -> None:
        """Metadata index supports large 64-bit size values (>4GB) without overflow."""
        with make_temp_dir() as tmp_dir:
            index_path = Path(tmp_dir) / "metadata" / "index.db"
            with SQLiteMetadataIndex(index_path) as index:
                h = compute_sha256(b"large_size_mock")
                large_size = 10 * 1024 * 1024 * 1024  # 10 GiB
                entry = CacheEntry(sha256=h, size_bytes=large_size)
                index.put(entry)

                retrieved = index.get(h)
                self.assertEqual(retrieved.size_bytes, large_size)

    def test_boundary_duplicate_hashes_in_resolution_request(self) -> None:
        """Duplicate hashes in resolution input are deduplicated without metric distortion."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"dedup_req_data"
            h = compute_sha256(data)
            store.put_bytes(data)

            # Request same hash 5 times
            res = store.resolve_hashes([h, h, h, h, h])
            self.assertEqual(res.hits, {h})
            self.assertEqual(res.misses, set())
            self.assertEqual(res.total_requested_bytes, len(data))
            self.assertEqual(res.byte_hit_ratio, 1.0)


class Tier3CrossFeatureCombinationsTests(unittest.TestCase):
    """Tier 3: Cross-Feature Combinations."""

    def test_combo_put_evict_resolve_lifecycle(self) -> None:
        """Lifecycle: Ingest A & B -> Evict A -> Resolve [A, B] -> Hit B, Miss A."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            da = b"asset_a_to_evict"
            db = b"asset_b_to_keep"
            ha = compute_sha256(da)
            hb = compute_sha256(db)

            store.put_bytes(da)
            time.sleep(0.02)
            store.put_bytes(db)

            store.evict_lru(len(da))
            self.assertFalse(store.contains(ha))
            self.assertTrue(store.contains(hb))

            res = store.resolve_hashes([ha, hb], hash_sizes={ha: len(da), hb: len(db)})
            self.assertEqual(res.hits, {hb})
            self.assertEqual(res.misses, {ha})
            self.assertEqual(res.hit_bytes, len(db))
            self.assertEqual(res.miss_bytes, len(da))

    def test_combo_put_corrupt_verify_evict_resolve(self) -> None:
        """Ingest A -> Corrupt on disk -> verify_all auto-evicts -> resolve reports miss."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"self_healing_pipeline_data"
            h = compute_sha256(data)
            store.put_bytes(data)

            # Tamper file
            store.get_path(h).write_bytes(b"tampered")

            report = store.verify_all(auto_evict=True)
            self.assertEqual(report.corrupted_count, 1)

            res = store.resolve_hashes([h], hash_sizes={h: len(data)})
            self.assertEqual(res.hits, set())
            self.assertEqual(res.misses, {h})

    def test_combo_concurrent_ingest_and_eviction(self) -> None:
        """Ingesting many assets under tight quota repeatedly evicts oldest while maintaining consistency."""
        with make_temp_dir() as tmp_dir:
            # 5 KB quota
            quota = 5000
            with DiskCacheStore(tmp_dir, max_size_bytes=quota) as store:
                added_hashes = []
                for i in range(50):
                    d = f"tight_quota_entry_{i}".encode("utf-8") * 20  # ~400 bytes
                    entry = store.put_bytes(d)
                    added_hashes.append(entry.sha256)

                stats = store.get_stats()
                self.assertLessEqual(stats["total_bytes"], quota)
                # Latest items should be in cache
                self.assertTrue(store.contains(added_hashes[-1]))
                self.assertTrue(store.contains(added_hashes[-2]))

    def test_combo_dedup_and_evict(self) -> None:
        """Deduplicated assets share storage; eviction properly frees the physical object."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"shared_mesh_data"
            h = compute_sha256(data)

            store.put_bytes(data, original_name="mesh_lod0.obj")
            store.put_bytes(data, original_name="mesh_lod1.obj")

            self.assertEqual(store.get_stats()["entry_count"], 1)
            store.evict_lru(len(data))
            self.assertEqual(store.get_stats()["entry_count"], 0)
            self.assertFalse(store.contains(h))

    def test_combo_plan_resolution_with_mixed_assets(self) -> None:
        """Complex PackagePlan with mixed hits and misses evaluates correct metrics."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            cached_textures = [f"tex_{i}".encode("utf-8") * 100 for i in range(5)]
            cached_hashes = [compute_sha256(t) for t in cached_textures]
            for t in cached_textures:
                store.put_bytes(t)

            missing_hashes = [compute_sha256(f"missing_tex_{i}".encode("utf-8")) for i in range(5)]

            plan = {
                "assets": [
                    {"sha256": h, "size_bytes": 500} for h in cached_hashes
                ] + [
                    {"sha256": h, "size_bytes": 500} for h in missing_hashes
                ]
            }

            res = store.resolve_plan(plan)
            self.assertEqual(len(res.hits), 5)
            self.assertEqual(len(res.misses), 5)
            self.assertEqual(res.total_requested_bytes, 5000)
            self.assertEqual(res.hit_bytes, 2500)
            self.assertEqual(res.miss_bytes, 2500)
            self.assertAlmostEqual(res.byte_hit_ratio, 0.5)

    def test_combo_touch_during_eviction_race(self) -> None:
        """Sequential touch operations reorder LRU chain accurately before eviction."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            items = [f"item_{i}".encode("utf-8") * 100 for i in range(4)]
            hashes = [compute_sha256(it) for it in items]

            for it in items:
                store.put_bytes(it)
                time.sleep(0.01)

            # Touch item 0 (making it newest)
            store.get_bytes(hashes[0])

            # Evict 1 item -> item 1 should be evicted (not item 0)
            store.evict_lru(len(items[1]))
            self.assertTrue(store.contains(hashes[0]))
            self.assertFalse(store.contains(hashes[1]))

    def test_combo_reingest_after_eviction(self) -> None:
        """Evicting an asset and subsequently re-ingesting it succeeds without conflict."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"reingest_payload_data"
            h = compute_sha256(data)

            store.put_bytes(data)
            store.evict_lru(len(data))
            self.assertFalse(store.contains(h))

            # Re-ingest
            entry = store.put_bytes(data)
            self.assertTrue(store.contains(h))
            self.assertEqual(store.get_bytes(h), data)

    def test_combo_full_cache_reconstruction_from_disk(self) -> None:
        """Opening an existing cache directory with a fresh DiskCacheStore instance preserves all entries."""
        with make_temp_dir() as tmp_dir:
            d1 = b"reconstruct_1"
            d2 = b"reconstruct_2"
            h1 = compute_sha256(d1)
            h2 = compute_sha256(d2)

            with DiskCacheStore(tmp_dir) as store1:
                store1.put_bytes(d1)
                store1.put_bytes(d2)

            # Create second instance pointing to same directory
            with DiskCacheStore(tmp_dir) as store2:
                self.assertTrue(store2.contains(h1))
                self.assertTrue(store2.contains(h2))
                self.assertEqual(store2.get_bytes(h1), d1)
                self.assertEqual(store2.get_bytes(h2), d2)


class Tier4RealWorldWorkloadScenariosTests(unittest.TestCase):
    """Tier 4: Real-World Workload Scenarios."""

    def test_scenario_cold_start_m4_scene_distribution(self) -> None:
        """Scenario 1: Cold start scene distribution with 50 assets -> 100% miss, transfers all assets."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            assets = []
            asset_sizes = {}
            for i in range(50):
                d = f"cold_start_asset_{i}".encode("utf-8") * 50
                h = compute_sha256(d)
                assets.append((h, d))
                asset_sizes[h] = len(d)

            # Cold start: resolve against empty cache
            res = store.resolve_hashes([h for h, _ in assets], hash_sizes=asset_sizes)
            self.assertEqual(len(res.hits), 0)
            self.assertEqual(len(res.misses), 50)
            self.assertEqual(res.byte_hit_ratio, 0.0)

            # Simulate network transfer ingestion
            for h, d in assets:
                store.put_bytes(d)

            # Re-resolve: 100% hit
            res2 = store.resolve_hashes([h for h, _ in assets], hash_sizes=asset_sizes)
            self.assertEqual(len(res2.hits), 50)
            self.assertEqual(len(res2.misses), 0)
            self.assertEqual(res2.byte_hit_ratio, 1.0)
            self.assertEqual(res2.network_saved_bytes, sum(asset_sizes.values()))

    def test_scenario_warm_start_incremental_frames_distribution(self) -> None:
        """Scenario 2: Frame 1 has 50 assets cached. Frame 2 reuses 45 assets and adds 5 new -> 90% hit ratio."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            # Frame 1 assets
            f1_assets = {}
            for i in range(50):
                d = f"frame1_asset_{i:03d}".encode("utf-8") * 10
                h = compute_sha256(d)
                store.put_bytes(d)
                f1_assets[h] = len(d)

            # Frame 2 assets: 45 reused + 5 new
            reused_hashes = list(f1_assets.keys())[:45]
            new_assets = {}
            for i in range(5):
                d = f"frame2_asset_{i:03d}".encode("utf-8") * 10
                h = compute_sha256(d)
                new_assets[h] = len(d)

            f2_requested = reused_hashes + list(new_assets.keys())
            f2_sizes = {**{h: f1_assets[h] for h in reused_hashes}, **new_assets}

            res = store.resolve_hashes(f2_requested, hash_sizes=f2_sizes)
            self.assertEqual(len(res.hits), 45)
            self.assertEqual(len(res.misses), 5)
            self.assertAlmostEqual(res.byte_hit_ratio, 0.9)
            self.assertEqual(res.network_saved_bytes, sum(f1_assets[h] for h in reused_hashes))


    def test_scenario_multi_worker_inventory_locality_optimization(self) -> None:
        """Scenario 3: Two distinct worker caches resolve shared workload to identify cache locality."""
        with make_temp_dir() as tmp_dir:
            worker1_dir = Path(tmp_dir) / "worker1"
            worker2_dir = Path(tmp_dir) / "worker2"

            with DiskCacheStore(worker1_dir) as cache1, DiskCacheStore(worker2_dir) as cache2:
                # Worker 1 has assets 0..29
                # Worker 2 has assets 20..49
                all_assets = {}
                for i in range(50):
                    d = f"multi_worker_asset_{i}".encode("utf-8") * 10
                    h = compute_sha256(d)
                    all_assets[h] = (d, len(d))
                    if i < 30:
                        cache1.put_bytes(d)
                    if i >= 20:
                        cache2.put_bytes(d)

                workload = list(all_assets.keys())
                sizes = {h: sz for h, (_, sz) in all_assets.items()}

                res1 = cache1.resolve_hashes(workload, hash_sizes=sizes)
                res2 = cache2.resolve_hashes(workload, hash_sizes=sizes)

                self.assertEqual(len(res1.hits), 30)
                self.assertEqual(len(res2.hits), 30)
                # Overlap is 10 assets (indices 20..29)
                self.assertEqual(len(res1.hits.intersection(res2.hits)), 10)

    def test_scenario_high_throughput_texture_stream_churn(self) -> None:
        """Scenario 4: Streaming large textures through quota-limited cache smoothly evicts LRU items."""
        with make_temp_dir() as tmp_dir:
            # 50 KB quota
            quota = 50 * 1024
            with DiskCacheStore(tmp_dir, max_size_bytes=quota) as store:
                texture_hashes = []
                for i in range(50):
                    # 5 KB texture each -> total 250 KB
                    tex_data = f"texture_{i}".encode("utf-8") * 500
                    h = compute_sha256(tex_data)
                    store.put_stream(io.BytesIO(tex_data), size_bytes=len(tex_data), sha256=h, asset_type="texture")
                    texture_hashes.append(h)

                stats = store.get_stats()
                self.assertLessEqual(stats["total_bytes"], quota)
                # Verify latest entries are readable
                last_h = texture_hashes[-1]
                self.assertTrue(store.contains(last_h))
                self.assertIsNotNone(store.get_bytes(last_h))

    def test_scenario_network_drop_and_interrupted_stream_recovery(self) -> None:
        """Scenario 5: Stream interrupted midway by exception leaves no broken file in objects directory."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            class FaultyStream(io.BytesIO):
                def __init__(self, data: bytes, fail_after: int) -> None:
                    super().__init__(data)
                    self.fail_after = fail_after
                    self.bytes_read = 0

                def read(self, size: int = -1) -> bytes:
                    if self.bytes_read >= self.fail_after:
                        raise ConnectionResetError("Simulated network drop")
                    chunk = super().read(size)
                    self.bytes_read += len(chunk)
                    return chunk

            data = b"interrupted_stream_content_" * 1000
            h = compute_sha256(data)
            faulty = FaultyStream(data, fail_after=500)

            with self.assertRaises((ConnectionResetError, CacheError)):
                store.put_stream(faulty, size_bytes=len(data), sha256=h)

            # Must not exist in cache
            self.assertFalse(store.contains(h))
            self.assertIsNone(store.get_path(h))

            # Retry with valid stream
            store.put_stream(io.BytesIO(data), size_bytes=len(data), sha256=h)
            self.assertTrue(store.contains(h))
            self.assertEqual(store.get_bytes(h), data)

    def test_scenario_multi_camera_asset_deduplication_and_savings(self) -> None:
        """Scenario 6: Multiple camera render plans sharing common background textures calculate aggregate savings."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            # Shared environment textures
            env_textures = [f"env_hdri_{i}".encode("utf-8") * 100 for i in range(3)]
            env_hashes = [compute_sha256(t) for t in env_textures]
            for t in env_textures:
                store.put_bytes(t)

            # Camera A plan (env textures + camA unique)
            cam_a_plan = {
                "camera_id": "Cam_A",
                "assets": [{"sha256": h, "size_bytes": 1000} for h in env_hashes] + [
                    {"sha256": compute_sha256(b"cam_a_unique"), "size_bytes": 1000}
                ],
            }

            # Camera B plan (env textures + camB unique)
            cam_b_plan = {
                "camera_id": "Cam_B",
                "assets": [{"sha256": h, "size_bytes": 1000} for h in env_hashes] + [
                    {"sha256": compute_sha256(b"cam_b_unique"), "size_bytes": 1000}
                ],
            }

            res_a = store.resolve_plan(cam_a_plan)
            res_b = store.resolve_plan(cam_b_plan)

            self.assertEqual(res_a.network_saved_bytes, 3000)
            self.assertEqual(res_b.network_saved_bytes, 3000)
            self.assertEqual(res_a.hit_bytes + res_b.hit_bytes, 6000)


if __name__ == "__main__":
    unittest.main()

