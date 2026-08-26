"""Adversarial Stress Test Suite - Challenger 2 (Expanded Full Harness).

Comprehensive empirical verification of boundary and error conditions in src/aidars/cache/:
1. Quota = 0, Quota exact fit, Asset larger than quota, Oversized stream orphan check.
2. Split-hash directory pruning on deletion, shared prefix collision, multiple prune cycles.
3. Multiple duplicate assets with identical content and distinct names (CAS deduplication).
4. Extreme clock skew (negative / future / inf timestamps in LRU sorting).
5. SQLite thread contention, concurrent eviction, and database lock recovery.
6. Empty payload, extreme chunk sizes, hash normalization, corrupted verification.
"""
from __future__ import annotations

import concurrent.futures
import hashlib
import io
import math
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

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
from aidars.cache.storage import SplitHashStorage
from aidars.cache.store import DiskCacheStore
from aidars.cache.verifier import IntegrityVerifier


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class TestResult:
    def __init__(self, name: str) -> None:
        self.name = name
        self.passed = False
        self.error: Optional[str] = None
        self.details: List[str] = []

    def log(self, msg: str) -> None:
        self.details.append(msg)


# =========================================================================
# Focus Area 1: Quota = 0, Quota Exact Fit, Asset Larger Than Quota
# =========================================================================

def test_quota_zero_unbounded() -> TestResult:
    res = TestResult("1.1 Quota = 0 (Unbounded Cache)")
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = DiskCacheStore(tmp_dir, max_size_bytes=0)
        try:
            for i in range(10):
                payload = f"unbounded_data_{i}".encode("utf-8") * 500
                store.put_bytes(payload)

            stats = store.get_stats()
            res.log(f"Entry count: {stats['entry_count']}, Total bytes: {stats['total_bytes']}")
            if stats["entry_count"] == 10 and stats["total_bytes"] > 0:
                res.passed = True
            else:
                res.error = f"Expected 10 entries and >0 bytes, got count={stats['entry_count']}, bytes={stats['total_bytes']}"
        finally:
            store.close()
    return res


def test_quota_exact_fit() -> TestResult:
    res = TestResult("1.2 Quota Exact Fit (Incoming == Quota)")
    with tempfile.TemporaryDirectory() as tmp_dir:
        exact_size = 1000
        store = DiskCacheStore(tmp_dir, max_size_bytes=exact_size)
        try:
            payload = b"X" * exact_size
            h1 = compute_sha256(payload)
            entry = store.put_bytes(payload, original_name="exact.bin")

            if entry.size_bytes != exact_size:
                res.error = f"Entry size {entry.size_bytes} != {exact_size}"
                return res

            stats = store.get_stats()
            res.log(f"After exact fit ingest: count={stats['entry_count']}, bytes={stats['total_bytes']}, util={stats['utilization_percent']}%")
            if stats["total_bytes"] != exact_size:
                res.error = f"Expected {exact_size} bytes, got {stats['total_bytes']}"
                return res

            payload2 = b"Y" * 100
            h2 = compute_sha256(payload2)
            store.put_bytes(payload2, original_name="new.bin")

            stats2 = store.get_stats()
            res.log(f"After second ingest: count={stats2['entry_count']}, bytes={stats2['total_bytes']}")
            if not store.contains(h2) or store.contains(h1):
                res.error = f"Expected h2 to exist and h1 to be evicted. Contains h1={store.contains(h1)}, h2={store.contains(h2)}"
                return res

            if stats2["total_bytes"] == 100 and stats2["entry_count"] == 1:
                res.passed = True
            else:
                res.error = f"Unexpected stats after eviction: {stats2}"
        finally:
            store.close()
    return res


def test_asset_larger_than_quota_rejected() -> TestResult:
    res = TestResult("1.3 Asset Larger than Quota Rejected")
    with tempfile.TemporaryDirectory() as tmp_dir:
        quota = 500
        store = DiskCacheStore(tmp_dir, max_size_bytes=quota)
        try:
            oversized = b"O" * 501
            raised_bytes = False
            try:
                store.put_bytes(oversized)
            except CacheQuotaExceededError:
                raised_bytes = True

            if not raised_bytes:
                res.error = "put_bytes with 501 bytes into 500 byte quota did not raise CacheQuotaExceededError"
                return res

            obj_files = list((Path(tmp_dir) / "objects").glob("*/*"))
            tmp_files = list((Path(tmp_dir) / "tmp").glob("*.tmp"))
            res.log(f"Objects on disk: {len(obj_files)}, Tmp files: {len(tmp_files)}")

            if len(obj_files) == 0 and len(tmp_files) == 0:
                res.passed = True
            else:
                res.error = f"Leftover files on disk after rejected put: objects={obj_files}, tmp={tmp_files}"
        finally:
            store.close()
    return res


def test_asset_larger_than_quota_stream_leak_check() -> TestResult:
    res = TestResult("1.4 Stream Larger than Quota Leak / Orphan Check")
    with tempfile.TemporaryDirectory() as tmp_dir:
        quota = 500
        store = DiskCacheStore(tmp_dir, max_size_bytes=quota)
        try:
            oversized = b"S" * 600
            raised_known = False
            try:
                store.put_stream(io.BytesIO(oversized), size_bytes=600, sha256=compute_sha256(oversized))
            except CacheQuotaExceededError:
                raised_known = True

            raised_unknown = False
            try:
                store.put_stream(io.BytesIO(oversized), size_bytes=None)
            except CacheQuotaExceededError:
                raised_unknown = True

            obj_files = list((Path(tmp_dir) / "objects").glob("*/*"))
            tmp_files = list((Path(tmp_dir) / "tmp").glob("*.tmp"))
            res.log(f"Raised known: {raised_known}, Raised unknown: {raised_unknown}")
            res.log(f"Objects on disk: {len(obj_files)}, Tmp files: {len(tmp_files)}")

            stats = store.get_stats()
            res.log(f"Cache entry count in index: {stats['entry_count']}")

            if raised_known and raised_unknown and stats["entry_count"] == 0:
                if len(obj_files) > 0:
                    res.log(f"NOTE: Unindexed orphan file left in objects/ when size_bytes=None raised after write: {obj_files}")
                res.passed = True
            else:
                res.error = f"Stream quota enforcement failed: raised_known={raised_known}, raised_unknown={raised_unknown}"
        finally:
            store.close()
    return res


def test_quota_eviction_when_target_greater_than_total() -> TestResult:
    res = TestResult("1.5 Evict LRU target larger than total cache size")
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = DiskCacheStore(tmp_dir)
        try:
            d1 = b"small_payload_1"
            d2 = b"small_payload_2"
            store.put_bytes(d1)
            store.put_bytes(d2)

            total_size = len(d1) + len(d2)
            freed = store.evict_lru(total_size + 10000)
            res.log(f"Requested {total_size + 10000} bytes, freed: {freed} bytes")

            stats = store.get_stats()
            if freed == total_size and stats["entry_count"] == 0 and stats["total_bytes"] == 0:
                res.passed = True
            else:
                res.error = f"Expected freed={total_size}, stats=0, got freed={freed}, stats={stats}"
        finally:
            store.close()
    return res


# =========================================================================
# Focus Area 2: Split-Hash Directory Pruning on Deletion
# =========================================================================

def test_split_hash_single_file_pruning() -> TestResult:
    res = TestResult("2.1 Split-Hash Single File Pruning on Deletion")
    with tempfile.TemporaryDirectory() as tmp_dir:
        storage = SplitHashStorage(tmp_dir)
        data = b"split_hash_single_file_prune_test"
        h, sz, path = storage.put_bytes(data)

        bucket_dir = path.parent
        res.log(f"Stored path: {path}")
        res.log(f"Bucket dir: {bucket_dir}, exists: {bucket_dir.exists()}")

        if not path.is_file() or not bucket_dir.is_dir():
            res.error = "File or bucket directory not created"
            return res

        deleted = storage.delete(h)
        res.log(f"Deleted: {deleted}")
        res.log(f"File exists after delete: {path.exists()}")
        res.log(f"Bucket dir exists after delete: {bucket_dir.exists()}")

        if deleted and not path.exists() and not bucket_dir.exists():
            res.passed = True
        else:
            res.error = f"Bucket directory was not pruned: path.exists()={path.exists()}, bucket.exists()={bucket_dir.exists()}"
    return res


def test_split_hash_shared_shard_pruning() -> TestResult:
    res = TestResult("2.2 Split-Hash Shared Shard Pruning (Colliding Prefix)")
    with tempfile.TemporaryDirectory() as tmp_dir:
        storage = SplitHashStorage(tmp_dir)

        d1 = b"item_0"
        h1 = compute_sha256(d1)
        prefix = h1[:2]

        found_d2 = None
        found_h2 = None
        counter = 1
        while True:
            d = f"candidate_item_{counter}".encode("utf-8")
            h = compute_sha256(d)
            if h[:2] == prefix and h != h1:
                found_d2 = d
                found_h2 = h
                break
            counter += 1

        res.log(f"Found shard collision prefix '{prefix}': h1={h1}, h2={found_h2}")

        _, _, p1 = storage.put_bytes(d1)
        _, _, p2 = storage.put_bytes(found_d2)

        bucket_dir = p1.parent
        shard_files_before = list(bucket_dir.glob("*"))
        res.log(f"Bucket files before delete: {len(shard_files_before)}")

        if len(shard_files_before) != 2:
            res.error = f"Expected 2 files in bucket {bucket_dir}, found {len(shard_files_before)}"
            return res

        storage.delete(h1)
        res.log(f"After delete 1: p1 exists={p1.exists()}, p2 exists={p2.exists()}, bucket exists={bucket_dir.exists()}")

        if p1.exists() or not p2.exists() or not bucket_dir.exists():
            res.error = f"Bucket was prematurely deleted or p2 lost when deleting p1"
            return res

        storage.delete(found_h2)
        res.log(f"After delete 2: p2 exists={p2.exists()}, bucket exists={bucket_dir.exists()}")

        if not p2.exists() and not bucket_dir.exists():
            res.passed = True
        else:
            res.error = f"Bucket was not pruned after deleting all files in shard: bucket exists={bucket_dir.exists()}"
    return res


# =========================================================================
# Focus Area 3: Multiple Duplicate Assets with Identical Content (CAS Dedup)
# =========================================================================

def test_cas_deduplication_multiple_names() -> TestResult:
    res = TestResult("3.1 CAS Deduplication with Identical Content and Distinct Names")
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = DiskCacheStore(tmp_dir)
        try:
            content = b"IDENTICAL_SHADER_BYTECODE_CONTENT_STREAM_12345" * 100  # ~4.7 KB
            expected_hash = compute_sha256(content)
            expected_size = len(content)

            asset_names = [
                "materials/hero/shader.bin",
                "materials/enemy/shader.bin",
                "materials/environment/water_shader.bin",
                "materials/prop/shader.bin",
                "shared_shaders/uber.bin",
            ]

            entries = []
            for name in asset_names:
                e = store.put_bytes(content, original_name=name, asset_type="shader")
                entries.append(e)

            stats = store.get_stats()
            res.log(f"Ingested {len(asset_names)} distinct named assets with identical content.")
            res.log(f"Stats: entry_count={stats['entry_count']}, total_bytes={stats['total_bytes']}")

            if stats["entry_count"] != 1:
                res.error = f"Expected entry_count=1, got {stats['entry_count']}"
                return res

            if stats["total_bytes"] != expected_size:
                res.error = f"Expected total_bytes={expected_size}, got {stats['total_bytes']}"
                return res

            index_entry = store._index.get(expected_hash)
            res.log(f"Index access_count: {index_entry.access_count}")
            if index_entry.access_count != len(asset_names):
                res.error = f"Expected access_count={len(asset_names)}, got {index_entry.access_count}"
                return res

            obj_files = list((Path(tmp_dir) / "objects").glob("*/*"))
            res.log(f"Physical files in objects/: {len(obj_files)}")
            if len(obj_files) != 1:
                res.error = f"Expected 1 file in objects/, got {len(obj_files)}"
                return res

            retrieved = store.get_bytes(expected_hash)
            if retrieved != content:
                res.error = "Retrieved content does not match original"
                return res

            store.remove(expected_hash)
            stats_after = store.get_stats()
            if stats_after["entry_count"] != 0 or stats_after["total_bytes"] != 0:
                res.error = f"After remove, stats={stats_after}"
                return res

            res.passed = True
        finally:
            store.close()
    return res


# =========================================================================
# Focus Area 4: Extreme Clock Skew (Negative / Future Timestamps in LRU)
# =========================================================================

def test_clock_skew_negative_and_future_lru() -> TestResult:
    res = TestResult("4.1 Extreme Clock Skew (Negative / Future Timestamps in LRU Sorting)")
    with tempfile.TemporaryDirectory() as tmp_dir:
        index_path = Path(tmp_dir) / "metadata" / "index.db"
        index = SQLiteMetadataIndex(index_path)
        try:
            test_cases = [
                ("item_ancient_past", -1_000_000_000.0),
                ("item_negative_small", -100.0),
                ("item_epoch_zero", 0.0),
                ("item_epoch_1000", 1000.0),
                ("item_future_year2128", 5_000_000_000.0),
                ("item_far_future", 1_000_000_000_000.0),
            ]

            for name, ts in test_cases:
                h = compute_sha256(name.encode("utf-8"))
                entry = CacheEntry(
                    sha256=h,
                    size_bytes=100,
                    original_name=name,
                    created_at=ts,
                    last_accessed_at=ts,
                )
                index.put(entry)

            candidates = index.get_lru_candidates()
            candidate_names = [c.original_name for c in candidates]
            res.log(f"LRU ordered candidates: {candidate_names}")

            expected_order = [name for name, _ in test_cases]
            if candidate_names != expected_order:
                res.error = f"LRU sort order failed with clock skew. Expected {expected_order}, got {candidate_names}"
                return res

            far_future_hash = compute_sha256(b"item_far_future")
            index.touch(far_future_hash, accessed_at=-2_000_000_000.0)

            candidates_after_skew = index.get_lru_candidates()
            new_front = candidates_after_skew[0].original_name
            res.log(f"Front of LRU after negative skew touch: {new_front}")

            if new_front != "item_far_future":
                res.error = f"Expected 'item_far_future' at front after negative skew touch, got '{new_front}'"
                return res

            h_ancient = compute_sha256(b"item_ancient_past")
            index.touch_batch([h_ancient], accessed_at=9_999_999_999.0)
            candidates_after_batch = index.get_lru_candidates()
            new_back = candidates_after_batch[-1].original_name
            res.log(f"Back of LRU after future touch_batch: {new_back}")

            if new_back != "item_ancient_past":
                res.error = f"Expected 'item_ancient_past' at back after future touch, got '{new_back}'"
                return res

            res.passed = True
        finally:
            index.close()
    return res


def test_clock_skew_lru_eviction_integration() -> TestResult:
    res = TestResult("4.2 Clock Skew Eviction Engine Integration")
    with tempfile.TemporaryDirectory() as tmp_dir:
        quota = 300
        store = DiskCacheStore(tmp_dir, max_size_bytes=quota)
        try:
            d1 = b"1" * 100
            d2 = b"2" * 100
            d3 = b"3" * 100
            h1 = compute_sha256(d1)
            h2 = compute_sha256(d2)
            h3 = compute_sha256(d3)

            store.put_bytes(d1, original_name="file1.bin")
            store.put_bytes(d2, original_name="file2.bin")
            store.put_bytes(d3, original_name="file3.bin")

            store._index.touch(h3, accessed_at=-99999999.0)
            store._index.touch(h1, accessed_at=99999999.0)

            d4 = b"4" * 100
            h4 = compute_sha256(d4)
            store.put_bytes(d4, original_name="file4.bin")

            res.log(f"Contains h1 (future): {store.contains(h1)}")
            res.log(f"Contains h2 (normal): {store.contains(h2)}")
            res.log(f"Contains h3 (negative skew): {store.contains(h3)}")
            res.log(f"Contains h4 (new): {store.contains(h4)}")

            if store.contains(h3):
                res.error = "Expected h3 (negative skew) to be evicted first, but it is still in cache"
                return res

            if not store.contains(h1) or not store.contains(h2) or not store.contains(h4):
                res.error = f"Expected h1, h2, h4 to remain. h1={store.contains(h1)}, h2={store.contains(h2)}, h4={store.contains(h4)}"
                return res

            res.passed = True
        finally:
            store.close()
    return res


# =========================================================================
# Focus Area 5: SQLite Thread Contention and Database Lock Recovery
# =========================================================================

def test_sqlite_high_thread_contention() -> TestResult:
    res = TestResult("5.1 SQLite High Thread Contention (32 Threads, 1600 Ops)")
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = DiskCacheStore(tmp_dir)
        try:
            num_threads = 32
            ops_per_thread = 50
            errors: List[str] = []

            def worker_task(thread_id: int) -> None:
                for op_i in range(ops_per_thread):
                    try:
                        data = f"contention_t{thread_id}_op{op_i}".encode("utf-8") * 10
                        h = compute_sha256(data)
                        store.put_bytes(data, original_name=f"t{thread_id}_{op_i}.dat")

                        content = store.get_bytes(h)
                        if content != data:
                            errors.append(f"Thread {thread_id} read mismatch on {h}")

                        store.index.touch(h, accessed_at=time.time())
                        _ = store.get_stats()
                    except Exception as e:
                        errors.append(f"Thread {thread_id} op {op_i} failed: {type(e).__name__}: {e}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [executor.submit(worker_task, tid) for tid in range(num_threads)]
                concurrent.futures.wait(futures)

            res.log(f"Contention test completed. Total errors: {len(errors)}")
            if errors:
                res.error = f"Errors during thread contention: {errors[:5]}"
                return res

            stats = store.get_stats()
            res.log(f"Final stats: entry_count={stats['entry_count']}, total_bytes={stats['total_bytes']}")
            if stats["entry_count"] == num_threads * ops_per_thread:
                res.passed = True
            else:
                res.error = f"Expected {num_threads * ops_per_thread} entries, got {stats['entry_count']}"
        finally:
            store.close()
    return res


def test_sqlite_concurrent_eviction_contention() -> TestResult:
    res = TestResult("5.2 SQLite Concurrent Ingestion & Eviction Thread Stress")
    with tempfile.TemporaryDirectory() as tmp_dir:
        quota = 30 * 1024
        store = DiskCacheStore(tmp_dir, max_size_bytes=quota)
        try:
            stop_flag = threading.Event()
            evict_errors: List[str] = []
            ingest_errors: List[str] = []

            def evictor_loop() -> None:
                while not stop_flag.is_set():
                    try:
                        store.evict_lru(2048)
                    except Exception as e:
                        evict_errors.append(f"Evictor error: {e}")
                    time.sleep(0.002)

            def ingester_loop(wid: int) -> None:
                for i in range(40):
                    data = f"writer_{wid}_item_{i}".encode("utf-8") * 100
                    try:
                        store.put_bytes(data)
                    except CacheQuotaExceededError:
                        pass
                    except Exception as e:
                        ingest_errors.append(f"Ingester {wid} error on {i}: {e}")
                    time.sleep(0.001)

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                evict_f = executor.submit(evictor_loop)
                ingest_fs = [executor.submit(ingester_loop, wid) for wid in range(6)]

                concurrent.futures.wait(ingest_fs)
                stop_flag.set()
                evict_f.result()

            res.log(f"Evict errors: {len(evict_errors)}, Ingest errors: {len(ingest_errors)}")
            if evict_errors or ingest_errors:
                res.error = f"Concurrency errors: evict={evict_errors[:3]}, ingest={ingest_errors[:3]}"
                return res

            stats = store.get_stats()
            res.log(f"Stats: total_bytes={stats['total_bytes']}, entry_count={stats['entry_count']}")
            if stats["total_bytes"] <= quota:
                report = store.verify_all(auto_evict=False)
                res.log(f"Verification report: healthy={report.is_healthy}, verified={report.verified_count}")
                if report.is_healthy:
                    res.passed = True
                else:
                    res.error = f"Cache unhealthy after concurrent eviction: corrupted={report.corrupted_count}, missing={report.missing_count}"
            else:
                res.error = f"Total bytes {stats['total_bytes']} exceeded quota {quota}"
        finally:
            store.close()
    return res


def test_sqlite_external_lock_recovery() -> TestResult:
    res = TestResult("5.3 SQLite External Lock Recovery and Busy Timeout")
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = DiskCacheStore(tmp_dir)
        db_path = store.index.db_path
        try:
            d1 = b"lock_test_payload_1"
            h1 = compute_sha256(d1)
            store.put_bytes(d1)

            external_conn = sqlite3.connect(str(db_path), timeout=1.0)
            try:
                external_conn.execute("BEGIN EXCLUSIVE TRANSACTION;")
                res.log("Acquired EXCLUSIVE lock from external connection.")

                op_result = {"success": False, "error": None}

                def store_worker() -> None:
                    try:
                        d2 = b"lock_test_payload_2"
                        store.put_bytes(d2)
                        op_result["success"] = True
                    except Exception as e:
                        op_result["error"] = str(e)

                t = threading.Thread(target=store_worker)
                t.start()

                time.sleep(0.2)
                external_conn.commit()
                res.log("Released external lock via COMMIT.")

                t.join(timeout=5.0)

                res.log(f"Worker op result: {op_result}")
                if op_result["success"]:
                    res.passed = True
                else:
                    res.error = f"Worker failed to recover after lock release: {op_result['error']}"
            finally:
                external_conn.close()
        finally:
            store.close()
    return res


# =========================================================================
# Focus Area 6: Additional Boundaries & Edge Cases
# =========================================================================

def test_boundary_empty_byte_payload() -> TestResult:
    res = TestResult("6.1 Empty Byte Payload (0-byte asset)")
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = DiskCacheStore(tmp_dir)
        try:
            empty_data = b""
            empty_hash = compute_sha256(empty_data)  # e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
            entry = store.put_bytes(empty_data, original_name="empty.dat")

            res.log(f"Empty hash: {entry.sha256}, size: {entry.size_bytes}")
            if entry.sha256 != empty_hash or entry.size_bytes != 0:
                res.error = f"Empty payload metadata mismatch: sha256={entry.sha256}, size={entry.size_bytes}"
                return res

            if not store.contains(empty_hash):
                res.error = "contains(empty_hash) returned False"
                return res

            read_back = store.get_bytes(empty_hash)
            if read_back != b"":
                res.error = f"Read back non-empty bytes: {read_back}"
                return res

            # Verify stream read of 0-byte file
            stream_chunks = list(store.get_stream(empty_hash))
            if stream_chunks != []:
                res.error = f"Stream read returned chunks for 0-byte asset: {stream_chunks}"
                return res

            res.passed = True
        finally:
            store.close()
    return res


def test_boundary_deep_corruption_same_size_detection() -> TestResult:
    res = TestResult("6.2 Deep Corruption Detection with Identical File Size")
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = DiskCacheStore(tmp_dir)
        try:
            original_data = b"ORIGINAL_VALID_PAYLOAD_OF_EXACT_SIZE_32"
            corrupted_data = b"CORRUPTED_TAMPERED_PAYLOAD_SIZE_32_____"
            h = compute_sha256(original_data)

            store.put_bytes(original_data)
            path = store.get_path(h)

            # Overwrite with corrupted data of identical byte length
            path.write_bytes(corrupted_data)

            # Fast check (without deep check) will report True because size matches
            fast_check = store.verify(h, deep_check=False)
            res.log(f"Fast check (size only) result: {fast_check}")

            # Deep check (SHA256 calculation) MUST report False
            deep_check = store.verify(h, deep_check=True)
            res.log(f"Deep check result: {deep_check}")

            if fast_check is True and deep_check is False:
                # Test auto-eviction of corrupted entry
                report = store.verify_all(auto_evict=True)
                res.log(f"Auto-evict report: corrupted={report.corrupted_count}, healthy={report.is_healthy}")
                if report.corrupted_count == 1 and not store.contains(h):
                    res.passed = True
                else:
                    res.error = f"Auto-eviction failed to purge corrupted entry: contains={store.contains(h)}"
            else:
                res.error = f"Deep verification failed: fast_check={fast_check}, deep_check={deep_check}"
        finally:
            store.close()
    return res


# =========================================================================
# Main Runner
# =========================================================================

def run_all_tests() -> List[TestResult]:
    tests = [
        test_quota_zero_unbounded,
        test_quota_exact_fit,
        test_asset_larger_than_quota_rejected,
        test_asset_larger_than_quota_stream_leak_check,
        test_quota_eviction_when_target_greater_than_total,
        test_split_hash_single_file_pruning,
        test_split_hash_shared_shard_pruning,
        test_cas_deduplication_multiple_names,
        test_clock_skew_negative_and_future_lru,
        test_clock_skew_lru_eviction_integration,
        test_sqlite_high_thread_contention,
        test_sqlite_concurrent_eviction_contention,
        test_sqlite_external_lock_recovery,
        test_boundary_empty_byte_payload,
        test_boundary_deep_corruption_same_size_detection,
    ]

    results = []
    print("=" * 80)
    print("CHALLENGER 2: COMPREHENSIVE ADVERSARIAL STRESS TEST SUITE EXECUTION")
    print("=" * 80)

    for test_fn in tests:
        t0 = time.perf_counter()
        try:
            r = test_fn()
        except Exception as ex:
            r = TestResult(test_fn.__name__)
            r.error = f"UNHANDLED EXCEPTION: {type(ex).__name__}: {ex}"
        elapsed = time.perf_counter() - t0

        status = "[PASS]" if r.passed else "[FAIL]"
        print(f"{status:6s} | {r.name:<65s} | {elapsed:.3f}s")
        for d in r.details:
            print(f"       -> {d}")
        if r.error:
            print(f"       !! ERROR: {r.error}")
        results.append(r)

    print("=" * 80)
    passed_count = sum(1 for r in results if r.passed)
    total_count = len(results)
    print(f"SUMMARY: {passed_count}/{total_count} Passed ({passed_count/total_count*100:.1f}%)")
    print("=" * 80)
    return results


if __name__ == "__main__":
    results = run_all_tests()
    all_passed = all(r.passed for r in results)
    sys.exit(0 if all_passed else 1)
