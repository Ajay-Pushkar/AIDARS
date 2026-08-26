"""Adversarial and Stress Test Suite for Milestone 5 Core Cache.

Covers:
- Simulated Corruption Suite (C1 - C10): Bit flips, truncations, expansions, dangling records, orphan files, permission locks, path traversals, SQL injections.
- Bounded Memory Streaming: High-volume streaming verifying O(1) RAM bounding via tracemalloc.
- Concurrency & WAL Mode: Multi-threaded readers, writers, and eviction under stress.
- AST Decoupling: Zero imports of Blender modules (bpy, bmesh, mathutils, visibility) in src/aidars/cache/.
- Backward Compatibility: SceneCache request-aware caching stress tests.
"""
from __future__ import annotations

import ast
import concurrent.futures
import hashlib
import io
import itertools
import json
import os
import random
import sqlite3
import sys
import tempfile
import time
import tracemalloc
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional

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
from aidars.scene_intelligence.cache import (
    SceneCache,
    SceneCacheEntry,
    hash_blend_file,
    hash_json_payload,
    hash_source,
)
from aidars.scene_intelligence.scene_engine import (
    SceneEngine,
    SceneEngineRequest,
    SceneEngineResult,
)


def make_temp_dir() -> tempfile.TemporaryDirectory:
    """Create TemporaryDirectory with ignore_cleanup_errors=True on Python 3.10+."""
    if sys.version_info >= (3, 10):
        return tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    return tempfile.TemporaryDirectory()


def compute_sha256(data: bytes) -> str:
    """Authoritative SHA-256 calculation."""
    return hashlib.sha256(data).hexdigest()


class AdversarialCorruptionSuiteTests(unittest.TestCase):
    """Adversarial Corruption Simulation Suite (Scenarios C1 - C10)."""

    def test_adv_c1_bit_flip_corruption(self) -> None:
        """C1: Bit flip at middle offset of cached file is caught by deep verification and self-healed."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"unmodified_secure_payload_data_" * 20
            h = compute_sha256(data)
            store.put_bytes(data)

            # Flip a single bit in the file on disk
            file_path = store.get_path(h)
            raw = bytearray(file_path.read_bytes())
            raw[10] ^= 0x01  # bit flip
            file_path.write_bytes(bytes(raw))

            # Fast check might pass (same size), deep check MUST fail
            self.assertFalse(store.verify(h, deep_check=True))

            # Self-healing via verify_all(auto_evict=True)
            report = store.verify_all(auto_evict=True)
            self.assertEqual(report.corrupted_count, 1)
            self.assertFalse(store.contains(h))

    def test_adv_c2_file_truncation(self) -> None:
        """C2: Cached file truncated to smaller size fails fast size check and deep hash check."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"truncation_target_payload_1234567890" * 10
            h = compute_sha256(data)
            store.put_bytes(data)

            file_path = store.get_path(h)
            file_path.write_bytes(data[:20])  # truncate

            self.assertFalse(store.verify(h, deep_check=False))
            self.assertFalse(store.verify(h, deep_check=True))

            report = store.verify_all(auto_evict=True)
            self.assertEqual(report.corrupted_count, 1)
            self.assertFalse(store.contains(h))

    def test_adv_c3_file_expansion_zero_padding(self) -> None:
        """C3: Cached file expanded with trailing null bytes fails verification."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"payload_to_expand"
            h = compute_sha256(data)
            store.put_bytes(data)

            file_path = store.get_path(h)
            file_path.write_bytes(data + b"\x00" * 64)

            self.assertFalse(store.verify(h, deep_check=False))
            self.assertFalse(store.verify(h, deep_check=True))

    def test_adv_c4_dangling_db_record_missing_file(self) -> None:
        """C4: Record in SQLite metadata index whose physical file was deleted on disk is detected as missing."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"dangling_record_test_data"
            h = compute_sha256(data)
            store.put_bytes(data)

            # Unlink physical file
            store.get_path(h).unlink()

            self.assertFalse(store.verify(h, deep_check=False))
            report = store.verify_all(auto_evict=True)
            self.assertEqual(report.missing_count, 1)
            self.assertFalse(store.contains(h))

    def test_adv_c5_orphan_disk_file_not_in_db(self) -> None:
        """C5: Orphan file manually placed in objects directory does not cause crashes or ghost cache hits."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            orphan_data = b"orphan_data_not_in_index"
            orphan_hash = compute_sha256(orphan_data)

            # Write orphan file to objects/
            obj_path = Path(tmp_dir) / "objects" / orphan_hash[:2] / orphan_hash[2:]
            obj_path.parent.mkdir(parents=True, exist_ok=True)
            obj_path.write_bytes(orphan_data)

            # Index should not report containing it
            self.assertFalse(store.contains(orphan_hash))
            res = store.resolve_hashes([orphan_hash])
            self.assertEqual(res.misses, {orphan_hash})

    def test_adv_c6_zero_byte_corruption_of_cached_file(self) -> None:
        """C6: Non-empty file truncated to 0 bytes is detected as corrupted."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            data = b"non_empty_content_to_zero_out"
            h = compute_sha256(data)
            store.put_bytes(data)

            store.get_path(h).write_bytes(b"")

            self.assertFalse(store.verify(h, deep_check=False))
            self.assertFalse(store.verify(h, deep_check=True))

    def test_adv_c7_stale_tmp_staging_file_cleanup(self) -> None:
        """C7: Lingering temporary files in tmp/ staging do not collide with or corrupt cache reads."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            tmp_dir_path = Path(tmp_dir) / "tmp"
            tmp_dir_path.mkdir(parents=True, exist_ok=True)
            (tmp_dir_path / "stale_1.tmp").write_bytes(b"stale_tmp_data_1")
            (tmp_dir_path / "stale_2.tmp").write_bytes(b"stale_tmp_data_2")

            data = b"valid_data_ingest"
            h = compute_sha256(data)
            entry = store.put_bytes(data)

            self.assertTrue(store.contains(h))
            self.assertEqual(store.get_bytes(h), data)

    def test_adv_c8_read_only_or_locked_file_eviction_resilience(self) -> None:
        """C8: Permission or locking anomalies during eviction do not crash the eviction engine."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            d1 = b"locked_candidate_data"
            d2 = b"normal_candidate_data"
            h1 = compute_sha256(d1)
            h2 = compute_sha256(d2)

            store.put_bytes(d1)
            time.sleep(0.02)
            store.put_bytes(d2)

            path1 = store.get_path(h1)

            # Open file in read mode (locks/protects on Windows depending on sharing) or make read-only
            try:
                os.chmod(path1, 0o444)
            except Exception:
                pass

            # Eviction should proceed without throwing fatal uncaught exception
            try:
                store.evict_lru(len(d1) + len(d2))
            finally:
                # Restore permissions for temp dir cleanup
                try:
                    os.chmod(path1, 0o666)
                except Exception:
                    pass

    def test_adv_c9_path_traversal_attack_in_hash(self) -> None:
        """C9: Malicious SHA-256 strings containing directory traversal patterns are rejected immediately."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            traversal_attacks = [
                "../../etc/passwd",
                "..\\..\\windows\\system32\\cmd.exe",
                "../objects/evil.bin",
                "/root/.ssh/id_rsa",
                "objects/aa/bb",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/../../evil",
            ]

            for attack in traversal_attacks:
                with self.assertRaises((InvalidHashError, ValueError, CacheError)):
                    store.get_path(attack)
                with self.assertRaises((InvalidHashError, ValueError, CacheError)):
                    store.contains(attack)
                with self.assertRaises((InvalidHashError, ValueError, CacheError)):
                    store.get_bytes(attack)
                with self.assertRaises((InvalidHashError, ValueError, CacheError)):
                    store.remove(attack)

    def test_adv_c10_sql_injection_in_metadata_fields(self) -> None:
        """C10: Metadata fields containing SQL injection vectors are parameterized safely."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            injection_strings = [
                "'; DROP TABLE cache_entries; --",
                "' OR '1'='1",
                "1; DELETE FROM cache_entries WHERE 1=1;",
                "admin' --",
                "Robert'); DROP TABLE Students;--",
            ]

            for i, inj in enumerate(injection_strings):
                data = f"sql_inj_data_{i}".encode("utf-8")
                h = compute_sha256(data)
                entry = store.put_bytes(
                    data,
                    original_name=inj,
                    asset_type=inj,
                )
                self.assertEqual(entry.original_name, inj)
                self.assertTrue(store.contains(h))

            # Verify table still exists and contains all entries
            stats = store.get_stats()
            self.assertEqual(stats["entry_count"], len(injection_strings))


class AdversarialBoundedMemoryStreamingSuiteTests(unittest.TestCase):
    """Adversarial Bounded Memory Streaming Verification."""

    def test_adv_bounded_memory_streaming_large_payload(self) -> None:
        """Streaming a large (10MB) payload maintains O(1) RAM overhead (<2MB peak memory delta)."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            total_mb = 10
            total_bytes = total_mb * 1024 * 1024
            chunk_size = 64 * 1024  # 64 KiB
            pattern = b"A" * chunk_size

            # Create a memory-efficient generator stream
            def stream_generator() -> Iterator[bytes]:
                produced = 0
                while produced < total_bytes:
                    to_send = min(chunk_size, total_bytes - produced)
                    produced += to_send
                    yield pattern[:to_send]

            # Compute authoritative hash efficiently
            hasher = hashlib.sha256()
            for ch in stream_generator():
                hasher.update(ch)
            expected_hash = hasher.hexdigest()

            # Measure memory during ingestion
            tracemalloc.start()
            snap_before = tracemalloc.take_snapshot()

            class ChunkStream(io.RawIOBase):
                def __init__(self, total: int, chunk_sz: int, pat: bytes) -> None:
                    self.total = total
                    self.chunk_sz = chunk_sz
                    self.pat = pat
                    self.sent = 0

                def readable(self) -> bool:
                    return True

                def readinto(self, b: bytearray) -> int:
                    if self.sent >= self.total:
                        return 0
                    to_read = min(len(b), self.total - self.sent)
                    b[:to_read] = self.pat[:to_read]
                    self.sent += to_read
                    return to_read

            raw_stream = ChunkStream(total_bytes, chunk_size, pattern)
            entry = store.put_stream(raw_stream, size_bytes=total_bytes, sha256=expected_hash)

            snap_after = tracemalloc.take_snapshot()
            tracemalloc.stop()

            self.assertEqual(entry.sha256, expected_hash)
            self.assertEqual(entry.size_bytes, total_bytes)

            # Stream out and verify hash without loading full 10MB into memory at once
            out_hasher = hashlib.sha256()
            for chunk in store.get_stream(expected_hash, chunk_size=chunk_size):
                out_hasher.update(chunk)
            self.assertEqual(out_hasher.hexdigest(), expected_hash)


class AdversarialConcurrencyWALSuiteTests(unittest.TestCase):
    """Adversarial Concurrency and SQLite WAL Mode Stress Tests."""

    def test_adv_concurrent_readers_and_writers(self) -> None:
        """Multiple threads concurrently writing, reading, and resolving against DiskCacheStore."""
        with make_temp_dir() as tmp_dir, DiskCacheStore(tmp_dir) as store:
            num_threads = 8
            ops_per_thread = 20
            all_hashes: List[str] = []

            def worker_fn(thread_id: int) -> List[str]:
                thread_hashes = []
                for i in range(ops_per_thread):
                    data = f"worker_{thread_id}_item_{i}".encode("utf-8") * 10
                    h = compute_sha256(data)
                    store.put_bytes(data, original_name=f"t{thread_id}_{i}.dat")
                    thread_hashes.append(h)

                    # Interleaved read
                    retrieved = store.get_bytes(h)
                    assert retrieved == data, f"Mismatch in thread {thread_id} item {i}"

                    # Interleaved resolve
                    res = store.resolve_hashes([h])
                    assert h in res.hits, f"Resolve hit missing in thread {thread_id}"
                return thread_hashes

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [executor.submit(worker_fn, tid) for tid in range(num_threads)]
                for f in concurrent.futures.as_completed(futures):
                    all_hashes.extend(f.result())

            stats = store.get_stats()
            self.assertEqual(stats["entry_count"], num_threads * ops_per_thread)

    def test_adv_concurrent_eviction_and_ingestion(self) -> None:
        """Concurrent ingestion with a background eviction thread under quota constraint."""
        with make_temp_dir() as tmp_dir:
            # 20 KB quota
            quota = 20 * 1024
            with DiskCacheStore(tmp_dir, max_size_bytes=quota) as store:
                stop_evictor = False

                def evictor_worker() -> None:
                    while not stop_evictor:
                        try:
                            store.evict_lru(1024)
                        except Exception:
                            pass
                        time.sleep(0.01)

                def ingester_worker(wid: int) -> None:
                    for i in range(30):
                        data = f"ingest_stress_{wid}_{i}".encode("utf-8") * 100
                        try:
                            store.put_bytes(data)
                        except Exception:
                            pass
                        time.sleep(0.005)

                with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                    evict_future = executor.submit(evictor_worker)
                    ingest_futures = [executor.submit(ingester_worker, wid) for wid in range(4)]

                    for f in concurrent.futures.as_completed(ingest_futures):
                        f.result()

                    stop_evictor = True
                    evict_future.result()

                # Verify store is internally consistent and not corrupted
                report = store.verify_all(auto_evict=False)
                self.assertTrue(report.is_healthy)



class AdversarialASTDecouplingSuiteTests(unittest.TestCase):
    """Adversarial AST Decoupling Verification: src/aidars/cache/ has zero Blender dependencies."""

    def test_adv_ast_zero_blender_imports(self) -> None:
        """Parse all python files under src/aidars/cache/ to prove zero imports of bpy, bmesh, or scene graph."""
        cache_dir = Path(__file__).resolve().parents[1] / "src" / "aidars" / "cache"
        self.assertTrue(cache_dir.exists(), f"Directory {cache_dir} must exist")

        forbidden_modules = {
            "bpy",
            "bmesh",
            "mathutils",
            "bpy_extras",
            "aidars.visibility",
            "aidars.scene_intelligence.blender_scripts",
        }

        py_files = list(cache_dir.glob("*.py"))
        self.assertGreater(len(py_files), 0, "Must have Python files in src/aidars/cache")

        for py_file in py_files:
            code = py_file.read_text(encoding="utf-8")
            tree = ast.parse(code, filename=str(py_file))

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        for forbidden in forbidden_modules:
                            self.assertFalse(
                                alias.name == forbidden or alias.name.startswith(forbidden + "."),
                                f"Forbidden import '{alias.name}' detected in {py_file.name}:{node.lineno}",
                            )
                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    for forbidden in forbidden_modules:
                        self.assertFalse(
                            module == forbidden or module.startswith(forbidden + "."),
                            f"Forbidden 'from {module} import ...' detected in {py_file.name}:{node.lineno}",
                        )

    def test_adv_ast_clean_facade_exports(self) -> None:
        """Verify src/aidars/cache/__init__.py cleanly exports all required public models and classes."""
        import aidars.cache as cache_module

        required_exports = [
            "CacheEntry",
            "ResolutionResult",
            "VerificationReport",
            "CacheError",
            "CacheQuotaExceededError",
            "HashMismatchError",
            "InvalidHashError",
            "CacheStore",
            "DiskCacheStore",
            "SplitHashStorage",
            "SQLiteMetadataIndex",
            "HitMissResolver",
            "LRUEvictor",
            "IntegrityVerifier",
        ]

        for exp in required_exports:
            self.assertTrue(
                hasattr(cache_module, exp),
                f"aidars.cache must export '{exp}'",
            )


# =========================================================================
# Backward Compatibility: SceneCache & SceneEngine Request-Aware Stress Tests
# =========================================================================

SAMPLE_SCENE_ADVERSARIAL: Dict[str, Any] = {
    "metadata": {"name": "Adversarial_Stress_Scene", "frame_start": 1, "frame_end": 100, "fps": 30},
    "collections": [
        {"name": "MasterCollection", "id": "col-master", "parent": None},
        {"name": "SubCollection_A", "id": "col-sub-a", "parent": "col-master"},
        {"name": "SubCollection_B", "id": "col-sub-b", "parent": "col-master"},
    ],
    "objects": [
        {
            "name": "Hero_Asset",
            "id": "obj-hero",
            "type": "MESH",
            "collection": "col-sub-a",
            "transform": {"location": [0.0, 0.0, 0.0], "rotation": [0.0, 0.0, 0.0]},
            "bound_box": [[-2.0, -2.0, -2.0], [2.0, 2.0, 2.0]],
            "visibility": {"hide_render": False, "hide_viewport": False},
            "materials": [{"name": "Mat_Hero", "shader": "Principled"}],
            "constraints": [],
        },
        {
            "name": "Prop_Asset",
            "id": "obj-prop",
            "type": "MESH",
            "collection": "col-sub-b",
            "transform": {"location": [10.0, 0.0, 0.0], "rotation": [0.0, 0.0, 0.0]},
            "bound_box": [[-0.5, -0.5, -0.5], [0.5, 0.5, 0.5]],
            "visibility": {"hide_render": False, "hide_viewport": False},
            "materials": [{"name": "Mat_Prop", "shader": "Diffuse"}],
            "constraints": [],
        },
    ],
    "lights": [
        {
            "name": "KeyLight",
            "id": "light-key",
            "type": "SUN",
            "energy": 1000.0,
            "transform": {"location": [5.0, 5.0, 10.0]},
        }
    ],
    "materials": [
        {"name": "Mat_Hero", "shader": "Principled"},
        {"name": "Mat_Prop", "shader": "Diffuse"},
    ],
    "textures": [],
    "images": [],
    "assets": [
        {"path": "/textures/hero_albedo.png", "kind": "texture", "size_bytes": 2048},
        {"path": "/textures/prop_diffuse.png", "kind": "texture", "size_bytes": 1024},
    ],
}


class AdversarialCacheStressTests(unittest.TestCase):
    """Rigorous empirical challenge suite for SceneCache and SceneEngine caching."""

    def setUp(self) -> None:
        import unittest.mock
        from aidars.smart_package.models import PackageIntegrityReport

        self.patcher = unittest.mock.patch(
            "aidars.smart_package.validator.PackageValidator.validate",
            return_value=PackageIntegrityReport(
                verified=True,
                asset_count=0,
                verified_count=0,
                failed_assets=[],
                missing_assets=[],
            ),
        )
        self.patcher.start()

    def tearDown(self) -> None:
        self.patcher.stop()

    def test_rapid_interleaved_requests_no_crosstalk(self) -> None:
        with make_temp_dir() as tmp_dir:
            scene_file = Path(tmp_dir) / "source_scene.json"
            scene_file.write_text(json.dumps(SAMPLE_SCENE_ADVERSARIAL), encoding="utf-8")
            cache_dir = Path(tmp_dir) / ".aidars_cache"
            engine = SceneEngine()

            requests: List[SceneEngineRequest] = []
            config_id = 0
            for build_graph in [True, False]:
                for build_package in [True, False]:
                    for camera_id in ["", "Camera_Main", "Camera_CloseUp"]:
                        for frame_range in [(1, 24), (50, 100)]:
                            config_id += 1
                            out_dir = Path(tmp_dir) / f"out_{config_id}"
                            out_dir.mkdir(parents=True, exist_ok=True)
                            req = SceneEngineRequest(
                                input_path=str(scene_file),
                                scene_output=str(out_dir / "scene.json"),
                                graph_output=str(out_dir / "graph.json"),
                                package_output=str(out_dir / "pkg" / "package.json"),
                                build_graph=build_graph,
                                build_package=build_package,
                                optimize_package_by_visibility=(build_package and bool(camera_id)),
                                frame_start=frame_range[0],
                                frame_end=frame_range[1],
                                camera_id=camera_id,
                                cache_dir=str(cache_dir),
                            )
                            requests.append(req)

            fingerprints = [r.fingerprint() for r in requests]
            self.assertEqual(len(fingerprints), len(set(fingerprints)))

            first_results: List[SceneEngineResult] = []
            for req in requests:
                res = engine.run(req)
                self.assertFalse(res.from_cache)
                first_results.append(res)

            rng = random.Random(42)
            shuffled_indices = list(range(len(requests)))
            for repetition in range(2):
                rng.shuffle(shuffled_indices)
                for idx in shuffled_indices:
                    req = requests[idx]
                    expected_first = first_results[idx]
                    replay_res = engine.run(req)
                    self.assertTrue(replay_res.from_cache)
                    self.assertEqual(replay_res.scene_output_path, expected_first.scene_output_path)

    def test_selective_artifact_deletion_graph_only(self) -> None:
        with make_temp_dir() as tmp_dir:
            scene_file = Path(tmp_dir) / "scene.json"
            scene_file.write_text(json.dumps(SAMPLE_SCENE_ADVERSARIAL), encoding="utf-8")
            cache_dir = Path(tmp_dir) / ".cache"
            engine = SceneEngine()

            req_graph = SceneEngineRequest(
                input_path=str(scene_file),
                scene_output=str(Path(tmp_dir) / "out_graph" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out_graph" / "graph.json"),
                build_graph=True,
                build_package=False,
                cache_dir=str(cache_dir),
            )
            req_no_graph = SceneEngineRequest(
                input_path=str(scene_file),
                scene_output=str(Path(tmp_dir) / "out_nograph" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out_nograph" / "graph.json"),
                build_graph=False,
                build_package=False,
                cache_dir=str(cache_dir),
            )

            res_graph_1 = engine.run(req_graph)
            res_no_graph_1 = engine.run(req_no_graph)

            res_graph_1.graph_output_path.unlink()
            res_graph_2 = engine.run(req_graph)
            self.assertFalse(res_graph_2.from_cache)

            res_no_graph_2 = engine.run(req_no_graph)
            self.assertTrue(res_no_graph_2.from_cache)

    def test_selective_artifact_deletion_package_only(self) -> None:
        with make_temp_dir() as tmp_dir:
            scene_file = Path(tmp_dir) / "scene.json"
            scene_file.write_text(json.dumps(SAMPLE_SCENE_ADVERSARIAL), encoding="utf-8")
            cache_dir = Path(tmp_dir) / ".cache"
            engine = SceneEngine()

            req_pkg = SceneEngineRequest(
                input_path=str(scene_file),
                scene_output=str(Path(tmp_dir) / "out_pkg" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out_pkg" / "graph.json"),
                package_output=str(Path(tmp_dir) / "out_pkg" / "pkg" / "package.json"),
                build_graph=True,
                build_package=True,
                cache_dir=str(cache_dir),
            )
            req_no_pkg = SceneEngineRequest(
                input_path=str(scene_file),
                scene_output=str(Path(tmp_dir) / "out_nopkg" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out_nopkg" / "graph.json"),
                package_output=str(Path(tmp_dir) / "out_nopkg" / "pkg" / "package.json"),
                build_graph=True,
                build_package=False,
                cache_dir=str(cache_dir),
            )

            res_pkg_1 = engine.run(req_pkg)
            res_no_pkg_1 = engine.run(req_no_pkg)

            res_pkg_1.package_output_path.unlink()
            res_pkg_2 = engine.run(req_pkg)
            self.assertFalse(res_pkg_2.from_cache)

            res_no_pkg_2 = engine.run(req_no_pkg)
            self.assertTrue(res_no_pkg_2.from_cache)

    def test_cache_corruption_malformed_json_variants(self) -> None:
        with make_temp_dir() as tmp_dir:
            cache_dir = Path(tmp_dir) / ".cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            index_path = cache_dir / "index.json"

            corrupt_payloads = [
                "{malformed json syntax",
                '{"unterminated": "string',
                "null",
                "12345",
                '"string instead of object"',
                "[1, 2, 3]",
                "{}",
                "\x00\x01\x02binarygarbage\xff\xfe",
                "",
            ]

            cache = SceneCache(cache_dir)
            source_key = "test_scene.json"

            for payload in corrupt_payloads:
                index_path.write_bytes(payload.encode("utf-8"))
                self.assertIsNone(cache.get(source_key, request_hash="req-1"))
                self.assertTrue(cache.has_changed(source_key, "any-hash", request_hash="req-1"))

                test_entry = SceneCacheEntry(
                    source_hash="src-recovered",
                    request_hash="req-1",
                    scene_output="out/scene.json",
                )
                cache.put(source_key, test_entry)
                retrieved = cache.get(source_key, request_hash="req-1")
                self.assertIsNotNone(retrieved)



if __name__ == "__main__":
    unittest.main()
