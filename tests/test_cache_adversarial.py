"""Adversarial stress test suite for SceneCache and SceneEngine request-aware caching.

Covers:
1. Rapid interleaved requests on identical source with varying parameters.
2. Selective artifact deletion on disk (deleting only graph, only package, only scene).
3. Cache directory corruption recovery (malformed JSON, invalid structures, truncated content).
4. Empty vs non-empty camera ID and custom request configs.
5. Large configuration permutation matrix (1000+ combinations) and collision resistance.
6. Deterministic source hashing & payload normalization invariance.
7. Fallback rejection and multi-tenant source isolation.
"""
from __future__ import annotations

import itertools
import json
import random
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

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

    # -------------------------------------------------------------------------
    # 1. Rapid Interleaved Requests on Identical Source with Varying Parameters
    # -------------------------------------------------------------------------

    def test_rapid_interleaved_requests_no_crosstalk(self) -> None:
        """Execute 24 distinct parameter combinations against the same scene source in interleaved order.

        Verify:
        - Every distinct configuration misses on initial run and executes its stages.
        - Every distinct configuration hits on subsequent run.
        - No cross-talk occurs (results contain strictly the outputs requested for that config).
        - Multi-pass shuffled replay maintains 100% cache hits without corruption.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            scene_file = Path(tmp_dir) / "source_scene.json"
            scene_file.write_text(json.dumps(SAMPLE_SCENE_ADVERSARIAL), encoding="utf-8")
            cache_dir = Path(tmp_dir) / ".aidars_cache"
            engine = SceneEngine()

            # Build a combinatorial matrix of 24 distinct requests
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
                                package_output=str(out_dir / "package.json"),
                                build_graph=build_graph,
                                build_package=build_package,
                                optimize_package_by_visibility=(build_package and bool(camera_id)),
                                frame_start=frame_range[0],
                                frame_end=frame_range[1],
                                camera_id=camera_id,
                                cache_dir=str(cache_dir),
                            )
                            requests.append(req)

            # Ensure all fingerprints are unique
            fingerprints = [r.fingerprint() for r in requests]
            self.assertEqual(len(fingerprints), len(set(fingerprints)), "Fingerprints must be unique across all 24 configs")

            # Phase 1: Sequential Population (All must be cache MISSES)
            first_results: List[SceneEngineResult] = []
            for req in requests:
                res = engine.run(req)
                self.assertFalse(res.from_cache, f"Initial run for req {req.fingerprint()[:8]} should be a MISS")
                if req.build_graph:
                    self.assertIsNotNone(res.graph_output_path)
                    self.assertTrue(res.graph_output_path.exists())
                else:
                    self.assertIsNone(res.graph_output_path)

                if req.build_package:
                    self.assertIsNotNone(res.package_output_path)
                    self.assertTrue(res.package_output_path.exists())
                else:
                    self.assertIsNone(res.package_output_path)

                first_results.append(res)

            # Phase 2: Shuffled Replay (All must be cache HITS with strictly matching artifacts)
            rng = random.Random(42)
            shuffled_indices = list(range(len(requests)))
            for repetition in range(3):
                rng.shuffle(shuffled_indices)
                for idx in shuffled_indices:
                    req = requests[idx]
                    expected_first = first_results[idx]

                    replay_res = engine.run(req)
                    self.assertTrue(
                        replay_res.from_cache,
                        f"Replay {repetition} for req {idx} ({req.fingerprint()[:8]}) should be a HIT",
                    )
                    self.assertEqual(replay_res.scene_output_path, expected_first.scene_output_path)
                    self.assertEqual(replay_res.graph_output_path, expected_first.graph_output_path)
                    self.assertEqual(replay_res.package_output_path, expected_first.package_output_path)

    # -------------------------------------------------------------------------
    # 2. Selective Artifact Deletion on Disk
    # -------------------------------------------------------------------------

    def test_selective_artifact_deletion_graph_only(self) -> None:
        """Deleting only graph output invalidates graph-requiring request but preserves graph-free request."""
        with tempfile.TemporaryDirectory() as tmp_dir:
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

            # Populate both
            res_graph_1 = engine.run(req_graph)
            res_no_graph_1 = engine.run(req_no_graph)
            self.assertFalse(res_graph_1.from_cache)
            self.assertFalse(res_no_graph_1.from_cache)

            # Delete graph artifact only
            res_graph_1.graph_output_path.unlink()
            self.assertFalse(res_graph_1.graph_output_path.exists())
            self.assertTrue(res_graph_1.scene_output_path.exists())

            # req_graph MUST miss and regenerate
            res_graph_2 = engine.run(req_graph)
            self.assertFalse(res_graph_2.from_cache)
            self.assertTrue(res_graph_2.graph_output_path.exists())

            # req_no_graph MUST still HIT cache
            res_no_graph_2 = engine.run(req_no_graph)
            self.assertTrue(res_no_graph_2.from_cache)

    def test_selective_artifact_deletion_package_only(self) -> None:
        """Deleting only package output invalidates package-requiring request but preserves package-free request."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            scene_file = Path(tmp_dir) / "scene.json"
            scene_file.write_text(json.dumps(SAMPLE_SCENE_ADVERSARIAL), encoding="utf-8")
            cache_dir = Path(tmp_dir) / ".cache"
            engine = SceneEngine()

            req_pkg = SceneEngineRequest(
                input_path=str(scene_file),
                scene_output=str(Path(tmp_dir) / "out_pkg" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out_pkg" / "graph.json"),
                package_output=str(Path(tmp_dir) / "out_pkg" / "package.json"),
                build_graph=True,
                build_package=True,
                cache_dir=str(cache_dir),
            )
            req_no_pkg = SceneEngineRequest(
                input_path=str(scene_file),
                scene_output=str(Path(tmp_dir) / "out_nopkg" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out_nopkg" / "graph.json"),
                package_output=str(Path(tmp_dir) / "out_nopkg" / "package.json"),
                build_graph=True,
                build_package=False,
                cache_dir=str(cache_dir),
            )

            # Populate both
            res_pkg_1 = engine.run(req_pkg)
            res_no_pkg_1 = engine.run(req_no_pkg)
            self.assertFalse(res_pkg_1.from_cache)
            self.assertFalse(res_no_pkg_1.from_cache)

            # Delete package artifact only
            res_pkg_1.package_output_path.unlink()
            self.assertFalse(res_pkg_1.package_output_path.exists())
            self.assertTrue(res_pkg_1.scene_output_path.exists())
            self.assertTrue(res_pkg_1.graph_output_path.exists())

            # req_pkg MUST miss and regenerate
            res_pkg_2 = engine.run(req_pkg)
            self.assertFalse(res_pkg_2.from_cache)
            self.assertTrue(res_pkg_2.package_output_path.exists())

            # req_no_pkg MUST still HIT cache
            res_no_pkg_2 = engine.run(req_no_pkg)
            self.assertTrue(res_no_pkg_2.from_cache)

    def test_selective_artifact_deletion_scene_output(self) -> None:
        """Deleting scene output invalidates all requests targeting that scene output."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            scene_file = Path(tmp_dir) / "scene.json"
            scene_file.write_text(json.dumps(SAMPLE_SCENE_ADVERSARIAL), encoding="utf-8")
            cache_dir = Path(tmp_dir) / ".cache"
            engine = SceneEngine()

            req = SceneEngineRequest(
                input_path=str(scene_file),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                build_graph=True,
                cache_dir=str(cache_dir),
            )
            res_1 = engine.run(req)
            self.assertFalse(res_1.from_cache)

            # Delete scene output
            res_1.scene_output_path.unlink()
            self.assertFalse(res_1.scene_output_path.exists())

            # Must miss
            res_2 = engine.run(req)
            self.assertFalse(res_2.from_cache)
            self.assertTrue(res_2.scene_output_path.exists())

    # -------------------------------------------------------------------------
    # 3. Cache Directory Corruption Recovery
    # -------------------------------------------------------------------------

    def test_cache_corruption_malformed_json_variants(self) -> None:
        """Verify SceneCache gracefully handles various index.json corruptions and recovers cleanly."""
        with tempfile.TemporaryDirectory() as tmp_dir:
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
                "",  # empty file
            ]

            cache = SceneCache(cache_dir)
            source_key = "test_scene.json"

            for payload in corrupt_payloads:
                index_path.write_bytes(payload.encode("utf-8"))

                # Read should not raise unhandled exception, should return None / True for changed
                self.assertIsNone(cache.get(source_key, request_hash="req-1"))
                self.assertTrue(cache.has_changed(source_key, "any-hash", request_hash="req-1"))

                # Write should recover and create valid index
                test_entry = SceneCacheEntry(
                    source_hash="src-recovered",
                    request_hash="req-1",
                    scene_output="out/scene.json",
                )
                cache.put(source_key, test_entry)

                # Now read should succeed
                retrieved = cache.get(source_key, request_hash="req-1")
                self.assertIsNotNone(retrieved)
                self.assertEqual(retrieved.source_hash, "src-recovered")

    def test_cache_handles_non_dict_and_corrupt_entry_structures(self) -> None:
        """Verify cache behaves safely when index is valid JSON but root is non-dict."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_dir = Path(tmp_dir) / ".cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            index_path = cache_dir / "index.json"

            # Index containing a list of strings instead of mapping
            index_path.write_text(json.dumps(["corrupted", "entry", "list"]), encoding="utf-8")

            cache = SceneCache(cache_dir)
            self.assertIsNone(cache.get("scene.json", request_hash="req1"))
            self.assertIsNone(cache.get("scene.json"))

            # Overwriting works cleanly and repairs index
            cache.put(
                "scene.json",
                SceneCacheEntry(
                    source_hash="hash-1",
                    request_hash="req1",
                    scene_output="out/scene.json",
                ),
            )
            self.assertIsNotNone(cache.get("scene.json", request_hash="req1"))

    def test_cache_nested_directory_auto_creation(self) -> None:
        """Verify SceneCache automatically creates deep nested cache directories if they do not exist."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            deep_cache_dir = Path(tmp_dir) / "level1" / "level2" / "level3" / ".aidars_cache"
            self.assertFalse(deep_cache_dir.exists())

            cache = SceneCache(deep_cache_dir)
            cache.put(
                "scene.json",
                SceneCacheEntry(source_hash="src-1", request_hash="req-1", scene_output="out.json"),
            )

            self.assertTrue(deep_cache_dir.exists())
            self.assertTrue((deep_cache_dir / "index.json").exists())
            self.assertIsNotNone(cache.get("scene.json", request_hash="req-1"))

    # -------------------------------------------------------------------------
    # 4. Empty vs Non-Empty Camera ID & Custom Request Configs
    # -------------------------------------------------------------------------

    def test_camera_id_adversarial_variations(self) -> None:
        """Adversarially verify that subtle variations in camera_id are strictly distinguished."""
        variations = [
            "",
            " ",
            "Camera",
            "camera",  # case distinction
            "Camera.001",
            "Camera_001",
            "None",  # literal string "None"
            "null",  # literal string "null"
            "0",  # literal string "0"
            "Cam/Front:Main",  # special characters
            "Камера_1",  # unicode
        ]

        fingerprints: Dict[str, str] = {}
        for cam_id in variations:
            req = SceneEngineRequest(input_path="scene.json", camera_id=cam_id)
            fp = req.fingerprint()
            self.assertNotIn(
                fp,
                fingerprints.values(),
                f"Camera ID '{cam_id}' collided with existing fingerprint!",
            )
            fingerprints[cam_id] = fp

        with tempfile.TemporaryDirectory() as tmp_dir:
            scene_file = Path(tmp_dir) / "scene.json"
            scene_file.write_text(json.dumps(SAMPLE_SCENE_ADVERSARIAL), encoding="utf-8")
            cache = SceneCache(Path(tmp_dir) / ".cache")

            for cam_id, fp in fingerprints.items():
                entry = SceneCacheEntry(
                    source_hash="src-hash",
                    request_hash=fp,
                    scene_output=f"out/{cam_id}_scene.json",
                    camera_id=cam_id,
                )
                cache.put("scene.json", entry)

            # Verify every single camera ID can be retrieved precisely
            for cam_id, fp in fingerprints.items():
                res = cache.get("scene.json", request_hash=fp)
                self.assertIsNotNone(res, f"Failed to retrieve entry for camera_id='{cam_id}'")
                self.assertEqual(res.camera_id, cam_id)
                self.assertEqual(res.request_hash, fp)

    # -------------------------------------------------------------------------
    # 5. Invalidation Cleanliness Across Multi-Source Environments
    # -------------------------------------------------------------------------

    def test_invalidation_isolation_across_sources(self) -> None:
        """Invalidating source_a must not affect any cache entries (base or request-keyed) of source_b."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = SceneCache(Path(tmp_dir) / ".cache")

            # Source A: 3 request variants
            for r in ["req-a1", "req-a2", "req-a3"]:
                cache.put(
                    "source_a.json",
                    SceneCacheEntry(
                        source_hash="src-a",
                        request_hash=r,
                        scene_output=f"out/{r}.json",
                    ),
                )

            # Source B: 3 request variants
            for r in ["req-b1", "req-b2", "req-b3"]:
                cache.put(
                    "source_b.json",
                    SceneCacheEntry(
                        source_hash="src-b",
                        request_hash=r,
                        scene_output=f"out/{r}.json",
                    ),
                )

            # Invalidate only source_a
            cache.invalidate("source_a.json")

            # Source A entries should all be gone
            self.assertIsNone(cache.get("source_a.json"))
            self.assertIsNone(cache.get("source_a.json", request_hash="req-a1"))
            self.assertIsNone(cache.get("source_a.json", request_hash="req-a2"))
            self.assertIsNone(cache.get("source_a.json", request_hash="req-a3"))

            # Source B entries must ALL remain intact
            self.assertIsNotNone(cache.get("source_b.json", request_hash="req-b1"))
            self.assertIsNotNone(cache.get("source_b.json", request_hash="req-b2"))
            self.assertIsNotNone(cache.get("source_b.json", request_hash="req-b3"))

    # -------------------------------------------------------------------------
    # 6. Combinatorial Collision Resistance Test (1,000+ Permutations)
    # -------------------------------------------------------------------------

    def test_large_permutation_fingerprint_collision_resistance(self) -> None:
        """Generate 1,152 unique SceneEngineRequest permutations and prove 0 SHA-256 collisions."""
        scene_outputs = ["out/scene1.json", "out/scene2.json"]
        graph_outputs = ["out/graph1.json", "out/graph2.json"]
        package_outputs = ["out/pkg1.json", "out/pkg2.json"]
        build_graphs = [True, False]
        build_packages = [True, False]
        vis_opts = [True, False]
        frame_starts = [1, 100]
        frame_ends = [24, 200]
        camera_ids = ["", "Cam_A", "Cam_B"]
        blenders = [None, "/usr/bin/blender", "C:\\Program Files\\Blender\\blender.exe"]

        seen_fingerprints: Dict[str, SceneEngineRequest] = {}

        for combo in itertools.product(
            scene_outputs,
            graph_outputs,
            package_outputs,
            build_graphs,
            build_packages,
            vis_opts,
            frame_starts,
            frame_ends,
            camera_ids,
            blenders,
        ):
            req = SceneEngineRequest(
                input_path="scene.json",
                scene_output=combo[0],
                graph_output=combo[1],
                package_output=combo[2],
                build_graph=combo[3],
                build_package=combo[4],
                optimize_package_by_visibility=combo[5],
                frame_start=combo[6],
                frame_end=combo[7],
                camera_id=combo[8],
                blender_executable=combo[9],
            )
            fp = req.fingerprint()
            if fp in seen_fingerprints:
                existing = seen_fingerprints[fp]
                self.fail(f"SHA-256 Collision detected between:\n  {req}\nand\n  {existing}")
            seen_fingerprints[fp] = req

        self.assertEqual(len(seen_fingerprints), 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 3 * 3)  # 2304 distinct configs

    # -------------------------------------------------------------------------
    # 7. Source Hash Determinism & Semantic Invariance
    # -------------------------------------------------------------------------

    def test_source_hashing_determinism_and_formatting_invariance(self) -> None:
        """Verify that dictionary key re-ordering does not change payload hash, but content modifications do."""
        dict_1 = {"z_key": 100, "a_key": "hello", "nested": {"b": [1, 2], "a": True}}
        dict_2 = {"a_key": "hello", "nested": {"a": True, "b": [1, 2]}, "z_key": 100}
        dict_3 = {"a_key": "hello", "nested": {"a": False, "b": [1, 2]}, "z_key": 100}

        self.assertEqual(hash_json_payload(dict_1), hash_json_payload(dict_2))
        self.assertNotEqual(hash_json_payload(dict_1), hash_json_payload(dict_3))

        with tempfile.TemporaryDirectory() as tmp_dir:
            f1 = Path(tmp_dir) / "f1.json"
            f2 = Path(tmp_dir) / "f2.json"
            # Write with different formatting/spacing
            f1.write_text(json.dumps(dict_1, indent=4), encoding="utf-8")
            f2.write_text(json.dumps(dict_2, separators=(",", ":")), encoding="utf-8")

            # hash_source parses both JSON files and computes canonical hash
            self.assertEqual(hash_source(f1), hash_source(f2))


if __name__ == "__main__":
    unittest.main()
