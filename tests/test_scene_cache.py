import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.cache import (
    SceneCache,
    SceneCacheEntry,
    hash_blend_file,
    hash_json_payload,
    hash_source,
)


class SceneCacheTests(unittest.TestCase):
    def test_hash_json_payload_is_order_independent(self) -> None:
        a = hash_json_payload({"name": "Scene", "frame_start": 1})
        b = hash_json_payload({"frame_start": 1, "name": "Scene"})
        self.assertEqual(a, b)

    def test_hash_json_payload_changes_with_content(self) -> None:
        a = hash_json_payload({"name": "Scene"})
        b = hash_json_payload({"name": "OtherScene"})
        self.assertNotEqual(a, b)

    def test_hash_blend_file_is_content_based(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path_a = Path(tmp_dir) / "a.blend"
            path_b = Path(tmp_dir) / "b.blend"
            path_a.write_bytes(b"identical-bytes")
            path_b.write_bytes(b"identical-bytes")
            self.assertEqual(hash_blend_file(path_a), hash_blend_file(path_b))

            path_a.write_bytes(b"changed-bytes")
            self.assertNotEqual(hash_blend_file(path_a), hash_blend_file(path_b))

    def test_hash_source_dispatches_on_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            json_path = Path(tmp_dir) / "scene.json"
            json_path.write_text('{"name": "Scene"}', encoding="utf-8")

            from_path = hash_source(json_path)
            from_dict = hash_source({"name": "Scene"})
            self.assertEqual(from_path, from_dict)

    def test_cache_detects_no_change_and_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = SceneCache(Path(tmp_dir) / ".cache")
            source_key = "my_scene.json"

            self.assertTrue(cache.has_changed(source_key, "hash-v1"))

            cache.put(
                source_key,
                SceneCacheEntry(source_hash="hash-v1", scene_output="output/scene.json", graph_output="output/graph.json"),
            )

            self.assertFalse(cache.has_changed(source_key, "hash-v1"))
            self.assertTrue(cache.has_changed(source_key, "hash-v2"))

            entry = cache.get(source_key)
            self.assertEqual(entry.scene_output, "output/scene.json")

    def test_cache_survives_reload_from_disk(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_dir = Path(tmp_dir) / ".cache"
            SceneCache(cache_dir).put(
                "scene.json", SceneCacheEntry(source_hash="abc", scene_output="output/scene.json")
            )

            # A fresh SceneCache instance (simulating a new CLI process) must
            # see what a previous instance wrote to disk.
            reloaded = SceneCache(cache_dir)
            self.assertFalse(reloaded.has_changed("scene.json", "abc"))

    def test_invalidate_removes_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = SceneCache(Path(tmp_dir) / ".cache")
            cache.put("scene.json", SceneCacheEntry(source_hash="abc", scene_output="output/scene.json"))
            cache.invalidate("scene.json")
            self.assertIsNone(cache.get("scene.json"))

    def test_corrupt_index_is_treated_as_empty_not_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_dir = Path(tmp_dir) / ".cache"
            cache_dir.mkdir(parents=True)
            (cache_dir / "index.json").write_text("{not valid json", encoding="utf-8")

            cache = SceneCache(cache_dir)
            self.assertIsNone(cache.get("scene.json"))
            self.assertTrue(cache.has_changed("scene.json", "anything"))

    def test_cache_with_request_hash_isolation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = SceneCache(Path(tmp_dir) / ".cache")
            source_key = "scene.json"

            entry_a = SceneCacheEntry(
                source_hash="source-sha",
                request_hash="req-a",
                scene_output="out/scene.json",
                graph_output="out/graph.json",
                build_graph=True,
                build_package=False,
            )
            entry_b = SceneCacheEntry(
                source_hash="source-sha",
                request_hash="req-b",
                scene_output="out/scene.json",
                graph_output="out/graph.json",
                package_output="out/package.json",
                build_graph=True,
                build_package=True,
                frame_start=500,
                frame_end=600,
                camera_id="Cam.001",
            )

            cache.put(source_key, entry_a)
            cache.put(source_key, entry_b)

            retrieved_a = cache.get(source_key, request_hash="req-a")
            self.assertIsNotNone(retrieved_a)
            self.assertEqual(retrieved_a.request_hash, "req-a")
            self.assertFalse(retrieved_a.build_package)

            retrieved_b = cache.get(source_key, request_hash="req-b")
            self.assertIsNotNone(retrieved_b)
            self.assertEqual(retrieved_b.request_hash, "req-b")
            self.assertTrue(retrieved_b.build_package)
            self.assertEqual(retrieved_b.frame_start, 500)
            self.assertEqual(retrieved_b.frame_end, 600)
            self.assertEqual(retrieved_b.camera_id, "Cam.001")

            self.assertIsNone(cache.get(source_key, request_hash="non-existent"))

    def test_cache_get_rejects_empty_or_mismatched_request_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = SceneCache(Path(tmp_dir) / ".cache")
            source_key = "scene.json"

            # Put entry without request_hash
            cache.put(
                source_key,
                SceneCacheEntry(source_hash="source-1", request_hash="", scene_output="out/scene.json"),
            )

            # Query with specific request_hash must NOT match un-hashed fallback
            self.assertIsNone(cache.get(source_key, request_hash="req-specific"))

            # Put entry with request_hash "req-1"
            cache.put(
                source_key,
                SceneCacheEntry(source_hash="source-1", request_hash="req-1", scene_output="out/scene.json"),
            )
            # Query with mismatched request_hash must return None
            self.assertIsNone(cache.get(source_key, request_hash="req-2"))

    def test_cache_has_changed_detects_request_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = SceneCache(Path(tmp_dir) / ".cache")
            source_key = "scene.json"

            cache.put(
                source_key,
                SceneCacheEntry(source_hash="src-1", request_hash="", scene_output="out/scene.json"),
            )
            # Empty request_hash in cache vs requested "req-1" -> changed
            self.assertTrue(cache.has_changed(source_key, "src-1", request_hash="req-1"))

            cache.put(
                source_key,
                SceneCacheEntry(source_hash="src-1", request_hash="req-1", scene_output="out/scene.json"),
            )
            self.assertFalse(cache.has_changed(source_key, "src-1", request_hash="req-1"))
            self.assertTrue(cache.has_changed(source_key, "src-1", request_hash="req-2"))
            self.assertTrue(cache.has_changed(source_key, "src-2", request_hash="req-1"))

    def test_cache_verify_artifacts_all_stages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = SceneCache(Path(tmp_dir) / ".cache")
            source_key = "scene.json"

            scene_file = Path(tmp_dir) / "scene.json"
            graph_file = Path(tmp_dir) / "graph.json"
            package_file = Path(tmp_dir) / "package.json"

            entry = SceneCacheEntry(
                source_hash="src-1",
                request_hash="req-1",
                scene_output=str(scene_file),
                graph_output=str(graph_file),
                package_output=str(package_file),
                build_graph=True,
                build_package=True,
            )
            cache.put(source_key, entry)

            # Neither file exists
            self.assertIsNone(cache.get(source_key, request_hash="req-1", verify_artifacts=True))

            # Only scene exists
            scene_file.write_text("{}", encoding="utf-8")
            self.assertIsNone(cache.get(source_key, request_hash="req-1", verify_artifacts=True))

            # Scene + graph exist (package still missing)
            graph_file.write_text("{}", encoding="utf-8")
            self.assertIsNone(cache.get(source_key, request_hash="req-1", verify_artifacts=True))

            # All exist
            package_file.write_text("{}", encoding="utf-8")
            self.assertIsNotNone(cache.get(source_key, request_hash="req-1", verify_artifacts=True))

            # If build_package was False, missing package file is ignored
            entry_no_pkg = SceneCacheEntry(
                source_hash="src-1",
                request_hash="req-no-pkg",
                scene_output=str(scene_file),
                graph_output=str(graph_file),
                package_output=str(package_file),
                build_graph=True,
                build_package=False,
            )
            cache.put(source_key, entry_no_pkg)
            package_file.unlink()
            self.assertIsNotNone(cache.get(source_key, request_hash="req-no-pkg", verify_artifacts=True))

    def test_cache_entry_camera_id_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_dir = Path(tmp_dir) / ".cache"
            cache = SceneCache(cache_dir)
            source_key = "scene.json"

            entry = SceneCacheEntry(
                source_hash="src-1",
                request_hash="req-cam",
                scene_output="out/scene.json",
                camera_id="Main_Camera_4K",
            )
            cache.put(source_key, entry)

            # Reload cache from disk
            reloaded = SceneCache(cache_dir)
            fetched = reloaded.get(source_key, request_hash="req-cam")
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched.camera_id, "Main_Camera_4K")

    def test_invalidate_removes_both_base_and_request_keyed_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = SceneCache(Path(tmp_dir) / ".cache")
            source_key = "scene.json"

            cache.put(
                source_key,
                SceneCacheEntry(source_hash="src-1", request_hash="req-1", scene_output="out/1.json"),
            )
            cache.put(
                source_key,
                SceneCacheEntry(source_hash="src-1", request_hash="req-2", scene_output="out/2.json"),
            )

            cache.invalidate(source_key)

            self.assertIsNone(cache.get(source_key))
            self.assertIsNone(cache.get(source_key, request_hash="req-1"))
            self.assertIsNone(cache.get(source_key, request_hash="req-2"))


if __name__ == "__main__":
    unittest.main()
