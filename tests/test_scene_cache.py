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


if __name__ == "__main__":
    unittest.main()
