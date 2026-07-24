import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.cli import main

SAMPLE_SCENE = {
    "metadata": {"name": "Demo", "frame_start": 1, "frame_end": 24, "fps": 24},
    "collections": [{"name": "Main", "id": "col-main", "parent": None}],
    "objects": [
        {
            "name": "Cube",
            "id": "obj-1",
            "type": "MESH",
            "collection": "col-main",
            "visibility": {"hide_render": False, "hide_viewport": False},
            "materials": [{"name": "Mat", "shader": "Principled"}],
        }
    ],
    "lights": [],
    "materials": [{"name": "Mat", "shader": "Principled"}],
    "textures": [],
    "images": [],
}


class CliTests(unittest.TestCase):
    def test_main_writes_scene_and_dependency_graph(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            scene_out = Path(tmp_dir) / "out" / "scene.json"
            graph_out = Path(tmp_dir) / "out" / "graph.json"

            rc = main(
                [
                    str(input_path),
                    "-o",
                    str(scene_out),
                    "--graph-output",
                    str(graph_out),
                ]
            )

            self.assertEqual(rc, 0)
            self.assertTrue(scene_out.exists())
            self.assertTrue(graph_out.exists())

            graph_payload = json.loads(graph_out.read_text(encoding="utf-8"))
            self.assertIn("nodes", graph_payload)
            self.assertIn("integrity", graph_payload)

    def test_no_graph_flag_skips_dependency_graph_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            scene_out = Path(tmp_dir) / "out" / "scene.json"
            graph_out = Path(tmp_dir) / "out" / "graph.json"

            rc = main([str(input_path), "-o", str(scene_out), "--graph-output", str(graph_out), "--no-graph"])

            self.assertEqual(rc, 0)
            self.assertTrue(scene_out.exists())
            self.assertFalse(graph_out.exists())

    def test_cache_dir_skips_reanalysis_on_unchanged_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            scene_out = Path(tmp_dir) / "out" / "scene.json"
            graph_out = Path(tmp_dir) / "out" / "graph.json"
            cache_dir = Path(tmp_dir) / ".cache"

            args = [
                str(input_path),
                "-o",
                str(scene_out),
                "--graph-output",
                str(graph_out),
                "--cache-dir",
                str(cache_dir),
            ]

            first_rc = main(args)
            self.assertEqual(first_rc, 0)
            first_mtime = scene_out.stat().st_mtime_ns

            # Second run with unchanged input should skip re-analysis and
            # print the "reusing cached outputs" message rather than
            # rewriting the scene snapshot.
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                second_rc = main(args)
            self.assertEqual(second_rc, 0)
            self.assertIn("reusing cached outputs", buffer.getvalue())
            self.assertEqual(scene_out.stat().st_mtime_ns, first_mtime)

            # Changing the input content should trigger re-analysis again.
            changed_scene = dict(SAMPLE_SCENE)
            changed_scene["metadata"] = {**SAMPLE_SCENE["metadata"], "name": "DemoChanged"}
            input_path.write_text(json.dumps(changed_scene), encoding="utf-8")

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                third_rc = main(args)
            self.assertEqual(third_rc, 0)
            self.assertNotIn("reusing cached outputs", buffer.getvalue())
            self.assertGreater(scene_out.stat().st_mtime_ns, first_mtime)


if __name__ == "__main__":
    unittest.main()
