import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.integrity import IntegrityReport
from aidars.scene_intelligence.scene_engine import SceneEngine, SceneEngineRequest

SAMPLE_SCENE = {
    "metadata": {"name": "Demo", "frame_start": 1, "frame_end": 24, "fps": 24},
    "collections": [
        {"name": "Main", "id": "col-main", "parent": None},
        {"name": "Empty", "id": "col-empty", "parent": None},
    ],
    "objects": [
        {
            "name": "Cube",
            "id": "obj-1",
            "type": "MESH",
            "collection": "col-main",
            "visibility": {"hide_render": False, "hide_viewport": False},
            "materials": [{"name": "Mat", "shader": "Principled"}],
            "constraints": [{"name": "Track", "type": "TRACK_TO", "target": "obj-ghost", "influence": 1.0}],
        }
    ],
    "lights": [],
    "materials": [{"name": "Mat", "shader": "Principled"}],
    "textures": [],
    "images": [],
    "assets": [{"path": "/assets/foo.png", "kind": "texture", "size_bytes": 1024}],
}


class SceneEngineTests(unittest.TestCase):
    """SceneEngine is the orchestration facade: business logic lives here,
    not in the CLI. These tests exercise it directly, the way a future API
    or GUI would, without going through argparse at all."""

    def test_run_produces_snapshot_graph_and_integrity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")

            engine = SceneEngine()
            request = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
            )
            result = engine.run(request)

            self.assertFalse(result.from_cache)
            self.assertIsNotNone(result.snapshot)
            self.assertIsNotNone(result.graph)
            self.assertIsInstance(result.integrity, IntegrityReport)
            self.assertIn("obj-ghost", result.integrity.missing_targets)
            self.assertTrue(any(node.identifier == "col-empty" for node in result.integrity.unused_nodes))
            self.assertTrue(any("referenced asset(s) could not be resolved" in w for w in result.warnings))
            self.assertTrue(any("appear unused" in w for w in result.warnings))
            self.assertTrue(result.scene_output_path.exists())
            self.assertTrue(result.graph_output_path.exists())

    def test_run_without_graph_skips_graph_and_integrity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")

            engine = SceneEngine()
            request = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                build_graph=False,
            )
            result = engine.run(request)

            self.assertIsNone(result.graph)
            self.assertIsNone(result.integrity)
            self.assertIsNone(result.graph_output_path)
            self.assertEqual(result.warnings, [])

    def test_run_with_package_builds_manifest_from_raw_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")

            engine = SceneEngine()
            request = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                build_package=True,
                package_output=str(Path(tmp_dir) / "out" / "package.json"),
                frame_start=1,
                frame_end=24,
            )
            result = engine.run(request)

            self.assertIsNotNone(result.package)
            self.assertEqual(len(result.package.assets), 1)
            self.assertEqual(result.package.assets[0].path, "/assets/foo.png")
            self.assertTrue(result.package_output_path.exists())

    def test_run_with_cache_dir_skips_reanalysis_on_unchanged_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")

            request = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                cache_dir=str(Path(tmp_dir) / ".cache"),
            )

            first = SceneEngine().run(request)
            self.assertFalse(first.from_cache)

            second = SceneEngine().run(request)
            self.assertTrue(second.from_cache)
            self.assertIsNone(second.snapshot)  # nothing re-analyzed
            self.assertEqual(second.scene_output_path, first.scene_output_path)

    def test_individual_stages_are_independently_callable(self) -> None:
        """A future API/GUI might want just one stage - e.g. only the graph,
        without writing a scene.json to disk at all."""
        engine = SceneEngine()
        snapshot = engine.analyze(SAMPLE_SCENE)
        graph = engine.build_dependency_graph(snapshot)
        integrity = engine.check_integrity(graph)

        self.assertGreater(len(graph.nodes), 0)
        self.assertIn("obj-ghost", integrity.missing_targets)

    def test_visibility_and_scheduling_stages_are_independently_callable(self) -> None:
        engine = SceneEngine()
        snapshot = engine.analyze(SAMPLE_SCENE)
        graph = engine.build_dependency_graph(snapshot)

        visibility = engine.analyze_visibility(snapshot, frame_start=1, frame_end=24)
        self.assertIn("obj-1", visibility.visible_object_ids)

        plan = engine.build_scheduling_plan(SAMPLE_SCENE, snapshot, graph, frame_start=1, frame_end=24, worker_count=2)
        self.assertEqual(len(plan.chunks), 2)
        covered = sorted(f for chunk in plan.chunks for f in range(chunk.frame_start, chunk.frame_end + 1))
        self.assertEqual(covered, list(range(1, 25)))

    def test_per_request_blender_executable_overrides_engine_default(self) -> None:
        """Regression test: request.blender_executable was previously stored
        but silently ignored by run()/load_source() in favor of whatever the
        SceneEngine happened to be constructed with. A single long-lived
        engine instance must be able to serve different requests that each
        specify their own Blender executable."""
        fake_payload = (
            '{"metadata": {"name": "external", "frame_start": 1, "frame_end": 1, "fps": 24}, '
            '"collections": [], "objects": [], "lights": [], "materials": [], '
            '"textures": [], "images": []}'
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            if os.name == "nt":
                fake_blender = Path(tmp_dir) / "fake_blender.cmd"
                fake_blender.write_text(f"@echo off\r\necho {fake_payload}", encoding="utf-8")
            else:
                fake_blender = Path(tmp_dir) / "fake_blender.sh"
                fake_blender.write_text(f"#!/bin/sh\ncat <<'EOF'\n{fake_payload}\nEOF\n", encoding="utf-8")
                fake_blender.chmod(fake_blender.stat().st_mode | 0o111)

            blend_path = Path(tmp_dir) / "example.blend"
            blend_path.write_text("placeholder", encoding="utf-8")

            # Constructed with NO executable - would fall back to the
            # placeholder/embedded-bpy path unless the per-call override works.
            engine = SceneEngine()
            scene_data = engine.load_source(blend_path, blender_executable=str(fake_blender))
            self.assertEqual(scene_data.metadata.name, "external")


if __name__ == "__main__":
    unittest.main()
