import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.integrity import IntegrityReport
from aidars.scene_intelligence.scene_engine import SceneEngine, SceneEngineRequest
from aidars.visibility import RenderRequirementReport

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
            "transform": {"location": [0.0, 5.0, 0.0]},
            "bound_box": [[-1.0, -1.0, -1.0], [1.0, 1.0, 1.0]],
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

    def test_cache_miss_when_pipeline_configuration_changes(self) -> None:
        """Cache correctness: changing requested stages/parameters (e.g. adding build_package)
        must trigger execution and not return an incomplete cached result."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            cache_dir = str(Path(tmp_dir) / ".cache")

            # Run A: build_graph=True, build_package=False
            req_a = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                build_package=False,
                cache_dir=cache_dir,
            )
            res_a = SceneEngine().run(req_a)
            self.assertFalse(res_a.from_cache)

            # Run B: same source file, but build_package=True with frame range 100-200
            req_b = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                build_package=True,
                package_output=str(Path(tmp_dir) / "out" / "package_b.json"),
                frame_start=100,
                frame_end=200,
                cache_dir=cache_dir,
            )
            res_b = SceneEngine().run(req_b)
            self.assertFalse(res_b.from_cache)
            self.assertIsNotNone(res_b.package_plan)
            self.assertTrue(res_b.package_output_path.exists())

    def test_cache_miss_when_cached_file_deleted_on_disk(self) -> None:
        """Cache correctness: if a cached output file is deleted on disk, cache must miss."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            cache_dir = str(Path(tmp_dir) / ".cache")

            request = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                cache_dir=cache_dir,
            )
            res_1 = SceneEngine().run(request)
            self.assertFalse(res_1.from_cache)

            # Delete the scene output file
            res_1.scene_output_path.unlink()

            # Next run must miss and recreate the file
            res_2 = SceneEngine().run(request)
            self.assertFalse(res_2.from_cache)
            self.assertTrue(res_2.scene_output_path.exists())

    def test_run_with_m3_render_requirement_optimization(self) -> None:
        """Verify SceneEngine connects to M3 RenderRequirementReport when optimizing packaging."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")

            request = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                build_package=True,
                optimize_package_by_visibility=True,
                frame_start=1,
                frame_end=24,
            )
            result = SceneEngine().run(request)

            self.assertIsNotNone(result.render_requirements)
            self.assertIsInstance(result.render_requirements, RenderRequirementReport)
            self.assertIn("Cube", result.render_requirements.required_objects)

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

            engine = SceneEngine()
            scene_data = engine.load_source(blend_path, blender_executable=str(fake_blender))
            self.assertEqual(scene_data.metadata.name, "external")

    def test_cache_interleaved_multi_request_hits(self) -> None:
        """Verify that distinct requests on the same scene coexist in cache without clobbering each other."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            cache_dir = str(Path(tmp_dir) / ".cache")
            engine = SceneEngine()

            # Request A: build_graph=True, build_package=False
            req_a = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene_a.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph_a.json"),
                build_package=False,
                cache_dir=cache_dir,
            )

            # Request B: build_graph=True, build_package=True, frame range 500-600
            req_b = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene_b.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph_b.json"),
                build_package=True,
                package_output=str(Path(tmp_dir) / "out" / "pkg_b" / "package_b.json"),
                frame_start=500,
                frame_end=600,
                cache_dir=cache_dir,
            )

            # First run: A -> misses cache
            res_a1 = engine.run(req_a)
            self.assertFalse(res_a1.from_cache)

            # First run: B -> misses cache
            res_b1 = engine.run(req_b)
            self.assertFalse(res_b1.from_cache)
            self.assertIsNotNone(res_b1.package_plan)
            self.assertTrue(res_b1.package_output_path.exists())

            # Second run: A -> hits cache
            res_a2 = engine.run(req_a)
            self.assertTrue(res_a2.from_cache)
            self.assertEqual(res_a2.scene_output_path, res_a1.scene_output_path)

            # Second run: B -> hits cache
            res_b2 = engine.run(req_b)
            self.assertTrue(res_b2.from_cache)
            self.assertEqual(res_b2.package_output_path, res_b1.package_output_path)

    def test_cache_miss_when_graph_output_deleted(self) -> None:
        """Verify deleting dependency graph output artifact forces re-analysis even if scene output exists."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            cache_dir = str(Path(tmp_dir) / ".cache")
            engine = SceneEngine()

            request = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                build_graph=True,
                cache_dir=cache_dir,
            )

            res_1 = engine.run(request)
            print(res_1.messages)
            self.assertFalse(res_1.from_cache)
            self.assertTrue(res_1.graph_output_path.exists())

            # Delete only graph artifact
            res_1.graph_output_path.unlink()

            # Next run must miss and regenerate graph
            res_2 = engine.run(request)
            self.assertFalse(res_2.from_cache)
            self.assertTrue(res_2.graph_output_path.exists())

    def test_cache_miss_when_package_output_deleted(self) -> None:
        """Verify deleting package manifest output artifact forces re-analysis when build_package=True."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            cache_dir = str(Path(tmp_dir) / ".cache")
            engine = SceneEngine()

            request = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                build_package=True,
                package_output=str(Path(tmp_dir) / "out" / "package.json"),
                cache_dir=cache_dir,
            )

            res_1 = engine.run(request)
            print(res_1.messages)
            self.assertFalse(res_1.from_cache)
            self.assertTrue(res_1.package_output_path.exists())

            # Delete only package artifact
            res_1.package_output_path.unlink()

            # Next run must miss and regenerate package
            res_2 = engine.run(request)
            self.assertFalse(res_2.from_cache)
            self.assertTrue(res_2.package_output_path.exists())

    def test_cache_camera_id_changes_cause_cache_miss(self) -> None:
        """Verify distinct camera_id values produce separate cache keys and trigger re-execution."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            cache_dir = str(Path(tmp_dir) / ".cache")
            engine = SceneEngine()

            req_cam1 = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene_cam1.json"),
                camera_id="Camera_Main",
                cache_dir=cache_dir,
            )
            req_cam2 = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene_cam2.json"),
                camera_id="Camera_Side",
                cache_dir=cache_dir,
            )

            self.assertNotEqual(req_cam1.fingerprint(), req_cam2.fingerprint())

            res_1 = engine.run(req_cam1)
            self.assertFalse(res_1.from_cache)

            res_2 = engine.run(req_cam2)
            self.assertFalse(res_2.from_cache)

            # Both should now hit cache independently
            res_1_hit = engine.run(req_cam1)
            self.assertTrue(res_1_hit.from_cache)
            res_2_hit = engine.run(req_cam2)
            self.assertTrue(res_2_hit.from_cache)

    def test_cache_visibility_flag_changes_cause_cache_miss(self) -> None:
        """Verify toggling optimize_package_by_visibility causes cache miss and executes visibility stage."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "scene.json"
            input_path.write_text(json.dumps(SAMPLE_SCENE), encoding="utf-8")
            cache_dir = str(Path(tmp_dir) / ".cache")
            engine = SceneEngine()

            req_plain = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                build_package=True,
                package_output=str(Path(tmp_dir) / "out" / "pkg" / "package.json"),
                optimize_package_by_visibility=False,
                cache_dir=cache_dir,
            )
            req_vis = SceneEngineRequest(
                input_path=str(input_path),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                build_package=True,
                package_output=str(Path(tmp_dir) / "out" / "pkg" / "package.json"),
                optimize_package_by_visibility=True,
                cache_dir=cache_dir,
            )

            res_plain = engine.run(req_plain)
            self.assertFalse(res_plain.from_cache)
            self.assertIsNone(res_plain.render_requirements)

            res_vis = engine.run(req_vis)
            self.assertFalse(res_vis.from_cache)
            self.assertIsNotNone(res_vis.render_requirements)

            res_vis_hit = engine.run(req_vis)
            self.assertTrue(res_vis_hit.from_cache)

    def test_request_fingerprint_exhaustive_field_sensitivity(self) -> None:
        """Verify that every single configuration attribute changes the request fingerprint."""
        base = SceneEngineRequest(input_path="scene.json")
        base_fp = base.fingerprint()

        variations = [
            SceneEngineRequest(input_path="scene.json", scene_output="other_scene.json"),
            SceneEngineRequest(input_path="scene.json", graph_output="other_graph.json"),
            SceneEngineRequest(input_path="scene.json", package_output="other_package.json"),
            SceneEngineRequest(input_path="scene.json", build_graph=False),
            SceneEngineRequest(input_path="scene.json", build_package=True),
            SceneEngineRequest(input_path="scene.json", optimize_package_by_visibility=True),
            SceneEngineRequest(input_path="scene.json", frame_start=10),
            SceneEngineRequest(input_path="scene.json", frame_end=99),
            SceneEngineRequest(input_path="scene.json", camera_id="Cam_Wide"),
            SceneEngineRequest(input_path="scene.json", blender_executable="blender-custom"),
        ]

        fingerprints = {base_fp}
        for v in variations:
            fp = v.fingerprint()
            self.assertNotIn(fp, fingerprints, f"Variation {v} produced duplicate fingerprint {fp}")
            fingerprints.add(fp)


if __name__ == "__main__":
    unittest.main()
