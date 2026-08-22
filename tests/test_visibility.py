import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.engine import SceneIntelligenceEngine
from aidars.visibility.engine import VisibilityAnalyzer, VisibilityEngine


def _object(name: str, obj_id: str, *, hide_render: bool = False, animation: dict | None = None) -> dict:
    entry = {
        "name": name,
        "id": obj_id,
        "type": "MESH",
        "visibility": {"hide_render": hide_render, "hide_viewport": False},
    }
    if animation is not None:
        entry["animation"] = animation
    return entry


BASE_SCENE = {
    "metadata": {"name": "Demo", "frame_start": 1, "frame_end": 100, "fps": 24},
    "collections": [],
    "lights": [],
    "materials": [],
    "textures": [],
    "images": [],
}


class VisibilityAnalyzerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = SceneIntelligenceEngine()
        self.analyzer = VisibilityAnalyzer()

    def test_static_visible_and_hidden_objects(self) -> None:
        scene_data = dict(BASE_SCENE)
        scene_data["objects"] = [
            _object("Visible", "obj-visible", hide_render=False),
            _object("Hidden", "obj-hidden", hide_render=True),
        ]
        snapshot = self.engine.analyze_scene_data(scene_data)

        report = self.analyzer.analyze(snapshot, frame_start=1, frame_end=24)

        self.assertTrue(report.is_visible("obj-visible"))
        self.assertFalse(report.is_visible("obj-hidden"))
        self.assertIn("obj-hidden", report.hidden_object_ids)

    def test_animated_hide_render_curve_visible_in_target_range(self) -> None:
        # Hidden for frames 1-49 (value 1.0), then revealed at frame 50 (value 0.0).
        animation = {
            "fcurves": 1,
            "is_animated": True,
            "curves": [
                {
                    "name": "hide_render",
                    "data_path": "hide_render",
                    "array_index": 0,
                    "keyframes": [
                        {"frame": 1, "value": 1.0, "interpolation": "CONSTANT"},
                        {"frame": 50, "value": 0.0, "interpolation": "CONSTANT"},
                    ],
                    "interpolation": "CONSTANT",
                }
            ],
        }
        scene_data = dict(BASE_SCENE)
        scene_data["objects"] = [_object("Reveal", "obj-reveal", hide_render=True, animation=animation)]
        snapshot = self.engine.analyze_scene_data(scene_data)

        # A worker rendering frames 1-10 never sees it revealed.
        early_report = self.analyzer.analyze(snapshot, frame_start=1, frame_end=10)
        self.assertFalse(early_report.is_visible("obj-reveal"))

        # A worker rendering frames 40-60 catches the reveal at frame 50.
        straddling_report = self.analyzer.analyze(snapshot, frame_start=40, frame_end=60)
        self.assertTrue(straddling_report.is_visible("obj-reveal"))

        # A worker rendering frames 60-80 (after the reveal, no keyframe in
        # range) should still see it as visible via the held value.
        late_report = self.analyzer.analyze(snapshot, frame_start=60, frame_end=80)
        self.assertTrue(late_report.is_visible("obj-reveal"))

    def test_animation_present_but_no_hide_render_curve_falls_back_to_static(self) -> None:
        animation = {
            "fcurves": 1,
            "is_animated": True,
            "curves": [
                {
                    "name": "location",
                    "data_path": "location",
                    "array_index": 0,
                    "keyframes": [{"frame": 1, "value": 0.0, "interpolation": "LINEAR"}],
                    "interpolation": "LINEAR",
                }
            ],
        }
        scene_data = dict(BASE_SCENE)
        scene_data["objects"] = [_object("Moving", "obj-moving", hide_render=True, animation=animation)]
        snapshot = self.engine.analyze_scene_data(scene_data)

        report = self.analyzer.analyze(snapshot, frame_start=1, frame_end=24)
        self.assertFalse(report.is_visible("obj-moving"))


class VisibilityEngineTests(unittest.TestCase):
    """Comprehensive test suite for VisibilityEngine frustum culling, raycast occlusion, and R4 schema compliance."""

    def setUp(self) -> None:
        self.engine = VisibilityEngine()
        self.fixture_path = Path(__file__).resolve().parent / "fixtures" / "visibility_scene_payload.json"
        with open(self.fixture_path, "r", encoding="utf-8") as f:
            self.scene_payload = json.load(f)

    def test_evaluate_backward_compatibility(self) -> None:
        """Verify backward compatibility of VisibilityEngine.evaluate()."""
        state = self.engine.evaluate({"visibility": {"hide_render": True, "hide_viewport": False}})
        self.assertTrue(state.hidden)
        self.assertTrue(state.render_disabled)
        self.assertFalse(state.viewport_disabled)

    def test_frustum_culling_front_vs_behind(self) -> None:
        """Objects behind the camera view plane must be culled into unused_objects."""
        camera = self.scene_payload["active_camera"]
        render_settings = self.scene_payload["render_settings"]
        result = self.engine.analyze(active_camera=camera, dependency_graph=self.scene_payload, render_settings=render_settings)

        self.assertIn("Cube_Visible", result["visible_objects"])
        self.assertIn("Rock_Behind_Camera", result["unused_objects"])
        self.assertNotIn("Rock_Behind_Camera", result["visible_objects"])

    def test_frustum_culling_lateral_fov(self) -> None:
        """Objects outside horizontal FOV boundaries must be culled into unused_objects."""
        camera = self.scene_payload["active_camera"]
        render_settings = self.scene_payload["render_settings"]
        result = self.engine.analyze(active_camera=camera, dependency_graph=self.scene_payload, render_settings=render_settings)

        self.assertIn("Sphere_Outside_FOV", result["unused_objects"])
        self.assertNotIn("Sphere_Outside_FOV", result["visible_objects"])

    def test_frustum_culling_clipping_planes(self) -> None:
        """Objects beyond far clipping plane or closer than near clipping plane must be culled."""
        payload = json.loads(json.dumps(self.scene_payload))
        # Add an object far beyond clip_end (100.0)
        payload["objects"].append({
            "name": "Far_Object",
            "id": "obj-far",
            "transform": {"location": [0.0, 500.0, 0.0]},
            "bound_box": [[-1.0, -1.0, -1.0], [1.0, 1.0, 1.0]],
            "visibility": {"hide_render": False},
        })
        camera = payload["active_camera"]
        result = self.engine.analyze(active_camera=camera, dependency_graph=payload)

        self.assertIn("Far_Object", result["unused_objects"])
        self.assertNotIn("Far_Object", result["visible_objects"])

    def test_raycast_occlusion_query(self) -> None:
        """Geometry completely obscured behind an opaque object must be placed in unused_objects."""
        camera = self.scene_payload["active_camera"]
        render_settings = self.scene_payload["render_settings"]
        result = self.engine.analyze(active_camera=camera, dependency_graph=self.scene_payload, render_settings=render_settings)

        self.assertIn("Wall_Blocker", result["visible_objects"])
        self.assertIn("Hidden_Internal_Gear", result["unused_objects"])
        self.assertNotIn("Hidden_Internal_Gear", result["visible_objects"])

    def test_dependency_tracing_visible_materials_textures(self) -> None:
        """Visible objects trace active materials and sampled textures; unused asset chains are excluded."""
        camera = self.scene_payload["active_camera"]
        render_settings = self.scene_payload["render_settings"]
        result = self.engine.analyze(active_camera=camera, dependency_graph=self.scene_payload, render_settings=render_settings)

        # Visible objects: Cube_Visible and Wall_Blocker (both use Material_Wall)
        self.assertIn("Material_Wall", result["required_materials"])
        self.assertIn("Wall_Albedo.png", result["required_textures"])
        self.assertIn("Wall_Normal.png", result["required_textures"])

        # Hidden/culled objects materials and textures must NOT be required
        self.assertNotIn("Material_Rock", result["required_materials"])
        self.assertNotIn("Rock_Diffuse.png", result["required_textures"])
        self.assertNotIn("Material_Sphere", result["required_materials"])
        self.assertNotIn("Sphere_Noise.png", result["required_textures"])

    def test_unused_objects_isolation(self) -> None:
        """Verify unused_objects is the exact set inversion of all scene objects minus visible_objects."""
        camera = self.scene_payload["active_camera"]
        result = self.engine.analyze(active_camera=camera, dependency_graph=self.scene_payload)

        all_names = [obj["name"] for obj in self.scene_payload["objects"]]
        for name in result["visible_objects"]:
            self.assertNotIn(name, result["unused_objects"])

        for name in result["unused_objects"]:
            self.assertNotIn(name, result["visible_objects"])

        combined = set(result["visible_objects"]).union(set(result["unused_objects"]))
        self.assertEqual(combined, set(all_names))

    def test_exact_r4_output_json_schema(self) -> None:
        """Output dictionary must strictly adhere to the R4 JSON schema."""
        camera = self.scene_payload["active_camera"]
        result = self.engine.analyze(active_camera=camera, dependency_graph=self.scene_payload)

        expected_keys = {"visible_objects", "unused_objects", "required_materials", "required_textures"}
        self.assertEqual(set(result.keys()), expected_keys)

        for key in expected_keys:
            self.assertIsInstance(result[key], list)
            for item in result[key]:
                self.assertIsInstance(item, str)

        # Validate JSON serializability
        json_output = json.dumps(result)
        self.assertIsInstance(json_output, str)
        deserialized = json.loads(json_output)
        self.assertEqual(deserialized, result)

    def test_flexible_inputs_json_strings_and_dataclasses(self) -> None:
        """Support JSON strings as inputs for active_camera, dependency_graph, and render_settings."""
        camera_str = json.dumps(self.scene_payload["active_camera"])
        graph_str = json.dumps(self.scene_payload)
        render_str = json.dumps(self.scene_payload["render_settings"])

        result = self.engine.analyze(active_camera=camera_str, dependency_graph=graph_str, render_settings=render_str)

        self.assertIn("visible_objects", result)
        self.assertIn("Cube_Visible", result["visible_objects"])
        self.assertIn("Rock_Behind_Camera", result["unused_objects"])

    def test_dependency_graph_object_input(self) -> None:
        """Verify VisibilityEngine.analyze handles typed DependencyGraph and SceneSnapshot objects."""
        from aidars.scene_intelligence.dependency_graph import DependencyGraphBuilder

        engine = SceneIntelligenceEngine()
        snapshot = engine.analyze_scene_data(self.scene_payload)
        builder = DependencyGraphBuilder()
        dep_graph = builder.build(snapshot)

        camera = self.scene_payload["active_camera"]
        result = self.engine.analyze(active_camera=camera, dependency_graph=dep_graph)

        self.assertIn("visible_objects", result)
        self.assertIn("Cube_Visible", result["visible_objects"])
        self.assertIn("Material_Wall", result["required_materials"])
        self.assertIn("Wall_Albedo.png", result["required_textures"])


if __name__ == "__main__":
    unittest.main()


