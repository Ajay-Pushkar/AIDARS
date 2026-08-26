import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.adapters.blender.intelligence.engine import SceneIntelligenceEngine
from aidars.adapters.blender.intelligence.dependency_graph import DependencyGraphBuilder
from aidars.adapters.blender.visibility import (
    BoundingBox,
    CameraAnalyzer,
    CameraModel,
    CullingResult,
    FrustumCuller,
    InfluenceAnalyzer,
    RenderRequest,
    RenderRequirementAnalyzer,
    RenderRequirementReport,
    RequirementReason,
    VisibilityAnalyzer,
)


class RenderRequirementAnalysisTests(unittest.TestCase):
    """Exhaustive test suite for Milestone 3 Render Requirement Analysis."""

    def setUp(self) -> None:
        self.analyzer = RenderRequirementAnalyzer()
        self.intelligence = SceneIntelligenceEngine()
        self.graph_builder = DependencyGraphBuilder()

    def test_case_1_object_outside_frustum_excluded(self) -> None:
        """Test 1: Object outside camera frustum is marked as unused and not visible."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0, "clip_start": 0.1, "clip_end": 100.0},
        }
        scene = {
            "objects": [
                {
                    "name": "OutsideObject",
                    "id": "obj-outside",
                    "type": "MESH",
                    "transform": {"location": [100.0, 50.0, 0.0], "scale": [1.0, 1.0, 1.0]},
                    "bound_box": [[-1.0, -1.0, -1.0], [1.0, 1.0, 1.0]],
                    "visibility": {"hide_render": False},
                }
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertNotIn("OutsideObject", report.required_objects)
        self.assertIn("OutsideObject", report.unused_objects)

    def test_case_2_object_inside_frustum_included(self) -> None:
        """Test 2: Object directly in camera frustum is included and given CAMERA_VISIBLE reason."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0, "clip_start": 0.1, "clip_end": 100.0},
        }
        scene = {
            "objects": [
                {
                    "name": "InsideObject",
                    "id": "obj-inside",
                    "type": "MESH",
                    "transform": {"location": [0.0, 10.0, 0.0], "scale": [1.0, 1.0, 1.0]},
                    "bound_box": [[-1.0, -1.0, -1.0], [1.0, 1.0, 1.0]],
                    "visibility": {"hide_render": False},
                }
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIn("InsideObject", report.required_objects)
        self.assertIn("CAMERA_VISIBLE", report.reasons.get("obj-inside", []) or report.reasons.get("InsideObject", []))

    def test_case_3_object_touching_frustum_boundary_included(self) -> None:
        """Test 3: Conservative guarantee: Object straddling/touching boundary is KEPT."""
        # FOV 60 deg -> half angle 30 deg, tan(30) = 0.57735
        # At y = 10.0 (dist 20.0 from camera at y = -10.0), frustum x boundary = 20.0 * 0.57735 = 11.547
        # Placing object at x = 12.0 with half-width 1.0 touches the boundary at x = 11.0
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0, "clip_start": 0.1, "clip_end": 100.0},
        }
        scene = {
            "objects": [
                {
                    "name": "BoundaryObject",
                    "id": "obj-boundary",
                    "type": "MESH",
                    "transform": {"location": [12.0, 10.0, 0.0], "scale": [1.0, 1.0, 1.0]},
                    "bound_box": [[-1.0, -1.0, -1.0], [1.0, 1.0, 1.0]],
                    "visibility": {"hide_render": False},
                }
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIn("BoundaryObject", report.required_objects)

    def test_case_4_hidden_object_excluded_unless_influencer(self) -> None:
        """Test 4: Object with hide_render=True inside frustum is excluded if not influencing render."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0},
        }
        scene = {
            "objects": [
                {
                    "name": "HiddenObject",
                    "id": "obj-hidden",
                    "type": "MESH",
                    "transform": {"location": [0.0, 5.0, 0.0]},
                    "visibility": {"hide_render": True},
                }
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertNotIn("HiddenObject", report.required_objects)
        self.assertIn("HiddenObject", report.unused_objects)

    def test_case_5_animated_reveal_frame_range_culling(self) -> None:
        """Test 5 & 6: Object revealed at frame 50 is excluded for 1-40, but included for 40-60."""
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
                }
            ],
        }
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0},
        }
        scene = {
            "objects": [
                {
                    "name": "AnimatedReveal",
                    "id": "obj-reveal",
                    "type": "MESH",
                    "transform": {"location": [0.0, 10.0, 0.0]},
                    "visibility": {"hide_render": True},
                    "animation": animation,
                }
            ]
        }

        # Request 1-40: Excluded
        req_early = RenderRequest(camera_id="cam", frame_start=1, frame_end=40)
        report_early = self.analyzer.analyze(snapshot=scene, active_camera=camera, request=req_early)
        self.assertNotIn("AnimatedReveal", report_early.required_objects)

        # Request 40-60: Included
        req_mid = RenderRequest(camera_id="cam", frame_start=40, frame_end=60)
        report_mid = self.analyzer.analyze(snapshot=scene, active_camera=camera, request=req_mid)
        self.assertIn("AnimatedReveal", report_mid.required_objects)

    def test_case_7_full_dependency_closure(self) -> None:
        """Test 7: Dependency closure traces Object -> Material -> Texture -> Image."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0},
        }
        scene = {
            "objects": [
                {
                    "name": "Chair",
                    "id": "obj-chair",
                    "type": "MESH",
                    "transform": {"location": [0.0, 5.0, 0.0]},
                    "mesh": {"name": "Chair_Mesh"},
                    "materials": [{"name": "Material_Wood", "image_textures": ["Texture_Wood.png"]}],
                    "visibility": {"hide_render": False},
                }
            ],
            "materials": [
                {"name": "Material_Wood", "image_textures": ["Texture_Wood.png"]},
            ],
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIn("Chair", report.required_objects)
        self.assertIn("Chair_Mesh", report.required_meshes)
        self.assertIn("Material_Wood", report.required_materials)
        self.assertIn("Texture_Wood.png", report.required_textures)
        self.assertIn("Texture_Wood.png", report.required_images)

    def test_case_8_shared_material_deduplication(self) -> None:
        """Test 8: Two objects sharing one material result in exactly one material entry."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0},
        }
        scene = {
            "objects": [
                {
                    "name": "Chair1",
                    "id": "obj-c1",
                    "transform": {"location": [-2.0, 5.0, 0.0]},
                    "materials": [{"name": "Shared_Material"}],
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "Chair2",
                    "id": "obj-c2",
                    "transform": {"location": [2.0, 5.0, 0.0]},
                    "materials": [{"name": "Shared_Material"}],
                    "visibility": {"hide_render": False},
                },
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertEqual(report.required_materials.count("Shared_Material"), 1)

    def test_case_9_active_lights_preserved(self) -> None:
        """Test 9: Active lights in scene are preserved with reason LIGHT_SOURCE."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0},
        }
        scene = {
            "objects": [
                {
                    "name": "SunLight",
                    "id": "light-sun",
                    "type": "LIGHT",
                    "transform": {"location": [0.0, 0.0, 100.0]},
                }
            ],
            "lights": [{"name": "SunLight", "id": "light-sun", "type": "SUN"}],
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIn("SunLight", report.required_objects)
        self.assertIn("LIGHT_SOURCE", report.reasons.get("light-sun", []) or report.reasons.get("SunLight", []))

    def test_case_10_parent_hierarchy_preserved(self) -> None:
        """Test 10: Parent of a visible object is included with reason PARENT_HIERARCHY."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0},
        }
        scene = {
            "objects": [
                {
                    "name": "InvisibleParentRig",
                    "id": "obj-parent",
                    "type": "EMPTY",
                    "transform": {"location": [1000.0, 1000.0, 1000.0]},  # Way off-screen
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "VisibleChild",
                    "id": "obj-child",
                    "type": "MESH",
                    "parent": "obj-parent",
                    "transform": {"location": [0.0, 5.0, 0.0]},
                    "visibility": {"hide_render": False},
                },
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIn("VisibleChild", report.required_objects)
        self.assertIn("InvisibleParentRig", report.required_objects)
        self.assertIn("PARENT_HIERARCHY", report.reasons.get("obj-parent", []))

    def test_case_11_simulation_modifier_safety(self) -> None:
        """Test 11: Objects with active simulation modifiers are preserved with reason SIMULATION."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0},
        }
        scene = {
            "objects": [
                {
                    "name": "OffscreenClothEmitter",
                    "id": "obj-cloth",
                    "type": "MESH",
                    "transform": {"location": [500.0, 0.0, 0.0]},  # Offscreen
                    "modifiers": [{"name": "ClothSim", "type": "CLOTH"}],
                    "visibility": {"hide_render": False},
                }
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIn("OffscreenClothEmitter", report.required_objects)
        self.assertIn("SIMULATION", report.reasons.get("obj-cloth", []))

    def test_case_12_orthographic_camera_projection(self) -> None:
        """Test 12: Orthographic projection culls correctly based on ortho_scale."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"type": "ORTHOGRAPHIC", "ortho_scale": 10.0, "clip_start": 0.1, "clip_end": 50.0},
        }
        scene = {
            "objects": [
                {
                    "name": "OrthoInside",
                    "id": "obj-in",
                    "transform": {"location": [2.0, 5.0, 0.0]},
                    "bound_box": [[-0.5, -0.5, -0.5], [0.5, 0.5, 0.5]],
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "OrthoOutside",
                    "id": "obj-out",
                    "transform": {"location": [100.0, 5.0, 0.0]},
                    "bound_box": [[-0.5, -0.5, -0.5], [0.5, 0.5, 0.5]],
                    "visibility": {"hide_render": False},
                },
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIn("OrthoInside", report.required_objects)
        self.assertNotIn("OrthoOutside", report.required_objects)

    def test_case_13_report_serialization_contracts(self) -> None:
        """Test 13: Report serialization produces both canonical M3 detailed dict and R4 dict."""
        req = RenderRequest(camera_id="cam-main", frame_start=1, frame_end=10, resolution=(1920, 1080))
        camera = {
            "id": "cam-main",
            "transform": {"location": [0.0, -10.0, 0.0], "rotation_euler": [math.pi / 2, 0.0, 0.0]},
            "camera": {"fov": 60.0},
        }
        scene = {
            "objects": [
                {
                    "name": "Box",
                    "id": "obj-box",
                    "transform": {"location": [0.0, 0.0, 0.0]},
                    "materials": [{"name": "Mat1", "image_textures": ["Tex1.png"]}],
                    "visibility": {"hide_render": False},
                }
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera, request=req)

        # Canonical M3 detailed report
        detailed = report.to_dict()
        self.assertEqual(detailed["request"]["camera"], "cam-main")
        self.assertEqual(detailed["request"]["frame_start"], 1)
        self.assertEqual(detailed["request"]["frame_end"], 10)
        self.assertIn("Box", detailed["required"]["objects"])
        self.assertIn("Mat1", detailed["required"]["materials"])
        self.assertIn("Tex1.png", detailed["required"]["textures"])
        self.assertTrue(detailed["analysis"]["conservative"])

        # Milestone 3 R4 schema
        r4 = report.to_r4_dict()
        self.assertIn("Box", r4["visible_objects"])
        self.assertIn("Mat1", r4["required_materials"])
        self.assertIn("Tex1.png", r4["required_textures"])


if __name__ == "__main__":
    unittest.main()
