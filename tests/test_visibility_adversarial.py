import json
import math
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.scene_engine import SceneEngine, SceneEngineRequest
from aidars.visibility import (
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
    VisibilityEngine,
)


class VisibilityEngineAdversarialTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = VisibilityEngine()
        self.analyzer = RenderRequirementAnalyzer()

    # =========================================================================
    # Section 1: Legacy Edge Cases & Regressions (Bugs 1 - 5)
    # =========================================================================

    def test_bug_1_camera_enclosing_object_occlusion_cascade(self) -> None:
        """BUG 1: An object straddling/enclosing the camera (0,0,0) causes hit_t = 0.0
        for all rays, incorrectly marking ALL other scene objects as occluded.
        """
        camera = {
            "transform": {"location": [0.0, 0.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "fov": 60.0,
            "clip_start": 0.1,
            "clip_end": 100.0,
        }
        scene = {
            "objects": [
                {
                    "name": "CamRigBox",
                    "id": "obj-camrig",
                    "transform": {"location": [0.0, 0.0, 0.0], "scale": [1.0, 1.0, 1.0]},
                    "bound_box": [[-0.5, -0.5, -0.5], [0.5, 0.5, 0.5]],
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "TargetCube",
                    "id": "obj-target",
                    "transform": {"location": [0.0, 10.0, 0.0], "scale": [2.0, 2.0, 2.0]},
                    "bound_box": [[-1.0, -1.0, -1.0], [1.0, 1.0, 1.0]],
                    "visibility": {"hide_render": False},
                },
            ]
        }
        result = self.engine.analyze(active_camera=camera, dependency_graph=scene)
        self.assertIn("TargetCube", result["visible_objects"])

    def test_bug_2_zero_resolution_x_crash(self) -> None:
        """BUG 2: Setting resolution_x = 0 in render_settings causes ZeroDivisionError."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "fov": 60.0,
        }
        scene = {
            "objects": [
                {
                    "name": "Cube",
                    "transform": {"location": [0.0, 0.0, 0.0]},
                    "visibility": {"hide_render": False},
                }
            ]
        }
        render_settings = {"resolution_x": 0, "resolution_y": 1080}
        result = self.engine.analyze(active_camera=camera, dependency_graph=scene, render_settings=render_settings)
        self.assertIn("Cube", result["visible_objects"])

    def test_bug_3_unhandled_non_numeric_transforms(self) -> None:
        """BUG 3: Non-numeric strings or invalid entries in location/scale cause ValueError/TypeError."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
        }
        scene = {
            "objects": [
                {
                    "name": "BadObj",
                    "transform": {"location": ["invalid", None, 0.0]},
                    "visibility": {"hide_render": False},
                }
            ]
        }
        result = self.engine.analyze(active_camera=camera, dependency_graph=scene)
        self.assertIsInstance(result, dict)

    def test_bug_4_object_rotation_ignored_in_world_aabb(self) -> None:
        """BUG 4: Object rotation is omitted when computing world AABB corners."""
        camera = {
            "transform": {"location": [0.0, 0.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "fov": 60.0,
            "clip_start": 0.1,
            "clip_end": 100.0,
        }
        scene = {
            "objects": [
                {
                    "name": "RotatedWall",
                    "id": "obj-wall",
                    "transform": {
                        "location": [0.0, 10.0, 0.0],
                        "rotation_euler": [0.0, 0.0, math.radians(90)],
                        "scale": [10.0, 0.1, 2.0],
                    },
                    "bound_box": [[-0.5, -0.5, -0.5], [0.5, 0.5, 0.5]],
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "SideTarget",
                    "id": "obj-side",
                    "transform": {"location": [2.0, 12.0, 0.0], "scale": [0.2, 0.2, 0.2]},
                    "bound_box": [[-0.5, -0.5, -0.5], [0.5, 0.5, 0.5]],
                    "visibility": {"hide_render": False},
                },
            ]
        }
        result = self.engine.analyze(active_camera=camera, dependency_graph=scene)
        self.assertIn("SideTarget", result["visible_objects"])

    def test_bug_5_material_name_resolution_failure(self) -> None:
        """BUG 5: required_materials returns node UUID instead of actual material name."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
        }
        scene = {
            "objects": [
                {
                    "name": "Obj1",
                    "id": "obj1_id",
                    "transform": {"location": [0.0, 0.0, 0.0]},
                    "visibility": {"hide_render": False},
                }
            ],
            "materials": [
                {"id": "mat_node_99", "name": "Mat_Real_Name", "image_textures": ["Albedo_Texture.png"]},
            ],
            "nodes": [
                {"identifier": "obj1_id", "label": "Obj1", "kind": "object"},
                {"identifier": "mat_node_99", "kind": "material"},
            ],
            "edges": [
                {"source": "obj1_id", "target": "mat_node_99", "relationship": "material"},
            ],
        }
        result = self.engine.analyze(active_camera=camera, dependency_graph=scene)
        self.assertIn("Mat_Real_Name", result["required_materials"])

    # =========================================================================
    # Section 2: Complex Camera Transforms, Extreme FOVs, Ortho & Degeneracy
    # =========================================================================

    def test_extreme_fov_zero_negative_and_excessive(self) -> None:
        """Adversarial FOV values: 0, negative, > 360 degrees, or non-numeric must not crash and fallback safely."""
        for bad_fov in [0.0, -45.0, 720.0, "not_a_number", None]:
            camera = {
                "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
                "camera": {"fov": bad_fov, "lens": 0.0, "sensor_width": -10.0},
            }
            scene = {
                "objects": [
                    {
                        "name": "Target",
                        "id": "obj-target",
                        "transform": {"location": [0.0, 0.0, 0.0]},
                        "visibility": {"hide_render": False},
                    }
                ]
            }
            report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
            self.assertIsInstance(report, RenderRequirementReport)
            self.assertIn("Target", report.required_objects)

    def test_degenerate_camera_gimbal_lock_and_zero_direction(self) -> None:
        """Camera aiming straight up (0,0,1), straight down (0,0,-1), or zero-length direction vector."""
        for dir_vec in [(0.0, 0.0, 1.0), (0.0, 0.0, -1.0), (0.0, 0.0, 0.0)]:
            camera = {
                "transform": {"location": [0.0, 0.0, -10.0], "direction": list(dir_vec)},
                "fov": 60.0,
            }
            scene = {
                "objects": [
                    {
                        "name": "Target",
                        "id": "obj-target",
                        "transform": {"location": [0.0, 0.0, 0.0]},
                        "visibility": {"hide_render": False},
                    }
                ]
            }
            report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
            self.assertIsInstance(report, RenderRequirementReport)

    def test_orthographic_extreme_scale_and_inverted_clipping(self) -> None:
        """Orthographic camera with extreme ortho_scale (1e8, 0, negative) or clip_start > clip_end."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "camera": {
                "type": "ORTHOGRAPHIC",
                "ortho_scale": -10.0,  # Negative ortho scale fallback
                "clip_start": 50.0,
                "clip_end": 10.0,      # Inverted clipping range
            },
        }
        scene = {
            "objects": [
                {
                    "name": "Target",
                    "id": "obj-target",
                    "transform": {"location": [0.0, 0.0, 0.0]},
                    "visibility": {"hide_render": False},
                }
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIsInstance(report, RenderRequirementReport)

    # =========================================================================
    # Section 3: Parent Hierarchy Cycles & Deep Nested Hierarchies
    # =========================================================================

    def test_parent_hierarchy_cyclic_dependency_safety(self) -> None:
        """Cyclic parent relationships (A -> B -> A, self-loop A -> A, multi-loop A -> B -> C -> A)
        must terminate immediately without infinite loop.
        """
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "fov": 60.0,
        }
        scene = {
            "objects": [
                {
                    "name": "A",
                    "id": "obj-a",
                    "parent": "obj-b",
                    "transform": {"location": [0.0, 0.0, 0.0]},  # In front of camera
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "B",
                    "id": "obj-b",
                    "parent": "obj-c",
                    "transform": {"location": [1000.0, 1000.0, 1000.0]},
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "C",
                    "id": "obj-c",
                    "parent": "obj-a",  # Creates cycle A -> B -> C -> A
                    "transform": {"location": [1000.0, 1000.0, 1000.0]},
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "SelfLoop",
                    "id": "obj-self",
                    "parent": "obj-self",  # Self-loop
                    "transform": {"location": [2.0, 0.0, 0.0]},
                    "visibility": {"hide_render": False},
                },
            ]
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIn("A", report.required_objects)
        self.assertIn("B", report.required_objects)
        self.assertIn("C", report.required_objects)
        self.assertIn("SelfLoop", report.required_objects)
        self.assertIn("PARENT_HIERARCHY", report.reasons.get("obj-b", []))
        self.assertIn("PARENT_HIERARCHY", report.reasons.get("obj-c", []))

    def test_deep_parent_hierarchy_50_levels(self) -> None:
        """Deep 50-level parent hierarchy where only the leaf node is camera-visible.
        All 50 ancestor rigs must be included with reason PARENT_HIERARCHY.
        """
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "fov": 60.0,
        }
        objects = []
        depth = 50
        for i in range(depth):
            obj_id = f"rig_{i}"
            parent_id = f"rig_{i+1}" if i < depth - 1 else None
            # Place only rig_0 in front of camera; ancestors are far offscreen
            loc = [0.0, 0.0, 0.0] if i == 0 else [1000.0 * i, 1000.0, 0.0]
            objects.append({
                "name": f"Rig_{i}",
                "id": obj_id,
                "parent": parent_id,
                "transform": {"location": loc},
                "visibility": {"hide_render": False},
            })

        scene = {"objects": objects}
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)

        # All 50 rigs must be required
        for i in range(depth):
            self.assertIn(f"Rig_{i}", report.required_objects)
        # Ancestors rig_1 to rig_49 must have PARENT_HIERARCHY reason
        for i in range(1, depth):
            self.assertIn("PARENT_HIERARCHY", report.reasons.get(f"rig_{i}", []))

    # =========================================================================
    # Section 4: Simulation Modifiers & Complex Influencers
    # =========================================================================

    def test_simulation_modifiers_comprehensive_preservation(self) -> None:
        """Test that all simulation keywords (CLOTH, FLUID, SMOKE, OCEAN, COLLISION,
        DYNAMIC_PAINT, PARTICLE, HAIR, ARMATURE, BONES) preserve offscreen objects with SIMULATION/ANIMATION_DRIVER.
        """
        sim_types = [
            ("ClothObj", "CLOTH", "SIMULATION"),
            ("FluidSimObj", "FLUID", "SIMULATION"),
            ("SmokeSimObj", "SMOKE", "SIMULATION"),
            ("OceanSimObj", "OCEAN", "SIMULATION"),
            ("CollisionObj", "COLLISION", "SIMULATION"),
            ("DynPaintObj", "DYNAMIC_PAINT", "SIMULATION"),
            ("HairObj", "HAIR", "SIMULATION"),
        ]

        objects = []
        for name, m_type, _ in sim_types:
            objects.append({
                "name": name,
                "id": f"id-{name}",
                "transform": {"location": [5000.0, 5000.0, 0.0]},  # Offscreen
                "modifiers": [{"name": f"Mod_{m_type}", "type": m_type}],
                "visibility": {"hide_render": False},
            })

        # Add Armature / Bones
        objects.append({
            "name": "ArmatureObj",
            "id": "id-armature",
            "type": "ARMATURE",
            "bones": [{"name": "RootBone"}],
            "transform": {"location": [5000.0, 5000.0, 0.0]},
            "visibility": {"hide_render": False},
        })

        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "fov": 60.0,
        }
        scene = {"objects": objects}
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)

        for name, _, expected_reason in sim_types:
            self.assertIn(name, report.required_objects)
            self.assertIn(expected_reason, report.reasons.get(f"id-{name}", []))

        self.assertIn("ArmatureObj", report.required_objects)
        self.assertIn("ANIMATION_DRIVER", report.reasons.get("id-armature", []))

    # =========================================================================
    # Section 5: Dependency Graph Cycles & Shared Material Deduplication
    # =========================================================================

    def test_dependency_graph_cycles_and_diamond_traversal(self) -> None:
        """Traverse diamond and cyclic dependency graphs without infinite recursion or duplicate entries."""
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "fov": 60.0,
        }
        scene = {
            "objects": [
                {
                    "name": "HeroObj1",
                    "id": "hero_1",
                    "mesh": {"name": "HeroMesh"},
                    "materials": [{"name": "DiamondMatA"}, {"name": "DiamondMatB"}],
                    "transform": {"location": [-1.0, 0.0, 0.0]},
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "HeroObj2",
                    "id": "hero_2",
                    "mesh": {"name": "HeroMesh"},  # Shared mesh
                    "materials": [{"name": "DiamondMatA"}],  # Shared material
                    "transform": {"location": [1.0, 0.0, 0.0]},
                    "visibility": {"hide_render": False},
                },
            ],
            "materials": [
                {"name": "DiamondMatA", "image_textures": ["SharedTex.png"]},
                {"name": "DiamondMatB", "image_textures": ["SharedTex.png", "UniqueTex.png"]},
            ],
            "nodes": [
                {"identifier": "DiamondMatA", "label": "DiamondMatA", "kind": "material"},
                {"identifier": "DiamondMatB", "label": "DiamondMatB", "kind": "material"},
                {"identifier": "tex:SharedTex", "label": "SharedTex.png", "kind": "texture"},
            ],
            "edges": [
                {"source": "DiamondMatA", "target": "DiamondMatB", "relationship": "material"},
                {"source": "DiamondMatB", "target": "DiamondMatA", "relationship": "material"},  # Cycle!
                {"source": "DiamondMatA", "target": "tex:SharedTex", "relationship": "texture"},
                {"source": "DiamondMatB", "target": "tex:SharedTex", "relationship": "texture"},
            ],
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)

        # Meshes deduplicated
        self.assertEqual(report.required_meshes, ["HeroMesh"])
        # Materials deduplicated and sorted
        self.assertEqual(report.required_materials, ["DiamondMatA", "DiamondMatB"])
        # Textures deduplicated and sorted
        self.assertEqual(report.required_textures, ["SharedTex.png", "UniqueTex.png"])
        self.assertEqual(report.required_images, ["SharedTex.png", "UniqueTex.png"])

    # =========================================================================
    # Section 6: None-Valued & Malformed Field Resilience
    # =========================================================================

    def test_malformed_and_none_valued_fields_resilience(self) -> None:
        """Scene with None values for modifiers, constraints, materials, particle_systems,
        animation, bound_box, and missing keys must not raise TypeError.
        """
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "fov": 60.0,
        }
        scene = {
            "objects": [
                {
                    "name": "NullHeavyObj",
                    "id": "obj-null",
                    "transform": None,
                    "bound_box": None,
                    "visibility": None,
                    "modifiers": None,
                    "constraints": None,
                    "materials": None,
                    "particle_systems": None,
                    "bones": None,
                    "animation": None,
                },
                {
                    "name": "EmptyDictObj",
                    "id": "obj-empty",
                    "transform": {},
                    "bound_box": [],
                    "visibility": {},
                    "modifiers": [],
                },
            ],
            "lights": None,
            "materials": None,
            "textures": None,
            "images": None,
            "nodes": None,
            "edges": None,
            "world": None,
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)
        self.assertIsInstance(report, RenderRequirementReport)
        self.assertIn("NullHeavyObj", report.required_objects)
        self.assertIn("EmptyDictObj", report.required_objects)

    # =========================================================================
    # Section 7: Reason Tracking Exhaustive Verification
    # =========================================================================

    def test_multi_reason_tracking_verification(self) -> None:
        """An entity that serves multiple functions (e.g. CAMERA_VISIBLE, LIGHT_SOURCE,
        and PARENT_HIERARCHY) must have all distinct reason tags tracked in RenderRequirementReport.
        """
        camera = {
            "transform": {"location": [0.0, -10.0, 0.0], "direction": [0.0, 1.0, 0.0]},
            "fov": 60.0,
        }
        scene = {
            "objects": [
                {
                    "name": "EmissiveParentLamp",
                    "id": "lamp-parent",
                    "type": "LIGHT",
                    "transform": {"location": [0.0, 0.0, 0.0]},  # In camera view
                    "visibility": {"hide_render": False},
                },
                {
                    "name": "ChildMesh",
                    "id": "child-mesh",
                    "parent": "lamp-parent",
                    "transform": {"location": [1.0, 0.0, 0.0]},  # In camera view
                    "modifiers": [{"name": "Cloth", "type": "CLOTH"}],
                    "visibility": {"hide_render": False},
                },
            ],
            "lights": [{"name": "EmissiveParentLamp", "id": "lamp-parent"}],
        }
        report = self.analyzer.analyze(snapshot=scene, active_camera=camera)

        lamp_reasons = report.reasons.get("lamp-parent", [])
        self.assertIn("CAMERA_VISIBLE", lamp_reasons)
        self.assertIn("LIGHT_SOURCE", lamp_reasons)
        self.assertIn("PARENT_HIERARCHY", lamp_reasons)

        child_reasons = report.reasons.get("child-mesh", [])
        self.assertIn("CAMERA_VISIBLE", child_reasons)
        self.assertIn("SIMULATION", child_reasons)

    # =========================================================================
    # Section 8: SceneEngine Pipeline Integration with Visibility Analysis
    # =========================================================================

    def test_scene_engine_integration_adversarial_workflow(self) -> None:
        """Verify SceneEngine.run() end-to-end with optimize_package_by_visibility=True
        under adversarial constraints.
        """
        sample_scene = {
            "metadata": {"name": "AdvScene", "frame_start": 1, "frame_end": 24, "fps": 24},
            "collections": [{"name": "Col1", "id": "col-1"}],
            "objects": [
                {
                    "name": "VisibleBox",
                    "id": "obj-vis",
                    "type": "MESH",
                    "transform": {"location": [0.0, 5.0, 0.0]},
                    "bound_box": [[-1.0, -1.0, -1.0], [1.0, 1.0, 1.0]],
                    "visibility": {"hide_render": False},
                    "materials": [{"name": "MatVis", "image_textures": ["vis.png"]}],
                },
                {
                    "name": "HiddenBox",
                    "id": "obj-hid",
                    "type": "MESH",
                    "transform": {"location": [500.0, 500.0, 0.0]},
                    "bound_box": [[-1.0, -1.0, -1.0], [1.0, 1.0, 1.0]],
                    "visibility": {"hide_render": False},
                    "materials": [{"name": "MatHid", "image_textures": ["hid.png"]}],
                },
            ],
            "materials": [
                {"name": "MatVis", "image_textures": ["vis.png"]},
                {"name": "MatHid", "image_textures": ["hid.png"]},
            ],
            "assets": [
                {"path": "vis.png", "kind": "texture", "size_bytes": 2048},
                {"path": "hid.png", "kind": "texture", "size_bytes": 4096},
            ],
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            input_file = Path(tmp_dir) / "adv_scene.json"
            input_file.write_text(json.dumps(sample_scene), encoding="utf-8")

            engine = SceneEngine()
            request = SceneEngineRequest(
                input_path=str(input_file),
                scene_output=str(Path(tmp_dir) / "out" / "scene.json"),
                graph_output=str(Path(tmp_dir) / "out" / "graph.json"),
                package_output=str(Path(tmp_dir) / "out" / "package.json"),
                build_graph=True,
                build_package=True,
                optimize_package_by_visibility=True,
                frame_start=1,
                frame_end=24,
            )
            result = engine.run(request)

            self.assertIsNotNone(result.render_requirements)
            self.assertIn("VisibleBox", result.render_requirements.required_objects)
            self.assertIn("HiddenBox", result.render_requirements.unused_objects)
            self.assertIn("MatVis", result.render_requirements.required_materials)
            self.assertIn("vis.png", result.render_requirements.required_textures)
            self.assertNotIn("MatHid", result.render_requirements.required_materials)


if __name__ == "__main__":
    unittest.main()
