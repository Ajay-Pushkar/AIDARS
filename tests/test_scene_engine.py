import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.engine import SceneIntelligenceEngine
from aidars.scene_intelligence.exporters import JsonSceneExporter
from aidars.scene_intelligence.dependency_graph import DependencyGraphBuilder
from aidars.scene_intelligence.loader import SceneLoader
from aidars.scene_intelligence.scanner import SceneScanner
from aidars.scene_intelligence.blender_adapter import BlenderAdapter


class SceneIntelligenceEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = SceneIntelligenceEngine()
        self.scene_data = {
            "metadata": {
                "name": "Test Scene",
                "frame_start": 1,
                "frame_end": 24,
                "fps": 24,
            },
            "collections": [
                {"name": "Characters", "id": "col-1", "parent": None},
                {"name": "Props", "id": "col-2", "parent": "col-1"},
            ],
            "objects": [
                {
                    "name": "Cube",
                    "id": "obj-1",
                    "type": "MESH",
                    "collection": "col-1",
                    "parent": None,
                    "children": ["obj-2"],
                    "visibility": {"hide_render": False, "hide_viewport": False},
                    "mesh": {"name": "CubeMesh", "vertex_count": 8, "face_count": 12},
                    "materials": [{"name": "MatRed", "shader": "Principled BSDF"}],
                    "modifiers": [
                        {
                            "name": "Subdivision",
                            "type": "SUBSURF",
                            "subtype": "Simple",
                            "show_viewport": True,
                            "show_render": True,
                            "settings": {"levels": 2},
                        }
                    ],
                    "constraints": [
                        {
                            "name": "Limit Rotation",
                            "type": "LIMIT_ROTATION",
                            "target": "obj-2",
                            "influence": 0.75,
                            "settings": {"min_x": -1.0},
                        }
                    ],
                    "animation": {
                        "fcurves": 3,
                        "is_animated": True,
                        "curves": [
                            {
                                "name": "location.x",
                                "data_path": "location",
                                "array_index": 0,
                                "keyframes": [{"frame": 1, "value": 0.0, "interpolation": "LINEAR"}],
                            }
                        ],
                    },
                },
                {
                    "name": "Camera",
                    "id": "obj-2",
                    "type": "CAMERA",
                    "collection": "col-2",
                    "parent": "obj-1",
                    "visibility": {"hide_render": False, "hide_viewport": False},
                    "camera": {"name": "MainCamera", "lens": 35.0},
                },
            ],
            "lights": [{"name": "Sun", "id": "light-1", "type": "SUN"}],
            "materials": [{"name": "MatRed", "shader": "Principled BSDF"}],
            "textures": [{"name": "TexNoise", "source": "noise.png"}],
            "images": [{"name": "noise.png", "filepath": "/tmp/noise.png"}],
        }

    def test_analyze_scene_data_builds_snapshot(self) -> None:
        snapshot = self.engine.analyze_scene_data(self.scene_data)

        self.assertEqual(snapshot.metadata.name, "Test Scene")
        self.assertEqual(snapshot.statistics.object_count, 2)
        self.assertEqual(snapshot.statistics.collection_count, 2)
        self.assertEqual(snapshot.statistics.camera_count, 1)
        self.assertEqual(snapshot.statistics.light_count, 1)
        self.assertEqual(snapshot.statistics.material_count, 1)
        self.assertEqual(snapshot.statistics.texture_count, 1)
        self.assertEqual(snapshot.statistics.image_count, 1)
        self.assertEqual(snapshot.statistics.animated_object_count, 1)
        self.assertEqual(snapshot.statistics.visible_object_count, 2)
        self.assertEqual(snapshot.objects[0].name, "Cube")
        self.assertEqual(snapshot.objects[0].mesh.vertex_count, 8)
        self.assertEqual(snapshot.objects[0].children, ["obj-2"])
        self.assertEqual(snapshot.objects[0].modifiers[0].settings["levels"], 2)
        self.assertEqual(snapshot.objects[0].constraints[0].target, "obj-2")
        self.assertEqual(snapshot.objects[0].animation.curves[0].keyframes[0].frame, 1)
        self.assertEqual(snapshot.relationships[0].relationship, "parent")

    def test_analyze_scene_data_extracts_transform_details(self) -> None:
        snapshot = self.engine.analyze_scene_data(
            {
                "metadata": {"name": "Transform Scene", "frame_start": 1, "frame_end": 24, "fps": 24},
                "collections": [],
                "objects": [
                    {
                        "name": "Cube",
                        "id": "obj-1",
                        "type": "MESH",
                        "collection": None,
                        "parent": None,
                        "children": [],
                        "visibility": {"hide_render": False, "hide_viewport": False},
                        "transform": {
                            "location": [1.0, 2.0, 3.0],
                            "rotation_euler": [0.1, 0.2, 0.3],
                            "rotation_quaternion": [0.0, 0.0, 0.0, 1.0],
                            "scale": [2.0, 2.0, 2.0],
                        },
                        "mesh": {"name": "CubeMesh", "vertex_count": 8, "face_count": 12},
                    }
                ],
                "lights": [],
                "materials": [],
                "textures": [],
                "images": [],
            }
        )

        self.assertEqual(snapshot.objects[0].transform.location, [1.0, 2.0, 3.0])
        self.assertEqual(snapshot.objects[0].transform.rotation_euler, [0.1, 0.2, 0.3])
        self.assertEqual(snapshot.objects[0].transform.rotation_quaternion, [0.0, 0.0, 0.0, 1.0])
        self.assertEqual(snapshot.objects[0].transform.scale, [2.0, 2.0, 2.0])

    def test_exporter_writes_json_file(self) -> None:
        snapshot = self.engine.analyze_scene_data(self.scene_data)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "scene.json"
            JsonSceneExporter.write_json(snapshot, output_path)

            self.assertTrue(output_path.exists())
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["metadata"]["scene_name"], "Test Scene")
            self.assertEqual(payload["metadata"]["statistics"]["object_count"], 2)

    def test_dependency_graph_builder_returns_placeholder_graph(self) -> None:
        snapshot = self.engine.analyze_scene_data(self.scene_data)
        graph = DependencyGraphBuilder().build(snapshot)

        self.assertTrue(graph.nodes)
        self.assertTrue(graph.edges)
        self.assertEqual(graph.edges[0].relationship, "parent")

    def test_scene_level_materials_preserve_full_detail(self) -> None:
        # Regression test: the scene-level material library previously kept
        # only name/shader and silently dropped node_tree/image_textures/settings.
        scene_data = dict(self.scene_data)
        scene_data["materials"] = [
            {
                "name": "MatRed",
                "shader": "Principled BSDF",
                "node_tree": "MatRed_NodeTree",
                "image_textures": ["TexNoise"],
                "settings": {"roughness": 0.4},
            }
        ]
        snapshot = self.engine.analyze_scene_data(scene_data)

        material = snapshot.materials[0]
        self.assertEqual(material.node_tree, "MatRed_NodeTree")
        self.assertEqual(material.image_textures, ["TexNoise"])
        self.assertEqual(material.settings, {"roughness": 0.4})

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "scene.json"
            JsonSceneExporter.write_json(snapshot, output_path)
            payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["materials"][0]["shader"], "Principled BSDF")
        self.assertEqual(payload["materials"][0]["textures"], ["TexNoise"])

    def test_dependency_graph_deduplicates_shared_assets(self) -> None:
        # Two objects sharing the same material/texture should not create
        # duplicate nodes or duplicate edges in the dependency graph.
        scene_data = dict(self.scene_data)
        shared_material = {
            "name": "SharedMat",
            "shader": "Principled BSDF",
            "image_textures": ["SharedTex"],
        }
        scene_data["objects"] = [
            {
                "name": "ObjA",
                "id": "obj-a",
                "type": "MESH",
                "visibility": {"hide_render": False, "hide_viewport": False},
                "materials": [shared_material],
            },
            {
                "name": "ObjB",
                "id": "obj-b",
                "type": "MESH",
                "visibility": {"hide_render": False, "hide_viewport": False},
                "materials": [shared_material],
            },
        ]

        snapshot = self.engine.analyze_scene_data(scene_data)
        graph = DependencyGraphBuilder().build(snapshot)

        material_nodes = [node for node in graph.nodes if node.identifier == "material:SharedMat"]
        texture_nodes = [node for node in graph.nodes if node.identifier == "texture:SharedTex"]
        self.assertEqual(len(material_nodes), 1)
        self.assertEqual(len(texture_nodes), 1)

        material_edges = [
            edge for edge in graph.edges if edge.target == "material:SharedMat" and edge.relationship == "material"
        ]
        self.assertEqual(len(material_edges), 2)  # one from each object, but each only once

    def test_dependency_graph_flags_missing_and_unused_assets(self) -> None:
        scene_data = dict(self.scene_data)
        scene_data["collections"] = [
            {"name": "Characters", "id": "col-1", "parent": None},
            {"name": "EmptyOrphanCollection", "id": "col-empty", "parent": None},
        ]
        scene_data["objects"] = [
            {
                "name": "Orphan",
                "id": "obj-orphan",
                "type": "MESH",
                "collection": "col-1",
                "visibility": {"hide_render": False, "hide_viewport": False},
                "constraints": [
                    {"name": "Track", "type": "TRACK_TO", "target": "does-not-exist", "influence": 1.0}
                ],
            }
        ]

        snapshot = self.engine.analyze_scene_data(scene_data)
        graph = DependencyGraphBuilder().build(snapshot)

        # A constraint pointing at an id no object/collection ever defined.
        self.assertIn("does-not-exist", graph.find_missing_targets())

        # No object was ever assigned to "EmptyOrphanCollection".
        unused_ids = {node.identifier for node in graph.find_unused_nodes()}
        self.assertIn("col-empty", unused_ids)
        self.assertNotIn("col-1", unused_ids)
        self.assertNotIn("obj-orphan", unused_ids)

    def test_referenced_assets_flow_through_full_pipeline(self) -> None:
        scene_data = dict(self.scene_data)
        scene_data["objects"] = [
            {
                "name": "LinkedChair",
                "id": "obj-chair",
                "type": "MESH",
                "visibility": {"hide_render": False, "hide_viewport": False},
                "referenced_assets": ["/assets/chair.blend"],
            }
        ]

        snapshot = self.engine.analyze_scene_data(scene_data)
        self.assertEqual(snapshot.objects[0].referenced_assets, ["/assets/chair.blend"])

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "scene.json"
            JsonSceneExporter.write_json(snapshot, output_path)
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        # referenced_assets are not printed into objects in the new v1 schema
        self.assertEqual(payload["objects"][0].get("referenced_assets"), None)

        graph = DependencyGraphBuilder().build(snapshot)
        asset_node = next(node for node in graph.nodes if node.kind == "asset")
        self.assertEqual(asset_node.identifier, "asset:/assets/chair.blend")
        self.assertTrue(
            any(
                edge.source == "obj-chair" and edge.target == asset_node.identifier and edge.relationship == "references"
                for edge in graph.edges
            )
        )
        # A known, resolved reference is not a "missing" target.
        self.assertNotIn(asset_node.identifier, graph.find_missing_targets())

    def test_scene_scanner_builds_snapshot_from_payload(self) -> None:
        scanner = SceneScanner(engine=self.engine)
        snapshot = scanner.scan(self.scene_data)

        self.assertEqual(snapshot.metadata.name, "Test Scene")
        self.assertEqual(snapshot.statistics.object_count, 2)

    def test_scene_loader_uses_adapter_and_scanner(self) -> None:
        class StubAdapter:
            def load_scene(self, source: str) -> dict:
                self.last_source = source
                return self.scene_data

            def __init__(self) -> None:
                self.last_source = None
                self.scene_data = {
                    "metadata": {"name": "Loaded Scene", "frame_start": 1, "frame_end": 10, "fps": 24},
                    "collections": [],
                    "objects": [],
                    "lights": [],
                    "materials": [],
                    "textures": [],
                    "images": [],
                }

        adapter = StubAdapter()
        loader = SceneLoader(adapter=adapter)
        snapshot = loader.load("example.blend")

        self.assertEqual(snapshot.metadata.name, "Loaded Scene")
        self.assertEqual(adapter.last_source, "example.blend")

    def test_blender_adapter_uses_external_blender_when_available(self) -> None:
        fake_payload = (
            '{"metadata": {"name": "external", "frame_start": 1, "frame_end": 120, "fps": 24}, '
            '"collections": [], "objects": [], "lights": [], "materials": [], '
            '"textures": [], "images": []}'
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            if os.name == "nt":
                fake_blender = Path(tmp_dir) / "fake_blender.cmd"
                fake_blender.write_text(
                    f"@echo off\r\necho {fake_payload}",
                    encoding="utf-8",
                )
            else:
                # subprocess.run executes this file directly (no shell), so it
                # needs a shebang and the executable bit set - a plain .cmd
                # batch file (Windows-only syntax) is neither runnable nor
                # marked executable on POSIX systems.
                fake_blender = Path(tmp_dir) / "fake_blender.sh"
                fake_blender.write_text(
                    f"#!/bin/sh\ncat <<'EOF'\n{fake_payload}\nEOF\n",
                    encoding="utf-8",
                )
                fake_blender.chmod(fake_blender.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

            blend_path = Path(tmp_dir) / "example.blend"
            blend_path.write_text("placeholder", encoding="utf-8")

            adapter = BlenderAdapter(
                blender_module="missing_module_for_test",
                blender_executable=str(fake_blender),
            )
            payload = adapter.load_scene(blend_path)

        self.assertEqual(payload.metadata.name, "external")
        self.assertEqual(payload.metadata.frame_end, 120)

    def test_blender_adapter_falls_back_to_placeholder_payload(self) -> None:
        adapter = BlenderAdapter(blender_module="missing_module_for_test")
        with tempfile.TemporaryDirectory() as tmp_dir:
            blend_path = Path(tmp_dir) / "example.blend"
            blend_path.write_text("placeholder", encoding="utf-8")
            payload = adapter.load_scene(blend_path)

        self.assertEqual(payload.metadata.name, "example")
        self.assertEqual(payload.metadata.frame_end, 250)


if __name__ == "__main__":
    unittest.main()
