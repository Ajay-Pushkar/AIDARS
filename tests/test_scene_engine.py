import json
import tempfile
import unittest
from pathlib import Path

import sys

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
            self.assertEqual(payload["metadata"]["name"], "Test Scene")
            self.assertEqual(payload["statistics"]["object_count"], 2)

    def test_dependency_graph_builder_returns_placeholder_graph(self) -> None:
        snapshot = self.engine.analyze_scene_data(self.scene_data)
        graph = DependencyGraphBuilder().build(snapshot)

        self.assertTrue(graph.nodes)
        self.assertTrue(graph.edges)
        self.assertEqual(graph.edges[0].relationship, "parent")

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
        with tempfile.TemporaryDirectory() as tmp_dir:
            fake_blender = Path(tmp_dir) / "fake_blender.cmd"
            fake_blender.write_text(
                "@echo off\r\necho {\"metadata\": {\"name\": \"external\", \"frame_start\": 1, \"frame_end\": 120, \"fps\": 24}, \"collections\": [], \"objects\": [], \"lights\": [], \"materials\": [], \"textures\": [], \"images\": []}",
                encoding="utf-8",
            )
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
