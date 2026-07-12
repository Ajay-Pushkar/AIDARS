import json
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.engine import SceneIntelligenceEngine
from aidars.scene_intelligence.exporters import JsonSceneExporter
from aidars.scene_intelligence.dependency_graph import DependencyGraphBuilder


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


if __name__ == "__main__":
    unittest.main()
