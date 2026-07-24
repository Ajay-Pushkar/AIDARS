"""Tests for the real Blender-side extractor scripts.

These scripts (scene_metadata.py, collection_extractor.py,
object_extractor.py) run *inside* Blender's embedded Python interpreter and
are invoked by inspect_scene.py via ``blender --background --python``. We
have no real Blender binary available in CI/sandbox, so this module builds a
minimal, structurally-faithful fake of the ``bpy`` API and:

1. Verifies each extractor produces the shape expected by
   ``aidars.scene_intelligence.builders``.
2. Feeds that output through the *real* builders to catch schema drift
   between the extractors and the builders early, without needing Blender.
"""
from __future__ import annotations

import io
import json
import sys
import unittest
import unittest.mock
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
BLENDER_SCRIPTS_DIR = SRC_DIR / "aidars" / "scene_intelligence" / "blender_scripts"

sys.path.insert(0, str(SRC_DIR))
# Mirrors how Blender inserts the executed script's own directory into
# sys.path[0], which is what makes `from object_extractor import ...` work
# inside inspect_scene.py.
sys.path.insert(0, str(BLENDER_SCRIPTS_DIR))


class _FakeChildren(dict):
    """Mimics bpy_prop_collection's mapping-style access (supports .keys())."""


class FakeLibrary:
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath


class FakeCollection:
    def __init__(self, name: str) -> None:
        self.name = name
        self.children = _FakeChildren()


class FakeMeshData:
    def __init__(self, name: str, quads: int = 2) -> None:
        self.name = name
        self.vertices = list(range(4 * quads))
        self.polygons = [SimpleNamespace(vertices=[0, 1, 2, 3]) for _ in range(quads)]
        self.edges = list(range(4 * quads))
        self.uv_layers = ["UVMap"]
        self.library = None


class FakeCameraData:
    def __init__(self) -> None:
        self.type = "PERSP"
        self.lens = 50.0
        self.sensor_width = 36.0
        self.clip_start = 0.1
        self.clip_end = 1000.0
        self.library = None


class FakeLink:
    def __init__(self, from_node_type: str) -> None:
        self.from_node = SimpleNamespace(type=from_node_type)


class FakeNodeTree:
    def __init__(self) -> None:
        bsdf = SimpleNamespace(type="BSDF_PRINCIPLED")
        tex_image_node = SimpleNamespace(type="TEX_IMAGE", image=SimpleNamespace(name="Albedo"))
        output_node = SimpleNamespace(
            type="OUTPUT_MATERIAL",
            inputs={"Surface": SimpleNamespace(links=[FakeLink("BSDF_PRINCIPLED")])},
        )
        # SimpleNamespace inputs dict needs a .get like bpy's collection prop.
        output_node.inputs = _DictWithGet(output_node.inputs)
        self.name = "ChairMaterialNodeTree"
        self.nodes = [bsdf, tex_image_node, output_node]


class _DictWithGet(dict):
    pass


class FakeMaterial:
    def __init__(self, name: str) -> None:
        self.name = name
        self.use_nodes = True
        self.node_tree = FakeNodeTree()


class FakeMaterialSlot:
    def __init__(self, material: FakeMaterial | None) -> None:
        self.material = material


class FakeModifier:
    def __init__(self, name: str, mod_type: str) -> None:
        self.name = name
        self.type = mod_type
        self.show_viewport = True
        self.show_render = True


class FakeConstraint:
    def __init__(self, name: str, ctype: str, target=None) -> None:
        self.name = name
        self.type = ctype
        self.target = target
        self.influence = 1.0


class FakeKeyframe:
    def __init__(self, frame: int, value: float, interpolation: str = "LINEAR") -> None:
        self.co = (frame, value)
        self.interpolation = interpolation


class FakeFCurve:
    def __init__(self, data_path: str, array_index: int, keyframes: list) -> None:
        self.data_path = data_path
        self.array_index = array_index
        self.keyframe_points = keyframes


class FakeAction:
    def __init__(self, fcurves: list) -> None:
        self.fcurves = fcurves


class FakeAnimationData:
    def __init__(self, action: FakeAction | None) -> None:
        self.action = action


class FakeObject:
    def __init__(self, name: str, obj_type: str, **kwargs) -> None:
        self.name = name
        self.type = obj_type
        self.users_collection = kwargs.get("users_collection", [])
        self.parent = kwargs.get("parent")
        self.children = kwargs.get("children", [])
        self.hide_render = kwargs.get("hide_render", False)
        self.hide_viewport = kwargs.get("hide_viewport", False)
        self.data = kwargs.get("data")
        self.material_slots = kwargs.get("material_slots", [])
        self.modifiers = kwargs.get("modifiers", [])
        self.constraints = kwargs.get("constraints", [])
        self.animation_data = kwargs.get("animation_data")
        self.vertex_groups = kwargs.get("vertex_groups", [])
        self.particle_systems = kwargs.get("particle_systems", [])
        self.location = kwargs.get("location", [0.0, 0.0, 0.0])
        self.rotation_euler = kwargs.get("rotation_euler", [0.0, 0.0, 0.0])
        self.rotation_quaternion = kwargs.get("rotation_quaternion", [1.0, 0.0, 0.0, 0.0])
        self.scale = kwargs.get("scale", [1.0, 1.0, 1.0])
        self.library = kwargs.get("library")


class BlenderExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.main_collection = FakeCollection("Main")
        self.nested_collection = FakeCollection("Props")
        self.main_collection.children[self.nested_collection.name] = self.nested_collection

        self.scene_root_children = _FakeChildren()
        self.scene_root_children[self.main_collection.name] = self.main_collection

        fake_bpy = SimpleNamespace(
            data=SimpleNamespace(collections=[self.main_collection, self.nested_collection]),
            context=SimpleNamespace(
                scene=SimpleNamespace(collection=SimpleNamespace(children=self.scene_root_children))
            ),
        )
        sys.modules["bpy"] = fake_bpy

        # Re-import fresh each test in case a previous test's sys.modules
        # entry for bpy would otherwise be stale.
        for module_name in ("collection_extractor", "object_extractor", "scene_metadata"):
            sys.modules.pop(module_name, None)

        import collection_extractor  # noqa: E402
        import object_extractor  # noqa: E402
        import scene_metadata  # noqa: E402

        self.collection_extractor = collection_extractor
        self.object_extractor = object_extractor
        self.scene_metadata = scene_metadata

    def tearDown(self) -> None:
        sys.modules.pop("bpy", None)

    def test_extract_scene_metadata(self) -> None:
        scene = SimpleNamespace(
            name="TestScene",
            frame_start=1,
            frame_end=120,
            unit_settings=SimpleNamespace(system="METRIC"),
            render=SimpleNamespace(fps=30, fps_base=1.001, engine="CYCLES"),
        )
        metadata = self.scene_metadata.extract_scene_metadata(scene)

        self.assertEqual(metadata["name"], "TestScene")
        self.assertEqual(metadata["frame_start"], 1)
        self.assertEqual(metadata["frame_end"], 120)
        self.assertAlmostEqual(metadata["fps"], 30 / 1.001, places=3)
        self.assertEqual(metadata["render_engine"], "CYCLES")

        from aidars.scene_intelligence.builders import MetadataBuilder

        built = MetadataBuilder().build(metadata)
        self.assertEqual(built.name, "TestScene")

    def test_extract_collections_resolves_nesting(self) -> None:
        collections = self.collection_extractor.extract_collections()
        by_name = {entry["name"]: entry for entry in collections}

        self.assertIsNone(by_name["Main"]["parent"])
        self.assertEqual(by_name["Props"]["parent"], "Main")

        from aidars.scene_intelligence.builders import CollectionBuilder

        built = CollectionBuilder().build(collections)
        self.assertEqual({c.name for c in built}, {"Main", "Props"})

    def test_extract_objects_covers_full_schema(self) -> None:
        material = FakeMaterial("ChairMat")
        mesh_obj = FakeObject(
            "Chair",
            "MESH",
            users_collection=[self.main_collection],
            data=FakeMeshData("ChairMesh", quads=2),
            material_slots=[FakeMaterialSlot(material)],
            modifiers=[FakeModifier("Bevel", "BEVEL")],
            constraints=[FakeConstraint("TrackTo", "TRACK_TO", target=SimpleNamespace(name="Camera"))],
            animation_data=FakeAnimationData(
                FakeAction([FakeFCurve("location", 0, [FakeKeyframe(1, 0.0), FakeKeyframe(24, 5.0)])])
            ),
            library=FakeLibrary("/assets/chair.blend"),
        )
        camera_obj = FakeObject(
            "Camera",
            "CAMERA",
            users_collection=[self.main_collection],
            data=FakeCameraData(),
        )

        scene = SimpleNamespace(objects=[mesh_obj, camera_obj])
        objects = self.object_extractor.extract_objects(scene)
        by_name = {entry["name"]: entry for entry in objects}

        chair = by_name["Chair"]
        self.assertEqual(chair["collection"], "Main")
        self.assertEqual(chair["mesh"]["vertex_count"], 8)
        self.assertEqual(chair["mesh"]["face_count"], 2)
        self.assertEqual(chair["materials"][0]["name"], "ChairMat")
        self.assertEqual(chair["materials"][0]["shader"], "BSDF_PRINCIPLED")
        self.assertEqual(chair["materials"][0]["image_textures"], ["Albedo"])
        self.assertEqual(chair["modifiers"][0]["type"], "BEVEL")
        self.assertEqual(chair["constraints"][0]["target"], "Camera")
        self.assertTrue(chair["animation"]["is_animated"])
        self.assertEqual(chair["animation"]["fcurves"], 1)
        self.assertEqual(chair["referenced_assets"], ["/assets/chair.blend"])

        camera = by_name["Camera"]
        self.assertEqual(camera["camera"]["type"], "PERSP")
        self.assertIsNone(camera["mesh"])
        # No animation_data on the camera fixture -> extractor reports None,
        # not a zero-value dict (that default lives in ObjectBuilder only).
        self.assertIsNone(camera["animation"])

        from aidars.scene_intelligence.builders import ObjectBuilder

        built = ObjectBuilder().build(objects)
        built_chair = next(o for o in built if o.name == "Chair")
        self.assertEqual(built_chair.referenced_assets, ["/assets/chair.blend"])
        self.assertEqual(built_chair.animation.fcurves, 1)
        self.assertTrue(built_chair.animation.is_animated)
        self.assertEqual(built_chair.materials[0].image_textures, ["Albedo"])

        built_camera = next(o for o in built if o.name == "Camera")
        self.assertIsNone(built_camera.animation)


class InspectSceneMainTests(unittest.TestCase):
    """End-to-end test of inspect_scene.main() itself (not just the
    extractors it calls), against a complete fake bpy module. This is what
    actually proves the thin-orchestration rewrite of inspect_scene.py -
    calling extractors, assembling via payload_builder, printing JSON -
    still produces a valid, correctly-shaped payload."""

    def setUp(self) -> None:
        main_collection = FakeCollection("Main")
        scene_root_children = _FakeChildren()
        scene_root_children[main_collection.name] = main_collection

        cube = FakeObject(
            "Cube",
            "MESH",
            users_collection=[main_collection],
            data=FakeMeshData("CubeMesh", quads=1),
        )

        fake_scene = SimpleNamespace(
            name="MainScene",
            frame_start=1,
            frame_end=48,
            unit_settings=SimpleNamespace(system="METRIC"),
            render=SimpleNamespace(fps=24, fps_base=1.0, engine="CYCLES"),
            objects=[cube],
            collection=SimpleNamespace(children=scene_root_children),
        )

        fake_bpy = SimpleNamespace(
            data=SimpleNamespace(collections=[main_collection]),
            context=SimpleNamespace(scene=fake_scene),
            app=SimpleNamespace(version=(4, 5, 0), background=True),
        )
        sys.modules["bpy"] = fake_bpy

        for module_name in ("collection_extractor", "object_extractor", "scene_metadata", "inspect_scene", "serializers.payload_builder"):
            sys.modules.pop(module_name, None)

    def tearDown(self) -> None:
        sys.modules.pop("bpy", None)
        sys.modules.pop("inspect_scene", None)

    def test_main_prints_valid_payload_and_returns_zero(self) -> None:
        import inspect_scene

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            exit_code = inspect_scene.main()

        self.assertEqual(exit_code, 0)
        payload = json.loads(buffer.getvalue())

        self.assertEqual(payload["generator"], "AIDARS")
        self.assertEqual(payload["metadata"]["name"], "MainScene")
        self.assertEqual(len(payload["objects"]), 1)
        self.assertEqual(payload["objects"][0]["name"], "Cube")
        self.assertEqual(payload["collections"][0]["name"], "Main")
        self.assertIn("inspection", payload)
        self.assertEqual(payload["inspection"]["blender_version"], [4, 5, 0])

        # And it must round-trip cleanly through BlenderAdapter's builder pipeline.
        from aidars.scene_intelligence.blender_adapter import BlenderAdapter

        scene_data = BlenderAdapter()._build_scene_data(payload)
        self.assertEqual(scene_data.metadata.name, "MainScene")
        self.assertEqual(len(scene_data.objects), 1)

    def test_main_reports_structured_error_on_failure(self) -> None:
        import inspect_scene

        # No active scene -> validate_environment() should raise, and
        # main() should turn that into a structured error on stderr rather
        # than an unhandled traceback.
        inspect_scene_bpy = sys.modules["bpy"]
        inspect_scene_bpy.context.scene = None

        buffer = io.StringIO()
        with redirect_stdout(io.StringIO()), unittest.mock.patch("sys.stderr", buffer):
            exit_code = inspect_scene.main()

        self.assertEqual(exit_code, 1)
        error_payload = json.loads(buffer.getvalue())
        self.assertEqual(error_payload["status"], "failed")
        self.assertEqual(error_payload["error"]["type"], "RuntimeError")


if __name__ == "__main__":
    unittest.main()
