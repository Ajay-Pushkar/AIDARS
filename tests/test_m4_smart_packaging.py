"""Tests for M4-A: Requirement Resolution and Dependency Closure.

Verifies:
  RenderRequirementReport
          ↓
  RequirementResolver
          ↓
  Set[str]
          ↓
  DependencyClosureResolver
          ↓
  Complete Set[str]
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.dependency_graph import (
    DependencyGraph,
    DependencyGraphBuilder,
    GraphEdge,
    GraphNode,
)
from aidars.scene_intelligence.scene_engine import SceneEngine
from aidars.smart_package.models import AssetType, SelectionReason
from aidars.smart_package.resolver import (
    DependencyClosureResolver,
    RequirementResolver,
)
from aidars.visibility.models import RenderRequest, RenderRequirementReport


def _make_report(**overrides) -> RenderRequirementReport:
    """Helper to create a RenderRequirementReport with sensible defaults."""
    defaults = {
        "request": RenderRequest(camera_id="cam-1", frame_start=1, frame_end=24),
        "required_objects": [],
        "required_meshes": [],
        "required_materials": [],
        "required_textures": [],
        "required_images": [],
        "required_lights": [],
        "required_cameras": [],
        "required_libraries": [],
        "required_simulation_caches": [],
        "unused_objects": [],
        "reasons": {},
    }
    defaults.update(overrides)
    return RenderRequirementReport(**defaults)


def _build_graph_from_scene(scene_payload: dict) -> DependencyGraph:
    """Run the real SceneEngine + DependencyGraphBuilder."""
    engine = SceneEngine()
    snapshot = engine.analyze(scene_payload)
    builder = DependencyGraphBuilder()
    return builder.build(snapshot)


# ──────────────────────────────────────────────────────────────────
# Test fixtures
# ──────────────────────────────────────────────────────────────────

# A scene with two objects: Chair (visible) and Table (hidden).
# Both share "WoodMat" material which references "wood.png" texture.
# Chair also has a unique "FabricMat".
PACKAGING_SCENE = {
    "metadata": {"name": "PackagingTest", "frame_start": 1, "frame_end": 24, "fps": 24},
    "collections": [{"name": "MainCollection", "id": "col-main"}],
    "objects": [
        {
            "name": "Chair",
            "id": "obj-chair",
            "type": "MESH",
            "collection": "col-main",
            "visibility": {"hide_render": False},
            "mesh": {"name": "ChairMesh", "vertex_count": 200, "face_count": 100},
            "materials": [
                {
                    "name": "WoodMat",
                    "image_textures": ["wood.png"],
                },
                {
                    "name": "FabricMat",
                    "image_textures": ["fabric.png"],
                },
            ],
            "modifiers": [],
            "constraints": [],
            "animation": {"is_animated": False, "curves": []},
            "children": [],
            "referenced_assets": [],
        },
        {
            "name": "Table",
            "id": "obj-table",
            "type": "MESH",
            "collection": "col-main",
            "visibility": {"hide_render": True},
            "mesh": {"name": "TableMesh", "vertex_count": 100, "face_count": 50},
            "materials": [
                {
                    "name": "WoodMat",
                    "image_textures": ["wood.png"],
                },
            ],
            "modifiers": [],
            "constraints": [],
            "animation": {"is_animated": False, "curves": []},
            "children": [],
            "referenced_assets": ["arch_library.blend"],
        },
        {
            "name": "SunLight",
            "id": "obj-sun",
            "type": "LIGHT",
            "collection": "col-main",
            "visibility": {"hide_render": False},
            "materials": [],
            "modifiers": [],
            "constraints": [],
            "animation": {"is_animated": False, "curves": []},
            "children": [],
            "referenced_assets": [],
        },
        {
            "name": "MainCamera",
            "id": "obj-cam",
            "type": "CAMERA",
            "collection": "col-main",
            "visibility": {"hide_render": False},
            "materials": [],
            "modifiers": [],
            "constraints": [],
            "animation": {"is_animated": False, "curves": []},
            "children": [],
            "referenced_assets": [],
        },
    ],
    "lights": [{"name": "SunLight", "id": "obj-sun", "type": "SUN"}],
    "materials": [
        {"name": "WoodMat", "image_textures": ["wood.png"]},
        {"name": "FabricMat", "image_textures": ["fabric.png"]},
    ],
    "textures": [{"name": "wood.png"}, {"name": "fabric.png"}],
    "images": [{"name": "wood.png"}, {"name": "fabric.png"}],
}


class RequirementResolverTests(unittest.TestCase):
    """Tests for RequirementResolver: RenderRequirementReport → Set[str]."""

    def test_empty_report_produces_empty_set(self) -> None:
        report = _make_report()
        ids = RequirementResolver.resolve(report)
        self.assertEqual(ids, set())

    def test_objects_are_included_as_bare_ids(self) -> None:
        """Required objects should appear as bare identifiers (no prefix)."""
        report = _make_report(required_objects=["obj-chair", "obj-sun"])
        ids = RequirementResolver.resolve(report)
        self.assertIn("obj-chair", ids)
        self.assertIn("obj-sun", ids)

    def test_materials_get_prefixed(self) -> None:
        """Materials should appear as 'material:{name}' AND bare name."""
        report = _make_report(required_materials=["WoodMat", "FabricMat"])
        ids = RequirementResolver.resolve(report)
        self.assertIn("material:WoodMat", ids)
        self.assertIn("material:FabricMat", ids)
        # Bare names also included for fuzzy matching
        self.assertIn("WoodMat", ids)
        self.assertIn("FabricMat", ids)

    def test_textures_get_prefixed(self) -> None:
        report = _make_report(required_textures=["wood.png"])
        ids = RequirementResolver.resolve(report)
        self.assertIn("texture:wood.png", ids)
        self.assertIn("wood.png", ids)

    def test_images_get_prefixed(self) -> None:
        report = _make_report(required_images=["wood.png"])
        ids = RequirementResolver.resolve(report)
        self.assertIn("image:wood.png", ids)
        self.assertIn("wood.png", ids)

    def test_libraries_get_prefixed(self) -> None:
        report = _make_report(required_libraries=["arch_library.blend"])
        ids = RequirementResolver.resolve(report)
        self.assertIn("asset:arch_library.blend", ids)
        self.assertIn("arch_library.blend", ids)

    def test_lights_and_cameras_are_bare_ids(self) -> None:
        report = _make_report(
            required_lights=["obj-sun"],
            required_cameras=["obj-cam"],
        )
        ids = RequirementResolver.resolve(report)
        self.assertIn("obj-sun", ids)
        self.assertIn("obj-cam", ids)

    def test_full_report_resolution(self) -> None:
        """A realistic report should produce the union of all prefixed IDs."""
        report = _make_report(
            required_objects=["obj-chair"],
            required_meshes=["ChairMesh"],
            required_materials=["WoodMat", "FabricMat"],
            required_textures=["wood.png", "fabric.png"],
            required_images=["wood.png", "fabric.png"],
            required_lights=["obj-sun"],
            required_cameras=["obj-cam"],
        )
        ids = RequirementResolver.resolve(report)
        # Should contain prefixed and bare versions
        self.assertIn("obj-chair", ids)
        self.assertIn("material:WoodMat", ids)
        self.assertIn("material:FabricMat", ids)
        self.assertIn("texture:wood.png", ids)
        self.assertIn("image:wood.png", ids)
        self.assertIn("obj-sun", ids)
        self.assertIn("obj-cam", ids)
        self.assertIn("ChairMesh", ids)

    def test_deduplication_across_lists(self) -> None:
        """Same name appearing in textures AND images should produce
        a single set (sets are inherently deduped)."""
        report = _make_report(
            required_textures=["wood.png"],
            required_images=["wood.png"],
        )
        ids = RequirementResolver.resolve(report)
        # "wood.png" appears as bare, texture:, and image: — all unique strings
        self.assertIn("wood.png", ids)
        self.assertIn("texture:wood.png", ids)
        self.assertIn("image:wood.png", ids)


class DependencyClosureResolverTests(unittest.TestCase):
    """Tests for DependencyClosureResolver: seed Set[str] + Graph → complete Set[str]."""

    def _build_simple_graph(self) -> DependencyGraph:
        """Build a hand-crafted graph:

        obj-chair ──material──► material:WoodMat ──texture──► texture:wood.png ──image──► image:wood.png
        obj-chair ──material──► material:FabricMat ──texture──► texture:fabric.png ──image──► image:fabric.png
        obj-table ──material──► material:WoodMat  (shared material)
        obj-table ──references──► asset:arch_library.blend
        """
        nodes = [
            GraphNode("obj-chair", "Chair", "object"),
            GraphNode("obj-table", "Table", "object"),
            GraphNode("material:WoodMat", "WoodMat", "material"),
            GraphNode("material:FabricMat", "FabricMat", "material"),
            GraphNode("texture:wood.png", "wood.png", "texture"),
            GraphNode("texture:fabric.png", "fabric.png", "texture"),
            GraphNode("image:wood.png", "wood.png", "image"),
            GraphNode("image:fabric.png", "fabric.png", "image"),
            GraphNode("asset:arch_library.blend", "arch_library.blend", "asset"),
        ]
        edges = [
            GraphEdge("obj-chair", "material:WoodMat", "material"),
            GraphEdge("obj-chair", "material:FabricMat", "material"),
            GraphEdge("material:WoodMat", "texture:wood.png", "texture"),
            GraphEdge("material:FabricMat", "texture:fabric.png", "texture"),
            GraphEdge("texture:wood.png", "image:wood.png", "image"),
            GraphEdge("texture:fabric.png", "image:fabric.png", "image"),
            GraphEdge("obj-table", "material:WoodMat", "material"),
            GraphEdge("obj-table", "asset:arch_library.blend", "references"),
        ]
        return DependencyGraph(nodes=nodes, edges=edges)

    def test_empty_seeds_produce_empty_closure(self) -> None:
        graph = self._build_simple_graph()
        closure = DependencyClosureResolver.compute_closure(set(), graph)
        self.assertEqual(closure, set())

    def test_single_object_closure_includes_all_dependencies(self) -> None:
        """Seeding with obj-chair should pull in both materials, textures, and images."""
        graph = self._build_simple_graph()
        closure = DependencyClosureResolver.compute_closure({"obj-chair"}, graph)

        self.assertIn("obj-chair", closure)
        self.assertIn("material:WoodMat", closure)
        self.assertIn("material:FabricMat", closure)
        self.assertIn("texture:wood.png", closure)
        self.assertIn("texture:fabric.png", closure)
        self.assertIn("image:wood.png", closure)
        self.assertIn("image:fabric.png", closure)

    def test_closure_does_not_include_unconnected_nodes(self) -> None:
        """Seeding with obj-chair should NOT pull in obj-table or its exclusive deps."""
        graph = self._build_simple_graph()
        closure = DependencyClosureResolver.compute_closure({"obj-chair"}, graph)

        self.assertNotIn("obj-table", closure)
        self.assertNotIn("asset:arch_library.blend", closure)

    def test_shared_material_traversed_once(self) -> None:
        """Both obj-chair and obj-table reference WoodMat.
        Closure from both should include WoodMat exactly once (set semantics)."""
        graph = self._build_simple_graph()
        closure = DependencyClosureResolver.compute_closure({"obj-chair", "obj-table"}, graph)

        self.assertIn("material:WoodMat", closure)
        self.assertIn("asset:arch_library.blend", closure)
        # WoodMat's downstream should still appear
        self.assertIn("texture:wood.png", closure)
        self.assertIn("image:wood.png", closure)

    def test_seed_with_prefixed_material_traverses_downstream(self) -> None:
        """Seeding directly with 'material:WoodMat' should pull in its textures and images."""
        graph = self._build_simple_graph()
        closure = DependencyClosureResolver.compute_closure({"material:WoodMat"}, graph)

        self.assertIn("material:WoodMat", closure)
        self.assertIn("texture:wood.png", closure)
        self.assertIn("image:wood.png", closure)
        # Should NOT pull in Chair or Table (no reverse edges)
        self.assertNotIn("obj-chair", closure)

    def test_fuzzy_label_matching_resolves_bare_names(self) -> None:
        """Seeding with bare name 'WoodMat' (as M3 outputs) should find
        'material:WoodMat' via label matching and traverse its deps."""
        graph = self._build_simple_graph()
        closure = DependencyClosureResolver.compute_closure({"WoodMat"}, graph)

        self.assertIn("material:WoodMat", closure)
        self.assertIn("texture:wood.png", closure)
        self.assertIn("image:wood.png", closure)

    def test_dangling_edge_target_does_not_crash(self) -> None:
        """Edge pointing to a non-existent node should be safely traversed."""
        graph = DependencyGraph(
            nodes=[GraphNode("obj-1", "Cube", "object")],
            edges=[GraphEdge("obj-1", "obj-ghost", "constraint")],
        )
        closure = DependencyClosureResolver.compute_closure({"obj-1"}, graph)
        self.assertIn("obj-1", closure)
        # Dangling target is visited but doesn't crash
        self.assertIn("obj-ghost", closure)

    def test_cycle_does_not_cause_infinite_loop(self) -> None:
        """A → B → C → A should terminate cleanly."""
        graph = DependencyGraph(
            nodes=[
                GraphNode("A", "A", "object"),
                GraphNode("B", "B", "material"),
                GraphNode("C", "C", "texture"),
            ],
            edges=[
                GraphEdge("A", "B", "material"),
                GraphEdge("B", "C", "texture"),
                GraphEdge("C", "A", "circular"),
            ],
        )
        closure = DependencyClosureResolver.compute_closure({"A"}, graph)
        self.assertEqual(closure, {"A", "B", "C"})


class DependencyClosureClassificationTests(unittest.TestCase):
    """Tests for classify_node and partition_closure."""

    def test_classify_node_maps_kinds_correctly(self) -> None:
        cases = [
            (GraphNode("m1", "WoodMat", "material"), AssetType.MATERIAL),
            (GraphNode("t1", "wood.png", "texture"), AssetType.TEXTURE),
            (GraphNode("i1", "wood.png", "image"), AssetType.IMAGE),
            (GraphNode("a1", "lib.blend", "asset"), AssetType.LIBRARY),
            (GraphNode("o1", "Cube", "object"), AssetType.UNKNOWN),  # needs obj.type
            (GraphNode("x1", "???", "something_new"), AssetType.UNKNOWN),
        ]
        for node, expected_type in cases:
            with self.subTest(node=node.identifier):
                self.assertEqual(DependencyClosureResolver.classify_node(node), expected_type)

    def test_partition_closure_groups_by_type(self) -> None:
        graph = DependencyGraph(
            nodes=[
                GraphNode("obj-1", "Chair", "object"),
                GraphNode("material:WoodMat", "WoodMat", "material"),
                GraphNode("texture:wood.png", "wood.png", "texture"),
                GraphNode("image:wood.png", "wood.png", "image"),
            ],
            edges=[],
        )
        closure = {"obj-1", "material:WoodMat", "texture:wood.png", "image:wood.png"}
        partitioned = DependencyClosureResolver.partition_closure(closure, graph)

        self.assertIn(AssetType.UNKNOWN, partitioned)  # object kind → UNKNOWN
        self.assertIn(AssetType.MATERIAL, partitioned)
        self.assertIn(AssetType.TEXTURE, partitioned)
        self.assertIn(AssetType.IMAGE, partitioned)
        self.assertEqual(partitioned[AssetType.MATERIAL], ["material:WoodMat"])

    def test_partition_unknown_ids_not_in_graph(self) -> None:
        """IDs in closure but not in graph should be classified as UNKNOWN."""
        graph = DependencyGraph(nodes=[], edges=[])
        closure = {"phantom-node"}
        partitioned = DependencyClosureResolver.partition_closure(closure, graph)
        self.assertIn(AssetType.UNKNOWN, partitioned)
        self.assertIn("phantom-node", partitioned[AssetType.UNKNOWN])


class EndToEndM4ATests(unittest.TestCase):
    """Integration tests: RenderRequirementReport → RequirementResolver → DependencyClosureResolver."""

    def test_full_pipeline_chair_only(self) -> None:
        """M3 says Chair is required. M4-A should resolve its full dependency closure."""
        graph = _build_graph_from_scene(PACKAGING_SCENE)

        # Simulate M3 output: only Chair is visible
        report = _make_report(
            required_objects=["obj-chair"],
            required_materials=["WoodMat", "FabricMat"],
            required_textures=["wood.png", "fabric.png"],
            required_images=["wood.png", "fabric.png"],
            required_lights=["obj-sun"],
            required_cameras=["obj-cam"],
        )

        # Step 1: Resolve to graph IDs
        seed_ids = RequirementResolver.resolve(report)
        self.assertIn("obj-chair", seed_ids)
        self.assertIn("material:WoodMat", seed_ids)

        # Step 2: Compute closure
        closure = DependencyClosureResolver.compute_closure(seed_ids, graph)

        # Chair and its dependencies should be present
        self.assertIn("obj-chair", closure)
        self.assertIn("material:WoodMat", closure)
        self.assertIn("material:FabricMat", closure)
        self.assertIn("texture:wood.png", closure)
        self.assertIn("image:wood.png", closure)

        # Table's EXCLUSIVE dependency (arch_library.blend) should NOT
        # be in closure since Table was not required and arch_library
        # is only reachable via Table
        self.assertNotIn("asset:arch_library.blend", closure)

    def test_full_pipeline_both_objects(self) -> None:
        """M3 says both Chair and Table are required. Everything should be in closure."""
        graph = _build_graph_from_scene(PACKAGING_SCENE)

        report = _make_report(
            required_objects=["obj-chair", "obj-table"],
            required_materials=["WoodMat", "FabricMat"],
            required_textures=["wood.png", "fabric.png"],
            required_images=["wood.png", "fabric.png"],
            required_libraries=["arch_library.blend"],
        )

        seed_ids = RequirementResolver.resolve(report)
        closure = DependencyClosureResolver.compute_closure(seed_ids, graph)

        self.assertIn("obj-chair", closure)
        self.assertIn("obj-table", closure)
        self.assertIn("material:WoodMat", closure)
        self.assertIn("asset:arch_library.blend", closure)

    def test_partition_matches_closure(self) -> None:
        """partition_closure should cover every ID in the closure."""
        graph = _build_graph_from_scene(PACKAGING_SCENE)
        report = _make_report(
            required_objects=["obj-chair"],
            required_materials=["WoodMat", "FabricMat"],
            required_textures=["wood.png", "fabric.png"],
            required_images=["wood.png", "fabric.png"],
        )
        seed_ids = RequirementResolver.resolve(report)
        closure = DependencyClosureResolver.compute_closure(seed_ids, graph)
        partitioned = DependencyClosureResolver.partition_closure(closure, graph)

        # Every ID in the closure should appear in exactly one partition
        all_partitioned_ids = set()
        for ids_list in partitioned.values():
            all_partitioned_ids.update(ids_list)

        # Only IDs that are actual graph nodes get partitioned
        node_index = graph.node_index()
        closure_in_graph = {cid for cid in closure if cid in node_index}
        closure_not_in_graph = closure - closure_in_graph

        self.assertTrue(closure_in_graph.issubset(all_partitioned_ids))
        # Non-graph IDs go into UNKNOWN
        for phantom in closure_not_in_graph:
            self.assertIn(phantom, partitioned.get(AssetType.UNKNOWN, []))


if __name__ == "__main__":
    unittest.main()
