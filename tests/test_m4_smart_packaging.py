"""Tests for Milestone 4 Smart Packaging: M4-A, M4-B, M4-C, M4-D and SceneEngine Integration.

Verifies:
  RenderRequirementReport
          ↓
  RequirementResolver (M4-A)
          ↓
  DependencyClosureResolver (M4-A)
          ↓
  PhysicalAssetResolver (M4-B)
          ↓
  PackagePlanner & PackageBuilder (M4-C)
          ↓
  PackageValidator (M4-D)
          ↓
  SceneEngine Orchestration Integration
"""
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.dependency_graph import (
    DependencyGraph,
    DependencyGraphBuilder,
    GraphEdge,
    GraphNode,
)
from aidars.scene_intelligence.scene_engine import (
    SceneEngine,
    SceneEngineRequest,
    SceneEngineResult,
)
from aidars.smart_package.builder import (
    PackageBuilder,
    PackagePlanner,
)
from aidars.smart_package.models import (
    AssetRecord,
    AssetStatus,
    AssetType,
    PackageIntegrityReport,
    PackagePlan,
    PackageStatistics,
    SelectionReason,
)
from aidars.smart_package.resolver import (
    DependencyClosureResolver,
    PhysicalAssetResolver,
    RequirementResolver,
)
from aidars.smart_package.validator import PackageValidator
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


# ──────────────────────────────────────────────────────────────────
# M4-A Unit Tests (Requirement & Closure Resolution)
# ──────────────────────────────────────────────────────────────────


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
        self.assertIn("texture:wood.png", closure)
        self.assertIn("image:wood.png", closure)

    def test_seed_with_prefixed_material_traverses_downstream(self) -> None:
        """Seeding directly with 'material:WoodMat' should pull in its textures and images."""
        graph = self._build_simple_graph()
        closure = DependencyClosureResolver.compute_closure({"material:WoodMat"}, graph)

        self.assertIn("material:WoodMat", closure)
        self.assertIn("texture:wood.png", closure)
        self.assertIn("image:wood.png", closure)
        self.assertNotIn("obj-chair", closure)

    def test_fuzzy_label_matching_resolves_bare_names(self) -> None:
        """Seeding with bare name 'WoodMat' should find 'material:WoodMat'."""
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
            (GraphNode("o1", "Cube", "object"), AssetType.UNKNOWN),
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

        self.assertIn(AssetType.UNKNOWN, partitioned)
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

        report = _make_report(
            required_objects=["obj-chair"],
            required_materials=["WoodMat", "FabricMat"],
            required_textures=["wood.png", "fabric.png"],
            required_images=["wood.png", "fabric.png"],
            required_lights=["obj-sun"],
            required_cameras=["obj-cam"],
        )

        seed_ids = RequirementResolver.resolve(report)
        self.assertIn("obj-chair", seed_ids)
        self.assertIn("material:WoodMat", seed_ids)

        closure = DependencyClosureResolver.compute_closure(seed_ids, graph)

        self.assertIn("obj-chair", closure)
        self.assertIn("material:WoodMat", closure)
        self.assertIn("material:FabricMat", closure)
        self.assertIn("texture:wood.png", closure)
        self.assertIn("image:wood.png", closure)
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

        all_partitioned_ids = set()
        for ids_list in partitioned.values():
            all_partitioned_ids.update(ids_list)

        node_index = graph.node_index()
        closure_in_graph = {cid for cid in closure if cid in node_index}
        closure_not_in_graph = closure - closure_in_graph

        self.assertTrue(closure_in_graph.issubset(all_partitioned_ids))
        for phantom in closure_not_in_graph:
            self.assertIn(phantom, partitioned.get(AssetType.UNKNOWN, []))


# ──────────────────────────────────────────────────────────────────
# M4-B Unit Tests (Physical Asset Resolution)
# ──────────────────────────────────────────────────────────────────


class PhysicalAssetResolverTests(unittest.TestCase):
    """Tests for PhysicalAssetResolver (M4-B)."""

    def setUp(self) -> None:
        self.resolver = PhysicalAssetResolver()

    def test_embedded_assets_classification(self) -> None:
        """Objects, meshes, and internal materials get AssetStatus.EMBEDDED with zero byte size."""
        graph = DependencyGraph(
            nodes=[
                GraphNode("obj-chair", "Chair", "object"),
                GraphNode("material:WoodMat", "WoodMat", "material"),
                GraphNode("modifier:1:Subsurf", "Subsurf", "modifier"),
            ],
            edges=[
                GraphEdge("obj-chair", "material:WoodMat", "material"),
            ],
        )
        closure_ids = {"obj-chair", "material:WoodMat", "modifier:1:Subsurf"}
        records = self.resolver.resolve(closure_ids, graph)

        record_map = {r.asset_id: r for r in records}
        self.assertEqual(len(records), 3)

        chair_rec = record_map["obj-chair"]
        self.assertEqual(chair_rec.status, AssetStatus.EMBEDDED)
        self.assertTrue(chair_rec.embedded)
        self.assertIsNone(chair_rec.source_path)
        self.assertIsNone(chair_rec.sha256)
        self.assertEqual(chair_rec.size_bytes, 0)
        self.assertEqual(chair_rec.dependencies, ["material:WoodMat"])

        mat_rec = record_map["material:WoodMat"]
        self.assertEqual(mat_rec.status, AssetStatus.EMBEDDED)
        self.assertEqual(mat_rec.asset_type, AssetType.MATERIAL)
        self.assertTrue(mat_rec.embedded)

    def test_resolved_external_asset_with_real_file(self) -> None:
        """Required asset with existing file on disk gets AssetStatus.RESOLVED and valid SHA-256 hash."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            tex_file = base_dir / "wood.png"
            tex_content = b"fake-png-texture-bytes-12345"
            tex_file.write_bytes(tex_content)
            expected_hash = hashlib.sha256(tex_content).hexdigest()

            graph = DependencyGraph(
                nodes=[
                    GraphNode("texture:wood.png", "wood.png", "texture"),
                ],
                edges=[],
            )

            records = self.resolver.resolve(
                closure_ids={"texture:wood.png"},
                graph=graph,
                base_dir=base_dir,
                seed_ids={"texture:wood.png"},
            )

            self.assertEqual(len(records), 1)
            rec = records[0]
            self.assertEqual(rec.asset_id, "texture:wood.png")
            self.assertEqual(rec.status, AssetStatus.RESOLVED)
            self.assertFalse(rec.embedded)
            self.assertEqual(rec.selection_reason, SelectionReason.RENDER_REQUIRED)
            self.assertEqual(rec.sha256, expected_hash)
            self.assertEqual(rec.size_bytes, len(tex_content))
            self.assertTrue(rec.package_path.endswith("_wood.png"))
            self.assertEqual(rec.source_path, str(tex_file.resolve()))

    def test_missing_file_flagged_as_missing(self) -> None:
        """Missing file gets AssetStatus.MISSING, sha256=None, size_bytes=0, and is not dropped."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            graph = DependencyGraph(
                nodes=[
                    GraphNode("texture:nonexistent.png", "nonexistent.png", "texture"),
                ],
                edges=[],
            )

            records = self.resolver.resolve(
                closure_ids={"texture:nonexistent.png"},
                graph=graph,
                base_dir=base_dir,
            )

            self.assertEqual(len(records), 1)
            rec = records[0]
            self.assertEqual(rec.asset_id, "texture:nonexistent.png")
            self.assertEqual(rec.status, AssetStatus.MISSING)
            self.assertFalse(rec.embedded)
            self.assertIsNone(rec.sha256)
            self.assertEqual(rec.size_bytes, 0)
            self.assertIsNotNone(rec.source_path)

    def test_blender_relative_path_resolution(self) -> None:
        """Blender '//relative' path is resolved against scene base directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            tex_sub = base_dir / "textures"
            tex_sub.mkdir(parents=True, exist_ok=True)
            diffuse_file = tex_sub / "diffuse.png"
            diffuse_file.write_bytes(b"diffuse-texture-data")

            resolved_path, rel_name = self.resolver.resolve_path("//textures/diffuse.png", base_dir=base_dir)
            self.assertTrue(resolved_path.exists())
            self.assertEqual(resolved_path, diffuse_file.resolve())
            self.assertEqual(rel_name.replace("\\", "/"), "textures/diffuse.png")

    def test_absolute_path_resolution(self) -> None:
        """Absolute paths resolve directly on the filesystem."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            abs_file = Path(tmp_dir) / "absolute_asset.blend"
            abs_file.write_bytes(b"blend-file-content")

            resolved_path, rel_name = self.resolver.resolve_path(str(abs_file.resolve()))
            self.assertTrue(resolved_path.exists())
            self.assertEqual(resolved_path, abs_file.resolve())
            self.assertEqual(rel_name, "absolute_asset.blend")

    def test_selection_reason_assignment(self) -> None:
        """Seed IDs get SelectionReason.RENDER_REQUIRED, others get DEPENDENCY."""
        graph = DependencyGraph(
            nodes=[
                GraphNode("obj-chair", "Chair", "object"),
                GraphNode("material:WoodMat", "WoodMat", "material"),
            ],
            edges=[GraphEdge("obj-chair", "material:WoodMat", "material")],
        )
        records = self.resolver.resolve(
            closure_ids={"obj-chair", "material:WoodMat"},
            graph=graph,
            seed_ids={"obj-chair"},
        )
        record_map = {r.asset_id: r for r in records}
        self.assertEqual(record_map["obj-chair"].selection_reason, SelectionReason.RENDER_REQUIRED)
        self.assertEqual(record_map["material:WoodMat"].selection_reason, SelectionReason.DEPENDENCY)

    def test_all_record_fields_populated_in_to_dict(self) -> None:
        """AssetRecord.to_dict() produces all schema fields with expected types."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            f = base_dir / "test.png"
            f.write_bytes(b"12345")

            graph = DependencyGraph(
                nodes=[GraphNode("image:test.png", "test.png", "image")],
                edges=[],
            )
            records = self.resolver.resolve({"image:test.png"}, graph, base_dir=base_dir)
            d = records[0].to_dict()

            expected_keys = {
                "asset_id",
                "type",
                "status",
                "selection_reason",
                "source_path",
                "relative_path",
                "package_path",
                "sha256",
                "size_bytes",
                "embedded",
                "conservative",
                "dependencies",
            }
            self.assertTrue(expected_keys.issubset(d.keys()))
            self.assertEqual(d["type"], "image")
            self.assertEqual(d["status"], "resolved")

    def test_duplicate_basename_collision_prevention(self) -> None:
        """Files with same name but different content should get unique package paths."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            dir1 = base_dir / "A"
            dir2 = base_dir / "B"
            dir1.mkdir()
            dir2.mkdir()
            
            file1 = dir1 / "texture.png"
            file2 = dir2 / "texture.png"
            file1.write_bytes(b"content-a")
            file2.write_bytes(b"content-b")
            
            graph = DependencyGraph(
                nodes=[
                    GraphNode("texture:A/texture.png", "A/texture.png", "texture"),
                    GraphNode("texture:B/texture.png", "B/texture.png", "texture"),
                ],
                edges=[]
            )
            
            records = self.resolver.resolve(
                closure_ids={"texture:A/texture.png", "texture:B/texture.png"},
                graph=graph,
                base_dir=base_dir,
            )
            
            self.assertEqual(len(records), 2)
            path1 = records[0].package_path
            path2 = records[1].package_path
            self.assertNotEqual(path1, path2)
            self.assertTrue(path1.startswith("assets/"))
            self.assertTrue(path2.startswith("assets/"))


# ──────────────────────────────────────────────────────────────────
# M4-C Unit Tests (Package Construction & Deduplication)
# ──────────────────────────────────────────────────────────────────


class PackageConstructionTests(unittest.TestCase):
    """Tests for PackagePlanner and PackageBuilder (M4-C)."""

    def setUp(self) -> None:
        self.planner = PackagePlanner()
        self.builder = PackageBuilder(planner=self.planner)

    def test_deduplication_by_sha256(self) -> None:
        """Two assets with identical SHA-256 hashes are deduplicated to one physical copy in deduplicated_assets."""
        records = [
            AssetRecord(
                asset_id="texture:wood_diffuse.png",
                asset_type=AssetType.TEXTURE,
                selection_reason=SelectionReason.RENDER_REQUIRED,
                source_path="/path/to/wood_diffuse.png",
                package_path="assets/wood_diffuse.png",
                status=AssetStatus.RESOLVED,
                sha256="abc123hash",
                size_bytes=1000,
                embedded=False,
            ),
            AssetRecord(
                asset_id="image:wood_copy.png",
                asset_type=AssetType.IMAGE,
                selection_reason=SelectionReason.DEPENDENCY,
                source_path="/path/to/wood_copy.png",
                package_path="assets/wood_copy.png",
                status=AssetStatus.RESOLVED,
                sha256="abc123hash",  # Same SHA-256
                size_bytes=1000,
                embedded=False,
            ),
            AssetRecord(
                asset_id="obj-chair",
                asset_type=AssetType.MESH,
                selection_reason=SelectionReason.RENDER_REQUIRED,
                status=AssetStatus.EMBEDDED,
                embedded=True,
            ),
        ]

        plan = self.planner.create_plan(records, package_id="pkg-1-24")

        self.assertEqual(len(plan.all_assets), 3)
        self.assertEqual(len(plan.deduplicated_assets), 1)
        self.assertEqual(plan.statistics.total_assets, 3)
        self.assertEqual(plan.statistics.resolved_assets, 2)
        self.assertEqual(plan.statistics.duplicate_assets, 1)
        self.assertEqual(plan.statistics.original_size_bytes, 2000)
        self.assertEqual(plan.statistics.package_size_bytes, 1000)
        self.assertEqual(plan.statistics.reduction_percent, 50.0)

    def test_statistics_computation(self) -> None:
        """Statistics compute total, resolved, embedded, missing, and duplicate metrics."""
        records = [
            AssetRecord(asset_id="a1", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="h1", size_bytes=500, embedded=False),
            AssetRecord(asset_id="a2", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="h2", size_bytes=300, embedded=False),
            AssetRecord(asset_id="a3", asset_type=AssetType.MESH, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.EMBEDDED, embedded=True),
            AssetRecord(asset_id="a4", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.MISSING, embedded=False),
        ]
        plan = self.planner.create_plan(records, package_id="pkg-test")

        stats = plan.statistics
        self.assertEqual(stats.total_assets, 4)
        self.assertEqual(stats.resolved_assets, 2)
        self.assertEqual(stats.embedded_assets, 1)
        self.assertEqual(stats.missing_assets, 1)
        self.assertEqual(stats.duplicate_assets, 0)
        self.assertEqual(stats.original_size_bytes, 800)
        self.assertEqual(stats.package_size_bytes, 800)
        self.assertEqual(stats.reduction_percent, 0.0)

    def test_manifest_schema_v1_generation(self) -> None:
        """Written manifest JSON contains schema_version, package_id, scene, assets array with hashes, and statistics."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir)
            records = [
                AssetRecord(
                    asset_id="tex-1",
                    asset_type=AssetType.TEXTURE,
                    selection_reason=SelectionReason.RENDER_REQUIRED,
                    status=AssetStatus.RESOLVED,
                    sha256="deadbeef",
                    size_bytes=128,
                    embedded=False,
                    package_path="assets/tex-1.png",
                )
            ]
            plan = self.planner.create_plan(records, package_id="pkg-v1", scene_name="TestScene", frame_start=5, frame_end=10)
            manifest_path = self.builder.build_package(plan, output_dir=out_dir)

            self.assertTrue(manifest_path.exists())
            data = json.loads(manifest_path.read_text(encoding="utf-8"))

            self.assertEqual(data["schema_version"], "1.0")
            self.assertEqual(data["package_id"], "pkg-v1")
            self.assertEqual(data["scene"]["name"], "TestScene")
            self.assertEqual(data["scene"]["frame_start"], 5)
            self.assertEqual(data["scene"]["frame_end"], 10)
            self.assertEqual(len(data["assets"]), 1)
            self.assertEqual(data["assets"][0]["sha256"], "deadbeef")
            self.assertIn("statistics", data)
            self.assertEqual(data["statistics"]["resolved_assets"], 1)

    def test_package_directory_contains_resolved_assets(self) -> None:
        """Package directory contains all resolved external assets copied to canonical paths."""
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as dst_dir:
            file_a = Path(src_dir) / "file_a.png"
            file_b = Path(src_dir) / "file_b.png"
            file_a.write_bytes(b"content-a")
            file_b.write_bytes(b"content-b")

            records = [
                AssetRecord(
                    asset_id="tex-a",
                    asset_type=AssetType.TEXTURE,
                    selection_reason=SelectionReason.RENDER_REQUIRED,
                    source_path=str(file_a),
                    package_path="assets/file_a.png",
                    status=AssetStatus.RESOLVED,
                    sha256=hashlib.sha256(b"content-a").hexdigest(),
                    size_bytes=len(b"content-a"),
                    embedded=False,
                ),
                AssetRecord(
                    asset_id="tex-b",
                    asset_type=AssetType.TEXTURE,
                    selection_reason=SelectionReason.DEPENDENCY,
                    source_path=str(file_b),
                    package_path="assets/file_b.png",
                    status=AssetStatus.RESOLVED,
                    sha256=hashlib.sha256(b"content-b").hexdigest(),
                    size_bytes=len(b"content-b"),
                    embedded=False,
                ),
            ]

            plan = self.planner.create_plan(records, package_id="pkg-real")
            self.builder.build_package(plan, output_dir=dst_dir)

            copied_a = Path(dst_dir) / "assets" / "file_a.png"
            copied_b = Path(dst_dir) / "assets" / "file_b.png"
            manifest = Path(dst_dir) / "manifest.json"

            self.assertTrue(copied_a.exists())
            self.assertTrue(copied_b.exists())
            self.assertTrue(manifest.exists())
            self.assertEqual(copied_a.read_bytes(), b"content-a")
            self.assertEqual(copied_b.read_bytes(), b"content-b")

    def test_blender_path_remapping(self) -> None:
        """Test that .blend scene source triggers path remapping script."""
        import subprocess
        from unittest.mock import patch
        
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as dst_dir:
            scene_file = Path(src_dir) / "test_scene.blend"
            scene_file.write_bytes(b"blend-content")
            
            asset_file = Path(src_dir) / "texture.png"
            asset_file.write_bytes(b"tex")
            
            records = [
                AssetRecord(
                    asset_id="tex",
                    asset_type=AssetType.TEXTURE,
                    selection_reason=SelectionReason.RENDER_REQUIRED,
                    source_path=str(asset_file),
                    package_path="assets/texture.png",
                    status=AssetStatus.RESOLVED,
                    sha256="abc",
                    size_bytes=3,
                    embedded=False,
                )
            ]
            plan = self.planner.create_plan(records, package_id="pkg")
            
            with patch("subprocess.run") as mock_run:
                self.builder.build_package(
                    plan, 
                    output_dir=dst_dir, 
                    scene_source_path=scene_file, 
                    blender_executable="/mock/blender"
                )
                
                # Check that .blend was copied
                dst_scene = Path(dst_dir) / "scene" / "test_scene.blend"
                self.assertTrue(dst_scene.exists())
                
                # Check that mapping file was created
                mapping_file = Path(dst_dir) / "path_mapping.json"
                self.assertTrue(mapping_file.exists())
                
                mapping = json.loads(mapping_file.read_text())
                self.assertEqual(mapping[str(asset_file.resolve())], "//assets/texture.png")
                
                # Check that subprocess.run was called with expected arguments
                mock_run.assert_called_once()
                args, kwargs = mock_run.call_args
                cmd = args[0]
                
                self.assertEqual(cmd[0], "/mock/blender")
                self.assertEqual(cmd[1], "-b")
                self.assertEqual(cmd[2], str(dst_scene))
                self.assertEqual(cmd[3], "-P")
                self.assertTrue(cmd[4].endswith("remap_paths.py"))
                self.assertEqual(cmd[5], "--")
                self.assertEqual(cmd[6], str(mapping_file))

    def test_deterministic_output_reproducibility(self) -> None:
        """Running package creation twice on identical inputs produces identical manifest content."""
        records = [
            AssetRecord(asset_id="z_asset", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="hash-z", size_bytes=10, embedded=False),
            AssetRecord(asset_id="a_asset", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.RENDER_REQUIRED, status=AssetStatus.RESOLVED, sha256="hash-a", size_bytes=20, embedded=False),
            AssetRecord(asset_id="m_asset", asset_type=AssetType.MESH, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.EMBEDDED, embedded=True),
        ]

        with tempfile.TemporaryDirectory() as dir1, tempfile.TemporaryDirectory() as dir2:
            plan1 = self.planner.create_plan(records, package_id="pkg-repro")
            plan2 = self.planner.create_plan(list(reversed(records)), package_id="pkg-repro")

            m1 = self.builder.build_package(plan1, output_dir=dir1)
            m2 = self.builder.build_package(plan2, output_dir=dir2)

            content1 = m1.read_text(encoding="utf-8")
            content2 = m2.read_text(encoding="utf-8")

            self.assertEqual(content1, content2)
            self.assertEqual(
                hashlib.sha256(content1.encode()).hexdigest(),
                hashlib.sha256(content2.encode()).hexdigest(),
            )

    def test_dry_run_mode_creates_no_files(self) -> None:
        """dry_run=True returns the expected manifest path without creating directories or files."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            target_pkg_dir = Path(tmp_dir) / "dry_package"
            records = [
                AssetRecord(asset_id="a1", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="h1", size_bytes=100, embedded=False, package_path="assets/a1.png")
            ]
            plan = self.planner.create_plan(records, package_id="pkg-dry")
            manifest_path = self.builder.build_package(plan, output_dir=target_pkg_dir, dry_run=True)

            self.assertEqual(manifest_path, target_pkg_dir / "manifest.json")
            self.assertFalse(target_pkg_dir.exists())

    def test_missing_assets_tracked_in_plan(self) -> None:
        """Missing assets appear in plan.missing_assets and in manifest 'missing' array."""
        records = [
            AssetRecord(asset_id="missing.png", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.MISSING, embedded=False)
        ]
        plan = self.planner.create_plan(records, package_id="pkg-miss")
        self.assertEqual(len(plan.missing_assets), 1)
        self.assertEqual(plan.missing_assets[0].asset_id, "missing.png")
        self.assertEqual(plan.statistics.missing_assets, 1)

    def test_symlink_escape_prevention(self) -> None:
        """Symlinks should be resolved and their contents copied, not copied as symlinks."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            src_dir = base / "src"
            pkg_dir = base / "pkg"
            src_dir.mkdir()
            
            real_file = src_dir / "real.txt"
            real_file.write_text("secret content")
            
            link_file = src_dir / "link.txt"
            try:
                os.symlink(real_file, link_file)
            except OSError:
                # Symlinks might not be supported/permitted on Windows without admin,
                # skip test if we cannot create one.
                return
                
            records = [
                AssetRecord(
                    asset_id="linked", 
                    asset_type=AssetType.TEXTURE, 
                    selection_reason=SelectionReason.DEPENDENCY, 
                    status=AssetStatus.RESOLVED, 
                    sha256="hash", 
                    size_bytes=100, 
                    embedded=False, 
                    package_path="assets/link.txt",
                    source_path=str(link_file)
                )
            ]
            plan = self.planner.create_plan(records, package_id="pkg-link")
            self.builder.build_package(plan, output_dir=pkg_dir)
            
            dst_file = pkg_dir / "assets/link.txt"
            self.assertTrue(dst_file.exists())
            self.assertFalse(dst_file.is_symlink())
            self.assertEqual(dst_file.read_text(), "secret content")


# ──────────────────────────────────────────────────────────────────
# M4-D Unit Tests (Post-Copy Validation)
# ──────────────────────────────────────────────────────────────────


class PackageValidatorTests(unittest.TestCase):
    """Tests for PackageValidator (M4-D)."""

    def setUp(self) -> None:
        self.validator = PackageValidator()

    def test_valid_package_integrity_report_verified_true(self) -> None:
        """Valid package with all files present and matching SHA-256 hashes produces verified=True."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pkg_dir = Path(tmp_dir)
            assets_dir = pkg_dir / "assets"
            assets_dir.mkdir(parents=True, exist_ok=True)

            file1 = assets_dir / "tex1.png"
            file1.write_bytes(b"valid-texture-1")
            hash1 = hashlib.sha256(b"valid-texture-1").hexdigest()

            plan = PackagePlan(
                package_id="pkg-val",
                scene_name="Demo",
                camera="",
                frame_start=1,
                frame_end=24,
                deduplicated_assets=[
                    AssetRecord(
                        asset_id="texture:tex1.png",
                        asset_type=AssetType.TEXTURE,
                        selection_reason=SelectionReason.RENDER_REQUIRED,
                        package_path="assets/tex1.png",
                        status=AssetStatus.RESOLVED,
                        sha256=hash1,
                        size_bytes=len(b"valid-texture-1"),
                        embedded=False,
                    )
                ],
            )

            report = self.validator.validate(plan, package_dir=pkg_dir)

            self.assertTrue(report.verified)
            self.assertEqual(report.asset_count, 1)
            self.assertEqual(report.verified_count, 1)
            self.assertEqual(report.failed_assets, [])
            self.assertEqual(report.missing_assets, [])

    def test_tampered_file_detected_in_failed_assets(self) -> None:
        """Modifying file contents produces verified=False with asset ID in failed_assets."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pkg_dir = Path(tmp_dir)
            assets_dir = pkg_dir / "assets"
            assets_dir.mkdir(parents=True, exist_ok=True)

            file1 = assets_dir / "tex1.png"
            file1.write_bytes(b"tampered-content-bytes")  # Content altered

            original_hash = hashlib.sha256(b"original-content-bytes").hexdigest()

            plan = PackagePlan(
                package_id="pkg-val",
                scene_name="Demo",
                camera="",
                frame_start=1,
                frame_end=24,
                deduplicated_assets=[
                    AssetRecord(
                        asset_id="texture:tex1.png",
                        asset_type=AssetType.TEXTURE,
                        selection_reason=SelectionReason.RENDER_REQUIRED,
                        package_path="assets/tex1.png",
                        status=AssetStatus.RESOLVED,
                        sha256=original_hash,
                        size_bytes=100,
                        embedded=False,
                    )
                ],
            )

            report = self.validator.validate(plan, package_dir=pkg_dir)

            self.assertFalse(report.verified)
            self.assertEqual(report.verified_count, 0)
            self.assertIn("texture:tex1.png", report.failed_assets)
            self.assertEqual(report.missing_assets, [])

    def test_deleted_file_detected_in_missing_assets(self) -> None:
        """Missing physical file produces verified=False with asset ID in missing_assets."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pkg_dir = Path(tmp_dir)

            plan = PackagePlan(
                package_id="pkg-val",
                scene_name="Demo",
                camera="",
                frame_start=1,
                frame_end=24,
                deduplicated_assets=[
                    AssetRecord(
                        asset_id="texture:missing.png",
                        asset_type=AssetType.TEXTURE,
                        selection_reason=SelectionReason.RENDER_REQUIRED,
                        package_path="assets/missing.png",
                        status=AssetStatus.RESOLVED,
                        sha256="expectedhash",
                        size_bytes=100,
                        embedded=False,
                    )
                ],
            )

            report = self.validator.validate(plan, package_dir=pkg_dir)

            self.assertFalse(report.verified)
            self.assertEqual(report.verified_count, 0)
            self.assertIn("texture:missing.png", report.missing_assets)

    def test_validate_manifest_json(self) -> None:
        """PackageValidator.validate_manifest parses manifest.json directly and validates files."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pkg_dir = Path(tmp_dir)
            assets_dir = pkg_dir / "assets"
            assets_dir.mkdir(parents=True, exist_ok=True)

            f = assets_dir / "tex.png"
            f.write_bytes(b"hello-manifest")
            h = hashlib.sha256(b"hello-manifest").hexdigest()

            manifest_file = pkg_dir / "manifest.json"
            manifest_content = {
                "schema_version": "1.0",
                "package_id": "pkg-test",
                "assets": [
                    {
                        "asset_id": "tex-1",
                        "status": "resolved",
                        "package_path": "assets/tex.png",
                        "sha256": h,
                        "embedded": False,
                    }
                ],
            }
            manifest_file.write_text(json.dumps(manifest_content), encoding="utf-8")

            report = self.validator.validate_manifest(manifest_file)
            self.assertTrue(report.verified)
            self.assertEqual(report.verified_count, 1)

    def test_path_traversal_prevention_in_validate(self) -> None:
        """Path traversal in package_path should be caught and marked as missing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pkg_dir = Path(tmp_dir) / "pkg"
            pkg_dir.mkdir()
            
            plan = PackagePlan(
                package_id="pkg-val",
                scene_name="Demo",
                camera="",
                frame_start=1,
                frame_end=24,
                deduplicated_assets=[
                    AssetRecord(
                        asset_id="texture:tex1.png",
                        asset_type=AssetType.TEXTURE,
                        selection_reason=SelectionReason.RENDER_REQUIRED,
                        package_path="../outside.png",
                        status=AssetStatus.RESOLVED,
                        sha256="expectedhash",
                        size_bytes=100,
                        embedded=False,
                    )
                ],
            )
            report = self.validator.validate(plan, package_dir=pkg_dir)
            self.assertFalse(report.verified)
            self.assertIn("texture:tex1.png", report.missing_assets)

    def test_path_traversal_prevention_in_validate_manifest(self) -> None:
        """Path traversal in manifest file should be caught."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pkg_dir = Path(tmp_dir) / "pkg"
            pkg_dir.mkdir()
            manifest_file = pkg_dir / "manifest.json"
            manifest_content = {
                "schema_version": "1.0",
                "package_id": "pkg-test",
                "assets": [
                    {
                        "asset_id": "tex-1",
                        "status": "resolved",
                        "package_path": "../outside.png",
                        "sha256": "expectedhash",
                        "embedded": False,
                    }
                ],
            }
            manifest_file.write_text(json.dumps(manifest_content), encoding="utf-8")
            report = self.validator.validate_manifest(manifest_file)
            self.assertFalse(report.verified)
            self.assertIn("tex-1", report.missing_assets)


# ──────────────────────────────────────────────────────────────────
# SceneEngine M4 Integration Tests
# ──────────────────────────────────────────────────────────────────


class SceneEngineM4IntegrationTests(unittest.TestCase):
    """Integration tests for SceneEngine running Milestone 4 Smart Packaging."""

    def test_scene_engine_run_with_m4_smart_packaging(self) -> None:
        """SceneEngine.run with build_package=True, optimize_package_by_visibility=True runs M4 pipeline."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            tex_file = tmp_path / "wood.png"
            tex_file.write_bytes(b"wood-texture-bytes")

            scene_path = tmp_path / "scene.json"
            scene_path.write_text(json.dumps(PACKAGING_SCENE), encoding="utf-8")

            engine = SceneEngine()
            request = SceneEngineRequest(
                input_path=str(scene_path),
                scene_output=str(tmp_path / "out" / "scene.json"),
                graph_output=str(tmp_path / "out" / "graph.json"),
                package_output=str(tmp_path / "out" / "package.json"),
                build_package=True,
                optimize_package_by_visibility=True,
                frame_start=1,
                frame_end=24,
            )

            result = engine.run(request)

            # M3 stage verification
            self.assertIsNotNone(result.render_requirements)
            self.assertIsNotNone(result.visibility)

            # M4 stage verification
            self.assertIsNotNone(result.package_plan)
            self.assertIsInstance(result.package_plan, PackagePlan)
            self.assertEqual(len(result.package_plan.package_id), 12)
            self.assertGreater(len(result.package_plan.all_assets), 0)
            self.assertIsNotNone(result.package_plan.statistics)

            # Post-copy integrity verification
            self.assertIsNotNone(result.package_integrity)
            self.assertIsInstance(result.package_integrity, PackageIntegrityReport)
            self.assertTrue(result.package_integrity.verified)

            # Backward compatibility verification
            self.assertIsNotNone(result.package)
            self.assertTrue(result.package_output_path.exists())

    def test_scene_engine_missing_asset_handling(self) -> None:
        """Scene referencing missing external files records them in missing_assets without crashing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            scene_path = tmp_path / "scene.json"
            # In PACKAGING_SCENE, fabric.png does not exist on disk in tmp_dir
            scene_path.write_text(json.dumps(PACKAGING_SCENE), encoding="utf-8")

            engine = SceneEngine()
            request = SceneEngineRequest(
                input_path=str(scene_path),
                scene_output=str(tmp_path / "out" / "scene.json"),
                graph_output=str(tmp_path / "out" / "graph.json"),
                package_output=str(tmp_path / "out" / "package.json"),
                build_package=True,
                optimize_package_by_visibility=True,
                frame_start=1,
                frame_end=24,
            )

            result = engine.run(request)

            self.assertIsNotNone(result.package_plan)
            # Missing files should be captured
            missing_ids = [m.asset_id for m in result.package_plan.missing_assets]
            self.assertTrue(len(missing_ids) > 0)


if __name__ == "__main__":
    unittest.main()
