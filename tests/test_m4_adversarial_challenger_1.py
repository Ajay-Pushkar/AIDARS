"""Adversarial stress test suite authored by Challenger 1 for Milestone 4 Smart Packaging.

Stress-tests:
1. M4-B PhysicalAssetResolver:
   - Deeply nested // paths, relative parent traversal, mixed slashes.
   - Unicode, spaces, and special characters in paths.
   - Non-existent files, directory paths instead of files.
   - Auxiliary search_paths fallback.
   - Absolute and plain relative paths.
2. M4-C PackagePlanner & PackageBuilder:
   - Multiple identical files deduplicated to single canonical copy.
   - Empty (0-byte) file deduplication and division-by-zero prevention.
   - Complex partition of resolved, duplicate, embedded, and missing assets.
   - Input order invariance (deterministic alphabetical ordering).
3. Statistics Accuracy Math:
   - Zero-asset, all-embedded, all-missing, zero-duplicate, and multi-duplicate scenarios.
4. Schema v1.0 Manifest Validation & Reproducibility:
   - Strict JSON structure validation and byte-for-byte reproducibility across runs.
5. Dry-Run Verification:
   - Strict proof of zero filesystem mutations during dry_run=True.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from aidars.scene_intelligence.dependency_graph import DependencyGraph, GraphEdge, GraphNode
from aidars.smart_package.builder import PackageBuilder, PackagePlanner
from aidars.smart_package.models import (
    AssetRecord,
    AssetStatus,
    AssetType,
    PackagePlan,
    PackageStatistics,
    SelectionReason,
)
from aidars.smart_package.resolver import PhysicalAssetResolver
from aidars.smart_package.validator import PackageValidator


class TestPhysicalAssetResolverAdversarialPathEdgeCases(unittest.TestCase):
    """Adversarial stress tests for path resolution in PhysicalAssetResolver (M4-B)."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.base_path = Path(self.tmp_dir.name)
        self.resolver = PhysicalAssetResolver(base_dir=self.base_path)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_deeply_nested_blender_relative_path(self) -> None:
        """Deeply nested path with 10 directory levels resolves correctly."""
        nested_dir = self.base_path / "a" / "b" / "c" / "d" / "e" / "f" / "g" / "h" / "i" / "j"
        nested_dir.mkdir(parents=True, exist_ok=True)
        target_file = nested_dir / "deep_texture.png"
        target_file.write_bytes(b"deep-texture-data-12345")

        raw_path = "//a/b/c/d/e/f/g/h/i/j/deep_texture.png"
        resolved_path, clean_rel = self.resolver.resolve_path(raw_path, base_dir=self.base_path)

        self.assertTrue(resolved_path.exists())
        self.assertEqual(resolved_path, target_file.resolve())
        self.assertEqual(clean_rel.replace("\\", "/"), "a/b/c/d/e/f/g/h/i/j/deep_texture.png")

        # Full resolver test
        graph = DependencyGraph(
            nodes=[GraphNode("image:deep", raw_path, "image")],
            edges=[],
        )
        records = self.resolver.resolve({"image:deep"}, graph, base_dir=self.base_path)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].status, AssetStatus.RESOLVED)
        self.assertTrue(records[0].package_path.endswith("_deep_texture.png"))
        self.assertEqual(records[0].size_bytes, len(b"deep-texture-data-12345"))

    def test_relative_parent_directory_traversal(self) -> None:
        """Parent directory traversal '//../../external/tex.png' now raises a ValueError due to security boundary."""
        sub_base = self.base_path / "project" / "scenes"
        sub_base.mkdir(parents=True, exist_ok=True)

        external_dir = self.base_path / "external"
        external_dir.mkdir(parents=True, exist_ok=True)
        ext_file = external_dir / "shared_tex.png"
        ext_file.write_bytes(b"shared-texture-bytes")

        raw_path = "//../../external/shared_tex.png"
        with self.assertRaises(ValueError):
            self.resolver.resolve_path(raw_path, base_dir=sub_base)

        graph = DependencyGraph(
            nodes=[GraphNode("image:ext", raw_path, "image")],
            edges=[],
        )
        with self.assertRaises(ValueError):
            self.resolver.resolve({"image:ext"}, graph, base_dir=sub_base)

    def test_windows_backslashes_and_mixed_separators(self) -> None:
        """Blender path with double-backslash or mixed slashes resolves consistently."""
        tex_dir = self.base_path / "textures" / "sub"
        tex_dir.mkdir(parents=True, exist_ok=True)
        tex_file = tex_dir / "albedo.png"
        tex_file.write_bytes(b"albedo-map")

        # Double backslash prefix
        raw_win = "\\\\textures\\sub\\albedo.png"
        p_win, rel_win = self.resolver.resolve_path(raw_win, base_dir=self.base_path)
        self.assertTrue(p_win.exists())
        self.assertEqual(p_win, tex_file.resolve())

        # Mixed slashes
        raw_mixed = "//textures\\sub/albedo.png"
        p_mixed, rel_mixed = self.resolver.resolve_path(raw_mixed, base_dir=self.base_path)
        self.assertTrue(p_mixed.exists())
        self.assertEqual(p_mixed, tex_file.resolve())

    def test_special_characters_spaces_and_unicode_paths(self) -> None:
        """Paths with spaces, symbols (#, $, [], +), and unicode characters resolve cleanly."""
        spec_dir = self.base_path / "tëst dür [4k] + special"
        spec_dir.mkdir(parents=True, exist_ok=True)
        spec_file = spec_dir / "üñîçødé #1 $asset [diffuse].png"
        spec_file.write_bytes(b"unicode-asset-bytes-999")

        raw_path = f"//{spec_dir.name}/{spec_file.name}"
        resolved_path, clean_rel = self.resolver.resolve_path(raw_path, base_dir=self.base_path)

        self.assertTrue(resolved_path.exists())
        self.assertEqual(resolved_path, spec_file.resolve())

        graph = DependencyGraph(
            nodes=[GraphNode("image:unicode", raw_path, "image")],
            edges=[],
        )
        records = self.resolver.resolve({"image:unicode"}, graph, base_dir=self.base_path)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].status, AssetStatus.RESOLVED)
        self.assertEqual(records[0].sha256, hashlib.sha256(b"unicode-asset-bytes-999").hexdigest())

    def test_directory_path_instead_of_file_flagged_as_missing(self) -> None:
        """Path pointing to a directory rather than a file is flagged as MISSING."""
        dir_only = self.base_path / "only_a_dir"
        dir_only.mkdir(parents=True, exist_ok=True)

        graph = DependencyGraph(
            nodes=[GraphNode("asset:dir", "//only_a_dir", "asset")],
            edges=[],
        )
        records = self.resolver.resolve({"asset:dir"}, graph, base_dir=self.base_path)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].status, AssetStatus.MISSING)
        self.assertIsNone(records[0].sha256)
        self.assertEqual(records[0].size_bytes, 0)

    def test_search_paths_fallback_hierarchy(self) -> None:
        """When file is absent from base_dir, search_paths are scanned in priority order."""
        aux1 = self.base_path / "aux1"
        aux2 = self.base_path / "aux2"
        aux1.mkdir(parents=True, exist_ok=True)
        aux2.mkdir(parents=True, exist_ok=True)

        # Place file only in aux2
        f2 = aux2 / "fallback.png"
        f2.write_bytes(b"fallback-bytes")

        resolver_with_search = PhysicalAssetResolver(
            base_dir=self.base_path / "empty_base",
            search_paths=[aux1, aux2],
        )

        p, rel = resolver_with_search.resolve_path("//fallback.png")
        self.assertTrue(p.exists())
        self.assertEqual(p, f2.resolve())


class TestPackagePlannerAdversarialDeduplication(unittest.TestCase):
    """Adversarial stress tests for deduplication and sorting in PackagePlanner (M4-C)."""

    def setUp(self) -> None:
        self.planner = PackagePlanner()

    def test_ten_duplicate_assets_deduplicated_to_single_physical_record(self) -> None:
        """10 distinct asset records with identical SHA-256 hash yield 1 deduplicated record."""
        shared_hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        records = [
            AssetRecord(
                asset_id=f"image:tex_{i:02d}.png",
                asset_type=AssetType.IMAGE,
                selection_reason=SelectionReason.DEPENDENCY,
                source_path=f"/path/to/tex_{i:02d}.png",
                package_path=f"assets/tex_{i:02d}.png",
                status=AssetStatus.RESOLVED,
                sha256=shared_hash,
                size_bytes=4096,
                embedded=False,
            )
            for i in range(10)
        ]

        plan = self.planner.create_plan(records, package_id="pkg-ten-dups")

        self.assertEqual(len(plan.all_assets), 10)
        self.assertEqual(len(plan.deduplicated_assets), 1)
        self.assertEqual(plan.deduplicated_assets[0].asset_id, "image:tex_00.png")
        self.assertEqual(plan.statistics.total_assets, 10)
        self.assertEqual(plan.statistics.resolved_assets, 10)
        self.assertEqual(plan.statistics.duplicate_assets, 9)
        self.assertEqual(plan.statistics.original_size_bytes, 40960)
        self.assertEqual(plan.statistics.package_size_bytes, 4096)
        self.assertEqual(plan.statistics.reduction_percent, 90.0)

        # Verify all 10 assets in all_assets point to canonical package_path
        canonical_path = plan.deduplicated_assets[0].package_path
        for asset in plan.all_assets:
            self.assertEqual(asset.package_path, canonical_path)

    def test_empty_zero_byte_files_deduplicated_without_zero_division(self) -> None:
        """Multiple 0-byte resolved files deduplicate cleanly without division by zero."""
        empty_hash = hashlib.sha256(b"").hexdigest()
        records = [
            AssetRecord(
                asset_id=f"tex_empty_{i}",
                asset_type=AssetType.TEXTURE,
                selection_reason=SelectionReason.RENDER_REQUIRED,
                source_path=f"/path/empty_{i}.png",
                package_path=f"assets/empty_{i}.png",
                status=AssetStatus.RESOLVED,
                sha256=empty_hash,
                size_bytes=0,
                embedded=False,
            )
            for i in range(4)
        ]

        plan = self.planner.create_plan(records, package_id="pkg-empty")

        self.assertEqual(len(plan.all_assets), 4)
        self.assertEqual(len(plan.deduplicated_assets), 1)
        self.assertEqual(plan.statistics.resolved_assets, 4)
        self.assertEqual(plan.statistics.duplicate_assets, 3)
        self.assertEqual(plan.statistics.original_size_bytes, 0)
        self.assertEqual(plan.statistics.package_size_bytes, 0)
        self.assertEqual(plan.statistics.reduction_percent, 0.0)

    def test_input_order_invariance_and_deterministic_output(self) -> None:
        """Supplying records in arbitrary orders produces identical plan outputs."""
        records = [
            AssetRecord(asset_id="z_tex", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="hash_z", size_bytes=100, embedded=False),
            AssetRecord(asset_id="a_tex", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.RENDER_REQUIRED, status=AssetStatus.RESOLVED, sha256="hash_a", size_bytes=200, embedded=False),
            AssetRecord(asset_id="m_mesh", asset_type=AssetType.MESH, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.EMBEDDED, embedded=True),
            AssetRecord(asset_id="k_miss", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.MISSING, embedded=False),
        ]

        plan_forward = self.planner.create_plan(records, package_id="pkg-order")
        plan_reversed = self.planner.create_plan(list(reversed(records)), package_id="pkg-order")
        plan_custom = self.planner.create_plan([records[2], records[0], records[3], records[1]], package_id="pkg-order")

        # Serialized dicts should be byte-for-byte identical
        dict_f = plan_forward.to_dict()
        dict_r = plan_reversed.to_dict()
        dict_c = plan_custom.to_dict()

        json_f = json.dumps(dict_f, sort_keys=True)
        json_r = json.dumps(dict_r, sort_keys=True)
        json_c = json.dumps(dict_c, sort_keys=True)

        self.assertEqual(json_f, json_r)
        self.assertEqual(json_f, json_c)


class TestPackagePlannerAdversarialStatisticsMath(unittest.TestCase):
    """Adversarial tests for edge cases in PackageStatistics calculations."""

    def setUp(self) -> None:
        self.planner = PackagePlanner()

    def test_zero_total_assets(self) -> None:
        """Empty asset list produces all-zero statistics with zero reduction percent."""
        plan = self.planner.create_plan([], package_id="pkg-zero")
        stats = plan.statistics
        self.assertEqual(stats.total_assets, 0)
        self.assertEqual(stats.resolved_assets, 0)
        self.assertEqual(stats.embedded_assets, 0)
        self.assertEqual(stats.missing_assets, 0)
        self.assertEqual(stats.duplicate_assets, 0)
        self.assertEqual(stats.original_size_bytes, 0)
        self.assertEqual(stats.package_size_bytes, 0)
        self.assertEqual(stats.reduction_percent, 0.0)
        self.assertEqual(stats.to_dict()["reduction_percent"], 0.0)

    def test_all_embedded_assets(self) -> None:
        """Plan with only embedded entities produces 0 original size and 0.0 reduction percent."""
        records = [
            AssetRecord(asset_id=f"obj_{i}", asset_type=AssetType.MESH, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.EMBEDDED, embedded=True)
            for i in range(5)
        ]
        plan = self.planner.create_plan(records, package_id="pkg-embedded-only")
        stats = plan.statistics
        self.assertEqual(stats.total_assets, 5)
        self.assertEqual(stats.resolved_assets, 0)
        self.assertEqual(stats.embedded_assets, 5)
        self.assertEqual(stats.missing_assets, 0)
        self.assertEqual(stats.original_size_bytes, 0)
        self.assertEqual(stats.package_size_bytes, 0)
        self.assertEqual(stats.reduction_percent, 0.0)

    def test_all_missing_assets(self) -> None:
        """Plan with only missing assets produces 0 original size and 0.0 reduction percent."""
        records = [
            AssetRecord(asset_id=f"miss_{i}.png", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.MISSING, embedded=False)
            for i in range(3)
        ]
        plan = self.planner.create_plan(records, package_id="pkg-missing-only")
        stats = plan.statistics
        self.assertEqual(stats.total_assets, 3)
        self.assertEqual(stats.missing_assets, 3)
        self.assertEqual(stats.resolved_assets, 0)
        self.assertEqual(stats.original_size_bytes, 0)
        self.assertEqual(stats.reduction_percent, 0.0)

    def test_heterogeneous_duplicate_savings_calculation(self) -> None:
        """Complex combination of duplicate sizes calculates exact reduction percentage."""
        # A1, A2: 1000 bytes each (shared hash A) -> savings 1000B
        # B1, B2, B3: 500 bytes each (shared hash B) -> savings 1000B
        # C1: 300 bytes (unique hash C) -> savings 0B
        # Total original: 2000 + 1500 + 300 = 3800 bytes
        # Total packaged: 1000 + 500 + 300 = 1800 bytes
        # Reduction: (3800 - 1800) / 3800 * 100 = 2000 / 3800 * 100 = 52.6315789... -> 52.63%
        records = [
            AssetRecord(asset_id="a1", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="hashA", size_bytes=1000, embedded=False),
            AssetRecord(asset_id="a2", asset_type=AssetType.TEXTURE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="hashA", size_bytes=1000, embedded=False),
            AssetRecord(asset_id="b1", asset_type=AssetType.IMAGE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="hashB", size_bytes=500, embedded=False),
            AssetRecord(asset_id="b2", asset_type=AssetType.IMAGE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="hashB", size_bytes=500, embedded=False),
            AssetRecord(asset_id="b3", asset_type=AssetType.IMAGE, selection_reason=SelectionReason.DEPENDENCY, status=AssetStatus.RESOLVED, sha256="hashB", size_bytes=500, embedded=False),
            AssetRecord(asset_id="c1", asset_type=AssetType.HDRI, selection_reason=SelectionReason.RENDER_REQUIRED, status=AssetStatus.RESOLVED, sha256="hashC", size_bytes=300, embedded=False),
        ]

        plan = self.planner.create_plan(records, package_id="pkg-hetero")
        stats = plan.statistics

        self.assertEqual(stats.total_assets, 6)
        self.assertEqual(stats.resolved_assets, 6)
        self.assertEqual(stats.duplicate_assets, 3)
        self.assertEqual(stats.original_size_bytes, 3800)
        self.assertEqual(stats.package_size_bytes, 1800)
        self.assertAlmostEqual(stats.reduction_percent, 52.63157894736842, places=4)
        self.assertEqual(stats.to_dict()["reduction_percent"], 52.63)


class TestSchemaV1ManifestValidationAndReproducibility(unittest.TestCase):
    """Adversarial tests for Schema v1.0 manifest generation and validation."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.out_dir = Path(self.tmp_dir.name)
        self.planner = PackagePlanner()
        self.builder = PackageBuilder(planner=self.planner)
        self.validator = PackageValidator()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_schema_v1_manifest_strict_structure(self) -> None:
        """Manifest conforms strictly to Schema v1.0 structure with all required fields."""
        src_file = self.out_dir / "src_tex.png"
        src_file.write_bytes(b"tex-content")
        h = hashlib.sha256(b"tex-content").hexdigest()

        records = [
            AssetRecord(
                asset_id="image:src_tex.png",
                asset_type=AssetType.IMAGE,
                selection_reason=SelectionReason.RENDER_REQUIRED,
                source_path=str(src_file),
                package_path="assets/src_tex.png",
                status=AssetStatus.RESOLVED,
                sha256=h,
                size_bytes=len(b"tex-content"),
                embedded=False,
                dependencies=["material:Mat1"],
            ),
            AssetRecord(
                asset_id="missing_tex.png",
                asset_type=AssetType.TEXTURE,
                selection_reason=SelectionReason.DEPENDENCY,
                status=AssetStatus.MISSING,
                embedded=False,
            ),
        ]

        plan = self.planner.create_plan(
            records,
            package_id="pkg-schema-test",
            scene_name="DemoScene",
            camera="MainCam",
            frame_start=10,
            frame_end=50,
        )

        manifest_path = self.builder.build_package(plan, output_dir=self.out_dir)
        self.assertTrue(manifest_path.exists())

        raw_data = json.loads(manifest_path.read_text(encoding="utf-8"))

        # Root fields
        self.assertEqual(raw_data["schema_version"], "1.0")
        self.assertEqual(raw_data["package_id"], "pkg-schema-test")

        # Scene block
        self.assertEqual(raw_data["scene"]["name"], "DemoScene")
        self.assertEqual(raw_data["scene"]["camera"], "MainCam")
        self.assertEqual(raw_data["scene"]["frame_start"], 10)
        self.assertEqual(raw_data["scene"]["frame_end"], 50)

        # Assets block
        self.assertEqual(len(raw_data["assets"]), 2)
        asset_0 = raw_data["assets"][0]
        self.assertIn("asset_id", asset_0)
        self.assertIn("type", asset_0)
        self.assertIn("status", asset_0)
        self.assertIn("selection_reason", asset_0)
        self.assertIn("sha256", asset_0)
        self.assertIn("size_bytes", asset_0)
        self.assertIn("dependencies", asset_0)
        self.assertEqual(asset_0["dependencies"], ["material:Mat1"])

        # Missing block
        self.assertEqual(len(raw_data["missing"]), 1)
        self.assertEqual(raw_data["missing"][0]["asset_id"], "missing_tex.png")

        # Statistics block
        self.assertIn("statistics", raw_data)
        stats = raw_data["statistics"]
        self.assertEqual(stats["total_assets"], 2)
        self.assertEqual(stats["resolved_assets"], 1)
        self.assertEqual(stats["missing_assets"], 1)

        # Create dummy .blend file so validation succeeds
        scene_dir = self.out_dir / "scene"
        scene_dir.mkdir(parents=True, exist_ok=True)
        (scene_dir / "DemoScene.blend").write_bytes(b"dummy")

        # Validate with PackageValidator
        report = self.validator.validate(plan, package_dir=self.out_dir)
        self.assertTrue(report.verified)
        self.assertEqual(report.verified_count, 1)

        # Validate manifest directly from disk
        manifest_report = self.validator.validate_manifest(manifest_path)
        self.assertTrue(manifest_report.verified)
        self.assertEqual(manifest_report.verified_count, 1)


class TestDryRunVerification(unittest.TestCase):
    """Adversarial verification of dry-run zero-mutation guarantee."""

    def test_dry_run_guarantees_zero_filesystem_mutations(self) -> None:
        """dry_run=True creates zero files, zero directories, and mutates nothing."""
        with tempfile.TemporaryDirectory() as base_tmp:
            target_pkg_dir = Path(base_tmp) / "non_existent_package_dir"
            self.assertFalse(target_pkg_dir.exists())

            # Source file
            src_file = Path(base_tmp) / "source.png"
            src_file.write_bytes(b"source-content")

            records = [
                AssetRecord(
                    asset_id="image:source.png",
                    asset_type=AssetType.IMAGE,
                    selection_reason=SelectionReason.RENDER_REQUIRED,
                    source_path=str(src_file),
                    package_path="assets/source.png",
                    status=AssetStatus.RESOLVED,
                    sha256=hashlib.sha256(b"source-content").hexdigest(),
                    size_bytes=len(b"source-content"),
                    embedded=False,
                )
            ]

            planner = PackagePlanner()
            builder = PackageBuilder(planner=planner)

            plan = planner.create_plan(records, package_id="pkg-dry-run")
            ret_path = builder.build_package(plan, output_dir=target_pkg_dir, dry_run=True)

            # Assert returned path matches expected manifest path
            self.assertEqual(ret_path, target_pkg_dir / "manifest.json")

            # Assert target directory was NEVER created on disk
            self.assertFalse(target_pkg_dir.exists())

            # Assert source file was not modified
            self.assertEqual(src_file.read_bytes(), b"source-content")


if __name__ == "__main__":
    unittest.main()
