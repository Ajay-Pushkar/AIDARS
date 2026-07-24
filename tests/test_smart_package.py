import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.smart_package.builder import PackageAsset, SmartPackageBuilder
from aidars.scene_intelligence.engine import SceneIntelligenceEngine
from aidars.scene_intelligence.dependency_graph import DependencyGraphBuilder
from aidars.visibility.engine import VisibilityAnalyzer


class SmartPackageBuilderTests(unittest.TestCase):
    def test_build_package_creates_manifest(self) -> None:
        builder = SmartPackageBuilder()
        manifest = builder.build_package(
            1,
            24,
            [PackageAsset(path="/assets/chair.blend", kind="blend", size_bytes=1024)],
        )

        self.assertEqual(manifest.package_id, "pkg-1-24")
        self.assertEqual(manifest.frame_start, 1)
        self.assertEqual(manifest.frame_end, 24)
        self.assertEqual(manifest.assets[0].kind, "blend")

    def test_build_package_selects_assets_by_frame_range_and_estimates_files(self) -> None:
        builder = SmartPackageBuilder()
        assets = [
            PackageAsset(path="/assets/chair.blend", kind="blend", size_bytes=1024, frame_start=1, frame_end=24),
            PackageAsset(path="/assets/hero.fbx", kind="fbx", size_bytes=2048, frame_start=25, frame_end=50),
        ]

        manifest = builder.build_package(20, 24, assets)

        self.assertEqual(len(manifest.assets), 1)
        self.assertEqual(manifest.assets[0].path, "/assets/chair.blend")
        self.assertEqual(manifest.metadata["required_file_count"], 1)
        self.assertEqual(manifest.metadata["estimated_total_size_bytes"], 1024)

    def test_write_manifest_writes_json(self) -> None:
        builder = SmartPackageBuilder()
        manifest = builder.build_package(10, 15)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "package.json"
            builder.write_manifest(manifest, output_path)

            self.assertTrue(output_path.exists())
            payload = output_path.read_text(encoding="utf-8")
            self.assertIn("package_id", payload)

    def test_build_optimized_package_prunes_assets_unreachable_from_visible_objects(self) -> None:
        # Two objects, each linking a different external asset. Only one
        # object is visible in the target frame range.
        scene_data = {
            "metadata": {"name": "Demo", "frame_start": 1, "frame_end": 100, "fps": 24},
            "collections": [],
            "objects": [
                {
                    "name": "Hero",
                    "id": "obj-hero",
                    "type": "MESH",
                    "visibility": {"hide_render": False, "hide_viewport": False},
                    "referenced_assets": ["/assets/hero.blend"],
                },
                {
                    "name": "Backdrop",
                    "id": "obj-backdrop",
                    "type": "MESH",
                    "visibility": {"hide_render": True, "hide_viewport": False},
                    "referenced_assets": ["/assets/backdrop.blend"],
                },
            ],
            "lights": [],
            "materials": [],
            "textures": [],
            "images": [],
        }

        engine = SceneIntelligenceEngine()
        snapshot = engine.analyze_scene_data(scene_data)
        graph = DependencyGraphBuilder().build(snapshot)
        visibility = VisibilityAnalyzer().analyze(snapshot, frame_start=1, frame_end=24)

        assets = [
            PackageAsset(path="/assets/hero.blend", kind="blend", size_bytes=1000),
            PackageAsset(path="/assets/backdrop.blend", kind="blend", size_bytes=5000),
            PackageAsset(path="/assets/unrelated.png", kind="texture", size_bytes=200),  # not in graph at all
        ]

        builder = SmartPackageBuilder()
        manifest = builder.build_optimized_package(1, 24, assets, graph, visibility.visible_object_ids)

        kept_paths = {asset.path for asset in manifest.assets}
        self.assertIn("/assets/hero.blend", kept_paths)
        self.assertNotIn("/assets/backdrop.blend", kept_paths)
        # An asset the graph has no node for at all is kept conservatively.
        self.assertIn("/assets/unrelated.png", kept_paths)

        self.assertEqual(manifest.metadata["visibility_pruned_asset_count"], 1)
        self.assertEqual(manifest.metadata["visibility_pruned_size_bytes"], 5000)


if __name__ == "__main__":
    unittest.main()
