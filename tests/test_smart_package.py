import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.smart_package.builder import PackageAsset, SmartPackageBuilder


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


if __name__ == "__main__":
    unittest.main()
