"""Milestone 4-D Post-Copy Validation.

Verifies that physical packages match their declared manifests and plan contracts:
1. Every resolved external asset exists at its expected package_path.
2. Every packaged asset's SHA-256 hash matches the manifest.
3. Missing or corrupted/tampered files are flagged in PackageIntegrityReport.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import List, Union

from .models import AssetStatus, PackageIntegrityReport, PackagePlan


class PackageValidator:
    """Validate physical integrity of a constructed render package."""

    @staticmethod
    def compute_sha256(file_path: Union[str, Path]) -> str:
        """Compute SHA-256 hash of a file incrementally."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def validate(
        self,
        plan: PackagePlan,
        package_dir: Union[str, Path],
        blender_executable: str | None = None,
    ) -> PackageIntegrityReport:
        """Verify that every packaged asset exists and its SHA-256 matches the plan.
        If a blender_executable is provided, also verify that the packaged .blend
        file correctly resolves all its external asset references.

        Args:
            plan: The PackagePlan containing expected assets.
            package_dir: Root directory of the constructed package.
            blender_executable: Optional path to blender.

        Returns:
            PackageIntegrityReport with verified=True iff all assets exist and match hashes.
        """
        pkg_path = Path(package_dir)
        failed_assets: List[str] = []
        missing_assets: List[str] = []
        verified_count = 0

        # Validate all deduplicated resolved assets that should exist physically
        target_records = [
            r
            for r in plan.deduplicated_assets
            if r.status == AssetStatus.RESOLVED and r.package_path and r.sha256
        ]
        asset_count = len(target_records)

        for record in target_records:
            try:
                dest_file = (pkg_path / record.package_path).resolve()
            except Exception:
                missing_assets.append(record.asset_id)
                continue
            
            if not dest_file.is_relative_to(pkg_path.resolve()) or not dest_file.exists() or not dest_file.is_file():
                missing_assets.append(record.asset_id)
            else:
                actual_hash = self.compute_sha256(dest_file)
                if actual_hash != record.sha256:
                    failed_assets.append(record.asset_id)
                else:
                    verified_count += 1

        verified = (
            len(failed_assets) == 0
            and len(missing_assets) == 0
            and verified_count == asset_count
        )

        if verified and blender_executable:
            # The packaged scene file is guaranteed to be named {plan.scene_name}.blend
            # and located in the scene/ directory.
            blend_file = pkg_path / "scene" / f"{plan.scene_name}.blend"
            if blend_file.exists():
                verify_script = Path(__file__).parent / "blender_scripts" / "verify_package.py"
                import subprocess
                try:
                    res = subprocess.run(
                        [
                            blender_executable,
                            "-b",
                            str(blend_file),
                            "-P",
                            str(verify_script),
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                except subprocess.CalledProcessError as e:
                    # Blender verification failed
                    verified = False
                    failed_assets.append("blender_asset_resolution_failed")

        return PackageIntegrityReport(
            verified=verified,
            asset_count=asset_count,
            verified_count=verified_count,
            failed_assets=sorted(failed_assets),
            missing_assets=sorted(missing_assets),
        )

    def validate_manifest(self, manifest_path: Union[str, Path]) -> PackageIntegrityReport:
        """Validate package integrity directly from a manifest.json file.

        Args:
            manifest_path: Path to manifest.json file.

        Returns:
            PackageIntegrityReport.
        """
        mpath = Path(manifest_path)
        if not mpath.exists():
            raise FileNotFoundError(f"Manifest file not found: {mpath}")

        package_dir = mpath.parent
        with mpath.open("r", encoding="utf-8") as f:
            data = json.load(f)

        failed_assets: List[str] = []
        missing_assets: List[str] = []
        verified_count = 0

        raw_assets = data.get("assets", [])
        # Only validate external resolved assets with package_path and sha256
        external_assets = [
            a
            for a in raw_assets
            if a.get("status") == AssetStatus.RESOLVED.value
            and a.get("package_path")
            and a.get("sha256")
            and not a.get("embedded", False)
        ]

        # Deduplicate by package_path to avoid counting same physical file multiple times
        seen_pkg_paths = set()
        unique_targets = []
        for a in external_assets:
            pp = a.get("package_path")
            if pp not in seen_pkg_paths:
                seen_pkg_paths.add(pp)
                unique_targets.append(a)

        asset_count = len(unique_targets)

        for a in unique_targets:
            try:
                dest_file = (package_dir / a["package_path"]).resolve()
            except Exception:
                missing_assets.append(a["asset_id"])
                continue
            
            if not dest_file.is_relative_to(package_dir.resolve()) or not dest_file.exists() or not dest_file.is_file():
                missing_assets.append(a["asset_id"])
            else:
                actual_hash = self.compute_sha256(dest_file)
                if actual_hash != a["sha256"]:
                    failed_assets.append(a["asset_id"])
                else:
                    verified_count += 1

        verified = (
            len(failed_assets) == 0
            and len(missing_assets) == 0
            and verified_count == asset_count
        )

        return PackageIntegrityReport(
            verified=verified,
            asset_count=asset_count,
            verified_count=verified_count,
            failed_assets=sorted(failed_assets),
            missing_assets=sorted(missing_assets),
        )
