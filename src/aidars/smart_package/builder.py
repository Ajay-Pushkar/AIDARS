from __future__ import annotations

import json
import logging
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

from .models import (
    AssetRecord,
    AssetStatus,
    PackagePlan,
    PackageStatistics,
)

if TYPE_CHECKING:
    from aidars.scene_intelligence.dependency_graph import DependencyGraph

logger = logging.getLogger(__name__)

class PackagePlanner:
    """Create an execution-ready PackagePlan from resolved AssetRecord objects.

    Responsibilities:
    - Deduplicate external assets by SHA-256 hash.
    - Track missing assets.
    - Compute package statistics (asset counts, sizes, deduplication savings).
    - Guarantee deterministic output ordering.
    """

    def create_plan(
        self,
        asset_records: List[AssetRecord],
        package_id: str,
        scene_name: str = "scene",
        camera: str = "",
        frame_start: int = 1,
        frame_end: int = 24,
    ) -> PackagePlan:
        """Produce a complete PackagePlan from resolved asset records.

        Args:
            asset_records: List of AssetRecord instances from PhysicalAssetResolver.
            package_id: Unique identifier for this package.
            scene_name: Name of the scene.
            camera: Active rendering camera name or identifier.
            frame_start: First frame in package.
            frame_end: Last frame in package.

        Returns:
            A fully-formed PackagePlan instance.
        """
        # Sort for deterministic processing
        sorted_records = sorted(asset_records, key=lambda a: a.asset_id)

        all_assets: List[AssetRecord] = list(sorted_records)
        missing_assets: List[AssetRecord] = [
            r for r in sorted_records if r.status == AssetStatus.MISSING
        ]
        embedded_assets: List[AssetRecord] = [
            r for r in sorted_records if r.status == AssetStatus.EMBEDDED
        ]
        resolved_assets: List[AssetRecord] = [
            r for r in sorted_records if r.status == AssetStatus.RESOLVED
        ]

        # Deduplicate resolved external assets by SHA-256 hash
        seen_hashes: Dict[str, AssetRecord] = {}
        deduplicated_assets: List[AssetRecord] = []

        for record in resolved_assets:
            if record.sha256 is None:
                deduplicated_assets.append(record)
                continue

            if record.sha256 not in seen_hashes:
                seen_hashes[record.sha256] = record
                deduplicated_assets.append(record)
            else:
                # Map duplicate asset's package_path to canonical package_path
                canonical = seen_hashes[record.sha256]
                record.package_path = canonical.package_path

        # Calculate statistics
        total_count = len(all_assets)
        resolved_count = len(resolved_assets)
        embedded_count = len(embedded_assets)
        missing_count = len(missing_assets)
        duplicate_count = resolved_count - len(deduplicated_assets)

        original_size = sum(r.size_bytes for r in resolved_assets)
        package_size = sum(r.size_bytes for r in deduplicated_assets)

        reduction_pct = (
            ((original_size - package_size) / original_size * 100.0)
            if original_size > 0
            else 0.0
        )

        statistics = PackageStatistics(
            total_assets=total_count,
            resolved_assets=resolved_count,
            embedded_assets=embedded_count,
            missing_assets=missing_count,
            duplicate_assets=duplicate_count,
            original_size_bytes=original_size,
            package_size_bytes=package_size,
            reduction_percent=reduction_pct,
        )

        return PackagePlan(
            package_id=package_id,
            scene_name=scene_name,
            camera=camera,
            frame_start=frame_start,
            frame_end=frame_end,
            all_assets=all_assets,
            deduplicated_assets=sorted(deduplicated_assets, key=lambda a: a.asset_id),
            missing_assets=sorted(missing_assets, key=lambda a: a.asset_id),
            statistics=statistics,
        )


class PackageBuilder:
    """Physically construct a smart package directory layout on disk.

    Copies resolved external assets to a canonical package layout and writes
    a deterministic schema v1.0 manifest.json.
    """

    def __init__(self, planner: Optional[PackagePlanner] = None) -> None:
        self.planner = planner or PackagePlanner()

    def build_and_validate(
        self,
        plan: PackagePlan,
        final_output_dir: Union[str, Path],
        validator: "PackageValidator",
        scene_source_path: Optional[Union[str, Path]] = None,
        blender_executable: Optional[str] = None,
    ) -> "PackageIntegrityReport":
        """Atomically construct, validate, and publish a package.
        
        This builds into a .tmp directory, runs the validator, and if verified,
        atomically replaces the final output directory.
        """
        import os
        import tempfile
        final_dir = Path(final_output_dir)
        
        # Use a secure, non-colliding temporary directory in the same filesystem
        tmp_dir_path = tempfile.mkdtemp(prefix="aidar_pkg_", dir=final_dir.parent if final_dir.parent.exists() else None)
        tmp_dir = Path(tmp_dir_path)
            
        try:
            self.build_package(
                plan=plan,
                output_dir=tmp_dir,
                scene_source_path=scene_source_path,
                blender_executable=blender_executable,
                dry_run=False,
            )
            
            report = validator.validate(
                plan=plan,
                package_dir=tmp_dir,
                blender_executable=blender_executable,
            )
            
            if report.verified:
                if final_dir.exists():
                    backup_dir = final_dir.with_name(f"{final_dir.name}.bak_{os.getpid()}")
                    if backup_dir.exists():
                        shutil.rmtree(backup_dir)
                    # Rename existing to backup (fast)
                    os.rename(final_dir, backup_dir)
                    try:
                        # Rename new to final (fast)
                        os.rename(tmp_dir, final_dir)
                    except Exception:
                        # Rollback on failure
                        os.rename(backup_dir, final_dir)
                        raise
                    # Cleanup backup
                    shutil.rmtree(backup_dir)
                else:
                    os.rename(tmp_dir, final_dir)
            else:
                raise RuntimeError(f"Package validation failed. Missing/failed assets: {report.failed_assets + report.missing_assets}")
                
            return report
            
        finally:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)

    def build_package(
        self,
        plan: PackagePlan,
        output_dir: Union[str, Path],
        dry_run: bool = False,
        scene_source_path: Optional[Union[str, Path]] = None,
        blender_executable: Optional[str] = None,
    ) -> Path:
        """Construct the package directory and serialize manifest.json.

        Args:
            plan: The PackagePlan containing all asset and metadata specifications.
            output_dir: Directory where package assets and manifest will be written.
            dry_run: If True, simulate operations without copying files or writing manifest.
            scene_source_path: The original scene file path (e.g. .blend file).
            blender_executable: Optional path to blender executable for remapping paths.

        Returns:
            Path to the written manifest.json (or expected path if dry_run).
        """
        dest_dir = Path(output_dir)
        manifest_path = dest_dir / "manifest.json"

        if dry_run:
            return manifest_path

        dest_dir.mkdir(parents=True, exist_ok=True)

        # Build mapping for remapping script
        mapping = {}

        # Copy deduplicated physical files
        for record in plan.deduplicated_assets:
            if (
                record.status == AssetStatus.RESOLVED
                and record.source_path
                and record.package_path
            ):
                src = Path(record.source_path)
                dst = dest_dir / record.package_path
                dst.parent.mkdir(parents=True, exist_ok=True)
                new_path = "//../" + record.package_path
                mapping[str(src.resolve())] = new_path
                
                if record.relative_path is not None:
                    mapping[record.relative_path.replace("\\", "/")] = new_path
                    mapping[record.relative_path] = new_path
                
                # Also map by asset_id (e.g. image:wood.png -> wood.png)
                if ":" in record.asset_id:
                    mapping[record.asset_id.split(":", 1)[1]] = new_path
                else:
                    mapping[record.asset_id] = new_path

                if src.exists() and src.is_file():
                    shutil.copy2(src.resolve(), dst)
                    if dst.is_symlink():
                        raise RuntimeError(f"Symlink copied instead of file content: {dst}")

        if scene_source_path:
            scene_src = Path(scene_source_path)
            if scene_src.exists() and scene_src.suffix.lower() == ".blend":
                scene_dst = dest_dir / "scene" / f"{plan.scene_name}.blend"
                scene_dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(scene_src, scene_dst)
                
                if blender_executable:
                    remap_script = Path(__file__).parent / "blender_scripts" / "remap_paths.py"
                    mapping_file = dest_dir / "path_mapping.json"
                    mapping_file.write_text(json.dumps(mapping, indent=2))
                    
                    try:
                        subprocess.run(
                            [
                                blender_executable,
                                "-b",
                                str(scene_dst),
                                "-P",
                                str(remap_script),
                                "--",
                                str(mapping_file),
                            ],
                            check=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                    except subprocess.CalledProcessError as e:
                        raise RuntimeError(
                            f"Blender path remapping failed for {scene_dst.name}: {e.stderr.decode('utf-8', errors='replace')}"
                        ) from e
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to execute blender path remapping: {e}"
                        ) from e

        # Write schema v1.0 manifest JSON with deterministic formatting
        manifest_dict = plan.to_dict()
        manifest_json = json.dumps(manifest_dict, indent=2, sort_keys=True)
        manifest_path.write_text(manifest_json, encoding="utf-8")

        return manifest_path
