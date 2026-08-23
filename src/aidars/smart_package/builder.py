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

@dataclass(slots=True)
class PackageAsset:
    """Represents an asset that should be included in a package."""

    path: str
    kind: str
    size_bytes: int = 0
    frame_start: Optional[int] = None
    frame_end: Optional[int] = None


@dataclass(slots=True)
class PackageManifest:
    """The manifest describing a smart package payload."""

    package_id: str
    frame_start: int
    frame_end: int
    assets: List[PackageAsset] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class SmartPackageBuilder:
    """Create a minimal package manifest for a scene frame range.

    This is intentionally phase-appropriate. It does not implement real asset
    packing or distributed worker logic yet. It creates a structured manifest
    that later phases can expand into real package creation and transfer.
    """

    def build_package(self, frame_start: int, frame_end: int, assets: Optional[List[PackageAsset]] = None) -> PackageManifest:
        """Build a package manifest for a set of required assets.

        Args:
            frame_start: First frame in the requested range.
            frame_end: Last frame in the requested range.
            assets: Assets that should be grouped into the package.

        Returns:
            A manifest describing the package contents.
        """
        selected_assets = [
            asset
            for asset in (assets or [])
            if self._asset_is_required_for_frame_range(asset, frame_start, frame_end)
        ]
        total_size = sum(asset.size_bytes for asset in selected_assets)
        return PackageManifest(
            package_id=self._build_package_id(frame_start, frame_end),
            frame_start=frame_start,
            frame_end=frame_end,
            assets=selected_assets,
            metadata={
                "asset_count": len(selected_assets),
                "required_file_count": len(selected_assets),
                "estimated_total_size_bytes": total_size,
                "output_format": "manifest",
            },
        )

    def _asset_is_required_for_frame_range(self, asset: PackageAsset, frame_start: int, frame_end: int) -> bool:
        if asset.frame_start is None or asset.frame_end is None:
            return True
        return not (asset.frame_end < frame_start or asset.frame_start > frame_end)

    def build_optimized_package(
        self,
        frame_start: int,
        frame_end: int,
        assets: List[PackageAsset],
        graph: "DependencyGraph",
        visible_object_ids: Set[str],
    ) -> PackageManifest:
        """Build a package manifest pruned to only visibility-reachable assets.

            Dependency Graph -> Visibility Analysis -> Asset Optimizer -> Package Manifest

        This is ``build_package`` plus one extra step: before frame-range
        selection, assets that no visible object's dependency chain reaches
        are dropped. See ``aidars.smart_package.optimizer`` for exactly
        what "reachable" covers today (externally-referenced assets only;
        materials/textures aren't individually prunable yet since
        PackageAsset has no per-material granularity).

        Args:
            frame_start: First frame in the requested range.
            frame_end: Last frame in the requested range.
            assets: Candidate assets for the package.
            graph: A built dependency graph for the scene.
            visible_object_ids: Object ids visible in [frame_start, frame_end]
                (see ``aidars.visibility.engine.VisibilityAnalyzer``).

        Returns:
            A manifest describing the (pruned) package contents, with
            optimization stats recorded in ``manifest.metadata``.
        """
        optimization = AssetOptimizer().optimize(graph, visible_object_ids, assets)
        manifest = self.build_package(frame_start, frame_end, optimization.kept_assets)
        manifest.metadata["visibility_pruned_asset_count"] = len(optimization.pruned_assets)
        manifest.metadata["visibility_pruned_size_bytes"] = optimization.pruned_size_bytes
        manifest.metadata["visible_object_count"] = optimization.visible_object_count
        return manifest

    def _build_package_id(self, frame_start: int, frame_end: int) -> str:
        return f"pkg-{frame_start}-{frame_end}"

    def write_manifest(self, manifest: PackageManifest, output_path: str | Path) -> Path:
        """Serialize a package manifest to JSON for downstream use."""

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "package_id": manifest.package_id,
            "frame_start": manifest.frame_start,
            "frame_end": manifest.frame_end,
            "assets": [
                {"path": asset.path, "kind": asset.kind, "size_bytes": asset.size_bytes}
                for asset in manifest.assets
            ],
            "metadata": manifest.metadata,
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path


# ──────────────────────────────────────────────────────────────────
# Milestone 4 Package Construction: PackagePlanner & PackageBuilder
# ──────────────────────────────────────────────────────────────────


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
                scene_dst = dest_dir / "scene" / scene_src.name
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
                        logger.warning(
                            f"Blender path remapping failed for {scene_dst.name}: {e.stderr.decode()}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to execute blender path remapping: {e}"
                        )

        # Write schema v1.0 manifest JSON with deterministic formatting
        manifest_dict = plan.to_dict()
        manifest_json = json.dumps(manifest_dict, indent=2, sort_keys=True)
        manifest_path.write_text(manifest_json, encoding="utf-8")

        return manifest_path
