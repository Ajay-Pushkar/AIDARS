from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from .optimizer import AssetOptimizer

if TYPE_CHECKING:
    from aidars.scene_intelligence.dependency_graph import DependencyGraph


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
