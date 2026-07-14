from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


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
        path.write_text(__import__("json").dumps(payload, indent=2), encoding="utf-8")
        return path
