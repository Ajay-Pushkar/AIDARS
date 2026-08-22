"""Milestone 4 data models for Smart Packaging.

These models define the contracts between M4 components. They do NOT
replace existing PackageAsset/PackageManifest in builder.py — those
remain for backward compatibility.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class AssetType(str, Enum):
    """Classification of a packaged asset."""

    SCENE = "scene"
    MESH = "mesh"
    MATERIAL = "material"
    TEXTURE = "texture"
    IMAGE = "image"
    HDRI = "hdri"
    LIGHT = "light"
    CAMERA = "camera"
    LIBRARY = "library"
    SIMULATION_CACHE = "simulation_cache"
    GENERATED = "generated"
    ACTION = "action"
    MODIFIER = "modifier"
    COLLECTION = "collection"
    UNKNOWN = "unknown"


class AssetStatus(str, Enum):
    """Resolution status of an asset on disk."""

    RESOLVED = "resolved"       # File found on disk
    EMBEDDED = "embedded"       # Lives inside .blend (no external file)
    MISSING = "missing"         # Expected but not found
    GENERATED = "generated"     # Runtime-generated (particles, geo nodes)


class SelectionReason(str, Enum):
    """Why M4 included this asset in the package."""

    RENDER_REQUIRED = "RENDER_REQUIRED"       # Directly required by M3
    DEPENDENCY = "DEPENDENCY"                 # Transitive dependency closure
    UNKNOWN_DEPENDENCY = "UNKNOWN_DEPENDENCY"  # Conservative keep (uncertain)


@dataclass(slots=True)
class AssetRecord:
    """A single asset tracked by the M4 packaging pipeline.

    This is the M4-level representation of a scene entity. It enriches
    the raw graph node with physical resolution data (paths, hashes,
    sizes) needed for packaging.

    During M4-A (logical packaging), only ``asset_id``, ``asset_type``,
    and ``selection_reason`` are populated.  M4-B fills in the physical
    fields (``source_path``, ``sha256``, ``size_bytes``, etc.).
    """

    asset_id: str
    asset_type: AssetType
    selection_reason: SelectionReason
    source_path: Optional[str] = None
    relative_path: Optional[str] = None
    package_path: Optional[str] = None
    status: AssetStatus = AssetStatus.EMBEDDED
    sha256: Optional[str] = None
    size_bytes: int = 0
    embedded: bool = True
    conservative: bool = False
    dependencies: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-compatible dictionary."""
        return {
            "asset_id": self.asset_id,
            "type": self.asset_type.value,
            "status": self.status.value,
            "selection_reason": self.selection_reason.value,
            "source_path": self.source_path,
            "relative_path": self.relative_path,
            "package_path": self.package_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "embedded": self.embedded,
            "conservative": self.conservative,
            "dependencies": self.dependencies,
        }


@dataclass(slots=True)
class PackageStatistics:
    """Summary statistics for a package plan."""

    total_assets: int = 0
    resolved_assets: int = 0
    embedded_assets: int = 0
    missing_assets: int = 0
    duplicate_assets: int = 0
    original_size_bytes: int = 0
    package_size_bytes: int = 0
    reduction_percent: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_assets": self.total_assets,
            "resolved_assets": self.resolved_assets,
            "embedded_assets": self.embedded_assets,
            "missing_assets": self.missing_assets,
            "duplicate_assets": self.duplicate_assets,
            "original_size_bytes": self.original_size_bytes,
            "package_size_bytes": self.package_size_bytes,
            "reduction_percent": round(self.reduction_percent, 2),
        }


@dataclass(slots=True)
class PackagePlan:
    """The complete plan for a render package — produced before any I/O.

    Contains the full asset roster, deduplication results, missing-asset
    flags, and statistics.  Physical file operations (copy, archive)
    happen only after the plan is validated.
    """

    package_id: str
    scene_name: str
    camera: str
    frame_start: int
    frame_end: int
    all_assets: List[AssetRecord] = field(default_factory=list)
    deduplicated_assets: List[AssetRecord] = field(default_factory=list)
    missing_assets: List[AssetRecord] = field(default_factory=list)
    statistics: PackageStatistics = field(default_factory=PackageStatistics)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": "1.0",
            "package_id": self.package_id,
            "scene": {
                "name": self.scene_name,
                "camera": self.camera,
                "frame_start": self.frame_start,
                "frame_end": self.frame_end,
            },
            "assets": [a.to_dict() for a in self.all_assets],
            "missing": [a.to_dict() for a in self.missing_assets],
            "statistics": self.statistics.to_dict(),
        }


@dataclass(slots=True)
class PackageIntegrityReport:
    """Post-copy verification report."""

    verified: bool = False
    asset_count: int = 0
    verified_count: int = 0
    failed_assets: List[str] = field(default_factory=list)
    missing_assets: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "verified": self.verified,
            "asset_count": self.asset_count,
            "verified_count": self.verified_count,
            "failed_assets": self.failed_assets,
            "missing_assets": self.missing_assets,
        }
