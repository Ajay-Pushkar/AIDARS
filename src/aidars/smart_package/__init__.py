"""Smart package builder and M4 smart packaging pipeline."""

from .builder import (
    PackageAsset,
    PackageBuilder,
    PackageManifest,
    PackagePlanner,
    SmartPackageBuilder,
)
from .models import (
    AssetRecord,
    AssetStatus,
    AssetType,
    PackageIntegrityReport,
    PackagePlan,
    PackageStatistics,
    SelectionReason,
)
from .optimizer import AssetOptimizer, OptimizationResult
from .resolver import (
    DependencyClosureResolver,
    PhysicalAssetResolver,
    RequirementResolver,
)
from .validator import PackageValidator

__all__ = [
    "AssetOptimizer",
    "AssetRecord",
    "AssetStatus",
    "AssetType",
    "DependencyClosureResolver",
    "OptimizationResult",
    "PackageAsset",
    "PackageBuilder",
    "PackageIntegrityReport",
    "PackageManifest",
    "PackagePlan",
    "PackagePlanner",
    "PackageStatistics",
    "PackageValidator",
    "PhysicalAssetResolver",
    "RequirementResolver",
    "SelectionReason",
    "SmartPackageBuilder",
]
