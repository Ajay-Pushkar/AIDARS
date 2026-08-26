"""Visibility and Render Requirement Analysis for AIDAR (Milestone 3).

Determines the minimum set of scene entities (objects, meshes, materials,
textures, images, lights, cameras, libraries, simulation caches) required
to produce a correct render for a given RenderRequest.
"""

from .analyzer import RenderRequirementAnalyzer
from .camera import CameraAnalyzer, CameraModel
from .eligibility import EligibilityAnalyzer
from .engine import VisibilityAnalyzer, VisibilityEngine
from .geometry import BoundingBox, CullingResult, FrustumCuller
from .influence import InfluenceAnalyzer
from .models import (
    RenderRequest,
    RenderRequirementReport,
    RequirementReason,
    VisibilityReport,
    VisibilityState,
)
from .resolver import DependencyResolver

__all__ = [
    "RenderRequest",
    "RenderRequirementReport",
    "RequirementReason",
    "VisibilityReport",
    "VisibilityState",
    "EligibilityAnalyzer",
    "CameraAnalyzer",
    "CameraModel",
    "BoundingBox",
    "CullingResult",
    "FrustumCuller",
    "InfluenceAnalyzer",
    "DependencyResolver",
    "RenderRequirementAnalyzer",
    "VisibilityAnalyzer",
    "VisibilityEngine",
]
