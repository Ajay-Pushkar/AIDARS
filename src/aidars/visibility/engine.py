from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from .analyzer import RenderRequirementAnalyzer
from .eligibility import EligibilityAnalyzer
from .models import (
    RenderRequest,
    RenderRequirementReport,
    RequirementReason,
    VisibilityReport,
    VisibilityState,
)

if TYPE_CHECKING:
    from aidars.scene_intelligence.models import SceneObject, SceneSnapshot


class VisibilityEngine:
    """Backward-compatible facade for visibility analysis and render requirement evaluation."""

    def __init__(self) -> None:
        self._eligibility_analyzer = EligibilityAnalyzer()
        self._analyzer = RenderRequirementAnalyzer()

    def evaluate(self, object_data: dict[str, Any]) -> VisibilityState:
        """Evaluate static visibility flags for an object dictionary."""
        return self._eligibility_analyzer.evaluate_static(object_data)

    def analyze(
        self,
        active_camera: Any = None,
        dependency_graph: Any = None,
        render_settings: Any = None,
        snapshot: Any = None,
        request: Any = None,
    ) -> Dict[str, List[str]]:
        """Run visibility and render requirement analysis, returning the Milestone 3 R4 schema.
        
        Returns:
            Dict[str, List[str]]:
            {
                "visible_objects": List[str],
                "unused_objects": List[str],
                "required_materials": List[str],
                "required_textures": List[str]
            }
        """
        target_scene = snapshot if snapshot is not None else dependency_graph
        report = self._analyzer.analyze(
            snapshot=target_scene,
            dependency_graph=dependency_graph,
            request=request,
            active_camera=active_camera,
            render_settings=render_settings,
        )
        return report.to_r4_dict()

    def analyze_requirements(
        self,
        snapshot: Any,
        dependency_graph: Any = None,
        request: Optional[RenderRequest | Dict[str, Any]] = None,
        active_camera: Any = None,
        render_settings: Any = None,
    ) -> RenderRequirementReport:
        """Run full Render Requirement Analysis and return the typed RenderRequirementReport."""
        return self._analyzer.analyze(
            snapshot=snapshot,
            dependency_graph=dependency_graph,
            request=request,
            active_camera=active_camera,
            render_settings=render_settings,
        )


class VisibilityAnalyzer:
    """Determines which objects are eligible to appear within an assigned frame range.
    
    Checks static hide_render flags as well as animated hide_render F-curves across [frame_start, frame_end].
    """

    def __init__(self) -> None:
        self._eligibility = EligibilityAnalyzer()

    def analyze(self, snapshot: SceneSnapshot, frame_start: int, frame_end: int) -> VisibilityReport:
        """Compute which objects are visible somewhere in [frame_start, frame_end]."""
        return self._eligibility.analyze(snapshot, frame_start, frame_end)
