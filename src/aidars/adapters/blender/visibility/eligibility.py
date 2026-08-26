from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, Set

from .models import VisibilityReport, VisibilityState

if TYPE_CHECKING:
    from aidars.adapters.blender.intelligence.models import AnimationCurveInfo, SceneObject, SceneSnapshot

_HIDE_RENDER_DATA_PATH = "hide_render"
_HIDDEN_THRESHOLD = 0.5


class EligibilityAnalyzer:
    """Evaluates whether scene objects are eligible to be rendered across a frame range.
    
    Checks static hide_render flags as well as animated hide_render F-curves.
    """

    def evaluate_static(self, object_data: dict[str, Any]) -> VisibilityState:
        visibility = object_data.get("visibility", {})
        hidden = bool(visibility.get("hide_render", False))
        return VisibilityState(
            hidden=hidden,
            render_disabled=hidden,
            viewport_disabled=bool(visibility.get("hide_viewport", False)),
            selectable=True,
        )

    def analyze(self, snapshot: SceneSnapshot, frame_start: int, frame_end: int) -> VisibilityReport:
        """Compute which objects are eligible (not hidden) anywhere in [frame_start, frame_end]."""
        report = VisibilityReport(frame_start=frame_start, frame_end=frame_end)
        for obj in snapshot.objects:
            if self._is_eligible_in_range(obj, frame_start, frame_end):
                report.visible_object_ids.add(obj.id)
            else:
                report.hidden_object_ids.add(obj.id)
        return report

    def _is_eligible_in_range(self, obj: SceneObject, frame_start: int, frame_end: int) -> bool:
        static_hidden = obj.visibility.hide_render

        curve = self._find_hide_render_curve(obj)
        if curve is None:
            return not static_hidden

        return self._curve_indicates_visible_in_range(curve, frame_start, frame_end, static_hidden)

    @staticmethod
    def _find_hide_render_curve(obj: SceneObject) -> Optional[AnimationCurveInfo]:
        if obj.animation is None:
            return None
        return next((curve for curve in obj.animation.curves if curve.data_path == _HIDE_RENDER_DATA_PATH), None)

    @staticmethod
    def _curve_indicates_visible_in_range(
        curve: AnimationCurveInfo,
        frame_start: int,
        frame_end: int,
        static_hidden: bool,
    ) -> bool:
        keyframes = sorted(curve.keyframes, key=lambda kf: kf.frame)
        if not keyframes:
            return not static_hidden

        in_range_values: List[float] = [kf.value for kf in keyframes if frame_start <= kf.frame <= frame_end]
        if in_range_values:
            # If ever below threshold in range, object was visible at least partially
            return any(value < _HIDDEN_THRESHOLD for value in in_range_values)

        # No keyframe in range: held value from previous keyframe
        held_value = keyframes[0].value
        for kf in keyframes:
            if kf.frame <= frame_start:
                held_value = kf.value
            else:
                break
        return held_value < _HIDDEN_THRESHOLD
