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
        curve: Any,
        frame_start: int,
        frame_end: int,
        static_hidden: bool = False,
        interpolation: str = "BEZIER",
    ) -> bool:
        if isinstance(curve, list):
            raw_kfs = curve
            interp = interpolation
        elif hasattr(curve, "keyframes"):
            raw_kfs = [(kf.frame, kf.value) for kf in curve.keyframes]
            interp = getattr(curve, "interpolation", "") or interpolation
        else:
            return not static_hidden

        if not raw_kfs:
            return not static_hidden

        sorted_kfs = sorted(raw_kfs, key=lambda kv: kv[0])

        # 1. Any keyframe inside [frame_start, frame_end] indicating visible -> KEEP
        in_range = [val for frame, val in sorted_kfs if frame_start <= frame <= frame_end]
        if any(val <= _HIDDEN_THRESHOLD for val in in_range):
            return True

        # 2. Check bounding keyframes straddling the interval
        prev_kfs = [val for frame, val in sorted_kfs if frame <= frame_start]
        next_kfs = [val for frame, val in sorted_kfs if frame >= frame_end]

        prev_val = prev_kfs[-1] if prev_kfs else sorted_kfs[0][1]
        next_val = next_kfs[0] if next_kfs else sorted_kfs[-1][1]

        if prev_val <= _HIDDEN_THRESHOLD:
            return True
        if interp.upper() != "CONSTANT" and next_val <= _HIDDEN_THRESHOLD:
            return True

        return not in_range and prev_val <= _HIDDEN_THRESHOLD


def _curve_indicates_visible_in_range(
    keyframes: Any,
    frame_start: int,
    frame_end: int,
    interpolation: str = "BEZIER",
) -> bool:
    """Module-level conservative F-curve visibility check over [frame_start, frame_end]."""
    return EligibilityAnalyzer._curve_indicates_visible_in_range(
        keyframes,
        frame_start,
        frame_end,
        static_hidden=False,
        interpolation=interpolation,
    )
