from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, List, Set

if TYPE_CHECKING:
    from aidars.scene_intelligence.models import AnimationCurveInfo, SceneObject, SceneSnapshot


@dataclass(slots=True)
class VisibilityState:
    """A placeholder visibility state for an object."""

    hidden: bool = False
    render_disabled: bool = False
    viewport_disabled: bool = False
    selectable: bool = True


class VisibilityEngine:
    """A placeholder engine for visibility-related analysis.

    Operates on raw payload dicts and reports a snapshot-in-time visibility
    state. For frame-range-aware visibility (does an object become visible
    at any point during a range of frames, accounting for animated
    hide_render/hide_viewport keyframes), use ``VisibilityAnalyzer`` below,
    which operates on the typed scene model instead.
    """

    def evaluate(self, object_data: dict[str, Any]) -> VisibilityState:
        visibility = object_data.get("visibility", {})
        return VisibilityState(
            hidden=bool(visibility.get("hide_render", False)),
            render_disabled=bool(visibility.get("hide_render", False)),
            viewport_disabled=bool(visibility.get("hide_viewport", False)),
            selectable=True,
        )


# Blender treats hide_render/hide_viewport as ordinary animatable float
# properties (0.0 = visible, 1.0 = hidden) when keyframed, which is exactly
# what object_extractor.py already captures as a normal animation curve -
# no new extraction work was needed to make this possible.
_HIDE_RENDER_DATA_PATH = "hide_render"
_HIDDEN_THRESHOLD = 0.5


@dataclass(slots=True)
class VisibilityReport:
    """Which objects are visible somewhere within a frame range, and why."""

    frame_start: int
    frame_end: int
    visible_object_ids: Set[str] = field(default_factory=set)
    hidden_object_ids: Set[str] = field(default_factory=set)

    def is_visible(self, object_id: str) -> bool:
        return object_id in self.visible_object_ids


class VisibilityAnalyzer:
    """Determines which objects are visible within an assigned frame range.

    This is a static-scene-graph analysis, not a render/occlusion analysis:
    it answers "is this object ever eligible to appear in a render of these
    frames" based on hide_render (and any keyframed animation of
    hide_render), not "is it actually in the camera's view" (that needs
    real camera frustum/occlusion analysis, which is out of scope until
    there's a concrete camera-frustum model to analyze against).

    hide_viewport is intentionally ignored here: it only affects the 3D
    viewport, not what gets rendered, so it isn't relevant to deciding what
    a render worker needs.
    """

    def analyze(self, snapshot: "SceneSnapshot", frame_start: int, frame_end: int) -> VisibilityReport:
        """Compute which objects are visible somewhere in [frame_start, frame_end].

        Args:
            snapshot: An already-analyzed scene snapshot.
            frame_start: First frame of the range (inclusive).
            frame_end: Last frame of the range (inclusive).

        Returns:
            A VisibilityReport partitioning every object into visible/hidden.
        """
        report = VisibilityReport(frame_start=frame_start, frame_end=frame_end)
        for obj in snapshot.objects:
            if self._is_visible_in_range(obj, frame_start, frame_end):
                report.visible_object_ids.add(obj.id)
            else:
                report.hidden_object_ids.add(obj.id)
        return report

    def _is_visible_in_range(self, obj: "SceneObject", frame_start: int, frame_end: int) -> bool:
        static_hidden = obj.visibility.hide_render

        curve = self._find_hide_render_curve(obj)
        if curve is None:
            return not static_hidden

        return self._curve_indicates_visible_in_range(curve, frame_start, frame_end, static_hidden)

    @staticmethod
    def _find_hide_render_curve(obj: "SceneObject") -> "AnimationCurveInfo | None":
        if obj.animation is None:
            return None
        return next((curve for curve in obj.animation.curves if curve.data_path == _HIDE_RENDER_DATA_PATH), None)

    @staticmethod
    def _curve_indicates_visible_in_range(
        curve: "AnimationCurveInfo",
        frame_start: int,
        frame_end: int,
        static_hidden: bool,
    ) -> bool:
        keyframes = sorted(curve.keyframes, key=lambda kf: kf.frame)
        if not keyframes:
            return not static_hidden

        in_range_values: List[float] = [kf.value for kf in keyframes if frame_start <= kf.frame <= frame_end]
        if in_range_values:
            # Step interpolation, matching how a boolean-ish property like
            # hide_render actually behaves in Blender: if it's ever below
            # the hidden threshold anywhere in the range, the object was
            # visible for at least part of it.
            return any(value < _HIDDEN_THRESHOLD for value in in_range_values)

        # No keyframe falls inside the range - the value is whatever the
        # nearest earlier keyframe held (or the first keyframe's value, if
        # the whole range is before the first keyframe).
        held_value = keyframes[0].value
        for kf in keyframes:
            if kf.frame <= frame_start:
                held_value = kf.value
            else:
                break
        return held_value < _HIDDEN_THRESHOLD
