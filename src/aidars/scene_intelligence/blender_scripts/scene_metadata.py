"""Scene-level metadata extraction.

Executed inside Blender's embedded Python runtime (see inspect_scene.py).
Produces the ``metadata`` block of the AIDARS scene payload, matching the
schema consumed by ``aidars.scene_intelligence.builders.MetadataBuilder``.
"""
from __future__ import annotations

from typing import Any


def extract_scene_metadata(scene: Any) -> dict:
    """Extract high-level metadata for the given Blender scene.

    Args:
        scene: A ``bpy.types.Scene`` (typically ``bpy.context.scene``).

    Returns:
        A dictionary with ``name``, ``frame_start``, ``frame_end``, ``fps``,
        ``units``, and ``render_engine`` keys.
    """

    render = scene.render
    fps_base = getattr(render, "fps_base", 1.0) or 1.0

    return {
        "name": scene.name,
        "frame_start": int(scene.frame_start),
        "frame_end": int(scene.frame_end),
        # fps_base divides the nominal fps (e.g. 30 / 1.001 for 29.97 NTSC).
        "fps": float(render.fps) / float(fps_base),
        "units": scene.unit_settings.system,
        "render_engine": render.engine,
    }
