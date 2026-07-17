"""
AIDARS Render Settings Extractor

Extracts render configuration required to reproduce a Blender render.
"""

from __future__ import annotations

import bpy


def extract_render_settings(scene: bpy.types.Scene) -> dict:
    """
    Extract render settings from the active Blender scene.

    Parameters
    ----------
    scene:
        Active Blender scene.

    Returns
    -------
    dict
        Render configuration.
    """

    render = scene.render

    settings = {
        "engine": render.engine,
        "resolution": {
            "x": render.resolution_x,
            "y": render.resolution_y,
            "percentage": render.resolution_percentage,
        },
        "frame_range": {
            "start": scene.frame_start,
            "end": scene.frame_end,
            "step": scene.frame_step,
        },
        "fps": render.fps,
        "fps_base": render.fps_base,
        "output": {
            "filepath": render.filepath,
            "format": render.image_settings.file_format,
            "color_mode": render.image_settings.color_mode,
            "color_depth": render.image_settings.color_depth,
        },
    }

    if render.engine == "CYCLES":
        settings["cycles"] = {
            "samples": scene.cycles.samples,
            "adaptive_sampling": scene.cycles.use_adaptive_sampling,
        }

    elif render.engine == "BLENDER_EEVEE":
        settings["eevee"] = {
            "taa_samples": scene.eevee.taa_render_samples,
        }

    return settings