"""Scene payload assembly and validation for the Blender inspection entry point.

inspect_scene.py should only call into this module and print the result -
it should not assemble, validate, stamp versions, or format errors itself.
Extractors (scene_metadata.py, collection_extractor.py, object_extractor.py)
own *extraction*; this module owns turning their output into the final,
versioned AIDARS payload (or a structured error payload on failure).
"""
from __future__ import annotations

import traceback
from typing import Any

SCHEMA_VERSION = "1.0"
GENERATOR = "AIDARS"
GENERATOR_VERSION = "1.0.0"


def validate_environment(scene: Any) -> None:
    """Raise if the Blender environment isn't in a usable state for inspection.

    Args:
        scene: ``bpy.context.scene``, or None if no scene is active.
    """
    if scene is None:
        raise RuntimeError("No active Blender scene available.")


def build_payload(
    metadata: dict,
    collections: list,
    objects: list,
    *,
    duration_seconds: float,
    blender_version: Any,
    background_mode: bool,
) -> dict:
    """Assemble extractor output into the final versioned AIDARS payload.

    Args:
        metadata: Output of ``scene_metadata.extract_scene_metadata``.
        collections: Output of ``collection_extractor.extract_collections``.
        objects: Output of ``object_extractor.extract_objects``.
        duration_seconds: Wall-clock time the inspection took.
        blender_version: ``bpy.app.version`` (a tuple-like of ints).
        background_mode: ``bpy.app.background``.

    Returns:
        The complete payload, ready to be JSON-serialized and printed.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "generator": GENERATOR,
        "generator_version": GENERATOR_VERSION,
        "metadata": metadata,
        "collections": collections,
        "objects": objects,
        # No extractor exists yet for scene-level (as opposed to per-object)
        # lights/materials/textures/images; per-object materials already
        # flow through `objects`. See docs/scene_engine_architecture.md.
        "lights": [],
        "materials": [],
        "textures": [],
        "images": [],
        "inspection": {
            "duration_seconds": round(duration_seconds, 6),
            "blender_version": list(blender_version),
            "background_mode": background_mode,
        },
    }


def build_error_payload(exc: BaseException) -> dict:
    """Format an exception into a structured error payload for stderr.

    Args:
        exc: The exception that aborted inspection.

    Returns:
        A JSON-serializable dict describing the failure.
    """
    return {
        "generator": GENERATOR,
        "status": "failed",
        "error": {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        },
    }
