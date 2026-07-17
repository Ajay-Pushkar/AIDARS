"""
AIDARS Scene Inspection Entry Point

Executed inside Blender's embedded Python runtime.

Responsibilities
----------------
- Validate Blender runtime.
- Collect scene metadata.
- Collect scene assets through specialized extractors.
- Produce a versioned JSON payload.
- Emit structured errors.
- Record inspection timing.

This file intentionally contains no extraction logic.
"""
from __future__ import annotations
from scene_analysis.render_settings import extract_render_settings

import json
import logging
import sys
import time
import traceback

import bpy

from collection_extractor import extract_collections
from object_extractor import extract_objects
from scene_metadata import extract_scene_metadata

LOGGER = logging.getLogger("aidars.scene_inspector")

PAYLOAD_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s | %(message)s",
    )


def validate_environment() -> None:
    if bpy.context.scene is None:
        raise RuntimeError("No active Blender scene available.")


def build_payload() -> dict:
    scene = bpy.context.scene
    render_settings = extract_render_settings(scene)
    metadata = extract_scene_metadata(scene)
    collections = extract_collections()
    objects = extract_objects(scene)

    return {
        "schema_version": SCHEMA_VERSION,
        "generator": "AIDARS",
        "generator_version": PAYLOAD_VERSION,
        "metadata": metadata,
        "collections": collections,
        "render_settings": render_settings,
        "objects": objects,
        "lights": [],
        "materials": [],
        "textures": [],
        "images": [],
    }


def main() -> int:
    configure_logging()

    start = time.perf_counter()

    try:
        LOGGER.info("Starting scene inspection.")

        validate_environment()

        payload = build_payload()

        payload["inspection"] = {
            "duration_seconds": round(
                time.perf_counter() - start,
                6,
            ),
            "blender_version": list(bpy.app.version),
            "background_mode": bpy.app.background,
        }

        print(json.dumps(payload))

        LOGGER.info("Inspection completed.")

        return 0

    except Exception as exc:
        error_payload = {
            "generator": "AIDARS",
            "status": "failed",
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            },
        }

        print(
            json.dumps(error_payload),
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())