"""AIDARS Scene Inspection Entry Point.

Executed inside Blender's embedded Python runtime (see
aidars.scene_intelligence.blender_adapter.BlenderAdapter). This file is
orchestration only: call extractors, assemble the payload, print it.

No business logic, transformation, or validation lives here - that all
lives in the extractor modules (scene_metadata.py, collection_extractor.py,
object_extractor.py) and in serializers/payload_builder.py.
"""
from __future__ import annotations

import json
import sys
import time

import bpy

from collection_extractor import extract_collections
from object_extractor import extract_objects
from scene_metadata import extract_scene_metadata
from serializers.payload_builder import build_error_payload, build_payload, validate_environment


def main() -> int:
    start = time.perf_counter()

    try:
        scene = bpy.context.scene
        validate_environment(scene)

        metadata = extract_scene_metadata(scene)
        collections = extract_collections()
        objects = extract_objects(scene)

        payload = build_payload(
            metadata,
            collections,
            objects,
            duration_seconds=time.perf_counter() - start,
            blender_version=bpy.app.version,
            background_mode=bpy.app.background,
        )

        print(json.dumps(payload))
        return 0

    except Exception as exc:
        print(json.dumps(build_error_payload(exc)), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
