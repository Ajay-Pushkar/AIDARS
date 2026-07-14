"""Export a Blender scene to a JSON payload consumable by AIDARS.

Usage in Blender:
    1. Open the .blend file in Blender.
    2. Open the Scripting workspace.
    3. Run this script.
    4. Choose an output path when prompted.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import bpy


def collect_scene_payload() -> dict[str, Any]:
    """Collect a normalized scene payload from Blender's current scene."""

    scene = bpy.context.scene
    objects: list[dict[str, Any]] = []

    for obj in scene.objects:
        payload: dict[str, Any] = {
            "name": obj.name,
            "id": obj.name,
            "type": obj.type,
            "collection": obj.users_collection[0].name if obj.users_collection else None,
            "parent": obj.parent.name if obj.parent else None,
            "children": [child.name for child in obj.children],
            "visibility": {
                "hide_render": obj.hide_render,
                "hide_viewport": obj.hide_viewport,
            },
            "mesh": None,
            "materials": [],
            "modifiers": [],
            "constraints": [],
            "animation": {
                "fcurves": 0,
                "is_animated": False,
                "curves": [],
            },
        }

        if obj.type == "MESH" and obj.data:
            payload["mesh"] = {
                "name": obj.data.name,
                "vertex_count": len(obj.data.vertices),
                "face_count": len(obj.data.polygons),
                "edge_count": len(obj.data.edges),
            }

        for mat_slot in obj.material_slots:
            if mat_slot.material:
                payload["materials"].append(
                    {
                        "name": mat_slot.material.name,
                        "shader": mat_slot.material.bl_rna.name,
                    }
                )

        for modifier in obj.modifiers:
            payload["modifiers"].append(
                {
                    "name": modifier.name,
                    "type": modifier.type,
                    "subtype": getattr(modifier, "subtype", ""),
                    "show_viewport": modifier.show_viewport,
                    "show_render": modifier.show_render,
                    "settings": {},
                }
            )

        for constraint in obj.constraints:
            payload["constraints"].append(
                {
                    "name": constraint.name,
                    "type": constraint.type,
                    "target": getattr(constraint.target, "name", None),
                    "influence": getattr(constraint, "influence", 1.0),
                    "settings": {},
                }
            )

        if obj.animation_data:
            payload["animation"]["is_animated"] = True
            action = obj.animation_data.action
            payload["animation"]["fcurves"] = len(action.fcurves) if action else 0

        objects.append(payload)

    return {
        "metadata": {
            "name": scene.name,
            "frame_start": scene.frame_start,
            "frame_end": scene.frame_end,
            "fps": scene.render.fps,
            "units": scene.unit_settings.system,
            "render_engine": scene.render.engine,
        },
        "collections": [
            {
                "name": coll.name,
                "id": coll.name,
                "parent": coll.parent.name if coll.parent else None,
            }
            for coll in bpy.data.collections
        ],
        "objects": objects,
        "lights": [],
        "materials": [],
        "textures": [],
        "images": [],
    }


def export_scene(output_path: str | None = None) -> Path:
    """Export the current Blender scene to a JSON file."""

    payload = collect_scene_payload()
    target_path = Path(output_path or "aidars_scene_payload.json")
    target_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target_path


if __name__ == "__main__":
    output_path = bpy.path.abspath("//aidars_scene_payload.json")
    result = export_scene(output_path)
    print(f"Exported scene payload to {result}")
