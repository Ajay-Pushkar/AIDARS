from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import SceneSnapshot


class JsonSceneExporter:
    """Exports a normalized scene snapshot to JSON."""

    @staticmethod
    def write_json(snapshot: SceneSnapshot, output_path: str | Path) -> Path:
        """Write the snapshot to disk as pretty-printed JSON.

        Args:
            snapshot: A scene snapshot to serialize.
            output_path: Destination file path.

        Returns:
            The resolved output path.
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = JsonSceneExporter._to_serializable(snapshot)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    @staticmethod
    def _to_serializable(snapshot: SceneSnapshot) -> dict[str, Any]:
        return {
            "metadata": {
                "name": snapshot.metadata.name,
                "frame_start": snapshot.metadata.frame_start,
                "frame_end": snapshot.metadata.frame_end,
                "fps": snapshot.metadata.fps,
                "units": snapshot.metadata.units,
                "render_engine": snapshot.metadata.render_engine,
            },
            "collections": [
                {
                    "name": collection.name,
                    "id": collection.id,
                    "parent": collection.parent,
                }
                for collection in snapshot.collections
            ],
            "objects": [
                {
                    "name": obj.name,
                    "id": obj.id,
                    "type": obj.type,
                    "collection": obj.collection,
                    "parent": obj.parent,
                    "visibility": {
                        "hide_render": obj.visibility.hide_render,
                        "hide_viewport": obj.visibility.hide_viewport,
                    },
                    "mesh": {
                        "name": obj.mesh.name if obj.mesh else "",
                        "vertex_count": obj.mesh.vertex_count if obj.mesh else 0,
                        "face_count": obj.mesh.face_count if obj.mesh else 0,
                        "edge_count": obj.mesh.edge_count if obj.mesh else 0,
                        "triangle_count": obj.mesh.triangle_count if obj.mesh else 0,
                        "uv_map_count": obj.mesh.uv_map_count if obj.mesh else 0,
                        "vertex_group_count": obj.mesh.vertex_group_count if obj.mesh else 0,
                        "has_normals": obj.mesh.has_normals if obj.mesh else True,
                    },
                    "materials": [
                        {
                            "name": material.name,
                            "shader": material.shader,
                            "node_tree": material.node_tree,
                            "image_textures": material.image_textures,
                            "settings": material.settings,
                        }
                        for material in obj.materials
                    ],
                    "modifiers": [
                        {
                            "name": modifier.name,
                            "type": modifier.type,
                            "subtype": modifier.subtype,
                            "show_viewport": modifier.show_viewport,
                            "show_render": modifier.show_render,
                            "settings": modifier.settings,
                        }
                        for modifier in obj.modifiers
                    ],
                    "constraints": [
                        {
                            "name": constraint.name,
                            "type": constraint.type,
                            "target": constraint.target,
                            "influence": constraint.influence,
                            "settings": constraint.settings,
                        }
                        for constraint in obj.constraints
                    ],
                    "animation": {
                        "fcurves": obj.animation.fcurves if obj.animation else 0,
                        "is_animated": bool(obj.animation and obj.animation.is_animated),
                        "curves": [
                            {
                                "name": curve.name,
                                "data_path": curve.data_path,
                                "array_index": curve.array_index,
                                "keyframes": [
                                    {
                                        "frame": keyframe.frame,
                                        "value": keyframe.value,
                                        "interpolation": keyframe.interpolation,
                                    }
                                    for keyframe in curve.keyframes
                                ],
                                "interpolation": curve.interpolation,
                            }
                            for curve in obj.animation.curves
                        ] if obj.animation else [],
                    },
                    "camera": obj.camera,
                    "children": obj.children,
                    "bones": [{"name": bone.name, "parent": bone.parent} for bone in obj.bones],
                    "particle_systems": [
                        {"name": system.name, "count": system.count}
                        for system in obj.particle_systems
                    ],
                }
                for obj in snapshot.objects
            ],
            "lights": snapshot.lights,
            "materials": [
                {"name": material.name, "shader": material.shader}
                for material in snapshot.materials
            ],
            "textures": snapshot.textures,
            "images": snapshot.images,
            "statistics": {
                "object_count": snapshot.statistics.object_count,
                "collection_count": snapshot.statistics.collection_count,
                "camera_count": snapshot.statistics.camera_count,
                "light_count": snapshot.statistics.light_count,
                "material_count": snapshot.statistics.material_count,
                "texture_count": snapshot.statistics.texture_count,
                "image_count": snapshot.statistics.image_count,
                "animated_object_count": snapshot.statistics.animated_object_count,
                "visible_object_count": snapshot.statistics.visible_object_count,
            },
            "relationships": [
                {
                    "source": relationship.source,
                    "target": relationship.target,
                    "relationship": relationship.relationship,
                }
                for relationship in snapshot.relationships
            ],
        }
