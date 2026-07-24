"""Per-object extraction.

Executed inside Blender's embedded Python runtime (see inspect_scene.py).
Produces the ``objects`` block of the AIDARS scene payload, matching the
schema consumed by ``aidars.scene_intelligence.builders.ObjectBuilder``.

Every extractor function here is defensive: Blender's Python API differs
slightly across object/modifier/constraint types and versions, so missing
attributes are treated as "nothing to report" (empty list/dict/None) rather
than raising and aborting the whole scan.

Division of responsibility with the builders (``aidars.scene_intelligence.
builders``): this module only extracts and does the type coercion required
to make bpy-native values (Vector, Euler, Quaternion, etc.) JSON-serializable.
It does not decide schema defaults or reject malformed data - that's the
builders' job, and it's why (for example) this module reports "no animation"
as ``None`` rather than inventing a zero-value animation dict: ObjectBuilder
already owns that default, and a value should only be defaulted in one
place.
"""
from __future__ import annotations

from typing import Any


def _extract_mesh(obj: Any) -> dict | None:
    if obj.type != "MESH" or obj.data is None:
        return None

    mesh = obj.data
    try:
        triangle_count = sum(max(len(polygon.vertices) - 2, 0) for polygon in mesh.polygons)
    except Exception:
        triangle_count = 0

    return {
        "name": mesh.name,
        "vertex_count": len(mesh.vertices),
        "face_count": len(mesh.polygons),
        "edge_count": len(mesh.edges),
        "triangle_count": triangle_count,
        "uv_map_count": len(mesh.uv_layers) if hasattr(mesh, "uv_layers") else 0,
        "vertex_group_count": len(obj.vertex_groups) if hasattr(obj, "vertex_groups") else 0,
        "has_normals": True,
    }


def _extract_materials(obj: Any) -> list:
    materials = []
    for slot in getattr(obj, "material_slots", []):
        material = slot.material
        if material is None:
            continue

        node_tree_name = ""
        image_textures: list = []
        if getattr(material, "use_nodes", False) and material.node_tree is not None:
            node_tree_name = material.node_tree.name
            for node in material.node_tree.nodes:
                if node.type == "TEX_IMAGE" and getattr(node, "image", None) is not None:
                    image_textures.append(node.image.name)

        materials.append(
            {
                "name": material.name,
                # bl_rna.name (e.g. "Material") is not a shader name; the
                # active Principled BSDF / shader node type is a much more
                # useful signal for downstream dependency-graph reasoning.
                "shader": _shader_name(material),
                "node_tree": node_tree_name,
                "image_textures": image_textures,
                "settings": {},
            }
        )
    return materials


def _shader_name(material: Any) -> str:
    if not getattr(material, "use_nodes", False) or material.node_tree is None:
        return "SHADERLESS"
    output_node = next(
        (node for node in material.node_tree.nodes if node.type == "OUTPUT_MATERIAL"),
        None,
    )
    if output_node is not None:
        surface_input = output_node.inputs.get("Surface")
        if surface_input is not None and surface_input.links:
            return surface_input.links[0].from_node.type
    return "UNKNOWN"


def _extract_modifiers(obj: Any) -> list:
    modifiers = []
    for modifier in getattr(obj, "modifiers", []):
        modifiers.append(
            {
                "name": modifier.name,
                "type": modifier.type,
                "subtype": getattr(modifier, "subtype", ""),
                "show_viewport": modifier.show_viewport,
                "show_render": modifier.show_render,
                "settings": {},
            }
        )
    return modifiers


def _extract_constraints(obj: Any) -> list:
    constraints = []
    for constraint in getattr(obj, "constraints", []):
        target = getattr(constraint, "target", None)
        constraints.append(
            {
                "name": constraint.name,
                "type": constraint.type,
                "target": getattr(target, "name", None),
                "influence": float(getattr(constraint, "influence", 1.0)),
                "settings": {},
            }
        )
    return constraints


def _extract_animation(obj: Any) -> dict | None:
    animation_data = getattr(obj, "animation_data", None)
    if animation_data is None or animation_data.action is None:
        # No animation to report. Deliberately return None rather than a
        # zero-value dict: ObjectBuilder already defaults a missing/None
        # "animation" key to animation=None, so there's no need for this
        # extractor to also decide what "not animated" looks like - that's
        # schema-default policy, and it belongs in exactly one place.
        return None

    action = animation_data.action
    curves = []
    for fcurve in action.fcurves:
        keyframes = [
            {
                "frame": int(keyframe.co[0]),
                "value": float(keyframe.co[1]),
                "interpolation": keyframe.interpolation,
            }
            for keyframe in fcurve.keyframe_points
        ]
        curves.append(
            {
                "name": fcurve.data_path,
                "data_path": fcurve.data_path,
                "array_index": fcurve.array_index,
                "keyframes": keyframes,
                "interpolation": keyframes[0]["interpolation"] if keyframes else "",
            }
        )

    return {
        "fcurves": len(action.fcurves),
        "is_animated": len(action.fcurves) > 0,
        "curves": curves,
    }


def _extract_bones(obj: Any) -> list:
    if obj.type != "ARMATURE" or obj.data is None:
        return []
    return [
        {"name": bone.name, "parent": bone.parent.name if bone.parent else None}
        for bone in obj.data.bones
    ]


def _extract_particle_systems(obj: Any) -> list:
    systems = []
    for system in getattr(obj, "particle_systems", []):
        # len(system.particles) is only meaningful after the depsgraph has
        # been evaluated (e.g. at a specific frame); settings.count is the
        # authored emission count and is always available in the raw scene.
        count = len(system.particles) if system.particles else getattr(system.settings, "count", 0)
        systems.append({"name": system.name, "count": int(count)})
    return systems


def _extract_transform(obj: Any) -> dict:
    return {
        "location": list(obj.location),
        "rotation_euler": list(obj.rotation_euler),
        "rotation_quaternion": list(obj.rotation_quaternion),
        "scale": list(obj.scale),
    }


def _extract_camera(obj: Any) -> dict | None:
    if obj.type != "CAMERA" or obj.data is None:
        return None
    camera = obj.data
    return {
        "type": camera.type,
        "lens": float(getattr(camera, "lens", 0.0)),
        "sensor_width": float(getattr(camera, "sensor_width", 0.0)),
        "clip_start": float(getattr(camera, "clip_start", 0.0)),
        "clip_end": float(getattr(camera, "clip_end", 0.0)),
    }


def _extract_referenced_assets(obj: Any) -> list:
    """Collect external .blend library paths this object (or its data) is linked from."""

    paths: list = []
    obj_library = getattr(obj, "library", None)
    if obj_library is not None and obj_library.filepath:
        paths.append(obj_library.filepath)

    data_library = getattr(getattr(obj, "data", None), "library", None)
    if data_library is not None and data_library.filepath and data_library.filepath not in paths:
        paths.append(data_library.filepath)

    return paths


def extract_objects(scene: Any) -> list:
    """Extract every object in the given scene into the AIDARS object schema.

    Args:
        scene: A ``bpy.types.Scene`` (typically ``bpy.context.scene``).

    Returns:
        A list of dictionaries matching
        ``aidars.scene_intelligence.builders.ObjectBuilder``'s expected shape.
    """

    objects = []
    for obj in scene.objects:
        objects.append(
            {
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
                "mesh": _extract_mesh(obj),
                "materials": _extract_materials(obj),
                "modifiers": _extract_modifiers(obj),
                "constraints": _extract_constraints(obj),
                "animation": _extract_animation(obj),
                "bones": _extract_bones(obj),
                "particle_systems": _extract_particle_systems(obj),
                "transform": _extract_transform(obj),
                "camera": _extract_camera(obj),
                "referenced_assets": _extract_referenced_assets(obj),
            }
        )
    return objects
