from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from .models import (
    AnimationCurveInfo,
    AnimationInfo,
    BoneInfo,
    CollectionInfo,
    ConstraintInfo,
    KeyframeInfo,
    MaterialInfo,
    MeshInfo,
    ModifierInfo,
    ParticleSystemInfo,
    RelationshipInfo,
    SceneMetadata,
    SceneObject,
    SceneStatistics,
    TransformInfo,
    Visibility,
)

logger = logging.getLogger(__name__)


class MetadataBuilder:
    """Builds canonical scene metadata from dictionaries or typed models."""

    def build(self, metadata: SceneMetadata | Dict[str, Any] | None) -> SceneMetadata:
        if isinstance(metadata, SceneMetadata):
            return metadata
        if isinstance(metadata, dict):
            return SceneMetadata(
                name=str(metadata.get("name", "")),
                frame_start=int(metadata.get("frame_start", 1)),
                frame_end=int(metadata.get("frame_end", 250)),
                fps=float(metadata.get("fps", 24.0)),
                units=str(metadata.get("units", "")),
                render_engine=str(metadata.get("render_engine", "")),
            )
        return SceneMetadata()


class CollectionBuilder:
    """Builds collection metadata from dictionaries or typed models."""

    def build(self, collection_data: Sequence[CollectionInfo | Dict[str, Any]]) -> List[CollectionInfo]:
        collections: List[CollectionInfo] = []
        for item in collection_data:
            if isinstance(item, CollectionInfo):
                collections.append(item)
                continue
            if not isinstance(item, dict):
                raise TypeError("each collection entry must be a dictionary")
            collections.append(
                CollectionInfo(
                    name=str(item.get("name", "")),
                    id=str(item.get("id", "")),
                    parent=str(item.get("parent")) if item.get("parent") is not None else None,
                    raw=item,
                )
            )
        return collections


class ObjectBuilder:
    """Builds scene objects from dictionaries or typed models."""

    def build(self, object_data: Sequence[SceneObject | Dict[str, Any]]) -> List[SceneObject]:
        objects: List[SceneObject] = []
        for item in object_data:
            if isinstance(item, SceneObject):
                objects.append(item)
                continue
            if not isinstance(item, dict):
                raise TypeError("each object entry must be a dictionary")

            visibility_payload = item.get("visibility", {})
            if not isinstance(visibility_payload, dict):
                raise TypeError("object visibility must be a dictionary")

            mesh_payload = item.get("mesh")
            mesh = None
            if isinstance(mesh_payload, dict):
                mesh = MeshInfo(
                    name=str(mesh_payload.get("name", "")),
                    vertex_count=int(mesh_payload.get("vertex_count", 0)),
                    face_count=int(mesh_payload.get("face_count", 0)),
                    edge_count=int(mesh_payload.get("edge_count", 0)),
                    triangle_count=int(mesh_payload.get("triangle_count", 0)),
                    uv_map_count=int(mesh_payload.get("uv_map_count", 0)),
                    vertex_group_count=int(mesh_payload.get("vertex_group_count", 0)),
                    has_normals=bool(mesh_payload.get("has_normals", True)),
                )

            materials = [
                MaterialInfo(
                    name=str(material.get("name", "")),
                    shader=str(material.get("shader", "")),
                    node_tree=str(material.get("node_tree", "")),
                    image_textures=[str(texture) for texture in material.get("image_textures", []) if texture is not None],
                    settings=dict(material.get("settings", {})) if isinstance(material.get("settings"), dict) else {},
                )
                for material in item.get("materials", [])
                if isinstance(material, dict)
            ]
            modifiers = [
                ModifierInfo(
                    name=str(modifier.get("name", "")),
                    type=str(modifier.get("type", "")),
                    subtype=str(modifier.get("subtype", "")),
                    show_viewport=bool(modifier.get("show_viewport", True)),
                    show_render=bool(modifier.get("show_render", True)),
                    settings=dict(modifier.get("settings", {})) if isinstance(modifier.get("settings"), dict) else {},
                )
                for modifier in item.get("modifiers", [])
                if isinstance(modifier, dict)
            ]
            constraints = [
                ConstraintInfo(
                    name=str(constraint.get("name", "")),
                    type=str(constraint.get("type", "")),
                    target=str(constraint.get("target")) if constraint.get("target") is not None else None,
                    influence=float(constraint.get("influence", 1.0)),
                    settings=dict(constraint.get("settings", {})) if isinstance(constraint.get("settings"), dict) else {},
                )
                for constraint in item.get("constraints", [])
                if isinstance(constraint, dict)
            ]

            animation_payload = item.get("animation")
            animation = None
            if isinstance(animation_payload, dict):
                curves = []
                for curve_payload in animation_payload.get("curves", []):
                    if not isinstance(curve_payload, dict):
                        continue
                    keyframes = [
                        KeyframeInfo(
                            frame=int(keyframe.get("frame", 0)),
                            value=float(keyframe.get("value", 0.0)),
                            interpolation=str(keyframe.get("interpolation", "")),
                        )
                        for keyframe in curve_payload.get("keyframes", [])
                        if isinstance(keyframe, dict)
                    ]
                    curves.append(
                        AnimationCurveInfo(
                            name=str(curve_payload.get("name", "")),
                            data_path=str(curve_payload.get("data_path", "")),
                            array_index=int(curve_payload.get("array_index", -1)),
                            keyframes=keyframes,
                            interpolation=str(curve_payload.get("interpolation", "")),
                        )
                    )
                animation = AnimationInfo(
                    fcurves=int(animation_payload.get("fcurves", 0)),
                    is_animated=bool(animation_payload.get("is_animated", False)),
                    curves=curves,
                )

            bones = [
                BoneInfo(name=str(bone.get("name", "")), parent=str(bone.get("parent")) if bone.get("parent") is not None else None)
                for bone in item.get("bones", [])
                if isinstance(bone, dict)
            ]
            particle_systems = [
                ParticleSystemInfo(name=str(system.get("name", "")), count=int(system.get("count", 0)))
                for system in item.get("particle_systems", [])
                if isinstance(system, dict)
            ]
            transform_payload = item.get("transform")
            transform = None
            if isinstance(transform_payload, dict):
                transform = TransformInfo(
                    location=self._coerce_float_list(transform_payload.get("location")),
                    rotation_euler=self._coerce_float_list(transform_payload.get("rotation_euler")),
                    rotation_quaternion=self._coerce_float_list(transform_payload.get("rotation_quaternion")),
                    scale=self._coerce_float_list(transform_payload.get("scale")),
                )
            child_ids = [str(child_id) for child_id in item.get("children", []) if child_id is not None]
            objects.append(
                SceneObject(
                    name=str(item.get("name", "")),
                    id=str(item.get("id", "")),
                    type=str(item.get("type", "")),
                    collection=str(item.get("collection")) if item.get("collection") is not None else None,
                    parent=str(item.get("parent")) if item.get("parent") is not None else None,
                    children=child_ids,
                    visibility=Visibility(
                        hide_render=bool(visibility_payload.get("hide_render", False)),
                        hide_viewport=bool(visibility_payload.get("hide_viewport", False)),
                    ),
                    mesh=mesh,
                    materials=materials,
                    modifiers=modifiers,
                    constraints=constraints,
                    animation=animation,
                    bones=bones,
                    particle_systems=particle_systems,
                    transform=transform,
                    camera=item.get("camera") if isinstance(item.get("camera"), dict) else None,
                    raw=item,
                )
            )
        return objects

    def _coerce_float_list(self, value: Any) -> List[float]:
        if not isinstance(value, list):
            return []
        return [float(item) for item in value if item is not None]


class MaterialBuilder:
    """Builds material models from dictionaries or typed models."""

    def build(self, materials_data: Sequence[MaterialInfo | Dict[str, Any]]) -> List[MaterialInfo]:
        materials: List[MaterialInfo] = []
        for item in materials_data:
            if isinstance(item, MaterialInfo):
                materials.append(item)
                continue
            if not isinstance(item, dict):
                raise TypeError("each material entry must be a dictionary")
            materials.append(MaterialInfo(name=str(item.get("name", "")), shader=str(item.get("shader", ""))))
        return materials


class StatisticsBuilder:
    """Builds aggregate scene statistics."""

    def build(
        self,
        objects: List[SceneObject],
        collections: List[CollectionInfo],
        lights: List[Dict[str, Any]],
        materials: List[MaterialInfo],
        textures: List[Dict[str, Any]],
        images: List[Dict[str, Any]],
    ) -> SceneStatistics:
        return SceneStatistics(
            object_count=len(objects),
            collection_count=len(collections),
            camera_count=sum(1 for obj in objects if obj.type == "CAMERA"),
            light_count=len(lights),
            material_count=len(materials),
            texture_count=len(textures),
            image_count=len(images),
            animated_object_count=sum(1 for obj in objects if obj.animation and obj.animation.is_animated),
            visible_object_count=sum(
                1 for obj in objects if not obj.visibility.hide_render and not obj.visibility.hide_viewport
            ),
        )


class RelationshipBuilder:
    """Builds scene relationships from objects."""

    def build(self, objects: List[SceneObject]) -> List[RelationshipInfo]:
        relationships: List[RelationshipInfo] = []
        object_index = {obj.id: obj for obj in objects}

        for obj in objects:
            if obj.parent and obj.parent in object_index:
                relationships.append(RelationshipInfo(source=obj.id, target=obj.parent, relationship="parent"))

        for obj in objects:
            for child_id in obj.children:
                if child_id in object_index:
                    relationships.append(RelationshipInfo(source=obj.id, target=child_id, relationship="child"))

        for obj in objects:
            if obj.collection:
                relationships.append(RelationshipInfo(source=obj.id, target=obj.collection, relationship="collection"))

        return relationships
