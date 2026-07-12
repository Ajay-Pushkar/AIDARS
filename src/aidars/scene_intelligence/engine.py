from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .models import (
    AnimationCurveInfo,
    AnimationInfo,
    CollectionInfo,
    ConstraintInfo,
    KeyframeInfo,
    MaterialInfo,
    MeshInfo,
    ModifierInfo,
    RelationshipInfo,
    SceneMetadata,
    SceneObject,
    SceneSnapshot,
    SceneStatistics,
    Visibility,
)

logger = logging.getLogger(__name__)


class SceneIntelligenceEngine:
    """Builds a normalized scene intelligence snapshot from Blender-like data.

    The engine is intentionally data-source agnostic. It accepts a dictionary
    structure produced by a Blender adapter or a synthetic fixture and converts
    it into a stable schema that later components can consume.
    """

    def __init__(self, logger_instance: Optional[logging.Logger] = None) -> None:
        self.logger = logger_instance or logger

    def analyze_scene_data(self, scene_data: Dict[str, Any]) -> SceneSnapshot:
        """Create a normalized snapshot from a scene payload.

        Args:
            scene_data: A dictionary containing Blender-like scene data.

        Returns:
            A fully populated SceneSnapshot.
        """
        if not isinstance(scene_data, dict):
            raise TypeError("scene_data must be a dictionary")

        self.logger.info("Analyzing scene payload with %d top-level keys", len(scene_data))

        metadata = self._build_metadata(scene_data.get("metadata", {}))
        collections = self._build_collections(scene_data.get("collections", []))
        objects = self._build_objects(scene_data.get("objects", []))
        lights = self._build_list(scene_data.get("lights", []))
        materials = self._build_materials(scene_data.get("materials", []))
        textures = self._build_list(scene_data.get("textures", []))
        images = self._build_list(scene_data.get("images", []))
        statistics = self._build_statistics(objects, collections, lights, materials, textures, images)
        relationships = self._build_relationships(objects)

        snapshot = SceneSnapshot(
            metadata=metadata,
            collections=collections,
            objects=objects,
            lights=lights,
            materials=materials,
            textures=textures,
            images=images,
            statistics=statistics,
            relationships=relationships,
            raw=scene_data,
        )

        self.logger.info("Scene analysis completed for %s", metadata.name or "unnamed")
        return snapshot

    def _build_metadata(self, metadata_data: Dict[str, Any]) -> SceneMetadata:
        if not isinstance(metadata_data, dict):
            raise TypeError("metadata must be a dictionary")

        return SceneMetadata(
            name=str(metadata_data.get("name", "")),
            frame_start=int(metadata_data.get("frame_start", 1)),
            frame_end=int(metadata_data.get("frame_end", 250)),
            fps=float(metadata_data.get("fps", 24.0)),
            units=str(metadata_data.get("units", "")),
            render_engine=str(metadata_data.get("render_engine", "")),
        )

    def _build_collections(self, collection_data: List[Dict[str, Any]]) -> List[CollectionInfo]:
        if not isinstance(collection_data, list):
            raise TypeError("collections must be a list")

        collections: List[CollectionInfo] = []
        for item in collection_data:
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

    def _build_objects(self, object_data: List[Dict[str, Any]]) -> List[SceneObject]:
        if not isinstance(object_data, list):
            raise TypeError("objects must be a list")

        objects: List[SceneObject] = []
        for item in object_data:
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
                )

            materials = [
                MaterialInfo(name=str(material.get("name", "")), shader=str(material.get("shader", "")))
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
                        )
                    )
                animation = AnimationInfo(
                    fcurves=int(animation_payload.get("fcurves", 0)),
                    is_animated=bool(animation_payload.get("is_animated", False)),
                    curves=curves,
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
                    camera=item.get("camera") if isinstance(item.get("camera"), dict) else None,
                    raw=item,
                )
            )
        return objects

    def _build_materials(self, materials_data: List[Dict[str, Any]]) -> List[MaterialInfo]:
        if not isinstance(materials_data, list):
            raise TypeError("materials must be a list")

        materials: List[MaterialInfo] = []
        for item in materials_data:
            if not isinstance(item, dict):
                raise TypeError("each material entry must be a dictionary")
            materials.append(
                MaterialInfo(name=str(item.get("name", "")), shader=str(item.get("shader", "")))
            )
        return materials

    def _build_list(self, payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, list):
            raise TypeError("expected a list")
        return [item for item in payload if isinstance(item, dict)]

    def _build_relationships(self, objects: List[SceneObject]) -> List[RelationshipInfo]:
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

    def _build_statistics(
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
