from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .builders import (
    CollectionBuilder,
    MaterialBuilder,
    MetadataBuilder,
    ObjectBuilder,
    RelationshipBuilder,
    StatisticsBuilder,
)
from .models import (
    SceneData,
    SceneSnapshot,
)

logger = logging.getLogger(__name__)


class SceneIntelligenceEngine:
    """Builds a normalized scene intelligence snapshot from Blender-like data.

    The engine is intentionally data-source agnostic. It accepts a dictionary
    structure produced by a Blender adapter or a synthetic fixture and converts
    it into a stable schema that later components can consume.

    This class does normalization only (SceneData/dict -> SceneSnapshot). It
    does not build the dependency graph, run integrity checks, or handle
    packaging - for the full pipeline, use
    ``aidars.scene_intelligence.scene_engine.SceneEngine``, which composes
    this class with the rest of the pipeline.
    """

    def __init__(self, logger_instance: Optional[logging.Logger] = None) -> None:
        self.logger = logger_instance or logger
        self.metadata_builder = MetadataBuilder()
        self.collection_builder = CollectionBuilder()
        self.object_builder = ObjectBuilder()
        self.material_builder = MaterialBuilder()
        self.statistics_builder = StatisticsBuilder()
        self.relationship_builder = RelationshipBuilder()

    def analyze_scene_data(self, scene_data: Dict[str, Any] | SceneData) -> SceneSnapshot:
        """Create a normalized snapshot from a scene payload.

        Args:
            scene_data: A dictionary containing Blender-like scene data or a typed SceneData object.

        Returns:
            A fully populated SceneSnapshot.
        """
        if isinstance(scene_data, SceneData):
            # scene_data already went through the builders once (typically via
            # BlenderAdapter). Reuse those typed fields directly instead of
            # re-deriving everything from .raw, which may be empty even when
            # the typed fields are populated (e.g. hand-constructed SceneData).
            metadata = self.metadata_builder.build(scene_data.metadata)
            collections = self.collection_builder.build(scene_data.collections)
            objects = self.object_builder.build(scene_data.objects)
            lights = self._build_list(scene_data.lights)
            materials = self.material_builder.build(scene_data.materials)
            textures = self._build_list(scene_data.textures)
            images = self._build_list(scene_data.images)
            raw = scene_data.raw
            self.logger.info("Analyzing typed SceneData with %d top-level raw keys", len(raw))
        elif isinstance(scene_data, dict):
            payload = scene_data
            self.logger.info("Analyzing scene payload with %d top-level keys", len(payload))

            metadata = self.metadata_builder.build(payload.get("metadata", {}))
            collections = self.collection_builder.build(payload.get("collections", []))
            objects = self.object_builder.build(payload.get("objects", []))
            lights = self._build_list(payload.get("lights", []))
            materials = self.material_builder.build(payload.get("materials", []))
            textures = self._build_list(payload.get("textures", []))
            images = self._build_list(payload.get("images", []))
            raw = payload
        else:
            raise TypeError("scene_data must be a dictionary or SceneData")

        statistics = self.statistics_builder.build(objects, collections, lights, materials, textures, images)
        relationships = self.relationship_builder.build(objects)

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
            raw=raw,
        )

        self.logger.info("Scene analysis completed for %s", metadata.name or "unnamed")
        return snapshot

    def _build_list(self, payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, list):
            raise TypeError("expected a list")
        return [item for item in payload if isinstance(item, dict)]
