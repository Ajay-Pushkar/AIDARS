from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class RequirementReason(str, Enum):
    """Reasons why an entity is required for a render."""

    CAMERA_VISIBLE = "CAMERA_VISIBLE"
    SHADOW_CASTER = "SHADOW_CASTER"
    LIGHT_SOURCE = "LIGHT_SOURCE"
    REFLECTION_SOURCE = "REFLECTION_SOURCE"
    REFRACTION_SOURCE = "REFRACTION_SOURCE"
    ANIMATION_DRIVER = "ANIMATION_DRIVER"
    DEPENDENCY = "DEPENDENCY"
    WORLD_ENVIRONMENT = "WORLD_ENVIRONMENT"
    SIMULATION = "SIMULATION"
    PARENT_HIERARCHY = "PARENT_HIERARCHY"
    UNSPECIFIED_SAFETY = "UNSPECIFIED_SAFETY"


@dataclass(slots=True)
class RenderRequest:
    """Formal description of a render job to analyze."""

    camera_id: str = ""
    frame_start: int = 1
    frame_end: int = 1
    resolution: Tuple[int, int] = (1920, 1080)
    view_layer: str = "ViewLayer"
    scene_name: str = ""
    conservative: bool = True
    custom_settings: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class VisibilityState:
    """Snapshot visibility state for an object."""

    hidden: bool = False
    render_disabled: bool = False
    viewport_disabled: bool = False
    selectable: bool = True


@dataclass(slots=True)
class VisibilityReport:
    """Report of objects eligible for rendering in a frame range."""

    frame_start: int
    frame_end: int
    visible_object_ids: Set[str] = field(default_factory=set)
    hidden_object_ids: Set[str] = field(default_factory=set)

    def is_visible(self, object_id: str) -> bool:
        return object_id in self.visible_object_ids


@dataclass(slots=True)
class RenderRequirementReport:
    """The canonical Milestone 3 output: exact minimum requirements for rendering."""

    request: RenderRequest
    required_objects: List[str] = field(default_factory=list)
    required_meshes: List[str] = field(default_factory=list)
    required_materials: List[str] = field(default_factory=list)
    required_textures: List[str] = field(default_factory=list)
    required_images: List[str] = field(default_factory=list)
    required_lights: List[str] = field(default_factory=list)
    required_cameras: List[str] = field(default_factory=list)
    required_libraries: List[str] = field(default_factory=list)
    required_simulation_caches: List[str] = field(default_factory=list)
    unused_objects: List[str] = field(default_factory=list)
    reasons: Dict[str, List[str]] = field(default_factory=dict)
    conservative: bool = True
    statistics: Dict[str, Any] = field(default_factory=dict)

    def add_reason(self, entity_id: str, reason: str | RequirementReason) -> None:
        val = reason.value if isinstance(reason, RequirementReason) else str(reason)
        if entity_id not in self.reasons:
            self.reasons[entity_id] = []
        if val not in self.reasons[entity_id]:
            self.reasons[entity_id].append(val)

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to the canonical detailed requirement JSON format."""
        return {
            "request": {
                "camera": self.request.camera_id,
                "frame_start": self.request.frame_start,
                "frame_end": self.request.frame_end,
                "resolution": list(self.request.resolution),
                "view_layer": self.request.view_layer,
                "scene_name": self.request.scene_name,
            },
            "required": {
                "objects": self.required_objects,
                "meshes": self.required_meshes,
                "materials": self.required_materials,
                "textures": self.required_textures,
                "images": self.required_images,
                "lights": self.required_lights,
                "cameras": self.required_cameras,
                "libraries": self.required_libraries,
                "simulation_caches": self.required_simulation_caches,
            },
            "unused_objects": self.unused_objects,
            "reasons": self.reasons,
            "analysis": {
                "conservative": self.conservative,
                "statistics": self.statistics,
            },
        }

    def to_r4_dict(self) -> Dict[str, List[str]]:
        """Convert report to the Milestone 3 R4 schema dictionary."""
        return {
            "visible_objects": self.required_objects,
            "unused_objects": self.unused_objects,
            "required_materials": self.required_materials,
            "required_textures": self.required_textures,
        }
