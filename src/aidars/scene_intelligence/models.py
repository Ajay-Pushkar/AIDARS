from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class SceneMetadata:
    """Represents high-level Blender scene metadata."""

    name: str = ""
    frame_start: int = 1
    frame_end: int = 250
    fps: float = 24.0
    units: str = ""
    render_engine: str = ""


@dataclass(slots=True)
class Visibility:
    """Visibility flags for an object."""

    hide_render: bool = False
    hide_viewport: bool = False


@dataclass(slots=True)
class MeshInfo:
    """Core mesh topology information."""

    name: str = ""
    vertex_count: int = 0
    face_count: int = 0
    edge_count: int = 0


@dataclass(slots=True)
class MaterialInfo:
    """Material data extracted from a Blender object."""

    name: str = ""
    shader: str = ""


@dataclass(slots=True)
class ModifierInfo:
    """Modifier data attached to an object."""

    name: str = ""
    type: str = ""
    subtype: str = ""
    show_viewport: bool = True
    show_render: bool = True
    settings: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ConstraintInfo:
    """Constraint data attached to an object."""

    name: str = ""
    type: str = ""
    target: Optional[str] = None
    influence: float = 1.0
    settings: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class KeyframeInfo:
    """A single keyframe entry for an animation curve."""

    frame: int = 0
    value: float = 0.0
    interpolation: str = ""


@dataclass(slots=True)
class AnimationCurveInfo:
    """A single F-Curve representation."""

    name: str = ""
    data_path: str = ""
    array_index: int = -1
    keyframes: List[KeyframeInfo] = field(default_factory=list)


@dataclass(slots=True)
class AnimationInfo:
    """Animation-related information."""

    fcurves: int = 0
    is_animated: bool = False
    curves: List[AnimationCurveInfo] = field(default_factory=list)


@dataclass(slots=True)
class SceneObject:
    """A Blender object extracted into a uniform model."""

    name: str
    id: str
    type: str
    collection: Optional[str] = None
    parent: Optional[str] = None
    children: List[str] = field(default_factory=list)
    visibility: Visibility = field(default_factory=Visibility)
    mesh: Optional[MeshInfo] = None
    materials: List[MaterialInfo] = field(default_factory=list)
    modifiers: List[ModifierInfo] = field(default_factory=list)
    constraints: List[ConstraintInfo] = field(default_factory=list)
    animation: Optional[AnimationInfo] = None
    camera: Optional[Dict[str, Any]] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CollectionInfo:
    """A Blender collection with parent relationships."""

    name: str
    id: str
    parent: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SceneStatistics:
    """Aggregate scene statistics for downstream consumers."""

    object_count: int = 0
    collection_count: int = 0
    camera_count: int = 0
    light_count: int = 0
    material_count: int = 0
    texture_count: int = 0
    image_count: int = 0
    animated_object_count: int = 0
    visible_object_count: int = 0


@dataclass(slots=True)
class RelationshipInfo:
    """Represents an extracted relationship between scene entities."""

    source: str
    target: str
    relationship: str


@dataclass(slots=True)
class SceneSnapshot:
    """The complete scene intelligence payload emitted by the engine."""

    metadata: SceneMetadata
    collections: List[CollectionInfo] = field(default_factory=list)
    objects: List[SceneObject] = field(default_factory=list)
    lights: List[Dict[str, Any]] = field(default_factory=list)
    materials: List[MaterialInfo] = field(default_factory=list)
    textures: List[Dict[str, Any]] = field(default_factory=list)
    images: List[Dict[str, Any]] = field(default_factory=list)
    statistics: SceneStatistics = field(default_factory=SceneStatistics)
    relationships: List[RelationshipInfo] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)
