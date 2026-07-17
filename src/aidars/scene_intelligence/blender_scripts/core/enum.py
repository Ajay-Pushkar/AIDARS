from __future__ import annotations

from enum import StrEnum


class ObjectType(StrEnum):
    """Supported Blender object types."""

    MESH = "MESH"
    CAMERA = "CAMERA"
    LIGHT = "LIGHT"
    CURVE = "CURVE"
    SURFACE = "SURFACE"
    FONT = "FONT"
    ARMATURE = "ARMATURE"
    EMPTY = "EMPTY"
    GREASE_PENCIL = "GREASEPENCIL"
    VOLUME = "VOLUME"
    POINT_CLOUD = "POINTCLOUD"
    CURVES = "CURVES"
    LATTICE = "LATTICE"
    META = "META"
    SPEAKER = "SPEAKER"
    LIGHT_PROBE = "LIGHT_PROBE"
class ExtractionStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
class RenderEngine(StrEnum):
    CYCLES = "CYCLES"
    EEVEE = "BLENDER_EEVEE"
    WORKBENCH = "BLENDER_WORKBENCH"