from __future__ import annotations

import importlib
import json
import logging
import subprocess
from pathlib import Path
from typing import Any, Optional

from .builders import CollectionBuilder, MaterialBuilder, MetadataBuilder, ObjectBuilder
from .models import SceneData

logger = logging.getLogger(__name__)


class BlenderAdapter:
    """Real Blender adapter interface for Phase 1.

    This adapter is designed to work with Blender's Python API when Blender is
    available and to fall back to a structured payload when a real integration is
    not present. The goal is to keep the engine interface stable while allowing
    future real Blender inspection.
    """

    def __init__(self, blender_module: Optional[str] = None, blender_executable: Optional[str] = None) -> None:
        self.blender_module = blender_module or "bpy"
        self.blender_executable = str(blender_executable).strip() if blender_executable else None
        self.logger = logger

    def load_scene(self, source: str | Path) -> SceneData:
        """Load a Blender scene from a .blend file path.

        When Blender's Python API is available in the runtime, this method will
        inspect the scene directly. In the current environment, it falls back to a
        structured placeholder payload so the repository remains testable.
        """
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Blend file not found: {path}")

        self.logger.info("Attempting to load Blender scene from %s", path)

        if self.blender_executable:
            try:
                return self._inspect_with_external_blender(path)
            except (FileNotFoundError, RuntimeError, ValueError) as exc:  # pragma: no cover - environment dependent
                self.logger.warning("External Blender invocation failed: %s", exc)

        try:
            module = importlib.import_module(self.blender_module)
        except ImportError:
            self.logger.warning("Blender API module is unavailable; using fallback payload")
            return self._fallback_scene_data(path)

        if not hasattr(module, "context"):
            self.logger.warning("Blender API module is present but not usable; using fallback payload")
            return self._fallback_scene_data(path)

        return self._inspect_with_blender(module, path)

    def _inspect_with_external_blender(self, blend_path: Path) -> SceneData:
        """Use an external Blender executable to inspect a .blend file.

        The adapter passes a small inspection script to Blender's Python runtime
        and expects a JSON payload on stdout. This supports both real Blender
        binaries and lightweight test wrappers.
        """
        inspection_script = """
import json
import bpy

scene = bpy.context.scene
objects = []
for obj in scene.objects:
    objects.append({
        'name': obj.name,
        'id': obj.name,
        'type': obj.type,
        'collection': obj.users_collection[0].name if obj.users_collection else None,
        'parent': obj.parent.name if obj.parent else None,
        'children': [child.name for child in obj.children],
        'visibility': {
            'hide_render': obj.hide_render,
            'hide_viewport': obj.hide_viewport,
        },
        'transform': {
            'location': list(obj.location),
            'rotation_euler': list(obj.rotation_euler),
            'rotation_quaternion': list(obj.rotation_quaternion),
            'scale': list(obj.scale),
        },
        'mesh': {
            'name': obj.data.name if getattr(obj, 'data', None) else '',
            'vertex_count': len(obj.data.vertices) if getattr(obj, 'data', None) and hasattr(obj.data, 'vertices') else 0,
            'face_count': len(obj.data.polygons) if getattr(obj, 'data', None) and hasattr(obj.data, 'polygons') else 0,
            'edge_count': len(obj.data.edges) if getattr(obj, 'data', None) and hasattr(obj.data, 'edges') else 0,
        } if getattr(obj, 'type', '') == 'MESH' else None,
        'materials': [],
        'modifiers': [],
        'constraints': [],
        'animation': {
            'fcurves': 0,
            'is_animated': bool(obj.animation_data),
            'curves': [],
        },
    })

payload = {
    'metadata': {
        'name': scene.name,
        'frame_start': scene.frame_start,
        'frame_end': scene.frame_end,
        'fps': scene.render.fps,
        'units': scene.unit_settings.system,
        'render_engine': scene.render.engine,
    },
    'collections': [
        {
            'name': collection.name,
            'id': collection.name,
            'parent': collection.parent.name if collection.parent else None,
        }
        for collection in bpy.data.collections
    ],
    'objects': objects,
    'lights': [],
    'materials': [],
    'textures': [],
    'images': [],
}
print(json.dumps(payload))
"""
        command = self._build_external_blender_command(blend_path, inspection_script)
        completed = subprocess.run(command, capture_output=True, text=True, check=False, timeout=60)
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr or completed.stdout or "external Blender invocation failed")

        output = completed.stdout.strip()
        if not output:
            raise RuntimeError("external Blender invocation returned no output")

        try:
            payload = json.loads(output)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"external Blender returned invalid JSON: {exc}") from exc

        if not isinstance(payload, dict):
            raise RuntimeError("external Blender returned a non-object payload")

        return self._build_scene_data(payload)

    def _build_external_blender_command(self, blend_path: Path, inspection_script: str) -> list[str]:
        """Build a subprocess command for an external Blender executable."""
        if not self.blender_executable:
            raise ValueError("blender_executable is required")

        executable = Path(self.blender_executable)
        if not executable.exists():
            raise FileNotFoundError(f"Blender executable not found: {executable}")

        if executable.suffix.lower() in {".cmd", ".bat"}:
            return ["cmd.exe", "/c", str(executable), "--background", str(blend_path), "--python-expr", inspection_script]

        return [str(executable), "--background", str(blend_path), "--python-expr", inspection_script]

    def _inspect_with_blender(self, bpy_module: Any, blend_path: Path) -> SceneData:
        """Inspect a Blender scene using Blender's Python API.

        This method is intentionally conservative. It will attempt to open the
        blend file, inspect the scene's objects, collections, and basic metadata,
        and return a normalized dict structure.
        """
        try:
            bpy_module.ops.wm.open_mainfile(filepath=str(blend_path), load_ui=False)
        except Exception as exc:  # pragma: no cover - environment dependent
            self.logger.warning("Could not open blend file through Blender: %s", exc)
            return self._fallback_payload(blend_path)

        scene = bpy_module.context.scene
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
                    "transform": {
                        "location": list(obj.location),
                        "rotation_euler": list(obj.rotation_euler),
                        "rotation_quaternion": list(obj.rotation_quaternion),
                        "scale": list(obj.scale),
                    },
                    "mesh": {
                        "name": obj.data.name if getattr(obj, "data", None) else "",
                        "vertex_count": len(obj.data.vertices) if getattr(obj, "data", None) and hasattr(obj.data, "vertices") else 0,
                        "face_count": len(obj.data.polygons) if getattr(obj, "data", None) and hasattr(obj.data, "polygons") else 0,
                        "edge_count": len(obj.data.edges) if getattr(obj, "data", None) and hasattr(obj.data, "edges") else 0,
                    } if getattr(obj, "type", "") == "MESH" else None,
                    "materials": [],
                    "modifiers": [],
                    "constraints": [],
                    "animation": {
                        "fcurves": 0,
                        "is_animated": bool(obj.animation_data),
                        "curves": [],
                    },
                }
            )

        payload = {
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
                    "name": collection.name,
                    "id": collection.name,
                    "parent": collection.parent.name if collection.parent else None,
                }
                for collection in bpy_module.data.collections
            ],
            "objects": objects,
            "lights": [],
            "materials": [],
            "textures": [],
            "images": [],
        }
        return self._build_scene_data(payload)

    def _fallback_scene_data(self, blend_path: Path) -> SceneData:
        """Provide a deterministic placeholder payload for non-Blender runs."""

        payload = {
            "metadata": {
                "name": blend_path.stem,
                "frame_start": 1,
                "frame_end": 250,
                "fps": 24,
                "units": "",
                "render_engine": "",
            },
            "collections": [],
            "objects": [],
            "lights": [],
            "materials": [],
            "textures": [],
            "images": [],
        }
        return self._build_scene_data(payload)

    def _build_scene_data(self, payload: dict[str, Any]) -> SceneData:
        metadata_builder = MetadataBuilder()
        collection_builder = CollectionBuilder()
        object_builder = ObjectBuilder()
        material_builder = MaterialBuilder()
        return SceneData(
            metadata=metadata_builder.build(payload.get("metadata", {})),
            collections=collection_builder.build(payload.get("collections", [])),
            objects=object_builder.build(payload.get("objects", [])),
            lights=list(payload.get("lights", [])),
            materials=material_builder.build(payload.get("materials", [])),
            textures=list(payload.get("textures", [])),
            images=list(payload.get("images", [])),
            raw=payload,
        )
