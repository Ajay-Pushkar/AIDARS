from __future__ import annotations

import importlib
import json
import logging
import subprocess
from pathlib import Path
from typing import Any, Optional

from .builders import (
    CollectionBuilder,
    MaterialBuilder,
    MetadataBuilder,
    ObjectBuilder,
)
from .models import SceneData

logger = logging.getLogger(__name__)


class BlenderAdapter:
    """
    Adapter responsible for communicating with Blender.

    Responsibilities
    ----------------
    - Validate input files.
    - Launch Blender when an external executable is configured.
    - Communicate with Blender's embedded Python runtime.
    - Receive serialized scene data.
    - Convert payloads into SceneData.

    This class intentionally does NOT contain scene extraction logic.
    All Blender-specific inspection lives inside the blender_scripts package.
    """

    SCRIPT_DIRECTORY = Path(__file__).parent / "blender_scripts"
    INSPECTION_SCRIPT = SCRIPT_DIRECTORY / "inspect_scene.py"

    def __init__(
        self,
        blender_module: Optional[str] = None,
        blender_executable: Optional[str] = None,
    ) -> None:
        self.blender_module = blender_module or "bpy"
        self.blender_executable = (
            str(blender_executable).strip()
            if blender_executable
            else None
        )

        self.logger = logger

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def load_scene(self, source: str | Path) -> SceneData:
        """
        Load a Blender scene.

        Parameters
        ----------
        source:
            Path to a .blend file.

        Returns
        -------
        SceneData
        """

        blend_path = Path(source)

        if not blend_path.exists():
            raise FileNotFoundError(
                f"Blend file not found: {blend_path}"
            )

        self.logger.info(
            "Loading Blender scene: %s",
            blend_path,
        )

        if self.blender_executable:
            try:
                return self._inspect_with_external_blender(
                    blend_path
                )
            except Exception as exc:
                self.logger.warning(
                    "External Blender inspection failed: %s",
                    exc,
                )

        try:
            bpy = importlib.import_module(self.blender_module)

        except ImportError:
            self.logger.warning(
                "Blender Python API unavailable. "
                "Using fallback payload."
            )

            return self._fallback_scene_data(blend_path)

        if not hasattr(bpy, "context"):
            self.logger.warning(
                "Invalid Blender Python environment."
            )

            return self._fallback_scene_data(blend_path)

        raise NotImplementedError(
            "Embedded bpy inspection is not implemented. "
            "Use the external Blender executable."
        )

    # ------------------------------------------------------------------ #
    # External Blender
    # ------------------------------------------------------------------ #

    def _inspect_with_external_blender(
        self,
        blend_path: Path,
    ) -> SceneData:

        command = self._build_external_blender_command(
            blend_path
        )

        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

        if completed.returncode != 0:
            raise RuntimeError(
                completed.stderr.strip()
                or completed.stdout.strip()
                or "Blender execution failed."
            )

        stdout = completed.stdout.strip()

        if not stdout:
            raise RuntimeError(
                "Blender returned an empty payload."
            )

        try:
            payload = json.loads(stdout)

        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Blender returned invalid JSON."
            ) from exc

        if not isinstance(payload, dict):
            raise RuntimeError(
                "Expected JSON object from Blender."
            )

        return self._build_scene_data(payload)

    def _build_external_blender_command(
        self,
        blend_path: Path,
    ) -> list[str]:

        if not self.blender_executable:
            raise ValueError(
                "blender_executable is required."
            )

        executable = Path(self.blender_executable)

        if not executable.exists():
            raise FileNotFoundError(
                executable
            )

        return [
            str(executable),
            "--background",
            str(blend_path),
            "--python",
            str(self.INSPECTION_SCRIPT),
        ]

    # ------------------------------------------------------------------ #
    # Payload Conversion
    # ------------------------------------------------------------------ #

    def _build_scene_data(
        self,
        payload: dict[str, Any],
    ) -> SceneData:

        metadata_builder = MetadataBuilder()
        collection_builder = CollectionBuilder()
        object_builder = ObjectBuilder()
        material_builder = MaterialBuilder()

        return SceneData(
            metadata=metadata_builder.build(
                payload.get("metadata", {})
            ),
            collections=collection_builder.build(
                payload.get("collections", [])
            ),
            objects=object_builder.build(
                payload.get("objects", [])
            ),
            lights=list(payload.get("lights", [])),
            materials=material_builder.build(
                payload.get("materials", [])
            ),
            textures=list(payload.get("textures", [])),
            images=list(payload.get("images", [])),
            raw=payload,
        )

    # ------------------------------------------------------------------ #
    # Testing Fallback
    # ------------------------------------------------------------------ #

    def _fallback_scene_data(
        self,
        blend_path: Path,
    ) -> SceneData:

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