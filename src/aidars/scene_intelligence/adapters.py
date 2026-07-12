from __future__ import annotations

import importlib
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class BlenderAdapter:
    """Adapter layer for future Blender API integration.

    The adapter is intentionally lightweight in Phase 1. It expects either:
    - a direct Blender-like payload already loaded into memory, or
    - a .blend file path that can be processed later by a real Blender integration.

    This design keeps the engine independent of Blender while making a real
    adapter easy to plug in when the environment supports it.
    """

    def __init__(self, blender_module: Optional[str] = None) -> None:
        self.blender_module = blender_module
        self.logger = logger

    def load_scene(self, source: str | Path | Dict[str, Any]) -> Dict[str, Any]:
        """Load scene data from a Blender payload or file-like source.

        Args:
            source: Either a path to a .blend file, a dict payload, or another
                supported input.

        Returns:
            A normalized dictionary structure ready for the engine.
        """
        if isinstance(source, dict):
            return self._normalize_payload(source)

        path = Path(source)
        if path.exists() and path.suffix.lower() == ".blend":
            return self._load_from_blender(path)

        raise ValueError("Unsupported scene source; expected a dict or .blend file")

    def _normalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dictionary")
        return payload

    def _load_from_blender(self, blend_path: Path) -> Dict[str, Any]:
        """Attempt to use Blender's Python API when available.

        This method is a placeholder for a real integration. If Blender is not
        installed or the module is unavailable, it raises a runtime error that
        clearly states the adapter is not fully wired in the current environment.
        """
        if self.blender_module:
            module = importlib.import_module(self.blender_module)
            if hasattr(module, "load_blend"):
                data = module.load_blend(str(blend_path))
                if isinstance(data, dict):
                    return data

        raise RuntimeError(
            "Blender API integration is not available in this environment; "
            "provide a pre-extracted scene payload instead."
        )
