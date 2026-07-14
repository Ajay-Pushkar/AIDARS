from __future__ import annotations

from typing import Any, Optional

from .engine import SceneIntelligenceEngine
from .models import SceneSnapshot


class SceneLoader:
    """Load a scene from a Blender-backed source and produce a scene snapshot.

    This module is the new entry point for the Phase 1 pipeline. It is the
    boundary between external inputs (for example a .blend path) and the scene
    scanning engine. In later phases it will call a real Blender adapter.
    """

    def __init__(self, adapter: Optional[Any] = None, engine: Optional[SceneIntelligenceEngine] = None) -> None:
        self.adapter = adapter
        self.engine = engine or SceneIntelligenceEngine()

    def load(self, source: str) -> SceneSnapshot:
        """Load a scene from a supported source and normalize it.

        Args:
            source: A path-like source, currently treated as a Blender-style
                identifier or file path.

        Returns:
            A scene snapshot built from the loaded scene payload.
        """
        if self.adapter is None:
            raise RuntimeError("No adapter configured for SceneLoader")

        payload = self.adapter.load_scene(source)
        return self.engine.analyze_scene_data(payload)
