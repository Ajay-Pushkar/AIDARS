from __future__ import annotations

from typing import Any, Optional

from .engine import SceneIntelligenceEngine
from .models import SceneSnapshot


class SceneScanner:
    """High-level scanner wrapper for scene payloads.

    This layer makes the architecture explicit: a scanner receives scene data and
    hands it to the engine for normalization. It is intentionally thin in Phase 1,
    but it creates the right abstraction for future specialized scanners.
    """

    def __init__(self, engine: Optional[SceneIntelligenceEngine] = None) -> None:
        self.engine = engine or SceneIntelligenceEngine()

    def scan(self, scene_data: dict[str, Any]) -> SceneSnapshot:
        """Scan a scene payload and return a scene snapshot."""

        return self.engine.analyze_scene_data(scene_data)
