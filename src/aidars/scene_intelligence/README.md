# Scene Intelligence Engine

## Purpose

This package provides the first Phase 1 capability for AIDARS: converting Blender-like scene data into a normalized, structured representation for downstream modules.

## Main components

- models.py: canonical domain objects
- engine.py: normalization and analysis pipeline
- exporters.py: JSON export support

## Usage

```python
from aidars.scene_intelligence.engine import SceneIntelligenceEngine
from aidars.scene_intelligence.exporters import JsonSceneExporter

engine = SceneIntelligenceEngine()
snapshot = engine.analyze_scene_data(scene_data)
JsonSceneExporter.write_json(snapshot, "output/scene.json")
```
