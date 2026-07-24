# AIDARS Phase 1: Scene Intelligence Engine

## 1. Objective

The Scene Intelligence Engine is the foundational module of AIDARS. Its responsibility is to ingest Blender scene data, normalize it into a stable internal schema, and emit structured JSON artifacts that downstream modules can consume.

This phase does not implement artificial intelligence, distributed execution, or asset packaging. It focuses exclusively on understanding a scene.

> **Status update:** dependency graph generation (`dependency_graph.py`) and a
> first cut of smart packaging (`smart_package/builder.py`) have since been
> implemented and wired into the CLI ahead of the original phase boundary
> described above. They're intentionally minimal (frame-range asset
> selection; no compression/pruning yet) and don't change this document's
> account of the *engine* layer itself, which remains accurate.
> Orchestration of all of this - including dependency graph construction,
> which used to happen directly in the CLI - has since moved into a
> dedicated `SceneEngine` facade; see `docs/scene_engine_architecture.md`.

## 2. Architecture

The architecture follows a clean separation of concerns:

- Input adapters: future adapters for Blender API, file parsing, or JSON fixtures.
- Core engine: converts raw scene payloads into a normalized scene snapshot.
- Domain models: define stable data structures for objects, collections, metadata, and statistics.
- Exporters: serialize the normalized snapshot into machine-readable JSON.

This design keeps the pipeline modular so it can later support Blender-specific extraction without changing the public contract.

## 3. Folder changes

- src/aidars/scene_intelligence/: core scene intelligence package
- src/aidars/scene_intelligence/models.py: dataclasses for scene concepts
- src/aidars/scene_intelligence/engine.py: orchestration and normalization logic
- src/aidars/scene_intelligence/exporters.py: JSON export layer
- tests/test_scene_engine.py: contract tests for the engine

## 4. Files to create

- src/aidars/scene_intelligence/models.py
- src/aidars/scene_intelligence/engine.py
- src/aidars/scene_intelligence/exporters.py
- tests/test_scene_engine.py
- docs/scene_intelligence_architecture.md

## 5. Detailed explanation

### Module responsibilities

- models.py defines the canonical scene representation.
- engine.py performs validation, normalization, and statistics generation.
- exporters.py converts the snapshot into JSON for persistence and inter-module use.

### Why the module exists

Without a normalized schema, every future module would need to understand Blender-specific quirks and inconsistent data shapes. This layer establishes a durable contract.

### How it fits into AIDARS

The Scene Intelligence Engine is the first layer in the pipeline. Later modules such as dependency graph generation, packaging, and scheduling can consume the snapshot without needing to understand raw Blender internals.

## 6. Expected output

A JSON document containing:

- metadata
- collections
- objects
- lights
- materials
- textures
- images
- statistics

## 7. How to test

Run:

python -m unittest discover -s tests -v

## 8. Possible bugs

- Invalid input shape
- Missing optional keys
- Non-dictionary list elements
- Incompatible Blender-like payloads

## 9. Future improvements

- Add Blender API adapters
- Extract full modifier and constraint details
- Add parent/child graph analysis
- Parse animation curves and keyframe ranges
- Emit richer scene dependency metadata
