# AIDARS (AI Driven Adaptive Render and Asset Distribution System)

## Goal

Reduce rendering cost by analyzing scene dependencies,
distributing assets intelligently, and optimizing worker usage.

## Current Stage

Stage 1:
Blender Scene Intelligence Engine

## Current Capabilities

- Scene analysis and normalization for Blender-like payloads
- Dependency graph generation for objects, collections, materials, textures, animation actions, and externally referenced/linked assets
- Missing-reference and unused-asset detection surfaced in the dependency graph report and as CLI warnings
- Smart packaging manifest generation for frame-range based asset selection
- A `SceneEngine` orchestration facade that owns all pipeline business logic (analysis, dependency graph, integrity checks, packaging, caching), reusable outside the CLI
- A thin CLI entry point (parse args -> invoke SceneEngine -> exit) that exports a scene snapshot, a dependency graph report, and (optionally) a package manifest in one run
- Manual (in-Blender script) and automatic (external Blender executable) export paths for real .blend files
- Opt-in incremental scanning via `--cache-dir`: unchanged inputs skip re-analysis (and skip launching Blender entirely for unchanged .blend files)

## Planned Modules

- Distributed Workers
- ML Prediction
- RAG Optimization Memory