# Original User Request

## Initial Request — 2026-08-18T21:29:57+05:30

You are the Project Orchestrator for the AIDAR project.

Your working directory is:
C:\Users\kurap\.gemini\antigravity\scratch\extracted_files\AIDAR_project\AIDAR\.agents\teamwork_preview_orchestrator_1

Project root workspace:
C:\Users\kurap\.gemini\antigravity\scratch\extracted_files\AIDAR_project\AIDAR

Original Request reference:
C:\Users\kurap\.gemini\antigravity\scratch\extracted_files\AIDAR_project\AIDAR\.agents\ORIGINAL_REQUEST.md

Task Summary:
Implement request-aware scene caching and the full Milestone 3 Render Requirement Analysis pipeline for AIDAR without touching M4 packaging, scheduler, workers, or networking.

Integrity mode: development

Requirements:
1. R1. Request-Aware Scene Caching: Ensure `SceneCache` and `SceneEngine.run()` incorporate the full `SceneEngineRequest` configuration (stage flags, frame ranges, output paths, camera IDs) into the cache identity. Ensure that two distinct requests for the same source file never return false cache hits, and that deleted artifacts on disk trigger re-analysis.
2. R2. Request-Aware Cache Verification Tests: Add rigorous tests proving that identical scene sources with differing request parameters (e.g. `build_package=False` vs `build_package=True` with frames `500-600`) produce distinct cache keys and execute the newly requested stages.
3. R3. Milestone 3 Render Requirement Architecture: Maintain the complete, decoupled M3 pipeline in `src/aidars/visibility/`:
- `RenderRequest` (camera ID, frame range, resolution, view layer)
- `EligibilityAnalyzer` (static `hide_render` and animated F-curves across requested frames)
- `CameraAnalyzer` (camera orientation, FOV, clipping planes, view basis vectors)
- `FrustumCuller` (world-space bounding boxes, perspective/orthographic frustum culling, conservative occlusion)
- `InfluenceAnalyzer` (active lights, parent hierarchy, simulation safety, world HDRI)
- `DependencyResolver` (BFS/DFS dependency closure over DependencyGraph for meshes, materials, textures, images)
- `RenderRequirementReport` (canonical report with reason tracking: `CAMERA_VISIBLE`, `LIGHT_SOURCE`, `PARENT_HIERARCHY`, `SIMULATION`, `DEPENDENCY`)

Acceptance Criteria:
- Cache Correctness:
  * Changing request configuration on an unchanged `.blend` or JSON source causes a cache miss and executes requested work.
  * Deleting an output artifact on disk causes a cache miss.
  * Running identical requests sequentially causes a verified cache hit.
- M3 Pipeline & Test Suite:
  * All 82+ unit, facade, cache, and adversarial tests pass cleanly (`python -m unittest discover tests`).
  * Please orchestrate the planning, exploration, implementation, testing, adversarial challenge, and verification according to your role instructions. Maintain your plan.md, progress.md, and context.md in your working directory. Notify me when victory is ready to be audited.

## Follow-up — 2026-08-23T18:19:43+05:30

# Teamwork Project Prompt — Draft

> Requested team: Full multi-agent team

Implement Milestone 5 Core (Local Content-Addressed Asset Cache) for the AIDAR project. The cache must use SHA-256 for identity, feature a SQLite-backed index, implement strict O(1) set-difference for hit/miss resolution, and include an LRU eviction policy. Ensure this subsystem is cleanly isolated from the M4 packaging logic. Use a very large team of agents to research the best approach and implement it thoroughly.

Working directory: `C:\AIDAR`
Integrity mode: development

## Requirements

### R1. Content-Addressed Storage & Hashing
Implement a local filesystem-backed cache store where asset identity is strictly defined by its SHA-256 hash, not its filename or path. Store objects using a split-hash directory structure (e.g., `objects/a9/1f3e...`) to prevent directory bloat.

### R2. Cache Metadata Index
Implement a SQLite-backed index (`cache/metadata/index.db`) to track cache entries. It must store the authoritative hash, size, type, original name, creation time, last accessed time, and verification status.

### R3. Hit/Miss Resolver & Set Difference
Given a requested set of asset hashes (from an M4 PackagePlan), the cache must compute the set difference (`missing = required - cached`) in O(A) average time using standard Sets to definitively identify which assets need to be transferred.

### R4. Integrity, Eviction, and Interfaces
Implement an LRU eviction policy based on `last_accessed` tracking to enforce a maximum cache quota. Define a clean `CacheStore` interface (`contains`, `get`, `put`, `verify`, `remove`) that abstracts the local implementation. Implement chunked transfer logic to bound memory usage when caching large assets.

### R5. Subsystem Independence
The M5 cache logic (in `src/aidars/cache/`) must be entirely decoupled from M4. M5 consumes the asset hashes and sizes provided by M4, but must not depend on Blender objects, materials, cameras, or visibility logic.

## Acceptance Criteria

### Implementation Verification
- [ ] A comprehensive `pytest` suite is built from scratch that rigorously tests cache hit/miss set differences, LRU eviction limits, and split-hash storage mechanics.
- [ ] The test suite calculates and reports `byte_hit_ratio` and `network_saved` metrics for simulated transfer requests.
- [ ] Tests simulate cache corruption (e.g., modifying cached files on disk) and verify that corrupted entries are detected and evicted.
- [ ] The cache code is isolated in `src/aidars/cache/` and does not import or depend on Blender-specific graph modules.
