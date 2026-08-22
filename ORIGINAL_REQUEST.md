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
  * `SceneEngine` produces a typed `RenderRequirementReport` when visibility optimization is requested.

Please orchestrate the planning, exploration, implementation, testing, adversarial challenge, and verification according to your role instructions. Maintain your plan.md, progress.md, and context.md in your working directory. Notify me when victory is ready to be audited.
