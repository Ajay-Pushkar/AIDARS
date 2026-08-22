# Project: AIDAR Request-Aware Scene Caching & Milestone 3 Visibility Pipeline

## Architecture
AIDAR is a Blender scene intelligence, visibility analysis, and dependency management framework.
The project architecture for this scope consists of two decoupled subsystems:

1. **Scene Intelligence & Request-Aware Caching (`src/aidars/scene_intelligence/`)**:
   - `SceneEngineRequest`: Full dataclass defining run configurations (input path, output paths for scene/graph/package, stage execution flags, frame ranges, camera ID, blender executable). Provides deterministic SHA-256 `fingerprint()` incorporating all 10 configuration options.
   - `SceneCache` & `SceneCacheEntry`: Multi-key SHA-256 caching system keyed by `f"{source_key}::{request_hash}"`. Validates both source payload SHA-256 and request configuration SHA-256. Enforces on-disk artifact existence verification (`scene_output`, `graph_output`, `package_output`) on every cache hit check.
   - `SceneEngine`: High-level facade executing scene inspection, dependency graph extraction, integrity checks, M3 render requirement analysis, and package manifest creation.

2. **Milestone 3 Render Requirement Analysis Pipeline (`src/aidars/visibility/`)**:
   - `RenderRequest`: Encapsulates camera ID, frame window (`[frame_start, frame_end]`), resolution, view layer, scene name, and conservatism flags.
   - `EligibilityAnalyzer`: Determines static `hide_render`/`hide_viewport` states and animates F-curve keyframe evaluation across frame ranges.
   - `CameraAnalyzer` / `CameraModel`: Parses camera matrices, FOV (horizontal & vertical), clipping planes, and orthonormal view basis vectors ($R, U, D$).
   - `FrustumCuller` / `BoundingBox`: Computes world-space bounding boxes (scaled, rotated, translated) and executes 6-plane frustum intersection (perspective and orthographic). Performs conservative 9-point raycast occlusion testing with camera-space forward depth sorting.
   - `InfluenceAnalyzer`: Preserves essential non-camera entities (`LIGHT_SOURCE`, `PARENT_HIERARCHY`, `SIMULATION`, `ANIMATION_DRIVER`, `WORLD_ENVIRONMENT`, `DEPENDENCY`).
   - `DependencyResolver`: Performs transitive BFS closure over DependencyGraph to extract required meshes, materials, textures, and images with strict node kind discrimination and None-safe unpacking.
   - `RenderRequirementReport`: Canonical typed report maintaining reasons (`RequirementReason`), statistics, `.to_dict()` (M3 schema) and `.to_r4_dict()` (R4 legacy schema).

3. **Isolated / Out-of-Bounds Subsystems (DO NOT TOUCH)**:
   - `src/aidars/scheduler/` (Frame scheduler)
   - `src/aidars/smart_package/` (M4 Smart packaging builder & asset optimizer)
   - Workers / Networking runtime

---

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| F1 | `SceneEngineRequest.fingerprint()` | Computes deterministic SHA-256 covering all request fields: scene_output, graph_output, package_output, build_graph, build_package, optimize_package_by_visibility, frame_start, frame_end, camera_id, blender_executable | M1 | Survey / R1 |
| F2 | `SceneCacheEntry` Dataclass | Data model holding source_hash, request_hash, output paths, stage flags, frame ranges, camera_id, and cached_at timestamp | M1 | Survey / R1 |
| F3 | Request-Aware `SceneCache.get()` & `has_changed()` | Keyed isolation by `source_key::request_hash`, strict fallback checking, on-disk artifact verification for all requested stages | M1 | Survey / R1 |
| F4 | `SceneCache.put()` Isolation | Isolates multi-request cache entries without clobbering base entries | M1 | Survey / R1 |
| F5 | Request-Aware Cache Invalidation on Config / Artifact Change | Ensures changing request flags (e.g. build_package=False vs True) or deleting output artifacts triggers cache miss and re-execution | M1 | Survey / R1, R2 |
| F6 | Rigorous Cache Verification Test Suite (R2) | Test suite in `test_scene_cache.py`, `test_scene_engine_facade.py`, and `test_cache_adversarial.py` proving multi-request coexistence, fallback rejection, artifact deletion misses, camera_id discrimination | M1 | Survey / R2 |
| F7 | `RenderRequest` Dataclass & Parsing | Camera ID, frame range, resolution, view layer, scene name, conservatism flags | M2 | Survey / R3 |
| F8 | `EligibilityAnalyzer` | Static render visibility + animated F-curve keyframe evaluation over frame ranges | M2 | Survey / R3 |
| F9 | `CameraAnalyzer` & `CameraModel` | Camera basis vectors ($R, U, D$), FOV derivation, clipping planes, perspective/orthographic matrices | M2 | Survey / R3 |
| F10 | `FrustumCuller` & Occlusion Testing | 6-plane frustum culling, world-space AABB transformation, 9-point raycast occlusion query with camera forward depth sorting | M2 | Survey / R3 |
| F11 | `InfluenceAnalyzer` | Preservation of lights, parent hierarchies (cycle-safe), simulation modifiers, particle systems, world HDRI | M2 | Survey / R3 |
| F12 | `DependencyResolver` | Transitive closure over DependencyGraph for meshes, materials, textures, and images with BFS material traversal and None-safe unpacking | M2 | Survey / R3 |
| F13 | `RenderRequirementReport` & Schema Serialization | Canonical typed report with reason tracking (`RequirementReason`), `.to_dict()`, `.to_r4_dict()` | M2 | Survey / R3 |
| F14 | `SceneEngine` M3 Visibility Integration | Direct `analyze_render_requirements()` and `SceneEngine.run(optimize_package_by_visibility=True)` returning typed report | M2 | Survey / R3 |
| F15 | Complete Test Suite Verification (115 tests) | All 115 unit, facade, cache, visibility, and adversarial tests pass cleanly | M3 | Acceptance Criteria |
| F16 | Adversarial Hardening & Forensic Integrity Audit | Adversarial verification of edge cases, zero cheating/hardcoding, CLEAN auditor verdict | M3 | Project Pattern |

---

## Milestones
| # | Name | Scope | Dependencies | Status | Key Output / Verification |
|---|------|-------|-------------|--------|---------------------------|
| 1 | M1: Request-Aware Scene Caching & Tests | Fix `cache.py`, `scene_engine.py` cache integration, and implement comprehensive R2 verification tests | none | DONE | 14 cache unit tests + 16 facade integration tests + 10 adversarial cache tests passing |
| 2 | M2: Milestone 3 Visibility Pipeline Verification | Verify and remediate M3 components in `src/aidars/visibility/`, reason tracking, and typed SceneEngine integration | none | DONE | 13 M3 pipeline tests + 12 visibility tests + 16 adversarial visibility tests passing |
| 3 | M3: Full Test Suite Integration & Forensic Audit | Run full 115-test suite, verify adversarial coverage, execute forensic integrity audit | M1, M2 | DONE | 115/115 tests green, 0 errors, 0 failures, CLEAN audit verdict |

---

## Interface Contracts
### `SceneEngineRequest` ↔ `SceneCache`
- `SceneEngineRequest.fingerprint() -> str`: SHA-256 of JSON canonical dict of all configuration options.
- `SceneCache.get(source_key: str | Path, request_hash: str = "", verify_artifacts: bool = False) -> Optional[SceneCacheEntry]`
- `SceneCache.put(source_key: str | Path, entry: SceneCacheEntry) -> None`
- `SceneCache.has_changed(source_key: str | Path, current_hash: str, request_hash: str = "", verify_artifacts: bool = False) -> bool`
- `SceneCache.invalidate(source_key: str | Path) -> None`

### `SceneEngine` ↔ `RenderRequirementAnalyzer`
- `SceneEngine.analyze_render_requirements(source: str | Path | dict, request: Optional[RenderRequest | dict] = None) -> RenderRequirementReport`
- `RenderRequirementReport.to_dict() -> Dict[str, Any]`
- `RenderRequirementReport.to_r4_dict() -> Dict[str, List[str]]`

---

## Code Layout
- `src/aidars/scene_intelligence/cache.py`: SceneCache, SceneCacheEntry, hash functions.
- `src/aidars/scene_intelligence/scene_engine.py`: SceneEngine, SceneEngineRequest, SceneEngineResult.
- `src/aidars/visibility/`:
  - `models.py`: RenderRequest, RenderRequirementReport, RequirementReason.
  - `eligibility.py`: EligibilityAnalyzer.
  - `camera.py`: CameraAnalyzer, CameraModel.
  - `geometry.py`: FrustumCuller, BoundingBox.
  - `influence.py`: InfluenceAnalyzer.
  - `resolver.py`: DependencyResolver.
  - `analyzer.py`: RenderRequirementAnalyzer.
  - `engine.py`: VisibilityEngine (legacy facade).
- `tests/`:
  - `test_scene_cache.py`: Unit tests for SceneCache.
  - `test_cache_adversarial.py`: Adversarial stress tests for SceneCache.
  - `test_scene_engine_facade.py`: Integration tests for SceneEngine caching and stages.
  - `test_visibility.py`: Geometry and visibility tests.
  - `test_visibility_adversarial.py`: Adversarial edge case tests for visibility.
  - `test_render_requirements.py`: Exhaustive M3 render requirement pipeline tests.
