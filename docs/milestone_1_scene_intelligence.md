# Milestone 1: Canonical Scene Intelligence & Snapshot Extraction

**Module:** `src/aidars/scene_intelligence/`  
**Status:** Completed & Validated  
**Schema Version:** `v1.0.0`

---

## 1. Overview
Milestone 1 provides high-fidelity, deterministic extraction and normalization of Blender scene data into a standardized canonical JSON snapshot (`v1.0.0`), cleanly decoupling raw Blender extraction concerns from downstream graph, visibility, and packaging consumers.

---

## 2. Core Capabilities
- **Blender Scene Extraction**: Extracts objects, meshes, materials, procedural textures, image textures, lights, cameras, and animation curves.
- **Transform & Bounding Volume Math**: Computes local bounding boxes, world-space transform matrices, Euler rotations, and dimensions.
- **Animation & F-Curve Sampling**: Evaluates `hide_render` keyframes and animated properties across user-specified frame ranges.
- **Standardized Schema**: Strict Pydantic models validating all entity relationships before downstream processing.

---

## 3. Key Components
- `scene_intelligence/blender_adapter.py`: Headless Blender script interfacing with `bpy.data`.
- `scene_intelligence/models.py`: Canonical data contracts for Scene, Object, Material, Texture, Camera, and Light entities.
- `scene_intelligence/extractor.py`: High-performance parser building the normalized snapshot.

---

## 4. Verification & Status
- Covered by unit and integration tests under `tests/unit/` and `tests/e2e/`.
- Validated on real `.blend` scenes with complex material graphs and multi-camera setups.
