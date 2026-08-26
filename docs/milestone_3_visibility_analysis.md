# Milestone 3: 3D Spatial Visibility, Frustum Culling & Render Influence

**Module:** `src/aidars/visibility/`  
**Status:** Completed & Validated  

---

## 1. Overview
Milestone 3 transforms AIDAR from a static asset checker into a full 3D spatial auditor with conservative guarantees:
$$\text{Uncertain} \implies \text{KEEP}$$

---

## 2. Core Capabilities
- **Eligibility Analysis**: Evaluates static visibility and animated `hide_render` curves across multi-frame ranges.
- **Camera Math & View Frustum**:
  - Calculates 3D camera basis vectors ($\vec{R}, \vec{U}, \vec{D}$), field-of-view (FOV), and clipping planes.
  - Supports perspective and orthographic camera projections.
- **Frustum Culling**:
  - Transforms local bounding boxes into world-space Axis-Aligned Bounding Boxes (AABBs) using 3D Euler angles.
  - Tests bounding volumes against 6 frustum planes with conservative inflation margins.
- **Render Influence Analyzer**:
  - Automatically retains objects casting shadows or illuminating the view (direct and indirect lighting).
  - Preserves active physics simulations (Cloth, Particles, Fluid, Armature deformers) and world HDRI environments.
- **Transitive Dependency Closure**: Resolves all required meshes, materials, and textures for visible objects via graph BFS.

---

## 3. Output: `RenderRequirementReport`
Produces an audit report categorizing each asset with explicit requirement reasons:
- `CAMERA_VISIBLE`
- `LIGHT_SOURCE`
- `PARENT_HIERARCHY`
- `SIMULATION`
- `DEPENDENCY`
