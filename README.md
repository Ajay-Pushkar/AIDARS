# AIDAR (AI-Driven Adaptive Render & Asset Distribution System)

AIDAR is an intelligent render pre-processing and distributed asset optimization engine designed to dramatically reduce cloud rendering costs and memory footprints. By analyzing 3D scene geometry, dependencies, lighting, and camera spatial relationships, AIDAR extracts the **exact minimal asset closure** required to produce mathematically identical renders.

---

## 🚀 Key Architecture & Milestones

```text
Input (.blend / JSON)
        │
        ▼
BlenderAdapter / Scene Loader
        │
        ▼
SceneIntelligenceEngine ─────────► [Milestone 1: Canonical Scene Snapshot (v1.0.0)]
        │
        ▼
DependencyGraphBuilder ──────────► [Milestone 2: Hierarchical Dependency Graph]
        │
        ▼
IntegrityChecker
        │
        ▼
RenderRequirementAnalyzer ───────► [Milestone 3: 3D Render Requirement Analysis]
  ├── EligibilityAnalyzer (hide_render & animated F-curves)
  ├── CameraAnalyzer (3D basis, FOV, clipping, projection)
  ├── FrustumCuller (AABB Euler transforms, frustum culling, Ray-AABB occlusion)
  ├── InfluenceAnalyzer (lights, hierarchy, simulations, world HDRI)
  └── DependencyResolver (BFS/DFS graph closure)
        │
        ▼
SmartPackageBuilder / Optimizer ─► [Milestone 4: Zero-Waste Smart Packaging Manifest]
        │
        ▼
FrameScheduler ──────────────────► [Milestone 7: Distributed Workload Planning]
```

---

## 📦 Core Capabilities

### 1. Milestone 1: Canonical Scene Intelligence
- High-fidelity extraction and normalization of Blender scene data (objects, meshes, materials, textures, cameras, lights, animation curves).
- Strict schema emission (`v1.0.0`) decoupling raw extraction concerns from downstream consumers.

### 2. Milestone 2: Hierarchical Dependency Graph
- Topological mapping of scene assets (Object -> Modifier -> Material -> Texture -> Image).
- Synthetic root grouping (`Scene`) with stable identifier keys and human-readable labels.
- Automated integrity reporting: detects unresolved references and orphaned/unused assets.

### 3. Milestone 3: Render Requirement Analysis (`src/aidars/visibility/`)
Transforms AIDAR from a static visibility checker into a full 3D spatial auditor with conservative guarantees (`Uncertain → KEEP`):
- **Data Contracts**: `RenderRequest` (camera, frame range, resolution) and `RenderRequirementReport` with granular reason tracking (`CAMERA_VISIBLE`, `LIGHT_SOURCE`, `PARENT_HIERARCHY`, `SIMULATION`, `DEPENDENCY`).
- **Eligibility**: Evaluates static visibility and animated `hide_render` curves across frame ranges.
- **Camera Math**: Calculates view basis vectors (`R`, `U`, `D`), perspective/orthographic projections, and clipping planes.
- **Frustum Culling & Occlusion**: Translates local bounding boxes into world-space AABBs using 3D Euler angles, tests against 6 frustum planes, and performs conservative Ray-AABB occlusion queries.
- **Render Influence**: Conservatively preserves active lighting, parent hierarchies, physics simulations (Cloth, Particles, Fluid, Armatures), and world HDRIs.
- **Dependency Closure**: Computes the transitive closure over the dependency graph to isolate only the required meshes, materials, textures, and images.

### 4. Request-Aware & Disk-Verified Caching
- **Deterministic Fingerprinting**: `SceneEngineRequest.fingerprint()` hashes stage flags, frame ranges, output targets, and camera IDs.
- **Multi-Request Coexistence**: Prevents false cache hits when identical source files are run with different parameters (e.g. frame ranges or packaging flags).
- **Artifact Verification**: Actively checks that cached output files still exist on disk before reusing them.

---

## 🛠️ Installation & Setup

```bash
# Clone the repository
git clone <repo-url>
cd AIDAR

# Install dependencies (Python 3.10+)
pip install -r requirements.txt
```

---

## 💻 CLI Usage

Run end-to-end analysis, dependency graph generation, and smart packaging:

```bash
# Basic scene snapshot & dependency graph
python -m aidars.scene_intelligence.cli sample_scene.json \
  -o output/scene.json \
  --graph-output output/dependency_graph.json

# Enable packaging with Milestone 3 visibility optimization and caching
python -m aidars.scene_intelligence.cli sample_scene.json \
  -o output/scene.json \
  --graph-output output/dependency_graph.json \
  --package \
  --package-output output/package.json \
  --optimize-package-by-visibility \
  --frame-start 1 \
  --frame-end 24 \
  --cache-dir output/.cache
```

---

## 🧪 Testing & Verification

AIDAR features an extensive unit, integration, facade, and adversarial test suite:

```bash
python -m unittest discover tests
```

**Status**: ✅ **115/115 tests passing** (0 failures, 0 errors).

---

## 🗺️ Roadmap & Next Milestones

- **Milestone 4**: Physical Zero-Waste Packaging (Bundling `.blend` and required assets).
- **Milestone 5**: Content-Addressed Asset Deduplication & Delta Hashing.
- **Milestone 6**: Distributed Worker Daemon & Asset Sync Protocol.
- **Milestone 7**: Intelligent Hardware-Aware Frame Scheduler.
- **Milestone 8**: Headless Distributed Render Execution & Validation.