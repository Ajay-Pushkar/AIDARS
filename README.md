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

### 4. Milestone 4: Smart Packaging (`src/aidars/smart_package/`)
Complete physical packaging pipeline transforming M3 render requirements into verified, portable render packages:
- **M4-A Logical Packaging**: `RequirementResolver` translates semantic M3 `RenderRequirementReport` into canonical graph node identifiers. `DependencyClosureResolver` computes transitive closures via BFS (cycle-safe, missing-edge tolerant).
- **M4-B Physical Resolution**: `PhysicalAssetResolver` resolves Blender `//relative` paths and absolute paths. Validates physical existence and streams SHA-256 hashes (64KB chunks). Categorizes assets as `RESOLVED`, `MISSING`, or `EMBEDDED`. Missing assets are **never silently dropped**.
- **M4-C Deduplication & Construction**: `PackagePlanner` performs content-addressed deduplication (identical SHA-256 hashes = single physical copy). `PackageBuilder` safely builds packages using a secure `tempfile.mkdtemp` and performs thread/process-safe atomic `.bak` directory swaps. Writes deterministic schema v1.0 manifest (`sort_keys=True`, assets sorted by `asset_id`). Automatically executes headless Blender via `remap_paths.py` with `O(1)` dict lookups to rewrite internal dependencies.
- **M4-D Validation**: `PackageValidator` strictly enforces the `.blend` contract (preventing phantom packages) and re-hashes every file in the package to compare against the manifest. Triggers a headless Blender secondary validation to ensure the packaged scene physically resolves all mapped images/volumes.
- **M4-E Blender Path Remapping**: Copies `.blend` into the package and rewrites internal asset references (images, libraries, caches, volumes) to portable `//assets/` paths via headless Blender invocation. Fails gracefully if Blender is unavailable.
- **Security Hardening**: Path traversal defense (`is_relative_to`), symlink escape prevention (`src.resolve()`), duplicate basename collision avoidance (SHA-256 hash prefixes), atomic package creation (concurrently safe `tempfile.mkdtemp` staging -> three-step `.bak` swap).
- **Content-Addressed Identity**: `package_id` is derived from `SHA-256(request.fingerprint())`, not frame ranges. Two different scenes with the same frame range produce different package IDs.

### 5. Request-Aware & Disk-Verified Caching
- **Deterministic Fingerprinting**: `SceneEngineRequest.fingerprint()` hashes stage flags, frame ranges, output targets, and camera IDs.
- **Multi-Request Coexistence**: Prevents false cache hits when identical source files are run with different parameters (e.g. frame ranges or packaging flags).
- **Artifact Verification**: Actively checks that cached output files still exist on disk before reusing them.

---

## 🛠️ Installation & Setup

```bash
# Clone the repository
git clone https://github.com/Ajay-Pushkar/AIDARS.git
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
python -m pytest tests/ -q
```

**Status**: ✅ **178/178 tests passing** (0 failures, 0 errors).

---

## 🗺️ Roadmap & Next Milestones

- **Milestone 4**: ✅ COMPLETE — Physical Zero-Waste Packaging (Bundling `.blend` and required assets, Manifest v1.0, Security Hardening, Blender Path Remapping).
- **Milestone 5**: Content-Addressed Asset Deduplication & Delta Hashing.
- **Milestone 6**: Distributed Worker Daemon & Asset Sync Protocol.
- **Milestone 7**: Intelligent Hardware-Aware Frame Scheduler.
- **Milestone 8**: Headless Distributed Render Execution & Validation.