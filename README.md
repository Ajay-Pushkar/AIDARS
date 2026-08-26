# AIDAR (AI-Driven Adaptive Render & Asset Distribution System)

AIDAR is an intelligent render pre-processing and distributed asset optimization engine designed to dramatically reduce cloud rendering costs and memory footprints. By analyzing 3D scene geometry, dependencies, lighting, and camera spatial relationships, AIDAR extracts the **exact minimal asset closure** required to produce mathematically identical renders.

---

## 🚀 Architecture & Milestone Progression

```text
Input (.blend / Scene JSON)
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
LocalCASAdapter / Storage Mesh ──► [Milestone 5: Content-Addressed Distributed Asset Layer]
  ├── Split-Hash 2-Level Fanout (`objects/XX/<hash>`)
  ├── Memory-Bounded Binary Streaming (`/api/v1/assets/{hash}/stream`)
  ├── Incremental SHA-256 Verification & Atomic Commit
  ├── Inverted Hash Index (`CoordinatorService` & `WorkerRegistry`)
  ├── 4-Tier Network Locality Prioritizer (Loopback, Subnet, LAN, WAN)
  └── Failover & Self-Healing Node Recovery
        │
        ▼
Adaptive Placement Engine ───────► [Milestone 6: Computational Resource System]
  ├── Hardware Profiling (CPU, RAM, GPU, VRAM)
  ├── Multi-Attribute Placement Scoring S(w, tau)
  ├── Single-Flight Request Deduplication
  └── Sandboxed Task Execution & Output CAS Ingestion
```

---

## 📚 Milestone Index & Documentation

| Milestone | Title | Module Path | Status | Documentation |
|---|---|---|---|---|
| **Milestone 1** | Canonical Scene Intelligence | `src/aidars/scene_intelligence/` | **COMPLETE** ✅ | [milestone_1_scene_intelligence.md](docs/milestone_1_scene_intelligence.md) |
| **Milestone 2** | Hierarchical Dependency Graph | `src/aidars/scene_intelligence/` | **COMPLETE** ✅ | [milestone_2_dependency_graph.md](docs/milestone_2_dependency_graph.md) |
| **Milestone 3** | 3D Spatial Visibility & Influence | `src/aidars/visibility/` | **COMPLETE** ✅ | [milestone_3_visibility_analysis.md](docs/milestone_3_visibility_analysis.md) |
| **Milestone 4** | Smart Packaging & Path Remap | `src/aidars/smart_package/` | **COMPLETE** ✅ | [milestone_4_smart_packaging.md](docs/milestone_4_smart_packaging.md) |
| **Milestone 5** | Distributed Asset Layer & Mesh | `src/aidars/distributed/` & `cache/` | **COMPLETE** ✅ | [milestone_5_distributed_asset_layer.md](docs/milestone_5_distributed_asset_layer.md) |
| **Milestone 6** | Computational Resource System | `src/aidars/distributed/` | **SPECIFIED** 🚀 | [AIDAR_M6_Architecture_and_Validation_Spec.md](AIDAR_M6_Architecture_and_Validation_Spec.md) |

---

## 📦 Milestone Summary

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
- **M4-A Logical Packaging**: `RequirementResolver` translates semantic M3 `RenderRequirementReport` into canonical graph node identifiers. `DependencyClosureResolver` computes transitive closures via BFS.
- **M4-B Physical Resolution**: `PhysicalAssetResolver` resolves Blender `//relative` paths and absolute paths. Validates physical existence and streams SHA-256 hashes (64KB chunks). Categorizes assets as `RESOLVED`, `MISSING`, or `EMBEDDED`.
- **M4-C Deduplication & Construction**: `PackagePlanner` performs content-addressed deduplication (identical SHA-256 hashes = single physical copy). `PackageBuilder` builds packages inside secure temporary directories and performs atomic `.bak` directory swaps. Automatically executes headless Blender via `remap_paths.py` to rewrite internal dependencies.
- **M4-D Validation**: `PackageValidator` strictly enforces `.blend` presence and re-hashes every file in the package against the manifest.
- **M4-E Blender Path Remapping**: Copies `.blend` into the package and rewrites internal asset references (images, libraries, caches, volumes) to portable `//assets/` paths.

### 5. Milestone 5: Distributed Content-Addressed Asset Layer (`src/aidars/distributed/`)
High-performance, fault-tolerant distributed storage and streaming network:
- **Local Content-Addressed Storage (`LocalCASAdapter`)**: Split-hash 2-level directory structure (`objects/<h[:2]>/<h[2:]>`), staging buffers in `staging/<uuid>.tmp`, and atomic `os.replace` commits.
- **Cluster Control Plane (`CoordinatorService` & `WorkerRegistry`)**: Inverted hash index (`SHA-256 -> Set[WorkerID]`), 4-tier network locality prioritizer (`LOOPBACK` > `SUBNET` > `LAN` > `WAN`), and 5-second heartbeat reaper.
- **Peer Streaming Transport (`transfer.py`)**: 1 MiB chunked streaming, progressive SHA-256 integrity verification, HTTP 206 Range resumption, and automatic candidate failover.
- **Adversarial Resilience**: Proven against partial stream aborts, bit-rot/corrupted byte streams, worker crashes, stale inventory poisoning, and multi-threaded race conditions.

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

## 🧪 Testing & Verification

AIDAR features an extensive unit, integration, facade, and adversarial test suite:

```bash
python -m pytest tests/ -q
```

**Status**: ✅ **795 passed, 6 subtests passed** (100% pass rate).

---

## 🗺️ Roadmap: Milestone 6 & Beyond

- **Milestone 5**: ✅ COMPLETE & FROZEN — Distributed Content-Addressed Asset Layer & Fault-Tolerant Streaming Mesh.
- **Milestone 6**: 🚀 IN PROGRESS — Adaptive Computational Resource System (`WorkloadSpec`, Hardware Profiling, Multi-Attribute Placement, SingleFlight Deduplication, Sandboxed Task Execution).
- **Milestone 7**: Headless Multi-Node Distributed Render Orchestration.