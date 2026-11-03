# AIDAR: AI-Driven Adaptive Render & Asset Distribution System

AIDAR is a fault-tolerant, hardware-adaptive distributed computing and asset optimization platform. Originally developed to eliminate cloud rendering overhead for 3D animation scenes, AIDAR has evolved into a general distributed workload engine that understands application resource constraints, dynamically assesses node hardware telemetry, streams minimal content-addressed data dependencies, isolates execution inside sandboxes, and verifies generated artifacts across heterogeneous physical PC clusters.

---

## 1. Project Overview

AIDAR addresses the physical and financial inefficiencies of distributed compute: data over-distribution, blind scheduling, visual inconsistency across heterogeneous nodes, stragglers, and worker failures.

The foundational design tenet of AIDAR is architectural decoupling:

> **"Adapters may know applications. Core may know workloads. The core must never need to understand application internals."**

By isolating domain-specific logic inside application adapters, AIDAR’s distributed core operates purely on generic primitives: **Jobs**, **WorkloadSpecs**, **ExecutionSpecs**, **Attempts**, **Content-Addressed Storage (CAS)** hashes, and **WorkerResourceProfiles**.

AIDAR currently supports:
- **Headless 3D Rendering (Blender Cycles & Eevee)** with spatial frustum culling, dependency closure isolation, node-local execution adaptation, visual determinism locking, and cooperative Monte Carlo sample buffer fusion.
- **Video Transcoding & Processing (FFmpeg & DaVinci Resolve)** with continuous throughput-weighted duration chunking and project archive parsing.
- **Local LLM Inference (Ollama & llama.cpp)** with prompt-batch task splitting, dynamic CPU thread scaling, and fast reasoning `<think>` tag bypass.
- **Machine Learning Training Scripts (PyTorch)** with CAS-staged datasets, sandboxed script execution, and model checkpoint verification.

---

## 2. What AIDAR Actually Does

1. **Eliminates Asset Over-Distribution**: Rather than transferring entire multi-gigabyte project directories, AIDAR performs 3D spatial visibility analysis (camera frustum culling, Ray-AABB occlusion, lighting/simulation influence) and graph dependency resolution to extract the exact minimal asset closure required for a task.
2. **Dynamic Hardware Profiling Without Hardcoding**: AIDAR inspects live telemetry across all connected workers (logical/physical CPU cores, clock frequency, available physical RAM, NVML GPU/VRAM). Decisions never rely on hardcoded worker names, tags, or IP addresses.
3. **Hardware Adaptation vs. Visual Determinism**: On heterogeneous clusters, execution mechanics adapt to each node's physical limits (CPU render threads scale to local core counts, tile sizes scale with available physical RAM to prevent `pagefile.sys` thrashing). Concurrently, visual and lighting parameters (resolution 100%, simplify disabled, color management, light tree, denoising prefilters, and Monte Carlo seeds) are locked 100% identical across all nodes to guarantee visual consistency.
4. **Resilient Local CAS & Network Locality**: Assets are content-addressed by SHA-256 with 2-level fanout storage. Multi-tier locality prioritization (`LOOPBACK` > `SUBNET` > `LAN` > `WAN`), single-flight concurrent download deduplication, and HTTP 206 partial-range resumption prevent redundant network transfers.
5. **Speculative Straggler Work-Stealing**: When a fast worker finishes its workload and becomes idle while another node is straggling on an in-flight chunk, the coordinator launches a speculative clone attempt on the idle worker. Whichever node finishes first commits its verified outputs to CAS, and the rival attempt is immediately canceled via process-tree termination.
6. **Decoupled Asynchronous Execution**: Workers execute workloads inside isolated filesystem sandboxes (`inputs/`, `outputs/`, `logs/`). Processes run asynchronously decoupled from HTTP request timeouts, with full Windows process-tree termination (`taskkill /F /T /PID`) preventing orphan worker processes on timeouts or cancellations.

---

## 3. End-to-End Architecture Diagram

```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                         CLIENT / WORKFLOW SUBMISSION                        │
 │           (Python SDK / DistributedClient / REST API / Web UI)             │
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │ (Job Request Dict)
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                         APPLICATION ADAPTER LAYER                           │
 │  ┌───────────────────┐ ┌───────────────────┐ ┌───────────────────────────┐  │
 │  │  BlenderAdapter   │ │   FFmpegAdapter   │ │   LLM / ML Adapters       │  │
 │  │ 3D Spatial Audit  │ │ DaVinci Resolution│ │ Prompt / Dataset Staging  │  │
 │  │ Sample Fusion Plan│ │ Proportional Split│ │ Thread / Context Scaling  │  │
 │  └─────────┬─────────┘ └─────────┬─────────┘ └─────────────┬─────────────┘  │
 └────────────┼─────────────────────┼─────────────────────────┼────────────────┘
              │ (Emits Generic WorkloadSpecs & Uploads Assets to Master CAS)
              ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                    COORDINATOR SERVICE (CONTROL PLANE)                      │
 │                                                                             │
 │  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐  │
 │  │   WorkerRegistry    │  │  WorkloadRegistry   │  │     JobRegistry     │  │
 │  │ Inverted Hash Index │  │ Workload Lifecycle  │  │ Aggregate State     │  │
 │  │ Heartbeat Monitor   │  │ Attempt Tracking    │  │ Job Confinement     │  │
 │  └──────────┬──────────┘  └──────────┬──────────┘  └──────────┬──────────┘  │
 │             │                        │                        │             │
 │  ┌──────────▼────────────────────────▼────────────────────────▼──────────┐  │
 │  │            Adaptive Placement Engine & Cluster Sizer                  │  │
 │  │  - Hard Constraint Filtering (Active, Health, Memory Headroom, Tags)  │  │
 │  │  - Multi-Attribute Placement Scoring S(w, tau)                        │  │
 │  │  - Continuous Hardware Throughput Scoring & Proportional Allocation  │  │
 │  │  - Speculative Straggler Work-Stealing Coordinator                    │  │
 │  └───────────────────────────────────┬───────────────────────────────────┘  │
 │                                      │                                      │
 │  ┌───────────────────────────────────▼───────────────────────────────────┐  │
 │  │        Persistence & Lifecycle: SQLite CoordinatorStateStore          │  │
 │  │       ArtifactRegistry (Active GC Loop, Retention Window 3600s)       │  │
 │  │       CredentialStore (Admin Bearer Tokens, Worker Bootstrap, Replay) │  │
 │  └───────────────────────────────────────────────────────────────────────┘  │
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │ HTTP /api/v1/workloads/execute (Attempt)
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                      DISTRIBUTED WORKER (DATA PLANE)                        │
 │                                                                             │
 │  ┌───────────────────────────────────────────────────────────────────────┐  │
 │  │ 1. Dependency Synchronization & Peer Transfer                         │  │
 │  │    - Missing Set Calculation (local CAS check)                        │  │
 │  │    - Coordinator /api/v1/assets/locate (Locality: LOOPBACK/LAN/WAN)   │  │
 │  │    - SingleFlight Request Coalescing per asset (`asset-{hash}`)       │  │
 │  │    - Chunked HTTP 206 Streaming (1 MiB buffers, Progressive SHA-256)  │  │
 │  └───────────────────────────────────┬───────────────────────────────────┘  │
 │                                      │ Staged Inputs                        │
 │  ┌───────────────────────────────────▼───────────────────────────────────┐  │
 │  │ 2. ExecutionManager & GenericSubprocessRuntime                        │  │
 │  │    - Sandboxed Environment: inputs/ -> outputs/ -> logs/              │  │
 │  │    - Universal Binary Auto-Discovery (Registry, WinGet, PATH, Steam)  │  │
 │  │    - Safe Permissions & Windows Read-Only Clearing On Cleanup         │  │
 │  │    - Async Process Execution, Poll Interval 0.2s                      │  │
 │  │    - Hard Process Tree Termination (taskkill /F /T /PID on Cancel)    │  │
 │  └───────────────────────────────────┬───────────────────────────────────┘  │
 │                                      │ Generated Output Files               │
 │  ┌───────────────────────────────────▼───────────────────────────────────┐  │
 │  │ 3. Output Verification & CAS Ingestion                                │  │
 │  │    - Non-blocking Threadpool Ingestion into LocalCASAdapter           │  │
 │  │    - OutputVerificationPolicy (File counts, hashes, sizes)            │  │
 │  │    - Heartbeat & Telemetry Monitor (WorkerResourceMonitor)            │  │
 │  └───────────────────────────────────────────────────────────────────────┘  │
 └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Detailed Architecture Layers

AIDAR is organized into four distinct architectural layers:

### A. Application Adapter Layer (`src/aidars/adapters/`)
Translates domain-specific requests into generic workload contracts. Adapters implement `ApplicationAdapter`:
- `validate(request)`: Ensures request syntax and required fields are valid.
- `evaluate_request(request)`: Parses assets, computes dependencies, registers input files with CAS, splits work into chunks, and returns `List[WorkloadSpec]`.
- `build_execution_spec(workload)`: Constructs safe executable commands, CLI flags, environment variables, and execution timeouts.
- `describe_expected_outputs(workload)`: Defines output verification policies independent of runtime execution.

### B. Control Plane / Coordination Layer (`src/aidars/distributed/`)
Manages cluster state, scheduling, and lifecycle:
- **`CoordinatorService`**: FastAPI/Starlette control-plane daemon exposing REST endpoints for registration, heartbeats, inverted index asset location, and workload dispatch.
- **`WorkerRegistry`**: Tracks active workers, last heartbeat timestamps, degraded/unhealthy states, failure penalties, and advertised capability tags (`cap:blender`, `cap:ffmpeg`, `cap:ollama`, `cap:pytorch`).
- **`WorkloadRegistry` & `JobRegistry`**: Tracks workload lifecycle state (`SUBMITTED`, `PLACED`, `EXECUTING`, `COMPLETED`, `FAILED`, `CANCELLED`), parent job aggregation, and job-scoped worker confinement.
- **`AttemptRegistry`**: Tracks individual execution attempts (`ASSIGNED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `LOST`), enabling transparent retries and speculative straggler clones.
- **`PlacementEngine`**: Evaluates hard worker eligibility and scores candidates using multi-attribute utility formulas.
- **`AdaptiveClusterSizer`**: Computes right-sized worker subsets for jobs to prevent over-allocating idle nodes.

### C. Data Plane & Execution Layer (`src/aidars/distributed/worker.py`, `execution.py`, `runtime.py`)
Executes workloads on physical host machines:
- **`DistributedWorker`**: Worker agent communicating with the coordinator via REST, advertising local hardware telemetry and inventory.
- **`StreamingTransferClient`**: Streams binary assets over HTTP from peer workers or coordinator CAS using 1 MiB chunk buffers, progressive SHA-256 verification, and HTTP 206 Range resumption.
- **`SingleFlight`**: Coalesces concurrent requests for identical asset hashes, ensuring each asset is downloaded across the network exactly once per node.
- **`ExecutionManager`**: Prepares isolated sandbox directories (`inputs/`, `outputs/`, `logs/`), stages input file aliases (`.blend`, `.py`), and enforces non-blocking CAS output ingestion.
- **`GenericSubprocessRuntime`**: Launches executables as subprocesses, monitors memory/stdout/stderr, enforces hard execution timeouts, and terminates the entire process tree on failure.

### D. Storage & Persistence Layer (`src/aidars/cache/`, `distributed/state_store.py`)
Provides durable content-addressed storage and coordinator recovery:
- **`LocalCASAdapter`**: Two-level split-hash directory fanout (`objects/<h[:2]>/<h[2:]>`), staging buffers (`staging/<uuid>.tmp`), atomic rename commits, and disk quota enforcement.
- **`CoordinatorStateStore`**: SQLite database persisting workers, workloads, jobs, attempts, and artifacts across coordinator restarts, protected by cross-platform single-writer process file locks.
- **`ArtifactRegistry`**: Tracks artifact provenance (`producer_workload_id`, `producer_job_id`) and executes active garbage collection sweeps, protecting terminal job artifacts for a configurable retention window (default 3600s).

---

## 5. Milestone Progression and Current Status

| Milestone / Phase | Title | Scope & Achievements | Status |
|---|---|---|---|
| **Milestone 1** | Canonical Scene Intelligence | High-fidelity extraction of Blender scene data (objects, meshes, materials, textures, cameras, lights, F-curves) into strict canonical JSON schema `v1.0.0`. | **COMPLETE** ✅ |
| **Milestone 2** | Hierarchical Dependency Graph | DAG generation (Object -> Modifier -> Material -> Texture -> Image), synthetic root grouping, integrity checking for orphaned and unresolved references. | **COMPLETE** ✅ |
| **Milestone 3** | 3D Spatial Visibility & Influence | Camera view basis math, Euler world AABB transformation, 6-plane frustum culling, Ray-AABB occlusion, render influence preservation (lights, physics, hierarchy, HDRIs). | **COMPLETE** ✅ |
| **Milestone 4** | Smart Packaging & Path Remapping | Physical asset resolver (SHA-256 chunking), content-addressed deduplication, atomic `.bak` directory swaps, headless Blender `remap_paths.py` execution. | **COMPLETE** ✅ |
| **Milestone 5** | Distributed Content-Addressed Storage | Split-hash 2-level CAS fanout, inverted hash index, 4-tier network locality prioritizer, HTTP 206 partial streaming, progressive SHA-256 validation. | **COMPLETE** ✅ |
| **Milestone 6** | Computational Resource System | `WorkerResourceProfile` telemetry, multi-attribute placement scoring $S(w, \tau)$, single-flight request coalescing, sandboxed execution runtime. | **COMPLETE** ✅ |
| **Milestone 7** | Predictive & Behavioral Intelligence | Advisory `M7Controller`, anomaly detection, probabilistic state classification (`HEALTHY`, `OVERLOADED`, `DEGRADED`, `ANOMALOUS`), placement risk evaluation. | **COMPLETE** ✅ |
| **Milestone 8** | Production Cluster Operations | Durable Job identity, parent-child workload aggregation, output verification policies, frame-coverage validation, and `Artifact` lifecycle state model. | **COMPLETE** ✅ |
| **Milestone 9** | Security & Adversarial Controls | Admin bearer tokens, worker bootstrap secret, high-entropy dynamic credential issuance, replay guard with TTL, bounded workload parameter sizes (65KB). | **COMPLETE** ✅ |
| **Milestone 10** | Production Execution & Persistence | Decoupled HTTP execution lifecycle, SQLite `CoordinatorStateStore` with cross-platform single-writer lock, `AttemptRegistry`, coordinated checkpoint recovery. | **COMPLETE** ✅ |
| **Milestone 11** | Advanced Scheduling & Capacity | `AdaptiveClusterSizer` right-sizing, job-scoped worker confinement, heterogeneous GPU capability routing, affinity group scheduling. | **COMPLETE** ✅ |
| **Milestone 12** | Distributed Intelligence History | Execution telemetry history, historical duration prediction models, temporal metrics retention. | **COMPLETE** ✅ |
| **Milestone 13** | Platform Expansion | Generic `ApplicationAdapter` contract; initial implementations for Blender, FFmpeg/video, Ollama/LLM, and PyTorch ML training. | **COMPLETE** ✅ |
| **Milestone 14** | Decoupled Subprocess Execution | `GenericSubprocessRuntime` subprocess isolation, structured argument passing, memory bounds, stdout/stderr snippet capture. | **COMPLETE** ✅ |
| **Milestone 15** | Performance & Concurrency | Async CAS ingestion threadpool offloading (96.4% event loop stall reduction on 500 MB assets), sub-2.5ms coordinator placement at 100-worker scale. | **COMPLETE** ✅ |
| **Milestone 16** | Active GC & Checkpointing | Active artifact garbage collection with retention window protection, coordinated execution group draining, structured failure classification. | **COMPLETE** ✅ |
| **Milestone 17–19** | Real Workload Integrations | End-to-end execution of real Blender CLI rendering, FFmpeg video transcoding, Ollama deepseek-r1 LLM inference, and PyTorch ML training. | **COMPLETE** ✅ |
| **Milestone 20** | Physical Multi-PC LAN Validation | Stage 0–8 physical multi-PC verification on real Windows LAN topology (`multi_pc_validation_report.txt`, `run_multi_pc_demo.py`). | **COMPLETE** ✅ |
| **Universal Architecture** | Heterogeneous $N$-PC Cluster | Zero hardcoded worker IDs/IPs, continuous hardware power scoring, visual determinism locking vs node-local execution adaptation, speculative straggler work-stealing, cooperative sample fusion. | **COMPLETE** ✅ |

---

## 6. Scene Intelligence and Visibility Analysis

Located in `src/aidars/adapters/blender/intelligence/` and `src/aidars/adapters/blender/visibility/`.

### 3D Spatial Audit Pipeline
1. **Camera Parameter Extraction**: Extracts focal length, sensor dimensions, clipping boundaries ($z_{\text{near}}, z_{\text{far}}$), camera position matrix, and 3D orientation.
2. **Coordinate Basis Calculation**: Evaluates forward direction vector $D$, right basis vector $R$, and up basis vector $U$.
3. **World Bounding Box Transformation**: Computes object-space local bounding boxes and transforms them into world-space Axis-Aligned Bounding Boxes (AABB) using local Euler angles, translation vectors, and parent transformation chains.
4. **Frustum Culling**: Evaluates world-space AABB coordinates against the six 3D camera frustum planes (Left, Right, Bottom, Top, Near, Far). Objects strictly outside the frustum are marked non-visible.
5. **Ray-AABB Occlusion Culling**: Casts conservative visibility rays to detect fully occluded geometry.
6. **Conservative Influence Preservation (`Uncertain -> KEEP`)**:
   - Objects flagged with animated `hide_render` F-curves are evaluated across the entire target frame range. If visible on any frame, they are retained.
   - Active light sources (Point, Sun, Spot, Area) illuminating visible geometry are preserved regardless of camera frustum intersection.
   - Hierarchy chains (parent empties, armatures, target constraints) are preserved to maintain coordinate transform integrity.
   - Active physics simulations (Cloth, Particles, Fluid, Smoke, Rigid Body, Armatures) are preserved.
   - World background textures and HDRI environment maps are preserved.
7. **Transitive Dependency Closure**: Resolves the complete graph closure from visible objects down to meshes, materials, procedural node trees, texture images, and VDB volume caches via breadth-first search (BFS).

---

## 7. Smart Packaging and Asset Optimization

Located in `src/aidars/adapters/blender/packaging/`.

### Physical Packaging Pipeline
1. **Physical Asset Resolution**: `PhysicalAssetResolver` maps logical texture and cache identifiers to disk paths. Resolves Blender relative (`//`) and absolute paths, validates physical existence, and categorizes files as `RESOLVED`, `MISSING`, or `EMBEDDED`.
2. **Deduplication & Manifest Generation**: Computes streaming SHA-256 hashes (64 KiB buffer chunks). Identical files with different paths are deduplicated to a single CAS storage entry.
3. **Atomic Assembly & Path Remapping**:
   - Assembles packages inside temporary workspaces.
   - Executes headless Blender with `remap_paths.py` to rewrite internal `.blend` asset references (image textures, point caches, linked libraries) to portable relative paths (`//assets/<hash>.<ext>`).
   - Performs atomic directory swaps using `.bak` rollback safety.
4. **Self-Contained Dependency Packing**: `prepare_self_contained_blend()` ensures external textures, fonts, and node groups are packed directly into `.blend` files before CAS distribution, eliminating broken external texture links on remote worker nodes.

---

## 8. Distributed CAS and Network Locality

Located in `src/aidars/distributed/cas_adapter.py`, `src/aidars/distributed/transfer.py`, and `src/aidars/cache/`.

### Storage Design
- **Two-Level Hash Fanout**: Stored as `objects/<hash[:2]>/<hash[2:]>`. Avoids OS filesystem directory traversal degradation with tens of thousands of asset objects.
- **Atomic Commits**: Incoming transfers are written to `staging/<uuid>.tmp` and committed via atomic `os.replace` only upon full SHA-256 verification.
- **Disk Quota Enforcement**: `DiskQuotaManager` enforces maximum cache capacity with LRU cleanup and cross-process file locks (`ProcessLock` using `msvcrt.LK_NBLCK` on Windows and `fcntl.flock` on POSIX).
- **Asynchronous Ingestion**: Ingestion offloads synchronous disk read operations to worker threadpools (`asyncio.to_thread`), maintaining sub-50ms event loop responsiveness even while writing 500+ MB assets.

### Network Locality Prioritization
When a worker requires missing assets, the coordinator’s `CandidatePrioritizer` evaluates all nodes possessing the asset hash and sorts them into four network tiers:
1. **Tier 1: `LOOPBACK` (127.0.0.1)** — Local process CAS sharing.
2. **Tier 2: `SUBNET`** — Same CIDR `/24` subnet (e.g., rack switch).
3. **Tier 3: `LAN`** — Local private IP range (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`).
4. **Tier 4: `WAN`** — Cross-network or remote internet candidate.

### Transfer Protocol
- **Chunked HTTP Streaming**: Streams binary payloads in 1 MiB chunk buffers.
- **HTTP 206 Partial Content**: Supports resuming interrupted transfers at byte offsets without restarting from byte zero.
- **Progressive SHA-256 Verification**: Computes cryptographic digest continuously as bytes arrive; corrupt streams are aborted and quarantined before hitting disk.
- **Single-Flight Coalescing**: `SingleFlight` locks downloads by asset hash (`asset-{hash}`). If five parallel Blender frame workers require the same 2 GB scene asset simultaneously, only one worker transfers the asset while the other four await the single local commit.

---

## 9. Hardware Profiling and Adaptive Cluster Sizing

Located in `src/aidars/distributed/resources.py`, `placement.py`, and `sizing.py`.

### Hardware Telemetry (`WorkerResourceProfile`)
Workers capture and transmit live telemetry in periodic heartbeats:
- Logical CPU core count (`psutil.cpu_count(logical=True)`)
- Physical CPU core count (`psutil.cpu_count(logical=False)`)
- CPU clock frequency (`psutil.cpu_freq()`)
- Real-time CPU utilization percentage
- Total and available physical RAM (`psutil.virtual_memory()`)
- GPU device name, compute capability, driver version, total and free VRAM via NVML (`pynvml`)

### Continuous Compute Throughput Scoring
To allocate chunks proportionally across any $N$ heterogeneous machines without hardcoded profiles, `compute_worker_throughput_score` evaluates continuous power:

$$\text{render\_threads} = \begin{cases} \max(1, \text{cpus} - 2) & \text{if } \text{cpus} \ge 8 \\ \max(1, \text{cpus} - 1) & \text{otherwise} \end{cases}$$

$$\text{freq\_factor} = \text{clamp}\left(\frac{\text{freq\_mhz}}{3000.0}, 0.6, 1.8\right)$$

$$\text{ram\_factor} = \begin{cases} 0.75 & \text{if } \text{ram\_gb} < 3.0 \\ 1.0 + \min(0.25, \text{ram\_gb} \times 0.02) & \text{otherwise} \end{cases}$$

$$\text{cpu\_score} = \text{render\_threads} \times \text{freq\_factor} \times \text{ram\_factor}$$

$$\text{gpu\_bonus} = \begin{cases} 8.0 + (\text{vram\_gb} \times 1.5) & \text{if GPU available} \\ 0.0 & \text{otherwise} \end{cases}$$

$$\text{Throughput Score} = \max(1.0, \text{cpu\_score} + \text{gpu\_bonus})$$

### Placement Engine Scoring
Candidates passing all hard eligibility checks (active status, memory headroom, capability tags, retry exclusion) are ranked by multi-attribute utility:

$$S(w, \tau) = w_c \cdot C_w + w_m \cdot m_w + w_l \cdot L_w + w_g \cdot g_w - \text{penalties} - \text{risk}$$

- $C_w$: Normalized CPU availability.
- $m_w$: Available RAM headroom ratio, clamped to $[0.0, 2.0]$.
- $L_w$: Data locality fraction (percentage of required asset bytes already cached on node).
- $g_w$: VRAM headroom factor if GPU required.
- $\text{penalties}$: Accumulated failure penalty score from recent dispatch failures.
- $\text{risk}$: M7 behavioral risk assessment.

### Adaptive Cluster Sizing
`AdaptiveClusterSizer` prevents cluster over-allocation. For a 2-chunk render job submitted to a 10-node cluster, the sizer selects exactly 2 workers and leaves 8 workers free for other concurrent jobs. Once a job is sized, **job-scoped worker confinement** restricts chunk placement and failover to that chosen worker subset.

---

## 10. Job Orchestration and Parallel Chunk Execution

Located in `src/aidars/distributed/workload.py`, `workload_registry.py`, `job_registry.py`, and `attempt.py`.

### Workload State Machine
```text
  [SUBMITTED]
       │
       ▼ (PlacementEngine)
   [PLACED]
       │
       ▼ (Worker Dispatch & Attempt Created)
  [EXECUTING] ──────────────────────────┐
       │                                │ (Unrecoverable Error / Exceeded Retries)
       ├─────────────────┐              ▼
       │                 ▼           [FAILED]
       │       [CANCELLATION_REQUESTED]
       │                 │
       ▼ (Verified)      ▼
  [COMPLETED]       [CANCELLED]
```

### Execution Features
- **Decoupled Asynchronous Polling**: Workload submission initiates execution and immediately returns an attempt ID. Status is reconciled via non-blocking polling (`execution_poll_interval_seconds = 0.2`), isolating execution from HTTP connection lifetime.
- **Speculative Straggler Work-Stealing**: When an active worker finishes its assigned workload and has zero pending tasks while another single-machine chunk remains executing in the job, the coordinator dispatches a speculative clone attempt to the idle worker. Whichever worker completes and verifies its output first commits to CAS; the coordinator immediately sends an HTTP cancel request to terminate the slower rival attempt.
- **Output Verification Policy**: Successful exit codes alone do not mark workloads completed. `OutputVerificationPolicy` enforces that expected output file counts, patterns, and non-empty byte sizes match expectations before transitioning workloads to `COMPLETED`.
- **Frame Coverage Auditing**: For multi-frame animation jobs, `frame_coverage.py` verifies that every frame in the requested $[F_{\text{start}}, F_{\text{end}}]$ range is accounted for without gaps or duplicate frames.

---

## 11. Blender Rendering

Located in `src/aidars/adapters/blender/adapter.py`.

### Universal Hardware-Adaptive Prepass (`BLENDER_UNIVERSAL_PREPASS_CODE`)
Blender workloads are executed with headless python expressions that enforce the separation of local hardware adaptation from visual determinism:

#### Node-Local Hardware Adaptation
- **Render Threads**: Configured dynamically to `max(1, os.cpu_count() - 2)` (if cores $\ge 8$) or `max(1, os.cpu_count() - 1)`, preventing OS scheduler starvation.
- **RAM-Adaptive Cycles Tile Size**: Derived dynamically from the worker's own physical free memory:
  - `< 5 GB Free RAM` $\rightarrow$ Tile size `256`
  - `5–12 GB Free RAM` $\rightarrow$ Tile size `512`
  - `> 12 GB Free RAM` $\rightarrow$ Tile size `1024`
- **Memory Thrash Prevention**: `use_persistent_data` is explicitly set to `False` on memory-constrained workers to prevent bloating the Windows pagefile (`pagefile.sys`).

#### Locked Visual Determinism
- **Resolution**: Locked to 100% (`resolution_percentage = 100`) and `use_simplify = False`.
- **Color Management**: View transform, look, exposure, and gamma settings are locked identically across all nodes.
- **Denoising**: OpenImageDenoise configured with `denoising_prefilter = "ACCURATE"`.
- **Lighting & Sampling**: Light tree disabled (`use_light_tree = False`), World cycles sampling set to `NONE`, missing environment map texture nodes automatically muted.
- **Visibility Rays**: Full shadow, glossy, and diffuse ray visibility enforced on mesh objects.
- **Camera Fallback**: If scene camera binding is missing, the adapter automatically resolves the active camera or instantiates a fallback camera bound to the scene collection.

### Cooperative Monte Carlo Sample Buffer Fusion
When an animation has fewer total frames than available workers (e.g., rendering a single frame on 4 PCs), AIDAR switches to cooperative sample fusion (`fuse_sample_buffers`):
- Each worker renders the same frame at a reduced sample count.
- Monte Carlo seeds are set deterministically per node rank: $\text{seed} = 1001 + \text{rank} \times 997$.
- Output float32 image buffers are fused and averaged proportionally by throughput weights to yield the final high-sample image.

---

## 12. FFmpeg / Video Processing

Located in `src/aidars/adapters/video_rendering/adapter.py` (and re-exported via `src/aidars/adapters/ffmpeg/`).

### Capabilities
- **DaVinci Resolve Project Resolution**: `resolve_davinci_or_video_input` inspects `.drp` project files (ZIP archives containing `project.db`) and `.dra` directory archives, extracting the underlying media clips or image sequences.
- **Proportional Duration Splitting**: Splits long video jobs into chunks weighted proportionally by each worker's `compute_worker_throughput_score`.
- **Executable Auto-Discovery**: Resolves `ffmpeg` across `AIDAR_FFMPEG_EXECUTABLE`, system `PATH`, WinGet packages (`Gyan.FFmpeg`), and default paths (`C:\ffmpeg\bin\ffmpeg.exe`).
- **CAS Integration**: Input video clips are content-addressed and verified; transcoded video segments (`.mp4`, `.mov`, `.mkv`) are ingested and verified as durable CAS artifacts.

---

## 13. Ollama / Local LLM Execution

Located in `src/aidars/adapters/llm/adapter.py`.

### Capabilities
- **Local Ollama & llama-cli Integration**: Directs inference tasks to locally running Ollama daemons (`/api/generate` and `/api/chat`) or standalone `llama-cli` executables.
- **Prompt-Batch Parallel Task Splitting**: When multiple independent prompts are submitted across multiple workers, AIDAR distributes prompts across available nodes.
- **Dynamic Thread & Resource Scaling**: Worker execution runner queries local CPU counts, setting `num_thread` to `max(1, cpus - 1)`.
- **Configurable Context & Prediction Limits**: Honors `AIDAR_LLM_NUM_CTX` (default 1024) and `AIDAR_LLM_MAX_TOKENS` (default 128).
- **Fast Reasoning Mode**: For reasoning models (such as `deepseek-r1`), the adapter can bypass latency-inducing reasoning chains by seeding `<think>\n</think>\n` in `/api/chat` or stripping `<think>` tags from completed responses.

---

## 14. ML Training Execution

Located in `src/aidars/adapters/ml_training/adapter.py`.

### Capabilities
- **Script & Dataset Staging**: Python training scripts and datasets are content-addressed, uploaded, and staged into worker execution sandboxes via CAS.
- **Sandboxed Execution**: Runs training scripts through `GenericSubprocessRuntime` with memory and execution timeout boundaries.
- **Artifact Ingestion**: Captures trained model checkpoint artifacts (`model.pt`, `.bin`, `.safetensors`) into CAS upon successful exit.
- **Architectural Boundary**: Designed for independent training workloads and hyperparameter search tasks. Distributed data-parallel training (e.g. PyTorch DDP / NCCL all-reduce across nodes) is intentionally not implemented.

---

## 15. Failure Recovery and Worker Lifecycle

Located in `src/aidars/distributed/workload.py` and `coordinator.py`.

### Resilient Operations
- **Heartbeat Reaper**: Coordinator monitors worker heartbeats every 5 seconds. If no heartbeat arrives within 15 seconds, the worker is marked `OFFLINE`.
- **Protection for Busy Workers**: Workers running intensive renders at 100% CPU utilization are never marked `DRAINING` or aborted due to high resource usage while running active attempts.
- **Graceful Draining**: `drain_worker` safely reassigns `PLACED` workloads; actively executing workloads continue running unless the node becomes `OFFLINE`.
- **Automatic Retry with Worker Exclusion**: If an attempt fails due to worker loss or transport disconnect, `should_retry()` triggers a re-placement, explicitly adding the failed worker to `exhausted_worker_ids` so identical failing nodes are not repeatedly selected.
- **Process Tree Termination**: On cancellation or timeout, `_terminate_process_tree()` executes `taskkill /F /T /PID <pid>` on Windows and recursive process group SIGKILL on POSIX, guaranteeing child rendering processes do not remain orphaned in the background.
- **Windows Read-Only Sandbox Cleanup**: Staged input files have read-only bits cleared via an `onerror` handler (`stat.S_IWRITE | stat.S_IREAD`) before `shutil.rmtree` is called, eliminating Windows `WinError 5 Access is denied` deletion failures.

---

## 16. Installation

### Prerequisites
- **Operating System**: Windows 10/11, Linux (Ubuntu 20.04+), or macOS.
- **Python**: Version 3.10 to 3.14 (tested against Python 3.14 on Windows).
- **Optional External Tools**:
  - **Blender**: 3.6 LTS, 4.x (for 3D rendering).
  - **FFmpeg**: 5.x / 6.x / 7.x (for video processing).
  - **Ollama**: (for local LLM inference).
  - **PyTorch**: (for machine learning execution).

### Step-by-Step Installation
```bash
# 1. Clone repository
git clone https://github.com/Ajay-Pushkar/AIDARS.git
cd AIDAR

# 2. Create and activate virtual environment
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

# 3. Install core dependencies
pip install -r requirements.txt

# 4. (Optional) Install GPU and development dependencies
pip install -e ".[gpu,dev]"
```

---

## 17. Coordinator and Worker Setup

AIDAR processes can be started via setuptools console entrypoints, direct Python module invocations, or Docker Compose.

### A. Environment Configuration (`.env`)
Create a `.env` file or export environment variables:
```bash
# Authentication secrets (required when AIDAR_INSECURE_MODE=0)
AIDAR_ADMIN_TOKENS=admin-secret-token-123
AIDAR_WORKER_BOOTSTRAP_SECRET=worker-join-secret-xyz

# Set to 1 only for local automated tests without auth:
AIDAR_INSECURE_MODE=0
```

### B. Starting the Coordinator
```bash
# Using CLI entrypoint:
aidars-coordinator --host 0.0.0.0 --port 8000 --db-path ./data/coordinator_state.db

# Or using Python module:
python -m aidars.distributed.cli coordinator --port 8000
```

### C. Starting a Worker
```bash
# On the same or remote PC:
aidars-worker \
  --coordinator-url http://<COORDINATOR-IP>:8000 \
  --worker-id worker-node-1 \
  --port 8001 \
  --ip-address <WORKER-ROUTABLE-IP> \
  --cas-dir ./data/worker_cas \
  --bootstrap-secret worker-join-secret-xyz

# Or using Python module:
python -m aidars.distributed.cli worker --coordinator-url http://<COORDINATOR-IP>:8000 --port 8001
```

### D. Docker Compose (Multi-Worker Local Cluster)
```bash
cd deploy
cp .env.example .env
docker compose up --build
```
Spawns 1 Coordinator (`172.28.0.10`) and 3 Workers (`worker-a`, `worker-b`, `worker-c`) on an isolated bridge network.

---

## 18. Running Actual Workloads

Workloads can be orchestrated programmatically using `DistributedClient`:

### A. Submitting a Blender Render Job
```python
import asyncio
from aidars.distributed.client import DistributedClient
from aidars.adapters.blender.adapter import BlenderAdapter

async def main():
    client = DistributedClient(coordinator_url="http://localhost:8000")
    adapter = BlenderAdapter(asset_manager=client.asset_manager)

    request = {
        "input_path": "scenes/cube.blend",
        "frame_start": 1,
        "frame_end": 10,
        "worker_count": 2,
    }

    workloads = adapter.evaluate_request(request)
    job_id = workloads[0].job_id
    
    await client.submit_job(job_id=job_id, workloads=workloads)
    print(f"Submitted Blender Job: {job_id}")

    # Await completion
    result = await client.await_job_completion(job_id, timeout_seconds=300)
    print(f"Job Status: {result.state}")

asyncio.run(main())
```

### B. Submitting an FFmpeg Video Transcoding Job
```python
from aidars.adapters.video_rendering.adapter import FFmpegAdapter

adapter = FFmpegAdapter(asset_manager=client.asset_manager)
workloads = adapter.evaluate_request({
    "input_video_path": "media/source.mp4",
    "output_format": "mp4",
    "total_duration": 60.0,
    "worker_count": 2,
})
```

### C. Submitting an Ollama LLM Inference Job
```python
from aidars.adapters.llm.adapter import LLMAdapter

adapter = LLMAdapter(asset_manager=client.asset_manager)
workloads = adapter.evaluate_request({
    "model": "deepseek-r1:8b",
    "prompts": ["Explain quantum entanglement in simple terms.", "What is ray tracing?"],
    "worker_count": 2,
    "max_tokens": 128,
})
```

---

## 19. Testing and Validation

AIDAR enforces strict verification across unit, integration, performance, and physical multi-machine LAN environments.

### Test Execution
```bash
# Run all unit tests:
pytest tests/unit -v

# Run all integration tests:
pytest tests/integration -v

# Run end-to-end multi-worker tests (Docker / local subprocesses):
pytest tests/e2e -v

# Run performance microbenchmarks:
pytest tests/performance -v
```

### Validation Levels
1. **Unit & Adversarial Tests** (840 passed, 3 skipped): Validates edge cases, single-flight deadlocks, bit-flip corruption rejection, 100-way concurrent CAS locks, SQLite recovery, and placement math.
2. **Integration Tests** (25 passed): Validates cross-adapter contracts, physical asset hashing, Ollama markdown `<think>` tag parsing, and multi-chunk job identity.
3. **Performance Profiling (M15.5 Report)**: Proved CAS threadpool offloading reduced event loop blocking on 500 MB files from 1259 ms to 44 ms (96.4% reduction); coordinator placement latency measured at 2.27 ms p50 across 100 simulated workers.
4. **Physical Multi-PC LAN Validation (`multi_pc_validation_report.txt`)**:
   - Tested on physical Windows topology across PC-A (`192.168.29.234:8001`) and PC-B (`192.168.29.234:8002`).
   - Verified 9 sequential stages: Stage 0 (Connectivity), Stage 1 (Distributed Subprocess), Stage 2 (Cross-Worker CAS), Stage 3 (Real Blender), Stage 4 (Real FFmpeg), Stage 5 (Real Ollama deepseek-r1:8b), Stage 6 (Real PyTorch ML), Stage 7 (Mid-flight worker kill & recovery), Stage 8 (Cluster right-sizing & capability matrix routing).

---

## 20. Known Limitations

To maintain architectural integrity, the following boundaries reflect the true current codebase:

1. **Independent ML Workloads Only**: `MLTrainingAdapter` orchestrates independent training scripts and hyperparameter searches. Distributed data-parallel training (PyTorch DDP, Horovod, NCCL all-reduce across nodes) is **not implemented**.
2. **Task-Split LLM Inference Only**: `LLMAdapter` splits batches of prompts across worker instances. It does **not** perform tensor-parallel or pipeline-parallel model weight sharding (such as vLLM or Megatron-LM across multiple GPUs).
3. **Flat Network Connectivity Required**: Workers must be able to reach each other via their advertised IP addresses and ports. NAT hole punching, STUN/TURN, and relay proxies are not implemented; deployments require a local LAN, VPN mesh (e.g. Tailscale), or Docker network bridge.
4. **No Native TLS Termination**: Communication between coordinator and workers uses plain HTTP REST. TLS must be terminated externally via an edge proxy (such as Nginx, Caddy, or a cloud load balancer).
5. **Single-File SQLite State Storage**: `CoordinatorStateStore` runs on SQLite with file locks. While durable against coordinator process crashes and restarts, it is not a distributed Raft/Paxos consensus store.
6. **NVML-Dependent GPU Telemetry**: Real-time GPU and VRAM metrics require an NVIDIA GPU with `nvidia-ml-py`. AMD ROCm, Apple Silicon Metal, and Intel Arc GPUs fall back to CPU-only telemetry.
7. **Cross-Architecture Floating-Point Variations**: While AIDAR enforces strict visual determinism (locking resolution, sampling methods, seeds, and denoisers), minor sub-perceptual floating-point differences can naturally arise when path-tracing across heterogeneous CPU architectures (e.g., AVX2 vs AVX-512) or CPU vs GPU Cycles kernels.

---

## 21. Roadmap

- [ ] **Distributed PyTorch DDP Adapter**: Coordinated multi-node training using rendezvous endpoints.
- [ ] **Tensor-Parallel LLM Serving**: Integration with vLLM / llama.cpp RPC server backends for sharded large-model weights.
- [ ] **Automated Video Stitching**: Native concatenate/merge phase following multi-chunk FFmpeg video transcoding.
- [ ] **High-Availability Coordinator**: Pluggable PostgreSQL or etcd/Raft consensus backend for active-active coordinator clustering.
- [ ] **Automated WireGuard / Tailscale VPN Sidecar**: Automatic secure overlay networking for remote multi-region worker discovery.
