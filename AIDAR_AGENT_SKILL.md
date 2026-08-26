---
name: aidar-project
description: >
  Load this skill for ANY work on the AIDAR project (intelligent distributed
  Blender rendering platform). Applies to scene analysis, dependency graphs,
  render requirement analysis, packaging, caching, worker registry,
  scheduling, worker runtime, asset transfer, result collection, fault
  tolerance, or any file under the AIDAR codebase. This is the single source
  of truth for architecture, milestone order, tech stack, and component
  boundaries — do not re-derive or re-invent these decisions.
---

# AIDAR — Agent Operating Rules

## 0. One-sentence definition (do not drift from this)

AIDAR is an intelligent distributed rendering platform that **understands**
a Blender scene, **minimizes** the assets/computation a render actually
needs, **caches** reusable results, **schedules** work across heterogeneous
machines, **executes** it, and **verifies** the output.

AIDAR is NOT "a script that copies a .blend to N machines and runs Blender."
If a proposed change reduces to that, reject it and go back to this file.

The pipeline, always in this order:
```
understand → minimize → cache → schedule → execute → verify
```

## 1. Three layers — never blur them

| Layer | Lives where | Components | Answers |
|---|---|---|---|
| Intelligence | Master | Scene Engine, Dependency Graph, Render Requirement Analysis, Smart Packaging | "What does this render actually need?" |
| Orchestration | Master | Cache Engine, Worker Registry, Scheduler, Job Manager, Dispatcher | "Where should work happen, what can we reuse?" |
| Execution | Every worker | Job Receiver, Cache Manager, Asset Downloader, Blender Launcher, Render Monitor, Result Uploader, Health Reporter | "How do I execute this job?" |

Rule: a component in one layer never reaches into another layer's internals.
Always go through a public method, never an internal attribute.

```python
# ❌ Bad
scheduler.visibility_engine.internal_objects

# ✅ Good
scheduler.get_render_requirements()
```

## 2. Milestone order (M1 → M13) — build in this sequence, no skipping

1. **M1 Scene Analysis** — `bpy` inside `blender --background --python` →
   `SceneSnapshot` / `scene.json`.
2. **M2 Dependency Graph** — `GraphNode`/`GraphEdge`/`DependencyGraph`
   independent of JSON → `dependency_graph.json`.
3. **M3 Render Requirement Analysis** — the hardest and most important
   milestone. Question: *what can affect the final rendered result for the
   requested frames?* Not just camera visibility — also shadows, hidden
   lights, drivers, modifiers, geometry nodes, simulations, linked
   libraries, world/reflection/refraction. Output: `RenderRequirementReport`
   with `required_objects/meshes/materials/textures/images/lights/cameras/
   libraries/simulation_cache`. **When uncertain whether an asset matters,
   keep it.** A larger package beats a wrong render.
4. **M4 Smart Packaging** — takes the M3 report and builds the *smallest
   correct* transferable package (`manifest.json` + asset dirs), resolved
   by asset → path → hash → exists? → package. Never blind-copy everything.
5. **M5 Distributed Asset Layer** — High-performance distributed content-addressed storage
   mesh (`LocalCASAdapter`), inverted hash index (`CoordinatorService` & `WorkerRegistry`),
   4-tier network locality prioritizer (`LOOPBACK` > `SUBNET` > `LAN` > `WAN`), memory-bounded
   chunked streaming (`/api/v1/assets/{hash}/stream`), progressive SHA-256 integrity verification,
   candidate failover, and self-healing node recovery.
6. **M6 Adaptive Computational Resource System** — Workload specification (`WorkloadSpec`),
   live hardware resource profiling (CPU, RAM, GPU, VRAM), multi-attribute placement decision
   engine ($S(w, \tau)$), SingleFlight request deduplication, and sandboxed task execution.
7. **M7 Distributed Render Scheduler** — Multi-node frame batching, camera-aware load balancing,
   and headless Blender render dispatch across physical clusters.
8. **M8 Result Collector & Verification** — Multi-pass frame verification, cryptographic hash
   matching, and durable artifact ingestion into CAS.
9. **M9 Cluster Hardening & Production Operations** — Auth, TLS encryption, worker sandboxing,
   and observability dashboards.
10. **M10 Result Collector** — verify each frame exists, hash matches,
    resolution/format/job_id correct; flag missing frames as `FAILED`.
11. **M11 Fault Tolerance** — heartbeat timeout → detect partial completion
    (e.g. 701–721 done of 701–800) → reassign only the remaining range
    (722–800), never the whole chunk.
12. **M12 Optimization** — only after M1–M11 work end-to-end. Track cache
    hit rate, analysis reuse, package size reduction, network savings,
    scheduling efficiency, worker idle time.
13. **M13 Production Hardening** — auth, encryption, permissions, worker
    isolation, crash recovery, logging, metrics, version/Blender
    compatibility. A worker must never execute arbitrary commands received
    over the network.

Do not implement an M-number's concerns inside an earlier milestone's code
(e.g. don't sneak scheduling logic into M3). If a task seems to need that,
flag the boundary violation instead of quietly merging layers.

## 3. Component contracts (who talks to whom)

```
.blend → Scene Engine → SceneSnapshot
SceneSnapshot → DependencyGraph
SceneSnapshot + DependencyGraph + RenderRequest → RenderRequirementReport
RenderRequirementReport + DependencyGraph + Cache → PackageManifest
Cache: asset/package/scene hashes + worker cache inventory
Scheduler: RenderJob + PackageManifest + WorkerRegistry + WorkerCacheState + HistoricalPerf
Dispatcher: Scheduler → Worker
Worker: Master ↕ Cache ↕ Blender
ResultCollector: Workers → Frames → Validation → FinalOutput
```

Scene Engine and Dependency Graph never talk directly to Worker or
Scheduler. Keep this one-directional contract intact when adding features.

## 4. Tech stack (frozen — don't introduce alternatives without a reason)

- **Core language:** Python everywhere (Blender's `bpy` is Python; keep one
  language for the whole core).
- **Blender integration:** `bpy`, invoked via
  `blender --background scene.blend --python inspect_scene.py`.
- **Master API:** FastAPI (`POST /projects`, `POST /jobs`,
  `GET /workers`, `GET /jobs/{id}`, `POST /workers/register`).
- **Worker communication:** gRPC for job commands, progress streaming,
  status/control. FastAPI stays the human/API-facing layer; gRPC is
  internal Master↔Worker.
- **Asset transfer:** HTTP or gRPC streaming initially; content-addressed
  peer-to-peer is a later optimization, not a v1 requirement.
- **Database:** SQLite for metadata only (jobs, workers, asset metadata,
  hashes, cache metadata, render history) — never store large assets in the
  DB. Move to PostgreSQL only when genuinely multi-user.
- **Storage:** local filesystem for assets/cache/packages/frames/logs.
  S3-compatible object storage is a later option, not now.
- **Data formats:** JSON for scene analysis, dependency graph, manifests,
  reports, config, API payloads. Protobuf for gRPC. EXR/PNG for frames.
- **Frontend:** none is the priority right now. Order is CLI → API →
  dashboard (React + TypeScript, later). Don't build UI before M7 works.

### Explicitly rejected — don't suggest these
- **ROS 2** — solves robotics comms, not render orchestration. Not the
  backbone.
- **Microservices / many Docker containers** — this is a modular monolith
  for now (Scene Engine, Cache Engine, Scheduler, Packaging, Worker
  Registry as internal modules, one deployed Master app). Split into
  services only when scaling actually forces it.
- **Peer-to-peer caching as a first implementation** — Master→Worker must
  be solid first.

## 5. The four genuinely hard problems — treat with extra care

1. **Render requirement analysis (M3)** — Blender has drivers, constraints,
   geometry nodes, modifiers, particles, hair, volumes, sims, linked
   libraries, compositing, world lighting, reflections, hidden deps. Don't
   expect one milestone to solve it fully. Default to conservative
   (over-include) analysis.
2. **Distributed caching (M5)** — hashing, dedup, eviction, integrity,
   versioning, concurrency, disk limits, corruption recovery are real
   systems problems, not an afterthought.
3. **Scheduling (M7)** — equal split is easy; cost-aware + cache-aware +
   reliability-aware assignment is the actual goal.
4. **Failure handling (M11)** — assume any worker can sleep, lose network,
   run out of disk, crash Blender, or disconnect mid-upload, at any time.

## 6. Scaling discipline

Don't design or test against 50 machines before 2 machines work end to end.
Order: 2 machines (Master + 1 worker) proven → 5 → 10 → 50. Never skip a
step to chase the "impressive" version of the demo.

## 7. When asked to add a feature

Before writing code, check:
1. Which milestone (M1–M13) does this belong to?
2. Which of the 3 layers owns it (Intelligence / Orchestration / Execution)?
3. Does it violate the golden rule (no reaching into another component's
   internals)?
4. Does it introduce a rejected technology (ROS 2, premature microservices,
   premature P2P)?
5. Is it consistent with "when uncertain, keep the asset" for
   correctness-affecting logic?

If any answer is unclear, say so explicitly rather than guessing — this
project has already suffered from architecture drifting mid-implementation,
and the whole point of this file is to stop that.
