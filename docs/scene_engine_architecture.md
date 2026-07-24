# Scene Engine Architecture (Orchestration Layer)

## 1. The refactor: business logic out of the CLI

**Before:** the CLI directly imported and wired together
`SceneIntelligenceEngine`, `DependencyGraphBuilder`, `SmartPackageBuilder`,
`SceneCache`, and both exporters. All orchestration logic - including the
cache-hit short-circuit and pulling `assets` out of the raw payload for
packaging - lived in `cli.py`'s `main()`.

**After:**

```text
SceneData
    |
    v
Scene Engine
    |
    +-- DependencyGraphBuilder
    +-- Integrity Checker
    +-- Smart Packaging
    +-- Exporters
```

`src/aidars/scene_intelligence/scene_engine.py` now owns all of that as
`SceneEngine`, a facade with one primary entry point:

```python
request = SceneEngineRequest(input_path="scene.blend", build_package=True)
result = SceneEngine().run(request)
```

`SceneEngineRequest` / `SceneEngineResult` are the boundary: plain dataclasses
that any caller (CLI, tests, a future HTTP API, a future GUI) can build and
read without needing to know how the pipeline is wired internally.

`cli.py` is now genuinely thin (three responsibilities, per the original
proposal):

1. Parse arguments -> `argparse.Namespace`.
2. Translate that into a `SceneEngineRequest` and call `SceneEngine.run()`.
3. Print `result.messages` / `result.warnings` and exit with the right code.

It contains no analysis, graph-building, packaging, or caching logic at all.
`Integrity Checker` was also split out of `DependencyGraphBuilder` into its
own class (`integrity.py`) during this refactor - construction and analysis
of the graph are different responsibilities, and this gives future checks
(cyclic parenting, duplicate names, etc.) a home that isn't the graph
builder itself.

Individual stages are also independently callable on `SceneEngine`
(`load_source`, `analyze`, `build_dependency_graph`, `check_integrity`,
`build_package`) for callers that only need one piece - e.g. a future API
endpoint that returns just the dependency graph for a scene someone's
already uploaded, without re-writing scene.json to disk.

## 2. Full pipeline vision

```text
.blend
      |
      v
Scene Scanner
      |
      v
SceneData
      |
      v
Dependency Graph
      |
      v
Visibility Analysis
      |
      v
Smart Packaging
      |
      v
Frame Scheduler
      |
      v
Queue
      |
      v
Workers
      |
      v
Rendered Frames
      |
      v
Frame Merger
```

A note on naming: the original roadmap spreads this across several
numbered phases (caching/incremental scanning as "Phase 2", packaging as
"Phase 3", visibility as "Phase 4", distributed rendering as "Phase 5").
This document describes the target end-to-end pipeline those phases build
towards; which numbered phase ships which stage is a scheduling detail, not
an architectural one; and this doc uses "Phase 2 pipeline" the way it was
proposed - as the next concrete pipeline extension, whatever it ends up
being numbered.

Where each stage stands today:

| Stage | Status |
|---|---|
| Scene Scanner -> SceneData | Done (Phase 1) |
| Dependency Graph | Done (Phase 1), now includes integrity checking |
| Visibility Analysis | Done: `VisibilityAnalyzer` (`src/aidars/visibility/engine.py`) determines which objects are visible somewhere within a frame range, including objects with an animated (keyframed) `hide_render` curve. It's scene-graph analysis, not render/occlusion analysis - see the class docstring for the exact scope |
| Asset Optimizer | Done: `AssetOptimizer` (`src/aidars/smart_package/optimizer.py`) walks the dependency graph outward from visible objects (BFS) to find reachable externally-referenced assets, so unreachable ones can be pruned. Currently only prunes whole "asset" nodes (linked libraries) - materials/textures aren't individually prunable yet, since `PackageAsset` has no per-material granularity |
| Smart Packaging | `SmartPackageBuilder.build_optimized_package()` composes Visibility Analysis + the Asset Optimizer on top of the original frame-range packaging (`build_package()`, unchanged and still available on its own) |
| Frame Scheduler | Done: `FrameScheduler` (`src/aidars/scheduler/frame_scheduler.py`) partitions a frame range across workers and reports each chunk's real estimated asset cost (via Visibility Analysis + the Asset Optimizer), not just its frame count. It does not yet rebalance chunk boundaries by cost - see the module docstring for why that's deliberately deferred |
| Queue / Worker Runtime / Rendered Frames / Frame Merger | Not started - no real worker pool exists yet to schedule or execute against |

All of the above are wired into `SceneEngine` as independently-callable stages (`analyze_visibility`, `build_optimized_package`, `build_scheduling_plan`); `optimize_package_by_visibility` on `SceneEngineRequest` opts a full `run()` into visibility-aware packaging. Frame scheduling isn't part of `run()`'s default pipeline yet, since there's nothing downstream (Queue/Worker Runtime) to consume a plan.

### Why Visibility Analysis before Smart Packaging is the right call

Today, `SmartPackageBuilder` decides what to include using each asset's own
declared `frame_start`/`frame_end` - it has no way to know an asset is
*never actually rendered* because it's on a hidden layer, behind the
camera, or occluded for the whole shot. Running visibility analysis first
and handing packaging a *visible-asset list per worker's frame range*
(instead of "every asset whose declared range overlaps this worker's
range") directly reduces package size and transfer time - which is the
stated goal of Smart Packaging in the first place. This also composes
cleanly with what's already built: `DependencyGraph` already knows exactly
which materials/textures/images each object pulls in, so "visible objects
for these frames" -> "walk the graph outward from those objects" -> "that's
the asset set to package" is a natural extension of code that already
exists, not a new subsystem bolted on sideways.

Concretely, this is now built: `VisibilityAnalyzer.analyze()` takes a frame
range and produces a set of visible object ids; `AssetOptimizer` walks the
graph outward from those ids and `SmartPackageBuilder.build_optimized_package()`
prunes to only the assets it reaches (`build_package()` is untouched and
still available for the non-visibility-aware case). The one part of this
that's still a projection rather than reality: pruning currently only
covers whole externally-referenced asset files, not individual
materials/textures within a `.blend` - see the Asset Optimizer scope note
in the status table above.

## 3. Where this leaves SceneEngine

`SceneEngine` is the natural home for wiring Visibility Analysis and Frame
Scheduling in once they're real: `run()` already has the right shape
(sequential optional stages, each producing something the next stage can
consume), so adding `visibility` and `scheduling` stages is additive -
new optional steps in `SceneEngineRequest`/`SceneEngineResult`, not a
redesign of the orchestration layer itself.
