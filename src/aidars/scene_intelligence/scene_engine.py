"""Scene Engine — the single orchestration entry point for the whole pipeline.

    SceneData
        |
        v
    Scene Engine
        |
        +-- DependencyGraphBuilder
        +-- Integrity Checker
        +-- VisibilityAnalyzer
        +-- Smart Packaging (+ Asset Optimizer)
        +-- Exporters

Every interface that needs "analyze a scene and produce reports" - the CLI
today, and a future API or GUI - should call SceneEngine.run() rather than
wiring SceneIntelligenceEngine / DependencyGraphBuilder / SmartPackageBuilder
/ SceneCache together itself. That wiring is business logic, and business
logic belongs here, not in any one interface.

The CLI's job is reduced to: parse arguments, build a SceneEngineRequest,
call SceneEngine.run(), print the result, exit.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional, Set

from .blender_adapter import BlenderAdapter
from .cache import SceneCache, SceneCacheEntry, hash_source
from .dependency_graph import DependencyGraph, DependencyGraphBuilder
from .engine import SceneIntelligenceEngine
from .exporters import DependencyGraphExporter, JsonSceneExporter
from .integrity import IntegrityChecker, IntegrityReport
from .models import SceneData, SceneSnapshot
from aidars.scheduler.frame_scheduler import FrameScheduler, SchedulingPlan
from aidars.smart_package.builder import PackageAsset, PackageManifest, SmartPackageBuilder
from aidars.visibility.engine import VisibilityAnalyzer, VisibilityReport


@dataclass(slots=True)
class SceneEngineRequest:
    """Everything needed to run the pipeline for one scene source.

    This is the boundary between "how the caller wants the pipeline
    configured" (a plain data object, easy to build from argparse, an HTTP
    request body, or a GUI form) and the orchestration logic itself.

    ``optimize_package_by_visibility`` (default False, only relevant when
    ``build_package`` is True): runs VisibilityAnalyzer for
    [frame_start, frame_end] and prunes packaged assets to only those
    reachable from objects visible in that range, via the dependency graph
    (see ``aidars.smart_package.optimizer.AssetOptimizer``). Requires
    building the dependency graph internally even if ``build_graph`` is
    False (the graph object still won't be written to disk unless
    ``build_graph`` is also True).
    """

    input_path: str
    scene_output: str = "output/scene.json"
    graph_output: str = "output/dependency_graph.json"
    build_graph: bool = True
    build_package: bool = False
    optimize_package_by_visibility: bool = False
    frame_start: int = 1
    frame_end: int = 24
    package_output: str = "output/package.json"
    cache_dir: Optional[str] = None
    blender_executable: Optional[str] = None


@dataclass(slots=True)
class SceneEngineResult:
    """Everything a caller might want after a pipeline run.

    ``messages``/``warnings`` are plain strings so the CLI (or any other
    caller) can display them without needing to know the pipeline's
    internals - the CLI should not be re-deriving "what should I tell the
    user" from raw snapshot/graph objects.

    When ``from_cache`` is True, nothing was re-analyzed: ``snapshot``,
    ``graph``, ``integrity``, and ``package`` are all left as None. Only the
    output paths (carried over from the previous run) are populated. A
    caller that needs the actual snapshot/graph objects even on a cache hit
    should not pass ``cache_dir`` in the request.
    """

    from_cache: bool = False
    snapshot: Optional[SceneSnapshot] = None
    graph: Optional[DependencyGraph] = None
    integrity: Optional[IntegrityReport] = None
    visibility: Optional[VisibilityReport] = None
    package: Optional[PackageManifest] = None
    scene_output_path: Optional[Path] = None
    graph_output_path: Optional[Path] = None
    package_output_path: Optional[Path] = None
    messages: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class SceneEngine:
    """High-level orchestration facade over the scene intelligence pipeline."""

    def __init__(self, blender_executable: Optional[str] = None) -> None:
        self._blender_executable = blender_executable
        self.intelligence_engine = SceneIntelligenceEngine()
        self.graph_builder = DependencyGraphBuilder()
        self.integrity_checker = IntegrityChecker()
        self.visibility_analyzer = VisibilityAnalyzer()
        self.package_builder = SmartPackageBuilder()
        self.frame_scheduler = FrameScheduler(visibility_analyzer=self.visibility_analyzer)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def run(self, request: SceneEngineRequest) -> SceneEngineResult:
        """Run the full pipeline for a single scene source.

        Order: (optional) cache check -> load -> analyze -> (optional)
        dependency graph + integrity check -> (optional) smart packaging ->
        export -> (optional) cache write.
        """
        result = SceneEngineResult()

        cache = SceneCache(request.cache_dir) if request.cache_dir else None
        source_hash: Optional[str] = None
        if cache is not None:
            # Hashing the raw input before doing any other work is what
            # makes this "incremental scanning" rather than just "don't
            # rewrite the output file": an unchanged .blend file never even
            # reaches BlenderAdapter, so Blender is never launched.
            source_hash = hash_source(request.input_path)
            cached_entry = cache.get(request.input_path)
            if cached_entry is not None and cached_entry.source_hash == source_hash:
                result.from_cache = True
                result.scene_output_path = Path(cached_entry.scene_output)
                result.graph_output_path = Path(cached_entry.graph_output) if cached_entry.graph_output else None
                result.messages.append(
                    f"No changes detected for {request.input_path}; reusing cached outputs."
                )
                result.messages.append(f"Scene snapshot: {result.scene_output_path}")
                if result.graph_output_path:
                    result.messages.append(f"Dependency graph: {result.graph_output_path}")
                return result

        payload = self.load_source(request.input_path, blender_executable=request.blender_executable)
        result.snapshot = self.analyze(payload)
        result.scene_output_path = JsonSceneExporter.write_json(result.snapshot, request.scene_output)
        result.messages.append(f"Scene snapshot written to {result.scene_output_path}")

        if request.build_graph:
            result.graph = self.build_dependency_graph(result.snapshot)
            result.integrity = self.check_integrity(result.graph)
            result.graph_output_path = DependencyGraphExporter.write_json(result.graph, request.graph_output)
            result.messages.append(f"Dependency graph written to {result.graph_output_path}")
            result.warnings.extend(self._format_integrity_warnings(result.integrity))

        if request.build_package:
            if request.optimize_package_by_visibility:
                graph_for_packaging = result.graph or self.build_dependency_graph(result.snapshot)
                result.visibility = self.analyze_visibility(result.snapshot, request.frame_start, request.frame_end)
                result.messages.append(
                    f"Visibility analysis: {len(result.visibility.visible_object_ids)} object(s) visible "
                    f"in frames {request.frame_start}-{request.frame_end}"
                )
                result.package = self.build_optimized_package(
                    payload,
                    request.frame_start,
                    request.frame_end,
                    graph_for_packaging,
                    result.visibility.visible_object_ids,
                )
            else:
                result.package = self.build_package(payload, request.frame_start, request.frame_end)
            result.package_output_path = self.package_builder.write_manifest(result.package, request.package_output)
            result.messages.append(f"Package manifest written to {result.package_output_path}")

        if cache is not None and source_hash is not None:
            cache.put(
                request.input_path,
                SceneCacheEntry(
                    source_hash=source_hash,
                    scene_output=str(result.scene_output_path),
                    graph_output=str(result.graph_output_path) if result.graph_output_path else None,
                ),
            )

        return result

    # ------------------------------------------------------------------ #
    # Individual pipeline stages (usable standalone, e.g. from tests
    # or a future API that only needs one stage)
    # ------------------------------------------------------------------ #

    def load_source(
        self,
        input_path: str | Path,
        *,
        blender_executable: Optional[str] = None,
    ) -> dict[str, Any] | SceneData:
        """Load a scene source from a JSON payload file or a .blend file.

        Returns a plain dict for JSON input, or a SceneData for .blend
        input (already normalized by BlenderAdapter).

        Args:
            input_path: Path to a JSON scene payload or a .blend file.
            blender_executable: Overrides the executable this SceneEngine
                instance was constructed with, for this call only. Lets one
                long-lived SceneEngine serve requests that each specify
                their own Blender executable (e.g. a future API/GUI).
        """
        path = Path(input_path)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")

        if path.suffix.lower() == ".blend":
            executable = blender_executable if blender_executable is not None else self._blender_executable
            adapter = BlenderAdapter(blender_executable=executable)
            return adapter.load_scene(path)

        with path.open("r", encoding="utf-8-sig") as handle:
            data = json.load(handle)

        if not isinstance(data, dict):
            raise TypeError("Scene payload must be a JSON object")

        return data

    def analyze(self, source: dict[str, Any] | SceneData) -> SceneSnapshot:
        """Normalize a raw scene source into a SceneSnapshot."""
        return self.intelligence_engine.analyze_scene_data(source)

    def build_dependency_graph(self, snapshot: SceneSnapshot) -> DependencyGraph:
        """Build a dependency graph from an already-analyzed snapshot."""
        return self.graph_builder.build(snapshot)

    def check_integrity(self, graph: DependencyGraph) -> IntegrityReport:
        """Run integrity checks (missing/unused assets) against a graph."""
        return self.integrity_checker.check(graph)

    def analyze_visibility(self, snapshot: SceneSnapshot, frame_start: int, frame_end: int) -> VisibilityReport:
        """Determine which objects are visible somewhere in [frame_start, frame_end]."""
        return self.visibility_analyzer.analyze(snapshot, frame_start, frame_end)

    def build_package(
        self,
        source: dict[str, Any] | SceneData,
        frame_start: int,
        frame_end: int,
    ) -> PackageManifest:
        """Build a smart packaging manifest for the given frame range."""
        assets = self._extract_raw_assets(source)
        return self.package_builder.build_package(frame_start, frame_end, assets)

    def build_optimized_package(
        self,
        source: dict[str, Any] | SceneData,
        frame_start: int,
        frame_end: int,
        graph: DependencyGraph,
        visible_object_ids: Set[str],
    ) -> PackageManifest:
        """Build a packaging manifest pruned to assets reachable from visible objects."""
        assets = self._extract_raw_assets(source)
        return self.package_builder.build_optimized_package(frame_start, frame_end, assets, graph, visible_object_ids)

    def build_scheduling_plan(
        self,
        source: dict[str, Any] | SceneData,
        snapshot: SceneSnapshot,
        graph: DependencyGraph,
        frame_start: int,
        frame_end: int,
        worker_count: int,
    ) -> SchedulingPlan:
        """Partition a frame range across workers, with a real asset-cost estimate per chunk.

        Not part of ``run()``'s default pipeline: there's no Queue/Worker
        Runtime yet to consume a scheduling plan, so this is available to
        call directly (from tests, or a future orchestrator) rather than
        forced into every CLI invocation.
        """
        assets = self._extract_raw_assets(source)
        return self.frame_scheduler.schedule(snapshot, graph, assets, frame_start, frame_end, worker_count)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _extract_raw_assets(source: dict[str, Any] | SceneData) -> list[PackageAsset]:
        """Pull a raw "assets" list out of a source for smart packaging, if present."""
        raw = source.raw if isinstance(source, SceneData) else source
        if not isinstance(raw, dict) or not isinstance(raw.get("assets"), list):
            return []
        return [
            PackageAsset(
                path=asset.get("path", ""),
                kind=asset.get("kind", "unknown"),
                size_bytes=int(asset.get("size_bytes", 0)),
            )
            for asset in raw.get("assets", [])
            if isinstance(asset, dict)
        ]

    @staticmethod
    def _format_integrity_warnings(integrity: IntegrityReport) -> list[str]:
        warnings: list[str] = []
        if integrity.missing_targets:
            preview = ", ".join(integrity.missing_targets[:5])
            suffix = ", ..." if len(integrity.missing_targets) > 5 else ""
            warnings.append(
                f"Warning: {len(integrity.missing_targets)} referenced asset(s) "
                f"could not be resolved: {preview}{suffix}"
            )
        if integrity.unused_nodes:
            preview = ", ".join(node.label for node in integrity.unused_nodes[:5])
            suffix = ", ..." if len(integrity.unused_nodes) > 5 else ""
            warnings.append(
                f"Warning: {len(integrity.unused_nodes)} asset(s) appear unused "
                f"(no incoming reference): {preview}{suffix}"
            )
        return warnings
