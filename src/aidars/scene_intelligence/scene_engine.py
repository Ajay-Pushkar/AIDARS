"""SceneEngine: high-level orchestration facade over the scene intelligence pipeline.

This module is the entry point for callers that want to run the whole
pipeline (load -> analyze -> graph -> integrity -> visibility -> package ->
export) or any individual stage in isolation, with unified error handling,
optional content-based caching, and formatted console/log outputs.
"""
from __future__ import annotations

import hashlib
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
from aidars.visibility import (
    RenderRequest,
    RenderRequirementAnalyzer,
    RenderRequirementReport,
    VisibilityAnalyzer,
    VisibilityReport,
)


@dataclass(slots=True)
class SceneEngineRequest:
    """Everything needed to run the pipeline for one scene source."""

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
    camera_id: str = ""

    def fingerprint(self) -> str:
        """Compute a deterministic hash of the pipeline request configuration."""
        cfg = {
            "scene_output": self.scene_output,
            "graph_output": self.graph_output,
            "package_output": self.package_output,
            "build_graph": self.build_graph,
            "build_package": self.build_package,
            "optimize_package_by_visibility": self.optimize_package_by_visibility,
            "frame_start": self.frame_start,
            "frame_end": self.frame_end,
            "camera_id": self.camera_id,
            "blender_executable": self.blender_executable,
        }
        canonical = json.dumps(cfg, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class SceneEngineResult:
    """Everything a caller might want after a pipeline run."""

    from_cache: bool = False
    snapshot: Optional[SceneSnapshot] = None
    graph: Optional[DependencyGraph] = None
    integrity: Optional[IntegrityReport] = None
    visibility: Optional[VisibilityReport] = None
    render_requirements: Optional[RenderRequirementReport] = None
    package: Optional[PackageManifest] = None
    scene_output_path: Optional[Path] = None
    graph_output_path: Optional[Path] = None
    package_output_path: Optional[Path] = None
    messages: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class SceneEngine:
    """High-level orchestration facade over the scene intelligence and visibility pipelines."""

    def __init__(self, blender_executable: Optional[str] = None) -> None:
        self._blender_executable = blender_executable
        self.intelligence_engine = SceneIntelligenceEngine()
        self.graph_builder = DependencyGraphBuilder()
        self.integrity_checker = IntegrityChecker()
        self.visibility_analyzer = VisibilityAnalyzer()
        self.render_requirement_analyzer = RenderRequirementAnalyzer()
        self.package_builder = SmartPackageBuilder()
        self.frame_scheduler = FrameScheduler(visibility_analyzer=self.visibility_analyzer)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def run(self, request: SceneEngineRequest) -> SceneEngineResult:
        """Run the full pipeline for a single scene source with request-aware caching."""
        result = SceneEngineResult()

        cache = SceneCache(request.cache_dir) if request.cache_dir else None
        source_hash: Optional[str] = None
        req_hash = request.fingerprint()

        if cache is not None:
            source_hash = hash_source(request.input_path)
            cached_entry = cache.get(request.input_path, request_hash=req_hash, verify_artifacts=True)
            if cached_entry is not None and cached_entry.source_hash == source_hash and cached_entry.request_hash == req_hash:
                result.from_cache = True
                result.scene_output_path = Path(cached_entry.scene_output)
                result.graph_output_path = Path(cached_entry.graph_output) if cached_entry.graph_output else None
                result.package_output_path = Path(cached_entry.package_output) if cached_entry.package_output else None
                result.messages.append(
                    f"No changes detected for {request.input_path} (request configuration match); reusing cached outputs."
                )
                result.messages.append(f"Scene snapshot: {result.scene_output_path}")
                if result.graph_output_path:
                    result.messages.append(f"Dependency graph: {result.graph_output_path}")
                if result.package_output_path:
                    result.messages.append(f"Package manifest: {result.package_output_path}")
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
                render_req = RenderRequest(
                    camera_id=request.camera_id,
                    frame_start=request.frame_start,
                    frame_end=request.frame_end,
                )
                req_report = self.analyze_render_requirements(
                    snapshot=result.snapshot,
                    graph=graph_for_packaging,
                    request=render_req,
                )
                result.render_requirements = req_report
                result.visibility = self.analyze_visibility(result.snapshot, request.frame_start, request.frame_end)

                # Prune to objects identified as required by the full M3 render requirement analysis
                required_object_ids = set(req_report.required_objects)
                result.messages.append(
                    f"Render requirement analysis: {len(required_object_ids)} object(s) required "
                    f"in frames {request.frame_start}-{request.frame_end}"
                )
                result.package = self.build_optimized_package(
                    payload,
                    request.frame_start,
                    request.frame_end,
                    graph_for_packaging,
                    required_object_ids,
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
                    request_hash=req_hash,
                    scene_output=str(result.scene_output_path),
                    graph_output=str(result.graph_output_path) if result.graph_output_path else None,
                    package_output=str(result.package_output_path) if result.package_output_path else None,
                    build_graph=request.build_graph,
                    build_package=request.build_package,
                    optimize_package_by_visibility=request.optimize_package_by_visibility,
                    frame_start=request.frame_start,
                    frame_end=request.frame_end,
                    camera_id=request.camera_id,
                ),
            )

        return result

    # ------------------------------------------------------------------ #
    # Individual pipeline stages
    # ------------------------------------------------------------------ #

    def load_source(
        self,
        input_path: str | Path,
        *,
        blender_executable: Optional[str] = None,
    ) -> dict[str, Any] | SceneData:
        """Load a scene source from a JSON payload file or a .blend file."""
        path = Path(input_path)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")

        if path.suffix.lower() == ".blend":
            executable = blender_executable if blender_executable is not None else self._blender_executable
            return BlenderAdapter(blender_executable=executable).load_scene(path)

        with path.open("r", encoding="utf-8-sig") as handle:
            return json.load(handle)

    def analyze(self, source: dict[str, Any] | SceneData) -> SceneSnapshot:
        """Normalize a raw scene source into a SceneSnapshot."""
        return self.intelligence_engine.analyze_scene_data(source)

    def build_dependency_graph(self, snapshot: SceneSnapshot) -> DependencyGraph:
        """Build a dependency graph from an already-analyzed snapshot."""
        return self.graph_builder.build(snapshot)

    def check_integrity(self, graph: DependencyGraph) -> IntegrityReport:
        """Run integrity checks against a graph."""
        return self.integrity_checker.check(graph)

    def analyze_visibility(self, snapshot: SceneSnapshot, frame_start: int, frame_end: int) -> VisibilityReport:
        """Determine which objects are eligible (visible) in [frame_start, frame_end]."""
        return self.visibility_analyzer.analyze(snapshot, frame_start, frame_end)

    def analyze_render_requirements(
        self,
        snapshot: Any,
        graph: Any = None,
        request: Optional[RenderRequest | dict[str, Any]] = None,
        active_camera: Any = None,
        render_settings: Any = None,
    ) -> RenderRequirementReport:
        """Run full Milestone 3 Render Requirement Analysis across geometry, lighting, and dependencies."""
        return self.render_requirement_analyzer.analyze(
            snapshot=snapshot,
            dependency_graph=graph,
            request=request,
            active_camera=active_camera,
            render_settings=render_settings,
        )

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
        """Build a packaging manifest pruned to assets reachable from required objects."""
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
        """Partition a frame range across workers with estimated asset-cost per chunk."""
        assets = self._extract_raw_assets(source)
        return self.frame_scheduler.schedule(snapshot, graph, assets, frame_start, frame_end, worker_count)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _extract_raw_assets(source: dict[str, Any] | SceneData) -> List[PackageAsset]:
        """Pull raw asset records from source."""
        raw_list = source.raw.get("assets", []) if isinstance(source, SceneData) else source.get("assets", [])
        assets: List[PackageAsset] = []
        for raw in raw_list:
            if isinstance(raw, dict) and "path" in raw:
                assets.append(
                    PackageAsset(
                        path=raw["path"],
                        kind=raw.get("kind", "unknown"),
                        size_bytes=int(raw.get("size_bytes", 0)),
                    )
                )
        return assets

    @staticmethod
    def _format_integrity_warnings(integrity: IntegrityReport) -> List[str]:
        warnings: List[str] = []
        if integrity.missing_targets:
            warnings.append(
                f"{len(integrity.missing_targets)} referenced asset(s) could not be resolved: "
                + ", ".join(sorted(integrity.missing_targets))
            )
        if integrity.unused_nodes:
            unused_labels = [n.label for n in integrity.unused_nodes]
            warnings.append(
                f"{len(integrity.unused_nodes)} asset(s) appear unused in the scene: "
                + ", ".join(sorted(unused_labels))
            )
        return warnings
