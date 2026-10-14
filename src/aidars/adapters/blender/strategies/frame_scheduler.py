"""Frame Scheduler.

    Dependency Graph
            +
    Frame Range
            +
    Asset Cost
            v
    Scheduling Decision

Splits a frame range into per-worker chunks and estimates each chunk's real
workload cost (total bytes of assets a worker would actually need to fetch,
via Visibility Analysis + the Asset Optimizer) rather than assuming every
frame costs the same to render. A chunk covering frames where a
multi-gigabyte set piece is on screen is a heavier chunk than one where it's
hidden, even if both chunks have the same frame count - this is the signal
naive round-robin/frame-count scheduling can't see.

Scope note: this computes and reports cost per chunk; it does not yet
rebalance chunk *boundaries* to equalize cost across workers (e.g. giving
the worker with the expensive set piece fewer frames to compensate). That's
a natural next step once there's a real worker pool to schedule against -
building a rebalancing algorithm without a Worker Runtime to validate it
against would be speculative, so it's deliberately left as reporting rather
than optimization for now.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Dict, Set, Optional

from aidars.adapters.blender.visibility.engine import VisibilityAnalyzer
from aidars.adapters.blender.visibility.analyzer import RenderRequirementAnalyzer
from aidars.adapters.blender.visibility.models import RenderRequest
from aidars.adapters.blender.packaging.resolver import RequirementResolver, DependencyClosureResolver

if TYPE_CHECKING:
    from aidars.adapters.blender.intelligence.dependency_graph import DependencyGraph
    from aidars.adapters.blender.intelligence.models import SceneSnapshot


@dataclass(slots=True)
class ChunkingConfig:
    """Configurable baseline estimates for frame chunking decisions."""

    default_overhead_seconds: float = 2.5       # Estimated subprocess launch + CAS ingest overhead
    max_overhead_fraction: float = 0.10         # Target maximum allowable overhead ratio (<= 10%)
    target_chunks_per_worker: int = 3           # Target multi-chunk pipelining multiplier
    fallback_frame_duration_seconds: float = 5.0 # Conservative duration fallback when unmeasured
    min_frames_per_chunk: int = 5               # Lower bound to prevent process startup thrashing


@dataclass(slots=True)
class ScheduledChunk:
    """One worker's assignment: a frame range and its estimated asset cost."""

    worker_id: str
    frame_start: int
    frame_end: int
    estimated_asset_bytes: int
    visible_object_count: int

    @property
    def frame_count(self) -> int:
        return self.frame_end - self.frame_start + 1


@dataclass(slots=True)
class SchedulingPlan:
    """A full assignment of a frame range across workers."""

    chunks: List[ScheduledChunk] = field(default_factory=list)

    @property
    def total_estimated_bytes(self) -> int:
        return sum(chunk.estimated_asset_bytes for chunk in self.chunks)

    @property
    def max_chunk_bytes(self) -> int:
        """The heaviest single chunk - the real lower bound on job wall-clock
        time if every worker starts at once, unlike total_estimated_bytes."""
        return max((chunk.estimated_asset_bytes for chunk in self.chunks), default=0)


class FrameScheduler:
    """Partitions a frame range across workers, with a real asset-cost estimate per chunk."""

    def __init__(
        self,
        visibility_analyzer: "VisibilityAnalyzer | None" = None,
        requirement_analyzer: "RenderRequirementAnalyzer | None" = None,
        config: "ChunkingConfig | None" = None,
    ) -> None:
        self.visibility_analyzer = visibility_analyzer or VisibilityAnalyzer()
        self.requirement_analyzer = requirement_analyzer or RenderRequirementAnalyzer()
        self.config = config or ChunkingConfig()

    def plan_adaptive_chunks(
        self,
        total_frames: int,
        worker_count: int = 1,
        estimated_frame_duration_seconds: Optional[float] = None,
    ) -> int:
        """Adaptive chunk planner balancing startup/ingest overhead against multi-chunk pipelining.

        Preserves complete frame coverage while avoiding sub-threshold chunk fragmentation.
        """
        if total_frames <= self.config.min_frames_per_chunk:
            return 1

        t_frame = (
            estimated_frame_duration_seconds
            if estimated_frame_duration_seconds is not None and estimated_frame_duration_seconds > 0
            else self.config.fallback_frame_duration_seconds
        )
        t_overhead = self.config.default_overhead_seconds
        max_ratio = self.config.max_overhead_fraction

        effective_workers = max(1, worker_count)
        candidate_m = min(total_frames, effective_workers * self.config.target_chunks_per_worker)

        while candidate_m > 1:
            chunk_frames = total_frames // candidate_m
            if chunk_frames < self.config.min_frames_per_chunk:
                candidate_m -= 1
                continue
            t_work = chunk_frames * t_frame
            ratio = t_overhead / (t_work + t_overhead)
            if ratio > max_ratio:
                candidate_m -= 1
                continue
            break

        return max(1, candidate_m)

    def schedule(
        self,
        snapshot: "SceneSnapshot",
        graph: "DependencyGraph",
        asset_sizes: Dict[str, int],
        frame_start: int,
        frame_end: int,
        worker_count: int = 1,
        camera_id: str = "",
        chunk_count: Optional[int] = None,
        estimated_frame_duration_seconds: Optional[float] = None,
        adaptive: bool = False,
    ) -> SchedulingPlan:
        """Split [frame_start, frame_end] into contiguous chunks.

        Args:
            snapshot: An already-analyzed scene snapshot.
            graph: A built dependency graph for the scene.
            asset_sizes: Mapping of physical file paths to size in bytes.
            frame_start: First frame of the job (inclusive).
            frame_end: Last frame of the job (inclusive).
            worker_count: How many workers to split across in legacy mode. Must be >= 1.
            camera_id: Optional camera name to restrict visibility.
            chunk_count: Explicit chunk count. If set, overrides adaptive calculation.
            estimated_frame_duration_seconds: Evidenced per-frame duration estimate.
            adaptive: Whether to use adaptive chunk planning if chunk_count is omitted.

        Returns:
            A SchedulingPlan with ScheduledChunk objects, each carrying its real estimated asset cost.
        """
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        if frame_end < frame_start:
            raise ValueError("frame_end must be >= frame_start")

        total_frames = frame_end - frame_start + 1

        if chunk_count is not None:
            if chunk_count < 1:
                raise ValueError("chunk_count must be at least 1")
            num_chunks = min(chunk_count, total_frames)
        elif adaptive:
            num_chunks = self.plan_adaptive_chunks(
                total_frames, worker_count, estimated_frame_duration_seconds
            )
        else:
            num_chunks = min(worker_count, total_frames)

        chunks: List[ScheduledChunk] = []
        for i in range(num_chunks):
            chunk_start = frame_start + (i * total_frames) // num_chunks
            chunk_end = frame_start + ((i + 1) * total_frames) // num_chunks - 1
            chunks.append(
                self._build_chunk(
                    snapshot, graph, asset_sizes, chunk_start, chunk_end, i, camera_id
                )
            )

        return SchedulingPlan(chunks=chunks)

    def _build_chunk(
        self,
        snapshot: "SceneSnapshot",
        graph: "DependencyGraph",
        asset_sizes: Dict[str, int],
        frame_start: int,
        frame_end: int,
        worker_index: int,
        camera_id: str,
    ) -> ScheduledChunk:
        request = RenderRequest(
            camera_id=camera_id,
            frame_start=frame_start,
            frame_end=frame_end,
        )
        report = self.requirement_analyzer.analyze(snapshot, graph, request)
        
        seed_ids = RequirementResolver.resolve(report)
        closure_ids = DependencyClosureResolver.compute_closure(seed_ids, graph)
        
        # Calculate size based on reached assets
        estimated_bytes = 0
        node_index = graph.node_index()
        known_labels = set(node.label for node in graph.nodes if node.kind == "asset")
        
        reached_paths = {
            node_index[nid].label
            for nid in closure_ids
            if nid in node_index and node_index[nid].kind == "asset"
        }
        
        for asset_path, size in asset_sizes.items():
            if asset_path not in known_labels or asset_path in reached_paths:
                estimated_bytes += size

        return ScheduledChunk(
            worker_id=f"worker-{worker_index}",
            frame_start=frame_start,
            frame_end=frame_end,
            estimated_asset_bytes=estimated_bytes,
            visible_object_count=len(report.required_objects),
        )
