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
from typing import TYPE_CHECKING, List, Dict, Set

from aidars.visibility.engine import VisibilityAnalyzer
from aidars.visibility.analyzer import RenderRequirementAnalyzer
from aidars.visibility.models import RenderRequest
from aidars.smart_package.resolver import RequirementResolver, DependencyClosureResolver

if TYPE_CHECKING:
    from aidars.scene_intelligence.dependency_graph import DependencyGraph
    from aidars.scene_intelligence.models import SceneSnapshot


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
    ) -> None:
        self.visibility_analyzer = visibility_analyzer or VisibilityAnalyzer()
        self.requirement_analyzer = requirement_analyzer or RenderRequirementAnalyzer()

    def schedule(
        self,
        snapshot: "SceneSnapshot",
        graph: "DependencyGraph",
        asset_sizes: Dict[str, int],
        frame_start: int,
        frame_end: int,
        worker_count: int,
        camera_id: str = "",
    ) -> SchedulingPlan:
        """Split [frame_start, frame_end] into worker_count contiguous chunks.

        Args:
            snapshot: An already-analyzed scene snapshot.
            graph: A built dependency graph for the scene.
            asset_sizes: Mapping of physical file paths to size in bytes.
            frame_start: First frame of the job (inclusive).
            frame_end: Last frame of the job (inclusive).
            worker_count: How many chunks to split the range into. Must be >= 1.
            camera_id: Optional camera name to restrict visibility.

        Returns:
            A SchedulingPlan with one ScheduledChunk per worker, each
            carrying its real estimated asset cost.
        """
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        if frame_end < frame_start:
            raise ValueError("frame_end must be >= frame_start")

        total_frames = frame_end - frame_start + 1
        chunk_size = max(1, math.ceil(total_frames / worker_count))

        chunks: List[ScheduledChunk] = []
        frame = frame_start
        worker_index = 0
        while frame <= frame_end:
            chunk_end = min(frame + chunk_size - 1, frame_end)
            chunks.append(self._build_chunk(snapshot, graph, asset_sizes, frame, chunk_end, worker_index, camera_id))
            frame = chunk_end + 1
            worker_index += 1

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
