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
from typing import TYPE_CHECKING, List

from aidars.smart_package.optimizer import AssetOptimizer
from aidars.visibility.engine import VisibilityAnalyzer

if TYPE_CHECKING:
    from aidars.scene_intelligence.dependency_graph import DependencyGraph
    from aidars.scene_intelligence.models import SceneSnapshot
    from aidars.smart_package.builder import PackageAsset


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
        optimizer: "AssetOptimizer | None" = None,
    ) -> None:
        self.visibility_analyzer = visibility_analyzer or VisibilityAnalyzer()
        self.optimizer = optimizer or AssetOptimizer()

    def schedule(
        self,
        snapshot: "SceneSnapshot",
        graph: "DependencyGraph",
        assets: List["PackageAsset"],
        frame_start: int,
        frame_end: int,
        worker_count: int,
    ) -> SchedulingPlan:
        """Split [frame_start, frame_end] into worker_count contiguous chunks.

        Args:
            snapshot: An already-analyzed scene snapshot.
            graph: A built dependency graph for the scene.
            assets: Candidate assets (e.g. from the raw scene payload).
            frame_start: First frame of the job (inclusive).
            frame_end: Last frame of the job (inclusive).
            worker_count: How many chunks to split the range into. Must be >= 1.

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
            chunks.append(self._build_chunk(snapshot, graph, assets, frame, chunk_end, worker_index))
            frame = chunk_end + 1
            worker_index += 1

        return SchedulingPlan(chunks=chunks)

    def _build_chunk(
        self,
        snapshot: "SceneSnapshot",
        graph: "DependencyGraph",
        assets: List["PackageAsset"],
        frame_start: int,
        frame_end: int,
        worker_index: int,
    ) -> ScheduledChunk:
        visibility = self.visibility_analyzer.analyze(snapshot, frame_start, frame_end)
        optimization = self.optimizer.optimize(graph, visibility.visible_object_ids, assets)
        estimated_bytes = sum(asset.size_bytes for asset in optimization.kept_assets)

        return ScheduledChunk(
            worker_id=f"worker-{worker_index}",
            frame_start=frame_start,
            frame_end=frame_end,
            estimated_asset_bytes=estimated_bytes,
            visible_object_count=len(visibility.visible_object_ids),
        )
