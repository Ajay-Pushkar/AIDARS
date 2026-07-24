"""Asset Optimizer.

    Dependency Graph
            |
            v
    Visibility Analysis
            |
            v
    Asset Optimizer
            |
            v
    Package Manifest

Given a dependency graph and a set of visible object ids (from
``aidars.visibility.engine.VisibilityAnalyzer``), determines which
externally-referenced assets are actually reachable from something visible,
so SmartPackageBuilder can exclude assets nothing in the rendered frames
will ever pull in.

Current scope: this only prunes "asset" kind graph nodes (i.e. externally
linked/referenced .blend libraries - SceneObject.referenced_assets). It does
not yet prune materials/textures/images individually, because
PackageAsset (aidars.smart_package.builder) has no per-material/texture
granularity to map onto - packaging currently only deals with whole
external asset files. Extending PackageAsset to reference specific
materials/textures is a natural next step once packaging needs to ship
partial asset files rather than whole ones.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Set

if TYPE_CHECKING:
    from aidars.scene_intelligence.dependency_graph import DependencyGraph
    from aidars.smart_package.builder import PackageAsset


@dataclass(slots=True)
class OptimizationResult:
    """The outcome of pruning a package's assets against graph reachability."""

    kept_assets: List["PackageAsset"] = field(default_factory=list)
    pruned_assets: List["PackageAsset"] = field(default_factory=list)
    visible_object_count: int = 0
    reachable_asset_count: int = 0

    @property
    def pruned_size_bytes(self) -> int:
        """Total size of everything pruned - the concrete transfer savings."""
        return sum(asset.size_bytes for asset in self.pruned_assets)


class AssetOptimizer:
    """Prunes assets to only those reachable from a set of visible objects."""

    def reachable_asset_paths(self, graph: "DependencyGraph", visible_object_ids: Set[str]) -> Set[str]:
        """Return the path/label of every "asset" node reachable from visible objects.

        Reachability is a forward BFS over the graph's edges starting from
        each visible object id - i.e. "what does this visible object pull
        in, directly or transitively" (object -> material -> texture -> ...,
        and object -> asset for linked external libraries).
        """
        adjacency: Dict[str, List[str]] = {}
        for edge in graph.edges:
            adjacency.setdefault(edge.source, []).append(edge.target)

        node_index = graph.node_index()
        visited: Set[str] = set()
        queue: deque[str] = deque(oid for oid in visible_object_ids if oid in node_index)

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            for neighbor in adjacency.get(current, []):
                if neighbor not in visited:
                    queue.append(neighbor)

        # `visited` can contain ids that were never added as real graph
        # nodes: a dangling edge target (e.g. a constraint pointing at a
        # deleted object - exactly what DependencyGraph.find_missing_targets
        # detects) still gets queued and visited during BFS, but has no
        # entry in node_index. Such ids can't be "asset" nodes, so they're
        # simply excluded here rather than raising.
        return {
            node_index[node_id].label
            for node_id in visited
            if node_id in node_index and node_index[node_id].kind == "asset"
        }

    def optimize(
        self,
        graph: "DependencyGraph",
        visible_object_ids: Set[str],
        assets: List["PackageAsset"],
    ) -> OptimizationResult:
        """Split assets into what's reachable from visible objects and what isn't.

        Args:
            graph: A built dependency graph for the scene.
            visible_object_ids: Object ids visible in the target frame range
                (see VisibilityAnalyzer.analyze()).
            assets: Candidate assets for the package (e.g. from the raw
                scene payload's "assets" list).

        Returns:
            An OptimizationResult with assets split into kept/pruned.
        """
        reachable_paths = self.reachable_asset_paths(graph, visible_object_ids)
        known_asset_paths = {node.label for node in graph.nodes if node.kind == "asset"}

        kept: List["PackageAsset"] = []
        pruned: List["PackageAsset"] = []
        for asset in assets:
            # An asset the graph has no node for at all (declared only in
            # the raw "assets" list, never linked from any object) can't be
            # proven unused - keep it conservatively rather than risk
            # dropping something a worker actually needs.
            if asset.path not in known_asset_paths or asset.path in reachable_paths:
                kept.append(asset)
            else:
                pruned.append(asset)

        return OptimizationResult(
            kept_assets=kept,
            pruned_assets=pruned,
            visible_object_count=len(visible_object_ids),
            reachable_asset_count=len(reachable_paths),
        )
