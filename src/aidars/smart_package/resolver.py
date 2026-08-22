"""M4 Requirement Resolution and Dependency Closure.

Two clean responsibilities:

1. RequirementResolver
   RenderRequirementReport  →  Set[str] (canonical graph node IDs)

2. DependencyClosureResolver
   Set[str] + DependencyGraph  →  Set[str] (complete transitive closure)

These components CONSUME the DependencyGraph built by M2.
They do NOT build a second graph.
"""
from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Dict, List, Set, Tuple

from .models import AssetType

if TYPE_CHECKING:
    from aidars.scene_intelligence.dependency_graph import DependencyGraph, GraphNode
    from aidars.visibility.models import RenderRequirementReport


# ── Identifier prefix conventions used by DependencyGraphBuilder ──
#
#   Objects:     bare id          ("obj-1", "Cube")
#   Materials:   "material:{name}"
#   Textures:    "texture:{name}"
#   Images:      "image:{name}"
#   Modifiers:   "modifier:{obj_id}:{name}"
#   Actions:     "action:{obj_id}"
#   Assets:      "asset:{reference}"
#   Collections: bare id

_PREFIX_MAP: Dict[str, str] = {
    "material": "material:",
    "texture": "texture:",
    "image": "image:",
    "asset": "asset:",
}


class RequirementResolver:
    """Flatten a RenderRequirementReport into canonical graph-node identifiers.

    M3 outputs bare names (e.g. ``"WoodMat"``).
    The DependencyGraph uses prefixed identifiers (e.g. ``"material:WoodMat"``).

    This resolver bridges that gap, producing a ``Set[str]`` of identifiers
    that can be fed directly into ``DependencyClosureResolver``.
    """

    @staticmethod
    def resolve(report: "RenderRequirementReport") -> Set[str]:
        """Convert all ``required_*`` lists from the M3 report into graph node IDs.

        Returns:
            A set of canonical identifiers matching DependencyGraph node format.
        """
        ids: Set[str] = set()

        # Objects, lights, cameras use bare identifiers in the graph
        for obj_id in report.required_objects:
            ids.add(obj_id)
        for light_id in report.required_lights:
            ids.add(light_id)
        for cam_id in report.required_cameras:
            ids.add(cam_id)

        # Materials, textures, images use "prefix:name" identifiers
        for mat_name in report.required_materials:
            ids.add(f"material:{mat_name}")
            # Also add bare name so fuzzy matching against node labels works
            ids.add(mat_name)
        for tex_name in report.required_textures:
            ids.add(f"texture:{tex_name}")
            ids.add(tex_name)
        for img_name in report.required_images:
            ids.add(f"image:{img_name}")
            ids.add(img_name)

        # Meshes: not separate graph nodes in current DependencyGraphBuilder,
        # but include bare names for forward compatibility
        for mesh_name in report.required_meshes:
            ids.add(mesh_name)

        # External libraries → "asset:{path}"
        for lib_path in report.required_libraries:
            ids.add(f"asset:{lib_path}")
            ids.add(lib_path)

        # Simulation caches → bare names (no graph node prefix yet)
        for cache_path in report.required_simulation_caches:
            ids.add(cache_path)

        return ids


class DependencyClosureResolver:
    """Compute the transitive dependency closure over a DependencyGraph.

    Given a set of seed identifiers (from ``RequirementResolver``),
    performs a forward BFS over the graph's edges to collect every
    node reachable from the seeds.

    This replaces the ad-hoc BFS currently scattered across
    ``AssetOptimizer.reachable_asset_paths()`` and
    ``DependencyResolver.resolve_closure()``.  Those callers can
    delegate here once migration is complete.
    """

    @staticmethod
    def compute_closure(
        seed_ids: Set[str],
        graph: "DependencyGraph",
    ) -> Set[str]:
        """BFS from *seed_ids* over forward edges in *graph*.

        Args:
            seed_ids: Starting set of graph node identifiers (objects,
                materials, textures, etc.) as produced by
                ``RequirementResolver.resolve()``.
            graph: The M2 DependencyGraph to traverse.

        Returns:
            The complete set of reachable node identifiers, including
            the original seeds.
        """
        # Build forward adjacency list once
        adjacency: Dict[str, List[str]] = {}
        for edge in graph.edges:
            adjacency.setdefault(edge.source, []).append(edge.target)

        # Also build a label→identifier index for fuzzy seed matching.
        # M3 outputs bare names like "WoodMat" which may not exactly match
        # the graph identifier "material:WoodMat".  The label index lets
        # us find the real graph node for a bare-name seed.
        node_index = graph.node_index()
        label_to_ids: Dict[str, List[str]] = {}
        for node in graph.nodes:
            label_to_ids.setdefault(node.label, []).append(node.identifier)

        # Expand seeds: for every seed that isn't already a graph node,
        # try matching it as a label to find the real node identifier(s).
        expanded_seeds: Set[str] = set()
        for seed in seed_ids:
            if seed in node_index:
                expanded_seeds.add(seed)
            # Also try label→identifier lookup
            for real_id in label_to_ids.get(seed, []):
                expanded_seeds.add(real_id)

        # BFS
        visited: Set[str] = set()
        queue: deque[str] = deque(expanded_seeds)

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            for neighbor in adjacency.get(current, []):
                if neighbor not in visited:
                    queue.append(neighbor)

        return visited

    @staticmethod
    def classify_node(node: "GraphNode") -> AssetType:
        """Map a DependencyGraph node kind to an M4 AssetType.

        This is a pure classification function — no I/O, no side effects.
        """
        kind = node.kind.lower() if node.kind else ""
        mapping: Dict[str, AssetType] = {
            "object": AssetType.UNKNOWN,  # further classified by obj.type later
            "material": AssetType.MATERIAL,
            "texture": AssetType.TEXTURE,
            "image": AssetType.IMAGE,
            "modifier": AssetType.MODIFIER,
            "action": AssetType.ACTION,
            "asset": AssetType.LIBRARY,
            "collection": AssetType.COLLECTION,
        }
        return mapping.get(kind, AssetType.UNKNOWN)

    @staticmethod
    def partition_closure(
        closure_ids: Set[str],
        graph: "DependencyGraph",
    ) -> Dict[AssetType, List[str]]:
        """Group closed-over identifiers by their AssetType.

        Useful for downstream stages (registry, planner) that need to
        handle different asset types differently.

        Args:
            closure_ids: Output of ``compute_closure()``.
            graph: The same DependencyGraph.

        Returns:
            Dict mapping ``AssetType`` to list of node identifiers.
        """
        node_index = graph.node_index()
        result: Dict[AssetType, List[str]] = {}
        for node_id in sorted(closure_ids):
            node = node_index.get(node_id)
            if node is not None:
                asset_type = DependencyClosureResolver.classify_node(node)
            else:
                asset_type = AssetType.UNKNOWN
            result.setdefault(asset_type, []).append(node_id)
        return result
