"""M4 Requirement Resolution, Dependency Closure, and Physical Asset Resolution.

Three clean responsibilities:

1. RequirementResolver
   RenderRequirementReport  →  Set[str] (canonical graph node IDs)

2. DependencyClosureResolver
   Set[str] + DependencyGraph  →  Set[str] (complete transitive closure)

3. PhysicalAssetResolver
   Set[str] + DependencyGraph + filesystem  →  List[AssetRecord] (physical resolution)

These components CONSUME the DependencyGraph built by M2.
They do NOT build a second graph.
"""
from __future__ import annotations

import hashlib
import os
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union

from .models import AssetRecord, AssetStatus, AssetType, SelectionReason

if TYPE_CHECKING:
    from aidars.scene_intelligence.dependency_graph import DependencyGraph, GraphNode
    from aidars.scene_intelligence.models import SceneSnapshot
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


class PhysicalAssetResolver:
    """Resolve logical asset identifiers into physical files on disk.

    Given closure IDs from DependencyClosureResolver and a DependencyGraph,
    this component:
    1. Classifies each ID into AssetType.
    2. Determines whether the asset is embedded in the scene or is an external file.
    3. Resolves filesystem paths (Blender '//' relative paths, absolute paths, relative paths).
    4. Verifies disk existence.
    5. Computes SHA-256 hash and file size for existing files.
    6. Flags missing files as AssetStatus.MISSING (never silently dropped).
    7. Creates fully-populated AssetRecord instances.
    """

    def __init__(
        self,
        base_dir: Optional[Union[str, Path]] = None,
        search_paths: Optional[List[Union[str, Path]]] = None,
    ) -> None:
        self.base_dir = Path(base_dir) if base_dir else None
        self.search_paths = [Path(p) for p in (search_paths or [])]

    @staticmethod
    def compute_sha256(file_path: Union[str, Path]) -> str:
        """Compute SHA-256 hash of a file incrementally."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def resolve_path(
        self,
        raw_path: str,
        base_dir: Optional[Union[str, Path]] = None,
    ) -> Tuple[Path, str]:
        """Resolve a raw path string to an absolute Path and normalized relative path.

        Handles:
        - Blender '//' relative syntax (e.g. '//textures/diffuse.png')
        - Absolute paths
        - Standard relative paths
        """
        effective_base = Path(base_dir) if base_dir else (self.base_dir or Path.cwd())

        # Blender relative path
        if raw_path.startswith("//") or raw_path.startswith("\\\\"):
            clean_rel = raw_path[2:].lstrip("/\\")
            candidate = (effective_base / clean_rel).resolve()
            if not candidate.exists() and self.search_paths:
                for sp in self.search_paths:
                    alt = (sp / clean_rel).resolve()
                    if alt.exists():
                        return alt, clean_rel
            return candidate, clean_rel

        p = Path(raw_path)
        if p.is_absolute():
            candidate = p.resolve()
            return candidate, p.name

        # Standard relative path
        candidate = (effective_base / raw_path).resolve()
        if not candidate.exists() and self.search_paths:
            for sp in self.search_paths:
                alt = (sp / raw_path).resolve()
                if alt.exists():
                    return alt, raw_path
        return candidate, raw_path

    def resolve(
        self,
        closure_ids: Set[str],
        graph: "DependencyGraph",
        base_dir: Optional[Union[str, Path]] = None,
        seed_ids: Optional[Set[str]] = None,
        snapshot: Optional["SceneSnapshot"] = None,
        search_paths: Optional[List[Union[str, Path]]] = None,
    ) -> List[AssetRecord]:
        """Resolve all closed-over asset identifiers into AssetRecord objects.

        Args:
            closure_ids: Node identifiers produced by DependencyClosureResolver.
            graph: The DependencyGraph.
            base_dir: Base directory for resolving relative paths.
            seed_ids: Initial seed IDs to determine SelectionReason.
            snapshot: Optional SceneSnapshot for fine-grained object classification.
            search_paths: Optional extra search directories.

        Returns:
            Deterministic list of AssetRecord objects sorted by asset_id.
        """
        effective_base = Path(base_dir) if base_dir else (self.base_dir or Path.cwd())
        extra_search = [Path(p) for p in (search_paths or [])] + self.search_paths

        node_index = graph.node_index()
        direct_seeds = seed_ids or set()

        # Build snapshot object map if snapshot is available
        object_type_map: Dict[str, str] = {}
        if snapshot is not None and hasattr(snapshot, "objects"):
            for obj in snapshot.objects:
                obj_type = getattr(obj, "type", "UNKNOWN")
                object_type_map[obj.id] = obj_type
                object_type_map[obj.name] = obj_type

        # Build outgoing dependencies map from graph edges
        edge_deps: Dict[str, Set[str]] = {}
        for edge in graph.edges:
            edge_deps.setdefault(edge.source, set()).add(edge.target)

        records: List[AssetRecord] = []

        for node_id in sorted(closure_ids):
            node = node_index.get(node_id)
            is_conservative = node is None

            # Determine SelectionReason
            if node_id in direct_seeds or (node and node.label in direct_seeds):
                reason = SelectionReason.RENDER_REQUIRED
            else:
                reason = SelectionReason.DEPENDENCY

            # Determine AssetType and path/embedded classification
            asset_type = AssetType.UNKNOWN
            raw_path: Optional[str] = None
            is_external = False

            if node is not None:
                kind = node.kind.lower() if node.kind else ""
                label = node.label

                if kind == "object":
                    obj_type = object_type_map.get(node.identifier) or object_type_map.get(label, "MESH")
                    if obj_type.upper() == "CAMERA":
                        asset_type = AssetType.CAMERA
                    elif obj_type.upper() == "LIGHT":
                        asset_type = AssetType.LIGHT
                    elif obj_type.upper() == "MESH":
                        asset_type = AssetType.MESH
                    else:
                        asset_type = AssetType.UNKNOWN
                    is_external = False

                elif kind == "material":
                    asset_type = AssetType.MATERIAL
                    is_external = False

                elif kind == "texture":
                    asset_type = AssetType.TEXTURE
                    is_external = True
                    raw_path = label

                elif kind == "image":
                    asset_type = AssetType.IMAGE
                    is_external = True
                    raw_path = label

                elif kind == "asset":
                    lower_label = label.lower()
                    if lower_label.endswith((".hdr", ".exr")):
                        asset_type = AssetType.HDRI
                    elif lower_label.endswith((".png", ".jpg", ".jpeg", ".tga", ".dds", ".tif", ".tiff")):
                        asset_type = AssetType.TEXTURE
                    else:
                        asset_type = AssetType.LIBRARY
                    is_external = True
                    raw_path = label

                elif kind == "modifier":
                    asset_type = AssetType.MODIFIER
                    is_external = False

                elif kind == "action":
                    asset_type = AssetType.ACTION
                    is_external = False

                elif kind == "collection":
                    asset_type = AssetType.COLLECTION
                    is_external = False

                else:
                    asset_type = DependencyClosureResolver.classify_node(node)
                    is_external = False

            else:
                # Handle identifiers not found directly as graph nodes
                if node_id.startswith("material:"):
                    asset_type = AssetType.MATERIAL
                    is_external = False
                elif node_id.startswith("texture:"):
                    asset_type = AssetType.TEXTURE
                    is_external = True
                    raw_path = node_id[len("texture:"):]
                elif node_id.startswith("image:"):
                    asset_type = AssetType.IMAGE
                    is_external = True
                    raw_path = node_id[len("image:"):]
                elif node_id.startswith("asset:"):
                    asset_type = AssetType.LIBRARY
                    is_external = True
                    raw_path = node_id[len("asset:"):]
                else:
                    # Check if string looks like a file path
                    if any(node_id.lower().endswith(ext) for ext in (".blend", ".png", ".jpg", ".jpeg", ".hdr", ".exr", ".vdb", ".abc")):
                        asset_type = AssetType.LIBRARY if node_id.lower().endswith(".blend") else AssetType.TEXTURE
                        is_external = True
                        raw_path = node_id
                    else:
                        asset_type = AssetType.UNKNOWN
                        is_external = False

            # Collect dependencies from graph
            node_deps = sorted(list(edge_deps.get(node_id, set())))

            if not is_external or raw_path is None:
                # Embedded asset
                records.append(
                    AssetRecord(
                        asset_id=node_id,
                        asset_type=asset_type,
                        selection_reason=reason,
                        source_path=None,
                        relative_path=None,
                        package_path=None,
                        status=AssetStatus.EMBEDDED,
                        sha256=None,
                        size_bytes=0,
                        embedded=True,
                        conservative=is_conservative,
                        dependencies=node_deps,
                    )
                )
            else:
                # Physical external asset resolution
                resolved_file, clean_rel = self.resolve_path(raw_path, base_dir=effective_base)

                if resolved_file.exists() and resolved_file.is_file():
                    file_size = resolved_file.stat().st_size
                    file_hash = self.compute_sha256(resolved_file)
                    package_path = f"assets/{resolved_file.name}"

                    records.append(
                        AssetRecord(
                            asset_id=node_id,
                            asset_type=asset_type,
                            selection_reason=reason,
                            source_path=str(resolved_file),
                            relative_path=clean_rel,
                            package_path=package_path,
                            status=AssetStatus.RESOLVED,
                            sha256=file_hash,
                            size_bytes=file_size,
                            embedded=False,
                            conservative=is_conservative,
                            dependencies=node_deps,
                        )
                    )
                else:
                    # Missing asset - MUST be flagged as MISSING, not silently dropped
                    records.append(
                        AssetRecord(
                            asset_id=node_id,
                            asset_type=asset_type,
                            selection_reason=reason,
                            source_path=str(resolved_file),
                            relative_path=clean_rel,
                            package_path=None,
                            status=AssetStatus.MISSING,
                            sha256=None,
                            size_bytes=0,
                            embedded=False,
                            conservative=is_conservative,
                            dependencies=node_deps,
                        )
                    )

        return records
