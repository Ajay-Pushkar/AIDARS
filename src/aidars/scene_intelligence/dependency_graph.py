from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set

from .models import SceneSnapshot


@dataclass(slots=True, frozen=True)
class GraphNode:
    """A graph node representing a scene element."""

    identifier: str
    label: str
    kind: str


@dataclass(slots=True, frozen=True)
class GraphEdge:
    """An edge representing a dependency between two scene elements."""

    source: str
    target: str
    relationship: str


@dataclass(slots=True)
class DependencyGraph:
    """A dependency graph for the scene."""

    nodes: List[GraphNode] = field(default_factory=list)
    edges: List[GraphEdge] = field(default_factory=list)

    def node_index(self) -> Dict[str, GraphNode]:
        """Return a lookup of node identifier to node."""
        return {node.identifier: node for node in self.nodes}

    def find_missing_targets(self) -> List[str]:
        """Return edge targets that don't correspond to any known node.

        These represent references (e.g. a constraint target or a parent id)
        that point at a scene element AIDAR never discovered - typically a
        missing or deleted asset.
        """
        known_ids: Set[str] = {node.identifier for node in self.nodes}
        missing: List[str] = []
        seen: Set[str] = set()
        for edge in self.edges:
            if edge.target not in known_ids and edge.target not in seen:
                missing.append(edge.target)
                seen.add(edge.target)
        return missing

    def find_unused_nodes(self, protected_kinds: Set[str] = frozenset({"object"})) -> List[GraphNode]:
        """Return nodes (e.g. materials/textures/images/collections) with no incoming edge.

        Nodes whose kind is in ``protected_kinds`` are never reported since
        top-level objects are expected to have no incoming dependency edge.
        An unreferenced collection (nothing assigned to it) is intentionally
        NOT protected, since that is a genuinely useful "unused asset" signal.
        """
        referenced: Set[str] = {edge.target for edge in self.edges}
        return [
            node
            for node in self.nodes
            if node.identifier not in referenced and node.kind not in protected_kinds
        ]


class DependencyGraphBuilder:
    """Build a dependency graph from a scene snapshot.

    This implementation creates meaningful links between objects, materials,
    textures, images, collections, and animation actions so the graph can be
    consumed by later packaging and optimization modules.
    """

    def build(self, snapshot: SceneSnapshot) -> DependencyGraph:
        nodes: Dict[str, GraphNode] = {}
        edges: Dict[tuple[str, str, str], GraphEdge] = {}

        def add_node(identifier: str, label: str, kind: str) -> None:
            # First writer wins so the earliest (most authoritative) label is
            # kept if the same identifier is discovered from multiple objects.
            nodes.setdefault(identifier, GraphNode(identifier=identifier, label=label, kind=kind))

        def add_edge(source: str, target: str, relationship: str) -> None:
            # Insertion-order preserving dedup: repeated (source, target,
            # relationship) triples - common when several objects share a
            # material/texture/image - collapse into a single edge.
            edges.setdefault((source, target, relationship), GraphEdge(source=source, target=target, relationship=relationship))

        for collection in snapshot.collections:
            add_node(collection.id, collection.name, "collection")

        for obj in snapshot.objects:
            add_node(obj.id, obj.name, "object")

        for obj in snapshot.objects:
            if obj.parent:
                add_edge(obj.id, obj.parent, "parent")

        for obj in snapshot.objects:
            if obj.collection:
                add_edge(obj.id, obj.collection, "collection")

        for obj in snapshot.objects:
            for child_id in obj.children:
                add_edge(obj.id, child_id, "child")

            for constraint in obj.constraints:
                if constraint.target:
                    add_edge(obj.id, constraint.target, "constraint")

            for material in obj.materials:
                material_id = f"material:{material.name}"
                add_node(material_id, material.name, "material")
                add_edge(obj.id, material_id, "material")

                for texture_name in material.image_textures:
                    texture_id = f"texture:{texture_name}"
                    add_node(texture_id, texture_name, "texture")
                    add_edge(material_id, texture_id, "texture")

                    image_id = f"image:{texture_name}"
                    add_node(image_id, texture_name, "image")
                    add_edge(texture_id, image_id, "image")

            if obj.animation and obj.animation.is_animated:
                action_id = f"action:{obj.id}"
                add_node(action_id, f"Action:{obj.name}", "action")
                add_edge(obj.id, action_id, "animation")

            for reference in obj.referenced_assets:
                asset_id = f"asset:{reference}"
                add_node(asset_id, reference, "asset")
                add_edge(obj.id, asset_id, "references")

        return DependencyGraph(nodes=list(nodes.values()), edges=list(edges.values()))
