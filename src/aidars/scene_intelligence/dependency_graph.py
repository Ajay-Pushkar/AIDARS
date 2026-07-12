from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .models import SceneSnapshot


@dataclass(slots=True)
class GraphNode:
    """A graph node representing a scene object."""

    identifier: str
    label: str


@dataclass(slots=True)
class GraphEdge:
    """An edge representing a relationship between two scene objects."""

    source: str
    target: str
    relationship: str


@dataclass(slots=True)
class DependencyGraph:
    """A lightweight dependency graph for the scene."""

    nodes: List[GraphNode] = field(default_factory=list)
    edges: List[GraphEdge] = field(default_factory=list)


class DependencyGraphBuilder:
    """Create a simple dependency graph from scene relationships.

    This is a Phase 1 placeholder designed to be expanded into a richer asset
    dependency graph in future phases. It currently models parent/child links
    and object-to-collection relationships.
    """

    def build(self, snapshot: SceneSnapshot) -> DependencyGraph:
        nodes = [GraphNode(identifier=obj.id, label=obj.name) for obj in snapshot.objects]
        edges: List[GraphEdge] = []

        for obj in snapshot.objects:
            if obj.parent:
                edges.append(GraphEdge(source=obj.id, target=obj.parent, relationship="parent"))

        for obj in snapshot.objects:
            if obj.collection:
                edges.append(GraphEdge(source=obj.id, target=obj.collection, relationship="collection"))

        return DependencyGraph(nodes=nodes, edges=edges)
