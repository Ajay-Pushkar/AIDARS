from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .models import SceneSnapshot


@dataclass(slots=True)
class GraphNode:
    """A graph node representing a scene element."""

    identifier: str
    label: str
    kind: str


@dataclass(slots=True)
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


class DependencyGraphBuilder:
    """Build a dependency graph from a scene snapshot.

    This implementation creates meaningful links between objects, materials,
    textures, images, collections, and animation actions so the graph can be
    consumed by later packaging and optimization modules.
    """

    def build(self, snapshot: SceneSnapshot) -> DependencyGraph:
        nodes: List[GraphNode] = []
        edges: List[GraphEdge] = []

        for collection in snapshot.collections:
            nodes.append(GraphNode(identifier=collection.id, label=collection.name, kind="collection"))

        for obj in snapshot.objects:
            nodes.append(GraphNode(identifier=obj.id, label=obj.name, kind="object"))

        for obj in snapshot.objects:
            if obj.parent:
                edges.append(GraphEdge(source=obj.id, target=obj.parent, relationship="parent"))

        for obj in snapshot.objects:
            if obj.collection:
                edges.append(GraphEdge(source=obj.id, target=obj.collection, relationship="collection"))

        for obj in snapshot.objects:
            for child_id in obj.children:
                edges.append(GraphEdge(source=obj.id, target=child_id, relationship="child"))

            for constraint in obj.constraints:
                if constraint.target:
                    edges.append(GraphEdge(source=obj.id, target=constraint.target, relationship="constraint"))

            for material in obj.materials:
                material_id = f"material:{material.name}"
                nodes.append(GraphNode(identifier=material_id, label=material.name, kind="material"))
                edges.append(GraphEdge(source=obj.id, target=material_id, relationship="material"))

                for texture_name in material.image_textures:
                    texture_id = f"texture:{texture_name}"
                    nodes.append(GraphNode(identifier=texture_id, label=texture_name, kind="texture"))
                    edges.append(GraphEdge(source=material_id, target=texture_id, relationship="texture"))

                    image_id = f"image:{texture_name}"
                    nodes.append(GraphNode(identifier=image_id, label=texture_name, kind="image"))
                    edges.append(GraphEdge(source=texture_id, target=image_id, relationship="image"))

            if obj.animation:
                action_id = f"action:{obj.id}"
                nodes.append(GraphNode(identifier=action_id, label=f"Action:{obj.name}", kind="action"))
                edges.append(GraphEdge(source=obj.id, target=action_id, relationship="animation"))

        for obj in snapshot.objects:
            for reference in getattr(obj, "referenced_assets", []):
                ref_id = f"asset:{reference}"
                nodes.append(GraphNode(identifier=ref_id, label=reference, kind="asset"))
                edges.append(GraphEdge(source=obj.id, target=ref_id, relationship="references"))

        return DependencyGraph(nodes=nodes, edges=edges)
