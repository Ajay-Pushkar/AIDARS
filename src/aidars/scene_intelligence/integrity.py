"""Asset integrity checking.

Split out from DependencyGraphBuilder on purpose: graph *construction* and
graph *analysis* are different responsibilities, and future checks (cyclic
parenting, duplicate names, orphaned materials with no image data, etc.)
should be able to grow here without DependencyGraph itself accumulating
policy it shouldn't own.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .dependency_graph import DependencyGraph, GraphNode


@dataclass(slots=True)
class IntegrityReport:
    """The result of running integrity checks against a dependency graph."""

    missing_targets: List[str] = field(default_factory=list)
    unused_nodes: List[GraphNode] = field(default_factory=list)

    def has_issues(self) -> bool:
        """Return True if any check found a problem."""
        return bool(self.missing_targets or self.unused_nodes)

    def to_dict(self) -> dict:
        """Convert to the JSON-serializable shape used in the dependency graph report."""
        return {
            "missing_targets": list(self.missing_targets),
            "unused_nodes": [
                {"id": node.identifier, "label": node.label, "kind": node.kind}
                for node in self.unused_nodes
            ],
        }


class IntegrityChecker:
    """Runs asset-integrity checks against a built dependency graph."""

    def check(self, graph: DependencyGraph) -> IntegrityReport:
        """Run all integrity checks and return a combined report.

        Args:
            graph: A dependency graph already built by DependencyGraphBuilder.

        Returns:
            An IntegrityReport describing anything the checks found.
        """
        return IntegrityReport(
            missing_targets=graph.find_missing_targets(),
            unused_nodes=graph.find_unused_nodes(),
        )
