import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.dependency_graph import DependencyGraphBuilder
from aidars.scene_intelligence.engine import SceneIntelligenceEngine
from aidars.smart_package.builder import PackageAsset
from aidars.smart_package.optimizer import AssetOptimizer

BASE_SCENE = {
    "metadata": {"name": "Demo", "frame_start": 1, "frame_end": 24, "fps": 24},
    "collections": [],
    "lights": [],
    "materials": [],
    "textures": [],
    "images": [],
}


class AssetOptimizerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = SceneIntelligenceEngine()
        self.graph_builder = DependencyGraphBuilder()
        self.optimizer = AssetOptimizer()

    def _build_graph(self, objects: list):
        scene_data = dict(BASE_SCENE)
        scene_data["objects"] = objects
        snapshot = self.engine.analyze_scene_data(scene_data)
        return self.graph_builder.build(snapshot)

    def test_reachable_asset_paths_from_visible_object(self) -> None:
        graph = self._build_graph(
            [
                {
                    "name": "Hero",
                    "id": "obj-hero",
                    "type": "MESH",
                    "visibility": {"hide_render": False, "hide_viewport": False},
                    "referenced_assets": ["/assets/hero.blend"],
                }
            ]
        )
        reachable = self.optimizer.reachable_asset_paths(graph, {"obj-hero"})
        self.assertEqual(reachable, {"/assets/hero.blend"})

    def test_asset_not_reachable_from_hidden_object_is_pruned(self) -> None:
        graph = self._build_graph(
            [
                {
                    "name": "Backdrop",
                    "id": "obj-backdrop",
                    "type": "MESH",
                    "visibility": {"hide_render": True, "hide_viewport": False},
                    "referenced_assets": ["/assets/backdrop.blend"],
                }
            ]
        )
        # No visible objects at all -> nothing reachable.
        reachable = self.optimizer.reachable_asset_paths(graph, visible_object_ids=set())
        self.assertEqual(reachable, set())

    def test_dangling_edge_target_does_not_crash_bfs(self) -> None:
        """Regression test: a constraint pointing at a non-existent object id
        creates a dangling edge whose target was never added as a real graph
        node (exactly what DependencyGraph.find_missing_targets() flags).
        BFS from a visible object still walks that edge and visits the
        dangling id - reachable_asset_paths must not crash on it."""
        graph = self._build_graph(
            [
                {
                    "name": "Hero",
                    "id": "obj-hero",
                    "type": "MESH",
                    "visibility": {"hide_render": False, "hide_viewport": False},
                    "constraints": [
                        {"name": "Track", "type": "TRACK_TO", "target": "does-not-exist", "influence": 1.0}
                    ],
                    "referenced_assets": ["/assets/hero.blend"],
                }
            ]
        )
        # Must not raise, and must still find the real reachable asset.
        reachable = self.optimizer.reachable_asset_paths(graph, {"obj-hero"})
        self.assertEqual(reachable, {"/assets/hero.blend"})

    def test_optimize_keeps_unknown_assets_conservatively(self) -> None:
        graph = self._build_graph(
            [
                {
                    "name": "Hero",
                    "id": "obj-hero",
                    "type": "MESH",
                    "visibility": {"hide_render": False, "hide_viewport": False},
                }
            ]
        )
        # This asset has no corresponding graph node at all (not referenced
        # by any object) - can't be proven unused, so it must be kept.
        assets = [PackageAsset(path="/assets/mystery.png", kind="texture", size_bytes=42)]
        result = self.optimizer.optimize(graph, {"obj-hero"}, assets)

        self.assertEqual(result.kept_assets, assets)
        self.assertEqual(result.pruned_assets, [])


if __name__ == "__main__":
    unittest.main()
