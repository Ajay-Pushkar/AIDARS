import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.dependency_graph import DependencyGraphBuilder
from aidars.scene_intelligence.engine import SceneIntelligenceEngine
from aidars.scheduler.frame_scheduler import FrameScheduler
from aidars.smart_package.builder import PackageAsset

BASE_SCENE = {
    "metadata": {"name": "Demo", "frame_start": 1, "frame_end": 100, "fps": 24},
    "collections": [],
    "lights": [],
    "materials": [],
    "textures": [],
    "images": [],
}


class FrameSchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = SceneIntelligenceEngine()
        self.graph_builder = DependencyGraphBuilder()
        self.scheduler = FrameScheduler()

    def _build(self, objects: list) -> tuple:
        scene_data = dict(BASE_SCENE)
        scene_data["objects"] = objects
        snapshot = self.engine.analyze_scene_data(scene_data)
        graph = self.graph_builder.build(snapshot)
        return snapshot, graph

    def test_even_split_across_workers(self) -> None:
        snapshot, graph = self._build(
            [
                {
                    "name": "Hero",
                    "id": "obj-hero",
                    "type": "MESH",
                    "visibility": {"hide_render": False, "hide_viewport": False},
                }
            ]
        )
        plan = self.scheduler.schedule(snapshot, graph, [], frame_start=1, frame_end=100, worker_count=4)

        self.assertEqual(len(plan.chunks), 4)
        self.assertEqual(plan.chunks[0].frame_start, 1)
        self.assertEqual(plan.chunks[0].frame_end, 25)
        self.assertEqual(plan.chunks[-1].frame_end, 100)
        # Every frame accounted for exactly once, no gaps or overlaps.
        covered = sorted(f for chunk in plan.chunks for f in range(chunk.frame_start, chunk.frame_end + 1))
        self.assertEqual(covered, list(range(1, 101)))

    def test_cost_varies_by_chunk_when_expensive_asset_only_visible_partway(self) -> None:
        # SetPiece is hidden for frames 1-49, revealed at frame 50.
        animation = {
            "fcurves": 1,
            "is_animated": True,
            "curves": [
                {
                    "name": "hide_render",
                    "data_path": "hide_render",
                    "array_index": 0,
                    "keyframes": [
                        {"frame": 1, "value": 1.0, "interpolation": "CONSTANT"},
                        {"frame": 50, "value": 0.0, "interpolation": "CONSTANT"},
                    ],
                    "interpolation": "CONSTANT",
                }
            ],
        }
        snapshot, graph = self._build(
            [
                {
                    "name": "SetPiece",
                    "id": "obj-setpiece",
                    "type": "MESH",
                    "visibility": {"hide_render": True, "hide_viewport": False},
                    "animation": animation,
                    "referenced_assets": ["/assets/setpiece.blend"],
                }
            ]
        )
        assets = [PackageAsset(path="/assets/setpiece.blend", kind="blend", size_bytes=9_000_000)]

        plan = self.scheduler.schedule(snapshot, graph, assets, frame_start=1, frame_end=100, worker_count=4)

        # Chunk 0 (frames 1-25): set piece still hidden -> zero cost.
        self.assertEqual(plan.chunks[0].estimated_asset_bytes, 0)
        # Chunk 2 (frames 51-75): set piece revealed -> full cost.
        self.assertEqual(plan.chunks[2].estimated_asset_bytes, 9_000_000)
        self.assertGreater(plan.max_chunk_bytes, plan.chunks[0].estimated_asset_bytes)

    def test_worker_count_exceeding_frame_count_yields_fewer_chunks(self) -> None:
        snapshot, graph = self._build([])
        plan = self.scheduler.schedule(snapshot, graph, [], frame_start=1, frame_end=3, worker_count=10)

        # Can't usefully split 3 frames across 10 workers - only 3 chunks make sense.
        self.assertEqual(len(plan.chunks), 3)
        for chunk in plan.chunks:
            self.assertEqual(chunk.frame_count, 1)

    def test_invalid_worker_count_raises(self) -> None:
        snapshot, graph = self._build([])
        with self.assertRaises(ValueError):
            self.scheduler.schedule(snapshot, graph, [], frame_start=1, frame_end=10, worker_count=0)

    def test_invalid_frame_range_raises(self) -> None:
        snapshot, graph = self._build([])
        with self.assertRaises(ValueError):
            self.scheduler.schedule(snapshot, graph, [], frame_start=10, frame_end=1, worker_count=2)


if __name__ == "__main__":
    unittest.main()
