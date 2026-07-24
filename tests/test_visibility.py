import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aidars.scene_intelligence.engine import SceneIntelligenceEngine
from aidars.visibility.engine import VisibilityAnalyzer


def _object(name: str, obj_id: str, *, hide_render: bool = False, animation: dict | None = None) -> dict:
    entry = {
        "name": name,
        "id": obj_id,
        "type": "MESH",
        "visibility": {"hide_render": hide_render, "hide_viewport": False},
    }
    if animation is not None:
        entry["animation"] = animation
    return entry


BASE_SCENE = {
    "metadata": {"name": "Demo", "frame_start": 1, "frame_end": 100, "fps": 24},
    "collections": [],
    "lights": [],
    "materials": [],
    "textures": [],
    "images": [],
}


class VisibilityAnalyzerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = SceneIntelligenceEngine()
        self.analyzer = VisibilityAnalyzer()

    def test_static_visible_and_hidden_objects(self) -> None:
        scene_data = dict(BASE_SCENE)
        scene_data["objects"] = [
            _object("Visible", "obj-visible", hide_render=False),
            _object("Hidden", "obj-hidden", hide_render=True),
        ]
        snapshot = self.engine.analyze_scene_data(scene_data)

        report = self.analyzer.analyze(snapshot, frame_start=1, frame_end=24)

        self.assertTrue(report.is_visible("obj-visible"))
        self.assertFalse(report.is_visible("obj-hidden"))
        self.assertIn("obj-hidden", report.hidden_object_ids)

    def test_animated_hide_render_curve_visible_in_target_range(self) -> None:
        # Hidden for frames 1-49 (value 1.0), then revealed at frame 50 (value 0.0).
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
        scene_data = dict(BASE_SCENE)
        scene_data["objects"] = [_object("Reveal", "obj-reveal", hide_render=True, animation=animation)]
        snapshot = self.engine.analyze_scene_data(scene_data)

        # A worker rendering frames 1-10 never sees it revealed.
        early_report = self.analyzer.analyze(snapshot, frame_start=1, frame_end=10)
        self.assertFalse(early_report.is_visible("obj-reveal"))

        # A worker rendering frames 40-60 catches the reveal at frame 50.
        straddling_report = self.analyzer.analyze(snapshot, frame_start=40, frame_end=60)
        self.assertTrue(straddling_report.is_visible("obj-reveal"))

        # A worker rendering frames 60-80 (after the reveal, no keyframe in
        # range) should still see it as visible via the held value.
        late_report = self.analyzer.analyze(snapshot, frame_start=60, frame_end=80)
        self.assertTrue(late_report.is_visible("obj-reveal"))

    def test_animation_present_but_no_hide_render_curve_falls_back_to_static(self) -> None:
        animation = {
            "fcurves": 1,
            "is_animated": True,
            "curves": [
                {
                    "name": "location",
                    "data_path": "location",
                    "array_index": 0,
                    "keyframes": [{"frame": 1, "value": 0.0, "interpolation": "LINEAR"}],
                    "interpolation": "LINEAR",
                }
            ],
        }
        scene_data = dict(BASE_SCENE)
        scene_data["objects"] = [_object("Moving", "obj-moving", hide_render=True, animation=animation)]
        snapshot = self.engine.analyze_scene_data(scene_data)

        report = self.analyzer.analyze(snapshot, frame_start=1, frame_end=24)
        self.assertFalse(report.is_visible("obj-moving"))


if __name__ == "__main__":
    unittest.main()
