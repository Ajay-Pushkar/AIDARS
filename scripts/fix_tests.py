import re
from pathlib import Path

def fix_tests():
    engine_path = Path(r"C:\Users\kurap\.gemini\antigravity\scratch\extracted_files\AIDAR_project\AIDAR\tests\test_scene_engine.py")
    facade_path = Path(r"C:\Users\kurap\.gemini\antigravity\scratch\extracted_files\AIDAR_project\AIDAR\tests\test_scene_engine_facade.py")
    
    # 1. Fix test_scene_engine.py
    engine_content = engine_path.read_text(encoding="utf-8")
    
    # Put back the missing tests correctly
    missing_tests = """        self.assertEqual(snapshot.objects[0].transform.rotation_quaternion, [0.0, 0.0, 0.0, 1.0])
        self.assertEqual(snapshot.objects[0].transform.scale, [2.0, 2.0, 2.0])

    def test_exporter_writes_json_file(self) -> None:
        snapshot = self.engine.analyze_scene_data(self.scene_data)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "scene.json"
            JsonSceneExporter.write_json(snapshot, output_path)

            self.assertTrue(output_path.exists())
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["metadata"]["scene_name"], "Test Scene")
            self.assertEqual(payload["metadata"]["statistics"]["object_count"], 2)

    def test_dependency_graph_builder_returns_placeholder_graph(self) -> None:
        snapshot = self.engine.analyze_scene_data(self.scene_data)
        graph = DependencyGraphBuilder().build(snapshot)

        self.assertTrue(graph.nodes)"""
    
    # The bad multi-replace left it as:
    #         self.assertEqual(snapshot.objects[0].transform.rotation_euler, [0.1, 0.2, 0.3])
    #         self.assertTrue(graph.nodes)
    
    engine_content = engine_content.replace(
        "        self.assertEqual(snapshot.objects[0].transform.rotation_euler, [0.1, 0.2, 0.3])\n        self.assertTrue(graph.nodes)",
        "        self.assertEqual(snapshot.objects[0].transform.rotation_euler, [0.1, 0.2, 0.3])\n" + missing_tests
    )
    
    engine_path.write_text(engine_content, encoding="utf-8")
    
    # 2. Fix test_scene_engine_facade.py
    facade_content = facade_path.read_text(encoding="utf-8")
    
    # Fix the executable OS condition
    facade_content = facade_content.replace(
        '            fake_blender = Path(tmp_dir) / "fake_blender.sh"\n            fake_blender.write_text(f"#!/bin/sh\\ncat <<\'EOF\'\\n{fake_payload}\\nEOF\\n", encoding="utf-8")\n            fake_blender.chmod(fake_blender.stat().st_mode | 0o111)',
        '            if os.name == "nt":\n                fake_blender = Path(tmp_dir) / "fake_blender.cmd"\n                fake_blender.write_text(f"@echo off\\r\\necho {fake_payload}", encoding="utf-8")\n            else:\n                fake_blender = Path(tmp_dir) / "fake_blender.sh"\n                fake_blender.write_text(f"#!/bin/sh\\ncat <<\'EOF\'\\n{fake_payload}\\nEOF\\n", encoding="utf-8")\n                fake_blender.chmod(fake_blender.stat().st_mode | 0o111)'
    )
    # Add import os at the top of test_scene_engine_facade.py if missing
    if "import os" not in facade_content:
        facade_content = facade_content.replace("import sys", "import os\nimport sys")
        
    facade_path.write_text(facade_content, encoding="utf-8")
    
if __name__ == "__main__":
    fix_tests()
