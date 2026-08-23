import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pytest

from aidars.scene_intelligence.scene_engine import SceneEngine, SceneEngineRequest
from aidars.scene_intelligence.scene_engine import SceneEngine


class BlenderIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            subprocess.run(["blender", "--version"], capture_output=True, check=True)
            cls.blender_available = True
        except (FileNotFoundError, subprocess.CalledProcessError):
            cls.blender_available = False

    def setUp(self) -> None:
        if not self.blender_available:
            self.skipTest("Blender not found on PATH")

    def test_end_to_end_m4_blender_packaging(self) -> None:
        """Integration test: .blend -> package -> Blender opens packaged .blend -> textures resolve."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # 1. Create a dummy texture
            tex_dir = tmp_path / "textures"
            tex_dir.mkdir()
            tex_path = tex_dir / "wood.png"
            # Just create a fake 1x1 png or simple file (Blender will load empty if it's invalid, 
            # but we just need it to exist physically and have Blender see the filepath)
            tex_path.write_bytes(b"dummy")
            
            # 2. Generate a source .blend file
            source_blend = tmp_path / "source.blend"
            
            create_script = f"""
import bpy

# Clear scene
bpy.ops.wm.read_factory_settings(use_empty=True)

# Create a mesh
bpy.ops.mesh.primitive_cube_add()
cube = bpy.context.active_object
cube.name = "HeroCube"

# Create material and texture
mat = bpy.data.materials.new(name="HeroMat")
mat.use_nodes = True
cube.data.materials.append(mat)

nodes = mat.node_tree.nodes
tex_node = nodes.new('ShaderNodeTexImage')
tex_node.name = "MyWoodTexture"

# Load image using ABSOLUTE path
# Then we rely on the M4 packaging to remap it
img = bpy.data.images.load(filepath=r"{tex_path.resolve().as_posix()}")
tex_node.image = img

bpy.ops.wm.save_as_mainfile(filepath=r"{source_blend.resolve().as_posix()}")
"""
            create_py = tmp_path / "create.py"
            create_py.write_text(create_script, encoding="utf-8")
            
            subprocess.run(
                ["blender", "-b", "-P", str(create_py)],
                capture_output=True,
                check=True
            )
            
            # Ensure it created the source .blend
            self.assertTrue(source_blend.exists())
            
            # 3. Run SceneEngine with M4 packaging
            engine = SceneEngine(blender_executable="blender")
            pkg_dir = tmp_path / "out_pkg" / "pkg"
            request = SceneEngineRequest(
                input_path=str(source_blend),
                scene_output=str(tmp_path / "out_pkg" / "scene.json"),
                graph_output=str(tmp_path / "out_pkg" / "graph.json"),
                package_output=str(pkg_dir / "manifest.json"),
                build_package=True,
                optimize_package_by_visibility=True,
            )
            
            result = engine.run(request)
            
            self.assertIsNotNone(result.package_plan)
            self.assertTrue(result.package_output_path.exists())
            
            # The package directory should contain the .blend and the remapped texture
            pkg_blend = pkg_dir / "scene" / "source.blend"
            self.assertTrue(pkg_blend.exists(), "The packaged .blend should exist")
            
            # Read manifest
            manifest = json.loads(result.package_output_path.read_text(encoding="utf-8"))
            self.assertGreater(len(manifest["assets"]), 0)
            
            # 4. Run Blender against the PACKAGED .blend to verify external reference remapping
            verify_script = f"""
import bpy
import sys

img = bpy.data.images.get("wood.png")
if not img:
    print("IMAGE_NOT_FOUND")
    sys.exit(1)

filepath = img.filepath
print(f"TEXTURE_FILEPATH: {{filepath}}")

# Verify it starts with //
if not filepath.startswith("//"):
    print("ERROR_NOT_RELATIVE")
    sys.exit(1)

# Verify the file physically exists relative to the .blend
import os
abs_path = bpy.path.abspath(filepath)
if not os.path.exists(abs_path):
    print("ERROR_FILE_MISSING")
    sys.exit(1)

print("SUCCESS")
"""
            verify_py = tmp_path / "verify.py"
            verify_py.write_text(verify_script, encoding="utf-8")
            
            proc = subprocess.run(
                ["blender", "-b", str(pkg_blend), "-P", str(verify_py)],
                capture_output=True,
                text=True
            )
            
            output = proc.stdout + proc.stderr
            self.assertIn("SUCCESS", output, f"Verification script failed. Output: {output}")
            self.assertIn("TEXTURE_FILEPATH: //", output)
