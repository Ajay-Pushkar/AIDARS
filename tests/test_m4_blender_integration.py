import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pytest

from aidars.adapters.blender.intelligence.scene_engine import SceneEngine, SceneEngineRequest


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
        """Integration test: .blend -> package -> Blender opens packaged .blend -> loads texture -> smoke render."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # 1. Generate a source .blend file and a REAL image texture using Blender itself
            tex_dir = tmp_path / "textures"
            tex_dir.mkdir()
            tex_path = tex_dir / "wood.png"
            source_blend = tmp_path / "source.blend"
            
            create_script = f"""
import bpy

# Clear scene
bpy.ops.wm.read_factory_settings(use_empty=True)

# Create a REAL 64x64 image and save it to disk
img = bpy.data.images.new("wood.png", width=64, height=64)
img.filepath = r"{tex_path.resolve().as_posix()}"
img.file_format = 'PNG'
# Fill with red color
img.pixels = [1.0, 0.0, 0.0, 1.0] * (64 * 64)
img.save()

# Create a mesh
bpy.ops.mesh.primitive_cube_add()
cube = bpy.context.active_object
cube.name = "HeroCube"

# Set up camera and lighting so a smoke render is possible
camera_data = bpy.data.cameras.new(name='Camera')
camera_object = bpy.data.objects.new('Camera', camera_data)
bpy.context.scene.collection.objects.link(camera_object)
bpy.context.scene.camera = camera_object
camera_object.location = (0, -5, 0)
camera_object.rotation_euler = (1.5708, 0, 0)

light_data = bpy.data.lights.new(name="Light", type='POINT')
light_data.energy = 1000
light_object = bpy.data.objects.new(name="Light", object_data=light_data)
bpy.context.scene.collection.objects.link(light_object)
light_object.location = (2, -2, 2)

# Create material and texture
mat = bpy.data.materials.new(name="HeroMat")
mat.use_nodes = True
cube.data.materials.append(mat)

nodes = mat.node_tree.nodes
tex_node = nodes.new('ShaderNodeTexImage')
tex_node.name = "MyWoodTexture"

# Load image using ABSOLUTE path
img_loaded = bpy.data.images.load(filepath=r"{tex_path.resolve().as_posix()}")
img_loaded.name = "RealWoodTexture"
tex_node.image = img_loaded

# Link texture to Principled BSDF
bsdf = nodes.get('Principled BSDF')
mat.node_tree.links.new(tex_node.outputs['Color'], bsdf.inputs['Base Color'])

bpy.ops.wm.save_as_mainfile(filepath=r"{source_blend.resolve().as_posix()}")
"""
            create_py = tmp_path / "create.py"
            create_py.write_text(create_script, encoding="utf-8")
            
            subprocess.run(
                ["blender", "-b", "-P", str(create_py)],
                capture_output=True,
                check=True
            )
            
            self.assertTrue(source_blend.exists())
            self.assertTrue(tex_path.exists()) # Real PNG created
            
            # 2. Run SceneEngine with M4 packaging
            engine = SceneEngine(blender_executable="blender")
            pkg_dir = tmp_path / "out_pkg" / "pkg"
            request = SceneEngineRequest(
                input_path=str(source_blend),
                scene_output=str(tmp_path / "out_pkg" / "scene.json"),
                graph_output=str(tmp_path / "out_pkg" / "graph.json"),
                package_output=str(pkg_dir / "manifest.json"),
                build_package=True,
                optimize_package_by_visibility=True,
                camera_id="Camera"
            )
            
            result = engine.run(request)
            
            self.assertIsNotNone(result.package_plan)
            self.assertTrue(result.package_output_path.exists())
            
            pkg_blend = pkg_dir / "scene" / "Scene.blend"
            self.assertTrue(pkg_blend.exists(), "The packaged .blend should exist")
            
            manifest = json.loads(result.package_output_path.read_text(encoding="utf-8"))
            self.assertGreater(len(manifest["assets"]), 0)
            
            # 3. SMOKE RENDER in packaged .blend to prove full resolution and engine capability
            render_out = tmp_path / "render_output" # Blender adds .png automatically
            
            verify_script = f"""
import bpy
import sys

img = bpy.data.images.get("RealWoodTexture")
if not img:
    print("IMAGE_NOT_FOUND")
    sys.exit(1)

# Ensure it has dimensions (Blender located the file header)
if img.size[0] == 0 or img.size[1] == 0:
    print("IMAGE_NOT_DECODED_PROPERLY")
    sys.exit(1)

# Do a headless smoke render
bpy.context.scene.render.engine = 'CYCLES'  # or EEVEE
bpy.context.scene.render.filepath = r"{render_out.resolve().as_posix()}"
bpy.context.scene.render.resolution_x = 32
bpy.context.scene.render.resolution_y = 32

try:
    bpy.ops.render.render(write_still=True)
except Exception as e:
    print(f"RENDER_FAILED: {{e}}")
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
            self.assertIn("SUCCESS", output, f"Smoke render script failed. Output: {output}")
            self.assertTrue((tmp_path / "render_output.png").exists(), "Render output file was not produced, meaning render failed.")
