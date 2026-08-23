import sys
import json
import bpy

def remap_paths(mapping_file):
    with open(mapping_file, "r") as f:
        mapping = json.load(f)

    changed = False
    
    # Remap images
    for img in bpy.data.images:
        if img.filepath:
            abs_path = bpy.path.abspath(img.filepath)
            for orig, new_path in mapping.items():
                if abs_path == orig or img.filepath == orig:
                    img.filepath = new_path
                    changed = True
                    break

    # Remap libraries
    for lib in bpy.data.libraries:
        if lib.filepath:
            abs_path = bpy.path.abspath(lib.filepath)
            for orig, new_path in mapping.items():
                if abs_path == orig or lib.filepath == orig:
                    lib.filepath = new_path
                    changed = True
                    break

    # Remap cache files (Alembic etc.)
    for cache in getattr(bpy.data, "cache_files", []):
        if cache.filepath:
            abs_path = bpy.path.abspath(cache.filepath)
            for orig, new_path in mapping.items():
                if abs_path == orig or cache.filepath == orig:
                    cache.filepath = new_path
                    changed = True
                    break

    # Remap volumes (OpenVDB)
    for volume in getattr(bpy.data, "volumes", []):
        if volume.filepath:
            abs_path = bpy.path.abspath(volume.filepath)
            for orig, new_path in mapping.items():
                if abs_path == orig or volume.filepath == orig:
                    volume.filepath = new_path
                    changed = True
                    break

    if changed:
        bpy.ops.wm.save_mainfile()

if __name__ == "__main__":
    argv = sys.argv
    if "--" not in argv:
        sys.exit(0)
    args = argv[argv.index("--") + 1:]
    if args:
        mapping_file = args[0]
        remap_paths(mapping_file)
