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
            new_path = mapping.get(abs_path) or mapping.get(img.filepath) or mapping.get(img.name)
            if new_path:
                img.filepath = new_path
                changed = True

    # Remap libraries
    for lib in bpy.data.libraries:
        if lib.filepath:
            abs_path = bpy.path.abspath(lib.filepath)
            new_path = mapping.get(abs_path) or mapping.get(lib.filepath) or mapping.get(lib.name)
            if new_path:
                lib.filepath = new_path
                changed = True

    # Remap cache files (Alembic etc.)
    for cache in getattr(bpy.data, "cache_files", []):
        if cache.filepath:
            abs_path = bpy.path.abspath(cache.filepath)
            new_path = mapping.get(abs_path) or mapping.get(cache.filepath) or mapping.get(cache.name)
            if new_path:
                cache.filepath = new_path
                changed = True

    # Remap volumes (OpenVDB)
    for volume in getattr(bpy.data, "volumes", []):
        if volume.filepath:
            abs_path = bpy.path.abspath(volume.filepath)
            new_path = mapping.get(abs_path) or mapping.get(volume.filepath) or mapping.get(volume.name)
            if new_path:
                volume.filepath = new_path
                changed = True

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
