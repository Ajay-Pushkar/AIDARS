import sys
import json
import os
import bpy

def verify_references():
    missing = []
    
    # Check images
    for img in bpy.data.images:
        if img.filepath and not img.packed_file:
            abs_path = bpy.path.abspath(img.filepath)
            if not os.path.exists(abs_path):
                missing.append(img.filepath)

    # Check libraries
    for lib in bpy.data.libraries:
        if lib.filepath:
            abs_path = bpy.path.abspath(lib.filepath)
            if not os.path.exists(abs_path):
                missing.append(lib.filepath)

    # Check caches (Alembic)
    for cache in getattr(bpy.data, "cache_files", []):
        if cache.filepath:
            abs_path = bpy.path.abspath(cache.filepath)
            if not os.path.exists(abs_path):
                missing.append(cache.filepath)

    # Check volumes (VDB)
    for volume in getattr(bpy.data, "volumes", []):
        if volume.filepath:
            abs_path = bpy.path.abspath(volume.filepath)
            if not os.path.exists(abs_path):
                missing.append(volume.filepath)

    if missing:
        print(json.dumps({"status": "invalid", "missing_assets": missing}))
        sys.exit(1)
    else:
        print(json.dumps({"status": "valid"}))
        sys.exit(0)

if __name__ == "__main__":
    verify_references()
