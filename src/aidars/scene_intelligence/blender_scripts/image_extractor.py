import bpy

def extract_images():
    images = []
    for img in bpy.data.images:
        images.append({
            "name": img.name,
            "id": img.name,
            "filepath": img.filepath,
            "source": getattr(img, "source", "FILE")
        })
    return images
