import re
from pathlib import Path

def patch_exporter():
    file_path = Path(r"C:\Users\kurap\.gemini\antigravity\scratch\extracted_files\AIDAR_project\AIDAR\src\aidars\scene_intelligence\exporters.py")
    content = file_path.read_text(encoding="utf-8")
    
    new_method = '''    @staticmethod
    def _to_serializable(snapshot: SceneSnapshot) -> dict[str, Any]:
        meshes = []
        cameras = []
        for obj in snapshot.objects:
            if obj.mesh:
                meshes.append({
                    "id": f"mesh_{obj.id}",
                    "name": obj.mesh.name,
                    "vertex_count": obj.mesh.vertex_count,
                    "edge_count": obj.mesh.edge_count,
                    "face_count": obj.mesh.face_count,
                    "uv_layers": [],
                    "vertex_groups": [],
                    "shape_keys": [],
                    "material_slots": []
                })
            if obj.camera:
                cameras.append({
                    "id": f"camera_{obj.id}",
                    "name": obj.name,
                    "type": obj.camera.get("type", "Perspective"),
                    "lens": obj.camera.get("lens", 0),
                    "clip_start": obj.camera.get("clip_start", 0),
                    "clip_end": obj.camera.get("clip_end", 0),
                    "location": obj.transform.location if obj.transform else [0,0,0],
                    "rotation": obj.transform.rotation_euler if obj.transform else [0,0,0]
                })

        return {
            "schema_version": "v1.0.0",
            "metadata": {
                "scene_name": snapshot.metadata.name,
                "scene_path": getattr(snapshot.metadata, "scene_path", ""),
                "engine": snapshot.metadata.render_engine,
                "format": getattr(snapshot.metadata, "format", ""),
                "unit": snapshot.metadata.units,
                "up_axis": getattr(snapshot.metadata, "up_axis", ""),
                "created_at": getattr(snapshot.metadata, "created_at", ""),
                "modified_at": getattr(snapshot.metadata, "modified_at", ""),
                "statistics": {
                    "object_count": snapshot.statistics.object_count,
                    "mesh_count": sum(1 for obj in snapshot.objects if obj.mesh),
                    "material_count": snapshot.statistics.material_count,
                    "texture_count": snapshot.statistics.texture_count,
                    "image_count": snapshot.statistics.image_count,
                    "light_count": snapshot.statistics.light_count,
                    "camera_count": snapshot.statistics.camera_count,
                    "collection_count": snapshot.statistics.collection_count,
                }
            },
            "objects": [
                {
                    "id": obj.id,
                    "name": obj.name,
                    "type": obj.type,
                    "collection": obj.collection or "",
                    "parent": obj.parent or "",
                    "children": obj.children,
                    "visible": not obj.visibility.hide_render and not obj.visibility.hide_viewport,
                    "renderable": not obj.visibility.hide_render,
                    "transform": {
                        "location": obj.transform.location if obj.transform else [0,0,0],
                        "rotation": obj.transform.rotation_euler if obj.transform else [0,0,0],
                        "scale": obj.transform.scale if obj.transform else [1,1,1],
                    },
                    "mesh": f"mesh_{obj.id}" if obj.mesh else "",
                    "materials": [m.name for m in obj.materials],
                    "modifiers": [m.name for m in obj.modifiers],
                    "constraints": [c.name for c in obj.constraints],
                    "custom_properties": {}
                }
                for obj in snapshot.objects
            ],
            "collections": [
                {
                    "id": collection.id,
                    "name": collection.name,
                    "parent": collection.parent or "",
                    "children": [],
                    "objects": [o.id for o in snapshot.objects if o.collection == collection.id]
                }
                for collection in snapshot.collections
            ],
            "meshes": meshes,
            "materials": [
                {
                    "id": getattr(material, "id", material.name),
                    "name": material.name,
                    "shader": material.shader,
                    "blend_mode": material.settings.get("blend_mode", ""),
                    "double_sided": material.settings.get("double_sided", True),
                    "textures": material.image_textures,
                    "referenced_by": []
                }
                for material in snapshot.materials
            ],
            "textures": [
                {
                    "id": tex.get("id", tex.get("name", "")),
                    "name": tex.get("name", ""),
                    "type": tex.get("type", "Image"),
                    "image": tex.get("image", ""),
                    "color_space": tex.get("color_space", ""),
                    "mapping": tex.get("mapping", {}),
                    "referenced_by": []
                }
                for tex in snapshot.textures
            ],
            "images": [
                {
                    "id": img.get("id", img.get("name", "")),
                    "name": img.get("name", ""),
                    "filepath": img.get("filepath", ""),
                    "width": img.get("width", 0),
                    "height": img.get("height", 0),
                    "format": img.get("format", ""),
                    "used_by": []
                }
                for img in snapshot.images
            ],
            "lights": [
                {
                    "id": light.get("id", light.get("name", "")),
                    "name": light.get("name", ""),
                    "type": light.get("type", "Point"),
                    "color": light.get("color", [1,1,1]),
                    "energy": light.get("energy", 0),
                    "radius": light.get("radius", 0),
                    "location": light.get("location", [0,0,0]),
                    "rotation": light.get("rotation", [0,0,0])
                }
                for light in snapshot.lights
            ],
            "cameras": cameras,
            "summary": {
                "objects": [obj.id for obj in snapshot.objects],
                "collections": [col.id for col in snapshot.collections],
                "materials": [mat.name for mat in snapshot.materials],
                "textures": [tex.get("id", tex.get("name", "")) for tex in snapshot.textures],
                "images": [img.get("id", img.get("name", "")) for img in snapshot.images],
                "lights": [light.get("id", light.get("name", "")) for light in snapshot.lights],
                "cameras": [c["id"] for c in cameras],
                "relationships": [
                    {
                        "from": rel.source,
                        "to": rel.target,
                        "relationship": rel.relationship,
                    }
                    for rel in snapshot.relationships
                ]
            }
        }'''
    
    # We will replace from "    @staticmethod\n    def _to_serializable" up to the end of the method body before "class DependencyGraphExporter:"
    
    pattern = re.compile(r'    @staticmethod\n    def _to_serializable\(snapshot: SceneSnapshot\) -> dict\[str, Any\]:.*?(?=    @staticmethod|class DependencyGraphExporter:)', re.DOTALL)
    
    new_content, count = pattern.subn(new_method + "\n\n\n", content, count=1)
    if count == 1:
        file_path.write_text(new_content, encoding="utf-8")
        print("Successfully patched exporters.py")
    else:
        print("Failed to find pattern in exporters.py")

if __name__ == "__main__":
    patch_exporter()
