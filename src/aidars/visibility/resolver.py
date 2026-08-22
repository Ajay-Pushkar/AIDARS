from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Set, Tuple

from .models import RequirementReason


class DependencyResolver:
    """Computes the dependency closure across the DependencyGraph from required objects."""

    @classmethod
    def resolve_closure(
        cls,
        required_entity_ids: Set[str],
        scene_data: Dict[str, Any],
        reasons_map: Dict[str, List[str]],
    ) -> Tuple[List[str], List[str], List[str], List[str], List[str]]:
        """Traverse scene dependencies to extract required meshes, materials, textures, and images."""
        objects = [cls._to_dict(o) for o in (scene_data.get("objects") or []) if not isinstance(o, str)]
        materials = [cls._to_dict(m) for m in (scene_data.get("materials") or []) if not isinstance(m, str)]
        textures = [cls._to_dict(t) for t in (scene_data.get("textures") or []) if not isinstance(t, str)]
        images = [cls._to_dict(i) for i in (scene_data.get("images") or []) if not isinstance(i, str)]
        nodes = [cls._to_dict(n) for n in (scene_data.get("nodes") or []) if not isinstance(n, str)]
        edges = [cls._to_dict(e) for e in (scene_data.get("edges") or []) if not isinstance(e, str)]

        # Mappings
        obj_by_id: Dict[str, Dict[str, Any]] = {}
        obj_by_name: Dict[str, Dict[str, Any]] = {}
        for obj in objects:
            if obj.get("id"):
                obj_by_id[str(obj["id"])] = obj
            if obj.get("name"):
                obj_by_name[str(obj["name"])] = obj

        mat_by_id: Dict[str, Dict[str, Any]] = {}
        mat_by_name: Dict[str, Dict[str, Any]] = {}
        mat_real_name_by_id: Dict[str, str] = {}
        for mat in materials:
            m_id = str(mat.get("id", ""))
            m_name = str(mat.get("name", ""))
            if m_id:
                mat_by_id[m_id] = mat
            if m_name:
                mat_by_name[m_name] = mat
            if m_id and m_name:
                mat_real_name_by_id[m_id] = m_name
                mat_real_name_by_id[f"material:{m_id}"] = m_name
                mat_real_name_by_id[m_id.replace("material:", "")] = m_name

        node_by_id: Dict[str, Dict[str, Any]] = {str(n.get("identifier", "")): n for n in nodes if n.get("identifier")}
        outgoing_edges: Dict[str, List[Dict[str, Any]]] = {}
        for edge in edges:
            src = str(edge.get("source", ""))
            if src:
                outgoing_edges.setdefault(src, []).append(edge)

        def resolve_mat_name(target_id: str) -> str:
            if not target_id:
                return ""
            if target_id in mat_real_name_by_id:
                return mat_real_name_by_id[target_id]
            clean_id = target_id.replace("material:", "").replace("node:", "")
            if clean_id in mat_real_name_by_id:
                return mat_real_name_by_id[clean_id]
            node = node_by_id.get(target_id, {})
            label = node.get("label")
            if label:
                return label
            return clean_id

        required_objects_list: List[str] = []
        required_meshes_set: Set[str] = set()
        required_materials_set: Set[str] = set()
        required_textures_set: Set[str] = set()
        required_images_set: Set[str] = set()

        # 1. Collect required objects in stable order
        for entity_id in required_entity_ids:
            name_or_id = entity_id
            if entity_id in obj_by_id:
                name_or_id = str(obj_by_id[entity_id].get("name") or entity_id)
            elif entity_id in node_by_id and node_by_id[entity_id].get("kind") == "object":
                name_or_id = str(node_by_id[entity_id].get("label") or entity_id)
            if name_or_id not in required_objects_list:
                required_objects_list.append(name_or_id)

        # 2. Extract meshes and direct materials from required objects
        for obj_name_or_id in required_objects_list:
            obj = obj_by_id.get(obj_name_or_id) or obj_by_name.get(obj_name_or_id)
            if not obj:
                continue

            # Mesh
            mesh = obj.get("mesh")
            if mesh:
                m_dict = cls._to_dict(mesh)
                m_name = m_dict.get("name") or str(m_dict.get("id", ""))
                if m_name:
                    required_meshes_set.add(m_name)
                    cls._add_reason(reasons_map, m_name, RequirementReason.DEPENDENCY)

            # Materials
            for mat_entry in obj.get("materials") or []:
                mat_name = ""
                if isinstance(mat_entry, str):
                    mat_name = mat_entry
                elif isinstance(mat_entry, dict) or is_dataclass(mat_entry):
                    m_dict = cls._to_dict(mat_entry)
                    mat_name = m_dict.get("name") or str(m_dict.get("id", ""))
                    # Extract inline image_textures from the embedded material dict
                    img_texs = m_dict.get("image_textures") or m_dict.get("textures") or []
                    for tex in img_texs:
                        tex_name = tex.get("name") or tex.get("source") if isinstance(tex, dict) else str(tex)
                        if tex_name:
                            required_textures_set.add(tex_name)
                            required_images_set.add(tex_name)
                            cls._add_reason(reasons_map, tex_name, RequirementReason.DEPENDENCY)

                if mat_name:
                    resolved = resolve_mat_name(mat_name)
                    required_materials_set.add(resolved)
                    cls._add_reason(reasons_map, resolved, RequirementReason.DEPENDENCY)

        # 3. Traverse DependencyGraph outgoing edges for materials & textures
        for entity_id in list(required_entity_ids) + required_objects_list:
            for edge in outgoing_edges.get(entity_id) or []:
                rel = edge.get("relationship", "")
                target = str(edge.get("target", ""))
                if not target:
                    continue
                target_node = node_by_id.get(target, {})
                target_kind = target_node.get("kind", rel)

                if target_kind == "material" or rel == "material":
                    clean_mat = resolve_mat_name(target)
                    required_materials_set.add(clean_mat)
                    cls._add_reason(reasons_map, clean_mat, RequirementReason.DEPENDENCY)
                elif target_kind in ("texture", "image") or rel in ("texture", "image"):
                    t_label = target_node.get("label", target.split(":")[-1])
                    required_textures_set.add(t_label)
                    cls._add_reason(reasons_map, t_label, RequirementReason.DEPENDENCY)

        # 4. Extract textures, images, and chained materials via BFS traversal
        visited_materials: Set[str] = set()
        materials_queue = list(required_materials_set)

        while materials_queue:
            mat_name = materials_queue.pop(0)
            if not mat_name or mat_name in visited_materials:
                continue
            visited_materials.add(mat_name)
            required_materials_set.add(mat_name)

            mat = mat_by_name.get(mat_name) or mat_by_id.get(mat_name) or mat_by_id.get(f"material:{mat_name}")
            if mat:
                img_texs = mat.get("image_textures") or mat.get("textures") or []
                for tex in img_texs:
                    tex_name = tex.get("name") or tex.get("source") if isinstance(tex, dict) else str(tex)
                    if tex_name:
                        required_textures_set.add(tex_name)
                        required_images_set.add(tex_name)
                        cls._add_reason(reasons_map, tex_name, RequirementReason.DEPENDENCY)

            # Follow graph edges starting at material:mat_name or raw mat_name
            mat_keys = [mat_name, f"material:{mat_name}"]
            for m_id, m_real in mat_real_name_by_id.items():
                if m_real == mat_name and m_id not in mat_keys:
                    mat_keys.append(m_id)

            for m_key in mat_keys:
                for edge in outgoing_edges.get(m_key) or []:
                    target = str(edge.get("target", ""))
                    if not target:
                        continue
                    rel = str(edge.get("relationship", "")).lower()
                    t_node = node_by_id.get(target, {})
                    t_kind = str(t_node.get("kind", rel)).lower()

                    if t_kind == "material" or rel == "material" or target in mat_real_name_by_id or target in mat_by_name or target in mat_by_id:
                        clean_mat = resolve_mat_name(target)
                        if clean_mat:
                            required_materials_set.add(clean_mat)
                            if clean_mat not in visited_materials:
                                materials_queue.append(clean_mat)
                            cls._add_reason(reasons_map, clean_mat, RequirementReason.DEPENDENCY)
                    elif t_kind in ("texture", "image") or rel in ("texture", "image") or target.startswith(("tex:", "image:")):
                        t_label = t_node.get("label", target.split(":")[-1])
                        if t_label:
                            required_textures_set.add(t_label)
                            required_images_set.add(t_label)
                            cls._add_reason(reasons_map, t_label, RequirementReason.DEPENDENCY)
                    else:
                        t_label = t_node.get("label", target.split(":")[-1])
                        if t_label:
                            required_textures_set.add(t_label)
                            required_images_set.add(t_label)
                            cls._add_reason(reasons_map, t_label, RequirementReason.DEPENDENCY)

        return (
            required_objects_list,
            sorted(list(required_meshes_set)),
            sorted(list(required_materials_set)),
            sorted(list(required_textures_set)),
            sorted(list(required_images_set)),
        )

    @staticmethod
    def _add_reason(reasons_map: Dict[str, List[str]], entity_id: str, reason: RequirementReason | str) -> None:
        val = reason.value if isinstance(reason, RequirementReason) else str(reason)
        if entity_id not in reasons_map:
            reasons_map[entity_id] = []
        if val not in reasons_map[entity_id]:
            reasons_map[entity_id].append(val)

    @staticmethod
    def _to_dict(val: Any) -> Dict[str, Any]:
        if is_dataclass(val) and not isinstance(val, type):
            return asdict(val)
        if isinstance(val, dict):
            return val
        if hasattr(val, "__dict__"):
            return {k: v for k, v in val.__dict__.items() if not k.startswith("_")}
        return {}
