from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Set

from .models import RequirementReason


class InfluenceAnalyzer:
    """Identifies scene entities that influence the render despite not being in direct camera view."""

    @classmethod
    def analyze_influences(
        cls,
        scene_data: Dict[str, Any],
        candidate_visible_ids: Set[str],
        reasons_map: Dict[str, List[str]],
        conservative: bool = True,
    ) -> Set[str]:
        """Determine all required entities due to lighting, environment, simulations, and hierarchies."""
        required_ids: Set[str] = set(candidate_visible_ids)

        objects_raw = scene_data.get("objects") or []
        objects = [cls._to_dict(o) for o in objects_raw if not isinstance(o, str)]
        obj_map = {str(o.get("id", o.get("name", ""))): o for o in objects}

        # 1. Direct Camera-Visible Objects
        for obj_id in candidate_visible_ids:
            cls._add_reason(reasons_map, obj_id, RequirementReason.CAMERA_VISIBLE)

        # 2. Parent Hierarchies (transforms of children depend on parents)
        for obj_id in list(required_ids):
            curr_id = obj_id
            visited_chain: Set[str] = {curr_id}
            while curr_id in obj_map:
                parent_id = obj_map[curr_id].get("parent")
                if parent_id and parent_id in obj_map:
                    if parent_id in visited_chain:
                        break
                    visited_chain.add(parent_id)
                    required_ids.add(parent_id)
                    cls._add_reason(reasons_map, parent_id, RequirementReason.PARENT_HIERARCHY)
                    curr_id = parent_id
                else:
                    break

        # 3. Active Lights (illumination & shadow influence)
        lights_raw = scene_data.get("lights") or []
        for light in lights_raw:
            l_dict = cls._to_dict(light)
            l_id = str(l_dict.get("id", l_dict.get("name", "light")))
            if l_id:
                required_ids.add(l_id)
                cls._add_reason(reasons_map, l_id, RequirementReason.LIGHT_SOURCE)

        for obj in objects:
            obj_type = str(obj.get("type", "")).upper()
            obj_id = str(obj.get("id", obj.get("name", "")))
            if obj_type in ("LIGHT", "LAMP", "SUN", "POINT_LIGHT", "SPOT_LIGHT", "AREA_LIGHT"):
                required_ids.add(obj_id)
                cls._add_reason(reasons_map, obj_id, RequirementReason.LIGHT_SOURCE)

        # 4. Active Modifiers & Simulation Safety
        for obj in objects:
            obj_id = str(obj.get("id", obj.get("name", "")))
            modifiers = obj.get("modifiers") or []
            has_sim = False
            for mod in modifiers:
                m_dict = cls._to_dict(mod)
                m_type = str(m_dict.get("type", "")).upper()
                if any(sim_kw in m_type for sim_kw in ("CLOTH", "FLUID", "SMOKE", "OCEAN", "COLLISION", "DYNAMIC_PAINT", "PARTICLE", "HAIR")):
                    has_sim = True
                    break

            particle_systems = obj.get("particle_systems") or []
            if particle_systems or has_sim:
                required_ids.add(obj_id)
                cls._add_reason(reasons_map, obj_id, RequirementReason.SIMULATION)

            # Armatures and Bones
            bones = obj.get("bones") or []
            if bones or str(obj.get("type", "")).upper() == "ARMATURE":
                required_ids.add(obj_id)
                cls._add_reason(reasons_map, obj_id, RequirementReason.ANIMATION_DRIVER)

            # Constraints pointing to or from required objects
            constraints = obj.get("constraints") or []
            for c in constraints:
                c_dict = cls._to_dict(c)
                target = c_dict.get("target")
                if target and (obj_id in required_ids or target in required_ids):
                    required_ids.add(obj_id)
                    if target in obj_map:
                        required_ids.add(target)
                    cls._add_reason(reasons_map, obj_id, RequirementReason.DEPENDENCY)
                    if target:
                        cls._add_reason(reasons_map, target, RequirementReason.DEPENDENCY)

        # 5. World / Environment (HDRI / background)
        world = scene_data.get("world") or scene_data.get("environment")
        if world:
            w_dict = cls._to_dict(world)
            w_id = str(w_dict.get("id", w_dict.get("name", "World")))
            required_ids.add(w_id)
            cls._add_reason(reasons_map, w_id, RequirementReason.WORLD_ENVIRONMENT)

        return required_ids

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
