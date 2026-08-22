from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Set

from .camera import CameraAnalyzer, CameraModel
from .eligibility import EligibilityAnalyzer
from .geometry import CullingResult, FrustumCuller
from .influence import InfluenceAnalyzer
from .models import RenderRequest, RenderRequirementReport, RequirementReason
from .resolver import DependencyResolver


class RenderRequirementAnalyzer:
    """Orchestrates comprehensive 3D render requirement analysis for AIDAR."""

    def __init__(self) -> None:
        self.eligibility_analyzer = EligibilityAnalyzer()
        self.camera_analyzer = CameraAnalyzer()
        self.frustum_culler = FrustumCuller()
        self.influence_analyzer = InfluenceAnalyzer()
        self.dependency_resolver = DependencyResolver()

    def analyze(
        self,
        snapshot: Any,
        dependency_graph: Any = None,
        request: Optional[RenderRequest | Dict[str, Any]] = None,
        active_camera: Any = None,
        render_settings: Any = None,
    ) -> RenderRequirementReport:
        """Analyze a scene to produce the minimum set of assets required for rendering."""
        # 1. Normalize Inputs
        scene_dict = self._normalize_dict(snapshot)
        graph_dict = self._normalize_dict(dependency_graph) if dependency_graph is not None else {}
        render_dict = self._normalize_dict(render_settings) if render_settings is not None else {}

        # Merge graph nodes/edges/materials into scene_dict if separate
        combined_data = dict(scene_dict)
        if graph_dict:
            for k, v in graph_dict.items():
                if k not in combined_data or not combined_data[k]:
                    combined_data[k] = v

        # If combined_data has nodes but no objects array (e.g. pure DependencyGraph input), reconstruct objects
        if not combined_data.get("objects") and combined_data.get("nodes"):
            reconstructed_objs = []
            for n in combined_data["nodes"]:
                n_dict = self._normalize_dict(n)
                if n_dict.get("kind") == "object":
                    reconstructed_objs.append({
                        "id": n_dict.get("identifier", ""),
                        "name": n_dict.get("label", n_dict.get("identifier", "")),
                        "visibility": {"hide_render": False},
                        "is_pure_graph_node": True,
                    })
            combined_data["objects"] = reconstructed_objs

        # 2. Build RenderRequest
        render_req = self._resolve_render_request(request, active_camera, render_dict, combined_data)

        # 3. Eligibility Analysis (M3.1: static hide_render + animated curves)
        eligible_obj_ids = self._evaluate_eligibility(combined_data, render_req)

        # 4. Camera Analysis (M3.3)
        cam_source = active_camera if active_camera is not None else render_req.camera_id
        if not cam_source and combined_data.get("active_camera"):
            cam_source = combined_data["active_camera"]

        camera_model = self.camera_analyzer.resolve_camera(
            camera_input=cam_source,
            scene_data=combined_data,
            render_settings=render_dict,
        )

        # 5. Frustum Culling & Geometric Visibility (M3.4 - M3.6)
        candidate_visible_ids: Set[str] = set()
        reasons_map: Dict[str, List[str]] = {}
        all_objects = combined_data.get("objects") or []
        all_obj_dicts = [self._normalize_dict(o) for o in all_objects if not isinstance(o, str)]

        # Collect candidate objects and compute bounding volumes
        frustum_candidates = []
        for obj in all_obj_dicts:
            obj_id = str(obj.get("id", obj.get("name", "Unknown")))
            obj_name = str(obj.get("name", obj_id))

            # If not eligible due to hide_render across the requested range, skip direct visibility
            if obj_id not in eligible_obj_ids and obj_name not in eligible_obj_ids:
                continue

            # Don't cull the active camera object itself via bounding box
            obj_type = str(obj.get("type", "")).upper()
            if obj_type == "CAMERA":
                continue

            # If pure graph node with no spatial metadata, conservatively include it
            if obj.get("is_pure_graph_node"):
                candidate_visible_ids.add(obj_id)
                candidate_visible_ids.add(obj_name)
                continue

            bounds = self.frustum_culler.compute_world_bounds(obj)
            cull_res = self.frustum_culler.test_frustum(bounds, camera_model)

            if cull_res in (CullingResult.INSIDE, CullingResult.INTERSECTING):
                cam_loc = camera_model.location
                cam_dir = camera_model.dir_vec
                cam_depth = min(
                    (p[0] - cam_loc[0]) * cam_dir[0]
                    + (p[1] - cam_loc[1]) * cam_dir[1]
                    + (p[2] - cam_loc[2]) * cam_dir[2]
                    for p in bounds.corners
                )
                frustum_candidates.append({
                    "id": obj_id,
                    "name": obj_name,
                    "bounds": bounds,
                    "depth": cam_depth,
                })

        # Front-to-back sorting for raycast occlusion testing
        frustum_candidates.sort(key=lambda item: item["depth"])
        opaque_blockers = []
        for cand in frustum_candidates:
            if not self.frustum_culler.test_occlusion(cand["bounds"], camera_model, opaque_blockers):
                candidate_visible_ids.add(cand["id"])
                candidate_visible_ids.add(cand["name"])
                opaque_blockers.append(cand["bounds"])

        # 6. Render Influence Analysis (M3.7 - M3.12: Lights, Hierarchy, Simulations, HDRI)
        required_entity_ids = self.influence_analyzer.analyze_influences(
            scene_data=combined_data,
            candidate_visible_ids=candidate_visible_ids,
            reasons_map=reasons_map,
            conservative=render_req.conservative,
        )

        # 7. Dependency Closure (M3.8: Graph traversal for Meshes, Materials, Textures, Images)
        req_objects, req_meshes, req_materials, req_textures, req_images = self.dependency_resolver.resolve_closure(
            required_entity_ids=required_entity_ids,
            scene_data=combined_data,
            reasons_map=reasons_map,
        )

        # 8. Set Inversion for Unused Objects
        all_object_identifiers = []
        for obj in all_obj_dicts:
            name = str(obj.get("name") or obj.get("id", ""))
            if name and name not in all_object_identifiers:
                all_object_identifiers.append(name)

        unused_objects = [name for name in all_object_identifiers if name not in req_objects]

        # 9. Extract Required Lights and Cameras
        required_lights = []
        for l in combined_data.get("lights") or []:
            l_dict = self._normalize_dict(l)
            l_id = str(l_dict.get("id", l_dict.get("name", "light")))
            if l_id and l_id not in required_lights:
                required_lights.append(l_id)

        required_cameras = [camera_model.id] if camera_model.id else []

        # 10. Assemble Final RenderRequirementReport
        report = RenderRequirementReport(
            request=render_req,
            required_objects=req_objects,
            required_meshes=req_meshes,
            required_materials=req_materials,
            required_textures=req_textures,
            required_images=req_images,
            required_lights=required_lights,
            required_cameras=required_cameras,
            required_libraries=[],
            required_simulation_caches=[],
            unused_objects=unused_objects,
            reasons=reasons_map,
            conservative=render_req.conservative,
            statistics={
                "total_objects": len(all_object_identifiers),
                "required_objects_count": len(req_objects),
                "unused_objects_count": len(unused_objects),
                "required_materials_count": len(req_materials),
                "required_textures_count": len(req_textures),
            },
        )
        return report

    def _resolve_render_request(
        self,
        request: Any,
        active_camera: Any,
        render_dict: Dict[str, Any],
        scene_data: Dict[str, Any],
    ) -> RenderRequest:
        if isinstance(request, RenderRequest):
            return request

        req_dict = self._normalize_dict(request) if request is not None else {}
        cam_id = req_dict.get("camera_id", req_dict.get("camera", ""))
        if not cam_id and active_camera:
            if isinstance(active_camera, str):
                try:
                    parsed = json.loads(active_camera)
                    if isinstance(parsed, dict):
                        cam_id = parsed.get("id", parsed.get("name", "camera-main"))
                    else:
                        cam_id = active_camera
                except Exception:
                    cam_id = active_camera
            elif isinstance(active_camera, dict):
                cam_id = active_camera.get("id", active_camera.get("name", "camera-main"))

        metadata = scene_data.get("metadata", {})
        if is_dataclass(metadata):
            metadata = asdict(metadata)
        elif not isinstance(metadata, dict):
            metadata = {}

        f_start = int(req_dict.get("frame_start", metadata.get("frame_start", 1)))
        f_end = int(req_dict.get("frame_end", metadata.get("frame_end", f_start)))
        res_x = int(render_dict.get("resolution_x", req_dict.get("resolution_x", 1920)))
        res_y = int(render_dict.get("resolution_y", req_dict.get("resolution_y", 1080)))

        return RenderRequest(
            camera_id=str(cam_id),
            frame_start=f_start,
            frame_end=f_end,
            resolution=(res_x, res_y),
            view_layer=str(req_dict.get("view_layer", "ViewLayer")),
            scene_name=str(metadata.get("name", "")),
            conservative=bool(req_dict.get("conservative", True)),
        )

    def _evaluate_eligibility(self, scene_data: Dict[str, Any], request: RenderRequest) -> Set[str]:
        eligible_ids: Set[str] = set()
        objects = scene_data.get("objects") or []
        for obj in objects:
            o_dict = self._normalize_dict(obj)
            obj_id = str(o_dict.get("id", o_dict.get("name", "")))
            obj_name = str(o_dict.get("name", obj_id))

            # If object is a typed SceneObject
            if hasattr(obj, "visibility") and hasattr(obj, "animation"):
                is_elig = self.eligibility_analyzer._is_eligible_in_range(obj, request.frame_start, request.frame_end)
                if is_elig:
                    eligible_ids.add(obj_id)
                    eligible_ids.add(obj_name)
                continue

            # Dict payload evaluation
            visibility = o_dict.get("visibility", {})
            static_hidden = bool(visibility.get("hide_render", False)) if isinstance(visibility, dict) else False

            # Check animation curve for hide_render if present
            anim = o_dict.get("animation", {})
            curves = anim.get("curves", []) if isinstance(anim, dict) else []
            hide_curve = next((c for c in curves if isinstance(c, dict) and c.get("data_path") == "hide_render"), None)

            if hide_curve is None:
                if not static_hidden:
                    eligible_ids.add(obj_id)
                    eligible_ids.add(obj_name)
            else:
                keyframes = sorted(hide_curve.get("keyframes", []), key=lambda k: k.get("frame", 0))
                if not keyframes:
                    if not static_hidden:
                        eligible_ids.add(obj_id)
                        eligible_ids.add(obj_name)
                else:
                    in_range = [k.get("value", 0.0) for k in keyframes if request.frame_start <= k.get("frame", 0) <= request.frame_end]
                    if in_range:
                        if any(v < 0.5 for v in in_range):
                            eligible_ids.add(obj_id)
                            eligible_ids.add(obj_name)
                    else:
                        held = keyframes[0].get("value", 0.0)
                        for k in keyframes:
                            if k.get("frame", 0) <= request.frame_start:
                                held = k.get("value", 0.0)
                            else:
                                break
                        if held < 0.5:
                            eligible_ids.add(obj_id)
                            eligible_ids.add(obj_name)

        return eligible_ids

    @staticmethod
    def _normalize_dict(val: Any) -> Dict[str, Any]:
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
            return {"id": val, "name": val}
        if is_dataclass(val) and not isinstance(val, type):
            return asdict(val)
        if isinstance(val, dict):
            return val
        if hasattr(val, "__dict__"):
            return {k: v for k, v in val.__dict__.items() if not k.startswith("_")}
        return {}
