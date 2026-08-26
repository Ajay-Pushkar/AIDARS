from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass(slots=True)
class CameraModel:
    """Parsed camera parameters and spatial orientation."""

    id: str = "camera-main"
    name: str = "Camera"
    location: Tuple[float, float, float] = (0.0, -10.0, 0.0)
    rotation_euler: Tuple[float, float, float] = (math.pi / 2, 0.0, 0.0)
    direction: Optional[Tuple[float, float, float]] = None
    look_at: Optional[Tuple[float, float, float]] = None
    fov_h: float = math.radians(60.0)
    fov_v: float = math.radians(36.0)
    clip_start: float = 0.1
    clip_end: float = 1000.0
    projection_type: str = "PERSPECTIVE"
    ortho_scale: float = 6.0
    aspect_ratio: float = 1.7777777777777777
    right_vec: Tuple[float, float, float] = (1.0, 0.0, 0.0)
    up_vec: Tuple[float, float, float] = (0.0, 0.0, 1.0)
    dir_vec: Tuple[float, float, float] = (0.0, 1.0, 0.0)
    is_specified: bool = True


class CameraAnalyzer:
    """Extracts, resolves, and analyzes camera data for 3D visibility calculations."""

    @classmethod
    def resolve_camera(
        cls,
        camera_input: Any,
        scene_data: Optional[Dict[str, Any]] = None,
        render_settings: Optional[Dict[str, Any]] = None,
    ) -> CameraModel:
        """Resolve any camera representation into CameraModel."""
        if isinstance(camera_input, CameraModel):
            return camera_input

        cam_dict = cls._to_dict(camera_input)
        scene_dict = cls._to_dict(scene_data) if scene_data is not None else {}
        render_dict = cls._to_dict(render_settings) if render_settings is not None else {}

        is_specified = bool(cam_dict)

        # If camera_input was just a string name/id that wasn't raw JSON
        if isinstance(camera_input, str) and scene_dict:
            found = cls._find_camera_in_scene(camera_input, scene_dict)
            if found:
                cam_dict = found
                is_specified = True

        if not cam_dict and scene_dict:
            found = cls._find_camera_in_scene("any", scene_dict)
            if found:
                cam_dict = found
                is_specified = True

        cam_id = str(cam_dict.get("id", cam_dict.get("name", "camera-main")))
        cam_name = str(cam_dict.get("name", cam_dict.get("id", "Camera")))

        # Extract transform
        transform = cam_dict.get("transform", cam_dict)
        transform_dict = cls._to_dict(transform)

        loc = cls._parse_vector3(transform_dict.get("location", [0.0, -10.0, 0.0]), default=(0.0, -10.0, 0.0))
        rot = cls._parse_vector3(transform_dict.get("rotation_euler", transform_dict.get("rotation", [math.pi / 2, 0.0, 0.0])), default=(math.pi / 2, 0.0, 0.0))
        direction = cls._parse_vector3_opt(transform_dict.get("direction", cam_dict.get("direction")))
        look_at = cls._parse_vector3_opt(transform_dict.get("look_at", cam_dict.get("look_at", transform_dict.get("target"))))

        # Camera lens / projection parameters
        cam_sub = cam_dict.get("camera", cam_dict)
        cam_info = cls._to_dict(cam_sub)

        proj_type = str(cam_info.get("type", cam_info.get("projection_type", "PERSPECTIVE"))).upper()
        try:
            ortho_scale = float(cam_info.get("ortho_scale", 6.0))
            if ortho_scale <= 0:
                ortho_scale = 6.0
        except (ValueError, TypeError):
            ortho_scale = 6.0

        # Resolution & Aspect Ratio
        try:
            res_x = float(render_dict.get("resolution_x", 1920))
            res_y = float(render_dict.get("resolution_y", 1080))
        except (ValueError, TypeError):
            res_x, res_y = 1920.0, 1080.0

        if res_x > 0 and res_y > 0:
            aspect_ratio = res_x / res_y
        else:
            aspect_ratio = 1.7777777777777777

        # FOV calculations
        fov = cam_info.get("fov", None)
        if fov is not None:
            try:
                fov_val = float(fov)
                if fov_val <= 0 or fov_val >= 360.0:
                    fov_h = math.radians(60.0)
                else:
                    fov_h = math.radians(fov_val) if fov_val > 6.28 else fov_val
            except (ValueError, TypeError):
                fov_h = math.radians(60.0)
        else:
            try:
                lens = float(cam_info.get("lens", 50.0))
            except (ValueError, TypeError):
                lens = 50.0
            try:
                sensor_width = float(cam_info.get("sensor_width", 36.0))
            except (ValueError, TypeError):
                sensor_width = 36.0

            if lens > 0 and sensor_width > 0:
                fov_h = 2.0 * math.atan(sensor_width / (2.0 * lens))
            else:
                fov_h = math.radians(60.0)

        fov_h = max(1e-4, min(fov_h, math.pi - 1e-4))
        safe_aspect = max(aspect_ratio, 1e-6)
        fov_v = 2.0 * math.atan(math.tan(fov_h / 2.0) / safe_aspect)

        try:
            clip_start = float(cam_info.get("clip_start", cam_info.get("near", render_dict.get("clip_start", 0.1))))
        except (ValueError, TypeError):
            clip_start = 0.1
        try:
            clip_end = float(cam_info.get("clip_end", cam_info.get("far", render_dict.get("clip_end", 1000.0))))
        except (ValueError, TypeError):
            clip_end = 1000.0

        # Build view basis vectors: Right (R), Up (U), Direction (D)
        R, U, D = cls._compute_view_basis(loc, rot, direction, look_at)

        return CameraModel(
            id=cam_id,
            name=cam_name,
            location=loc,
            rotation_euler=rot,
            direction=direction,
            look_at=look_at,
            fov_h=fov_h,
            fov_v=fov_v,
            clip_start=clip_start,
            clip_end=clip_end,
            projection_type=proj_type,
            ortho_scale=ortho_scale,
            aspect_ratio=aspect_ratio,
            right_vec=R,
            up_vec=U,
            dir_vec=D,
            is_specified=is_specified,
        )

    @classmethod
    def _compute_view_basis(
        cls,
        loc: Tuple[float, float, float],
        rot: Tuple[float, float, float],
        direction: Optional[Tuple[float, float, float]],
        look_at: Optional[Tuple[float, float, float]],
    ) -> Tuple[Tuple[float, float, float], Tuple[float, float, float], Tuple[float, float, float]]:
        """Compute camera coordinate space orthonormal basis (R, U, D)."""
        if look_at is not None:
            dx = look_at[0] - loc[0]
            dy = look_at[1] - loc[1]
            dz = look_at[2] - loc[2]
            length = math.sqrt(dx * dx + dy * dy + dz * dz)
            D = (dx / length, dy / length, dz / length) if length > 1e-9 else (0.0, 1.0, 0.0)
        elif direction is not None:
            dx, dy, dz = direction[0], direction[1], direction[2]
            length = math.sqrt(dx * dx + dy * dy + dz * dz)
            D = (dx / length, dy / length, dz / length) if length > 1e-9 else (0.0, 1.0, 0.0)
        else:
            rx, ry, rz = rot
            cx, sx = math.cos(rx), math.sin(rx)
            cy, sy = math.cos(ry), math.sin(ry)
            cz, sz = math.cos(rz), math.sin(rz)

            # R * [0, 0, -1] -> local -Z facing in world space
            D = (
                -(cz * sy * cx + sz * sx),
                -(sz * sy * cx - cz * sx),
                -(cy * cx),
            )
            U = (
                cz * sy * sx - sz * cx,
                sz * sy * sx + cz * cx,
                cy * sx,
            )
            R = (
                cz * cy,
                sz * cy,
                -sy,
            )
            return R, U, D

        # Construct orthonormal basis from direction vector
        up_ref = (0.0, 0.0, 1.0) if abs(D[2]) < 0.999 else (0.0, 1.0, 0.0)
        rx = D[1] * up_ref[2] - D[2] * up_ref[1]
        ry = D[2] * up_ref[0] - D[0] * up_ref[2]
        rz = D[0] * up_ref[1] - D[1] * up_ref[0]
        r_len = math.sqrt(rx * rx + ry * ry + rz * rz)
        R = (rx / r_len, ry / r_len, rz / r_len) if r_len > 1e-9 else (1.0, 0.0, 0.0)

        ux = R[1] * D[2] - R[2] * D[1]
        uy = R[2] * D[0] - R[0] * D[2]
        uz = R[0] * D[1] - R[1] * D[0]
        u_len = math.sqrt(ux * ux + uy * uy + uz * uz)
        U = (ux / u_len, uy / u_len, uz / u_len) if u_len > 1e-9 else (0.0, 1.0, 0.0)

        return R, U, D

    @classmethod
    def _find_camera_in_scene(cls, cam_ref: str, scene_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        objects = scene_dict.get("objects", [])
        if isinstance(objects, list):
            for obj in objects:
                o_dict = cls._to_dict(obj)
                if cam_ref == "any" and str(o_dict.get("type", "")).upper() == "CAMERA":
                    return o_dict
                if o_dict.get("id") == cam_ref or o_dict.get("name") == cam_ref:
                    return o_dict
                if str(o_dict.get("type", "")).upper() == "CAMERA" and (o_dict.get("name") == cam_ref or o_dict.get("id") == cam_ref):
                    return o_dict
        cameras = scene_dict.get("cameras", [])
        if isinstance(cameras, list):
            for cam in cameras:
                c_dict = cls._to_dict(cam)
                if cam_ref == "any":
                    return c_dict
                if c_dict.get("id") == cam_ref or c_dict.get("name") == cam_ref:
                    return c_dict
        active_cam = scene_dict.get("active_camera")
        if active_cam:
            ac_dict = cls._to_dict(active_cam)
            if cam_ref == "any" or ac_dict.get("id") == cam_ref or ac_dict.get("name") == cam_ref:
                return ac_dict
        return None

    @staticmethod
    def _to_dict(val: Any) -> Dict[str, Any]:
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

    @staticmethod
    def _parse_vector3(val: Any, default: Tuple[float, float, float] = (0.0, 0.0, 0.0)) -> Tuple[float, float, float]:
        if isinstance(val, (list, tuple)) and len(val) >= 3:
            try:
                return (float(val[0]), float(val[1]), float(val[2]))
            except (ValueError, TypeError):
                return default
        return default

    @staticmethod
    def _parse_vector3_opt(val: Any) -> Optional[Tuple[float, float, float]]:
        if isinstance(val, (list, tuple)) and len(val) >= 3:
            try:
                return (float(val[0]), float(val[1]), float(val[2]))
            except (ValueError, TypeError):
                return None
        return None
