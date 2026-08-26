from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from .camera import CameraModel


class CullingResult(str, Enum):
    OUTSIDE = "OUTSIDE"
    INTERSECTING = "INTERSECTING"
    INSIDE = "INSIDE"


@dataclass(slots=True)
class BoundingBox:
    """Axis-aligned bounding box representation with 8 world corner vertices."""

    min_pt: Tuple[float, float, float]
    max_pt: Tuple[float, float, float]
    corners: List[Tuple[float, float, float]] = field(default_factory=list)

    @classmethod
    def from_corners(cls, corners: List[Tuple[float, float, float]]) -> BoundingBox:
        if not corners:
            corners = [(-0.5, -0.5, -0.5), (0.5, 0.5, 0.5)]
        min_x = min(p[0] for p in corners)
        min_y = min(p[1] for p in corners)
        min_z = min(p[2] for p in corners)
        max_x = max(p[0] for p in corners)
        max_y = max(p[1] for p in corners)
        max_z = max(p[2] for p in corners)
        return cls(min_pt=(min_x, min_y, min_z), max_pt=(max_x, max_y, max_z), corners=corners)


class FrustumCuller:
    """Performs geometric frustum culling and conservative raycast occlusion testing."""

    @classmethod
    def compute_world_bounds(cls, obj: Any) -> BoundingBox:
        """Compute the world-space bounding box corners for a SceneObject or dictionary."""
        obj_dict = cls._to_dict(obj)
        transform_dict = cls._to_dict(obj_dict.get("transform", {}))

        loc = cls._parse_vec3(transform_dict.get("location", [0.0, 0.0, 0.0]), (0.0, 0.0, 0.0))
        rot = cls._parse_vec3(transform_dict.get("rotation_euler", transform_dict.get("rotation", [0.0, 0.0, 0.0])), (0.0, 0.0, 0.0))
        scale = cls._parse_vec3(transform_dict.get("scale", [1.0, 1.0, 1.0]), (1.0, 1.0, 1.0))

        bound_box = obj_dict.get("bound_box", obj_dict.get("bounding_box", None))
        local_corners = cls._get_local_corners(bound_box)

        world_corners: List[Tuple[float, float, float]] = []
        for pt in local_corners:
            # Scale -> Rotate (Euler ZYX/XYZ) -> Translate
            scaled = (pt[0] * scale[0], pt[1] * scale[1], pt[2] * scale[2])
            rotated = cls._rotate_euler(scaled, rot)
            world_pt = (
                rotated[0] + loc[0],
                rotated[1] + loc[1],
                rotated[2] + loc[2],
            )
            world_corners.append(world_pt)

        return BoundingBox.from_corners(world_corners)

    @classmethod
    def test_frustum(
        cls,
        bounds: BoundingBox,
        camera: CameraModel,
        conservative_tolerance: float = 1e-4,
    ) -> CullingResult:
        """Test a world-space bounding box against the camera viewing frustum."""
        cam_loc = camera.location
        R = camera.right_vec
        U = camera.up_vec
        D = camera.dir_vec

        # Transform world corners to camera coordinate space
        cam_corners = [cls._world_to_cam_space(p, cam_loc, R, U, D) for p in bounds.corners]

        z_cams = [p[2] for p in cam_corners]
        x_cams = [p[0] for p in cam_corners]
        y_cams = [p[1] for p in cam_corners]

        near = camera.clip_start - conservative_tolerance
        far = camera.clip_end + conservative_tolerance

        if camera.projection_type == "ORTHOGRAPHIC":
            half_w = (camera.ortho_scale * camera.aspect_ratio / 2.0) + conservative_tolerance
            half_h = (camera.ortho_scale / 2.0) + conservative_tolerance

            # Near / Far
            if all(z < near for z in z_cams) or all(z > far for z in z_cams):
                return CullingResult.OUTSIDE
            # Left / Right
            if all(x < -half_w for x in x_cams) or all(x > half_w for x in x_cams):
                return CullingResult.OUTSIDE
            # Bottom / Top
            if all(y < -half_h for y in y_cams) or all(y > half_h for y in y_cams):
                return CullingResult.OUTSIDE

            # Check if all corners are strictly inside
            all_inside = (
                all(near <= z <= far for z in z_cams)
                and all(-half_w <= x <= half_w for x in x_cams)
                and all(-half_h <= y <= half_h for y in y_cams)
            )
            return CullingResult.INSIDE if all_inside else CullingResult.INTERSECTING

        # Perspective projection
        tan_h = math.tan(camera.fov_h / 2.0)
        tan_v = math.tan(camera.fov_v / 2.0)

        # Near / Far plane tests
        if all(z < near for z in z_cams) or all(z > far for z in z_cams):
            return CullingResult.OUTSIDE

        # Left / Right plane tests (x vs z * tan_h)
        if all(x < (-z * tan_h - conservative_tolerance) for x, z in zip(x_cams, z_cams)):
            return CullingResult.OUTSIDE
        if all(x > (z * tan_h + conservative_tolerance) for x, z in zip(x_cams, z_cams)):
            return CullingResult.OUTSIDE

        # Bottom / Top plane tests (y vs z * tan_v)
        if all(y < (-z * tan_v - conservative_tolerance) for y, z in zip(y_cams, z_cams)):
            return CullingResult.OUTSIDE
        if all(y > (z * tan_v + conservative_tolerance) for y, z in zip(y_cams, z_cams)):
            return CullingResult.OUTSIDE

        # Determine if strictly INSIDE vs INTERSECTING
        all_inside = True
        for x, y, z in zip(x_cams, z_cams, y_cams):
            if not (near <= z <= far and -z * tan_h <= x <= z * tan_h and -z * tan_v <= y <= z * tan_v):
                all_inside = False
                break

        return CullingResult.INSIDE if all_inside else CullingResult.INTERSECTING

    @classmethod
    def test_occlusion(
        cls,
        candidate_bounds: BoundingBox,
        camera: CameraModel,
        opaque_blockers: List[BoundingBox],
    ) -> bool:
        """Conservative raycast occlusion query.
        
        Returns True only if candidate is conclusively occluded by closer opaque blockers.
        """
        if not opaque_blockers:
            return False

        cam_p = camera.location
        corners = candidate_bounds.corners
        cx = sum(p[0] for p in corners) / len(corners)
        cy = sum(p[1] for p in corners) / len(corners)
        cz = sum(p[2] for p in corners) / len(corners)
        sample_points = [(cx, cy, cz)] + corners

        for sp in sample_points:
            vx = sp[0] - cam_p[0]
            vy = sp[1] - cam_p[1]
            vz = sp[2] - cam_p[2]
            dist_to_target = math.sqrt(vx * vx + vy * vy + vz * vz)
            if dist_to_target < 1e-6:
                return False

            ray_dir = (vx / dist_to_target, vy / dist_to_target, vz / dist_to_target)
            point_is_blocked = False

            for blocker in opaque_blockers:
                b_min = blocker.min_pt
                b_max = blocker.max_pt

                # If camera origin is inside this blocker, ignore it to prevent false positive cascade
                if cls._point_in_aabb(cam_p, b_min, b_max):
                    continue

                hit_t = cls._ray_intersects_aabb(cam_p, ray_dir, b_min, b_max)
                if hit_t is not None and 1e-4 < hit_t < dist_to_target - 1e-4:
                    point_is_blocked = True
                    break

            if not point_is_blocked:
                # If any sample point has a clear line of sight, object is NOT occluded
                return False

        return True

    @staticmethod
    def _world_to_cam_space(
        world_pt: Tuple[float, float, float],
        cam_loc: Tuple[float, float, float],
        R: Tuple[float, float, float],
        U: Tuple[float, float, float],
        D: Tuple[float, float, float],
    ) -> Tuple[float, float, float]:
        rel_x = world_pt[0] - cam_loc[0]
        rel_y = world_pt[1] - cam_loc[1]
        rel_z = world_pt[2] - cam_loc[2]
        x_cam = rel_x * R[0] + rel_y * R[1] + rel_z * R[2]
        y_cam = rel_x * U[0] + rel_y * U[1] + rel_z * U[2]
        z_cam = rel_x * D[0] + rel_y * D[1] + rel_z * D[2]
        return (x_cam, y_cam, z_cam)

    @staticmethod
    def _rotate_euler(v: Tuple[float, float, float], rot: Tuple[float, float, float]) -> Tuple[float, float, float]:
        rx, ry, rz = rot
        cx, sx = math.cos(rx), math.sin(rx)
        cy, sy = math.cos(ry), math.sin(ry)
        cz, sz = math.cos(rz), math.sin(rz)
        x, y, z = v
        # Rx
        x1, y1, z1 = x, y * cx - z * sx, y * sx + z * cx
        # Ry
        x2, y2, z2 = x1 * cy + z1 * sy, y1, -x1 * sy + z1 * cy
        # Rz
        x3, y3, z3 = x2 * cz - y2 * sz, x2 * sz + y2 * cz, z2
        return (x3, y3, z3)

    @staticmethod
    def _point_in_aabb(
        pt: Tuple[float, float, float],
        b_min: Tuple[float, float, float],
        b_max: Tuple[float, float, float],
    ) -> bool:
        return b_min[0] <= pt[0] <= b_max[0] and b_min[1] <= pt[1] <= b_max[1] and b_min[2] <= pt[2] <= b_max[2]

    @staticmethod
    def _ray_intersects_aabb(
        origin: Tuple[float, float, float],
        direction: Tuple[float, float, float],
        aabb_min: Tuple[float, float, float],
        aabb_max: Tuple[float, float, float],
    ) -> Optional[float]:
        t_min = -1e30
        t_max = 1e30
        for i in range(3):
            d = direction[i]
            if abs(d) < 1e-9:
                if origin[i] < aabb_min[i] or origin[i] > aabb_max[i]:
                    return None
            else:
                inv_d = 1.0 / d
                t1 = (aabb_min[i] - origin[i]) * inv_d
                t2 = (aabb_max[i] - origin[i]) * inv_d
                if t1 > t2:
                    t1, t2 = t2, t1
                t_min = max(t_min, t1)
                t_max = min(t_max, t2)
                if t_min > t_max:
                    return None
        if t_max <= 1e-4:
            return None
        hit_t = t_min if t_min > 1e-4 else t_max
        return hit_t if hit_t > 1e-4 else None

    @classmethod
    def _get_local_corners(cls, bound_box: Any) -> List[Tuple[float, float, float]]:
        if isinstance(bound_box, list):
            try:
                if len(bound_box) == 2:
                    b0, b1 = bound_box[0], bound_box[1]
                    min_x, min_y, min_z = float(b0[0]), float(b0[1]), float(b0[2])
                    max_x, max_y, max_z = float(b1[0]), float(b1[1]), float(b1[2])
                    return [
                        (min_x, min_y, min_z), (min_x, min_y, max_z),
                        (min_x, max_y, min_z), (min_x, max_y, max_z),
                        (max_x, min_y, min_z), (max_x, min_y, max_z),
                        (max_x, max_y, min_z), (max_x, max_y, max_z),
                    ]
                elif len(bound_box) == 8:
                    return [(float(p[0]), float(p[1]), float(p[2])) for p in bound_box]
            except (ValueError, TypeError, IndexError):
                pass
        return [
            (-0.5, -0.5, -0.5), (-0.5, -0.5, 0.5),
            (-0.5, 0.5, -0.5), (-0.5, 0.5, 0.5),
            (0.5, -0.5, -0.5), (0.5, -0.5, 0.5),
            (0.5, 0.5, -0.5), (0.5, 0.5, 0.5),
        ]

    @staticmethod
    def _to_dict(val: Any) -> Dict[str, Any]:
        if is_dataclass(val) and not isinstance(val, type):
            return asdict(val)
        if isinstance(val, dict):
            return val
        if hasattr(val, "__dict__"):
            return {k: v for k, v in val.__dict__.items() if not k.startswith("_")}
        return {}

    @staticmethod
    def _parse_vec3(val: Any, default: Tuple[float, float, float]) -> Tuple[float, float, float]:
        if isinstance(val, (list, tuple)) and len(val) >= 3:
            try:
                return (float(val[0]), float(val[1]), float(val[2]))
            except (ValueError, TypeError):
                return default
        return default
