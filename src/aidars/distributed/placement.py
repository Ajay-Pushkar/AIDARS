"""Multi-attribute placement decision engine.

Evaluates available workers and selects the optimal node for a given WorkloadSpec.
"""

import time
from typing import Dict, List, Optional, Tuple

from aidars.distributed.models import (
    PlacementDecision,
    WorkerResourceProfile,
    WorkloadSpec,
)


class PlacementEngine:
    """Calculates placement scores and makes scheduling decisions."""

    def __init__(self) -> None:
        # Placement weights
        self.w_c = 1.0  # Compute headroom
        self.w_m = 1.0  # Memory headroom
        self.w_g = 2.0  # GPU suitability
        self.w_d = 2.0  # Data locality
        self.w_n = 1.0  # Network latency penalty
        self.w_l = 0.5  # Queue load penalty

    def _get_network_latency_penalty(self, worker_id: str) -> float:
        # For now, default to 0. In a full integration, this would query the Prioritizer
        return 0.0

    def _get_tier(self, worker_id: str) -> str:
        # Default to lan. Integration with Prioritizer needed for full accuracy.
        return "lan"

    def evaluate(
        self, spec: WorkloadSpec, profiles: List[WorkerResourceProfile]
    ) -> Optional[PlacementDecision]:
        """Find the optimal worker for the given workload specification."""
        best_score = -float("inf")
        best_decision: Optional[PlacementDecision] = None
        now = time.time()

        for profile in profiles:
            # Hard Constraint Filters
            if profile.ram_available_bytes < spec.min_ram_bytes:
                continue
            if spec.requires_gpu and not profile.gpu_available:
                continue
            if profile.vram_available_bytes < spec.min_vram_bytes:
                continue
            # Stale profile check
            if now - profile.timestamp_utc > 15.0:
                continue

            # 1. Compute Headroom
            c_w = (profile.cpu_cores_total * (1.0 - (profile.cpu_utilization_percent / 100.0))) / spec.min_cpu_cores
            
            # 2. Memory Headroom
            m_w = profile.ram_available_bytes / max(1, spec.min_ram_bytes)
            
            # 3. GPU Suitability
            if spec.requires_gpu and profile.gpu_available:
                g_w = 1.0 + (profile.vram_available_bytes / max(1, spec.min_vram_bytes))
            elif not spec.requires_gpu:
                g_w = 1.0
            else:
                g_w = 0.0
            
            # 4. Data Locality
            if spec.input_asset_hashes:
                cached = len(spec.input_asset_hashes.intersection(profile.local_cached_hashes))
                d_w = cached / len(spec.input_asset_hashes)
            else:
                d_w = 1.0
            
            # 5. Network Penalty
            n_w = self._get_network_latency_penalty(profile.worker_id)
            
            # 6. Queue Load Penalty
            l_w = float(profile.active_workload_count)

            # Total Score S(w, tau)
            score = (
                self.w_c * c_w +
                self.w_m * m_w +
                self.w_g * g_w +
                self.w_d * d_w +
                self.w_n * n_w -
                self.w_l * l_w
            )

            if score > best_score:
                best_score = score
                missing_assets = spec.input_asset_hashes - profile.local_cached_hashes
                
                best_decision = PlacementDecision(
                    workload_id=spec.workload_id,
                    selected_worker_id=profile.worker_id,
                    placement_score=score,
                    score_breakdown={
                        "compute": c_w,
                        "memory": m_w,
                        "gpu": g_w,
                        "locality": d_w,
                        "latency": n_w,
                        "queue": l_w,
                    },
                    missing_assets_on_worker=missing_assets,
                    execution_tier=self._get_tier(profile.worker_id),
                    decision_timestamp_utc=now,
                )

        return best_decision
