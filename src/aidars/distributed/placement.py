"""Explainable, hard-gated multi-attribute placement."""

import math
import time
from typing import Dict, List, Optional, Set, Tuple

from aidars.distributed.models import PlacementDecision, WorkerResourceProfile, WorkerStatus, WorkloadSpec


class PlacementEngine:
    """Evaluate hard eligibility first, then rank only eligible workers."""

    STALE_PROFILE_SECONDS = 10.0

    def __init__(self, m7_bridge=None) -> None:
        self.m7_bridge = m7_bridge
        self.w_c = 1.0
        self.w_m = 1.0
        self.w_g = 2.0
        self.w_d = 2.0
        self.w_n = 1.0
        self.w_l = 0.5
        self.last_evaluation: Dict[str, object] = {}

    def _get_network_latency_penalty(self, worker_id: str) -> float:
        return 0.0

    def _get_tier(self, worker_id: str) -> str:
        return "lan"

    @staticmethod
    def _capability_tuple(value: Optional[str]) -> Optional[Tuple[int, int]]:
        if not value:
            return None
        try:
            major, minor = value.split(".", 1)
            return int(major), int(minor)
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _tag_result(actual: Dict[str, str], required: Dict[str, str]) -> bool:
        return all(actual.get(key) == value for key, value in required.items())

    def _locality(self, spec: WorkloadSpec, profile: WorkerResourceProfile,
                  asset_sizes_bytes: Dict[str, int]) -> Dict[str, object]:
        assets = spec.input_asset_hashes
        cached = assets.intersection(profile.local_cached_hashes)
        count_fraction = len(cached) / len(assets) if assets else 1.0
        known = {h: size for h, size in asset_sizes_bytes.items() if h in assets}
        known_hashes = set(known)
        known_total = sum(known.values())
        known_cached = sum(size for h, size in known.items() if h in cached)
        unknown_count = len(assets - known_hashes)
        if known_total > 0 and unknown_count == 0:
            byte_fraction = known_cached / known_total
            rank_fraction = byte_fraction
            basis = "known_bytes"
        elif assets:
            byte_fraction = known_cached / known_total if known_total > 0 else None
            # If even one required size is unknown, byte weighting cannot
            # represent the complete transfer. Use the deterministic asset
            # count fraction for the complete set; never impute missing bytes.
            rank_fraction = count_fraction
            basis = "asset_count_fallback_incomplete_or_zero_sizes"
        else:
            byte_fraction = 1.0
            rank_fraction = 1.0
            basis = "no_input_assets"
        return {
            "locality_class": "all_local" if count_fraction == 1.0 else "partial" if count_fraction > 0 else "none",
            "asset_count_fraction": count_fraction,
            "known_byte_fraction": byte_fraction,
            "known_asset_count": len(known_hashes),
            "unknown_size_count": unknown_count,
            "ranking_basis": basis,
            "ranking_fraction": rank_fraction,
        }

    def evaluate(
        self,
        spec: WorkloadSpec,
        profiles: List[WorkerResourceProfile],
        *,
        worker_tags: Optional[Dict[str, Dict[str, str]]] = None,
        group_workers: Optional[Dict[str, Set[str]]] = None,
        now_utc: Optional[float] = None,
        predicted_duration_seconds: Optional[float] = None,
        asset_sizes_bytes: Optional[Dict[str, int]] = None,
    ) -> Optional[PlacementDecision]:
        """Return a selected candidate or None; diagnostics are attached to a decision
        whenever at least one candidate is eligible. Rejected candidates are retained
        alongside the selected decision for API and persistence explanations.
        """
        now = time.time() if now_utc is None else now_utc
        worker_tags = worker_tags or {}
        group_workers = group_workers or {}
        asset_sizes_bytes = asset_sizes_bytes or {}
        explanations: List[Dict[str, object]] = []
        eligible: List[WorkerResourceProfile] = []
        scores: Dict[str, float] = {}
        breakdowns: Dict[str, Dict[str, float]] = {}
        rank_scores: Dict[str, float] = {}
        localities: Dict[str, Dict[str, object]] = {}

        for p in profiles:
            reasons: List[str] = []
            raw_age = None if p.timestamp_utc is None else now - p.timestamp_utc
            age = raw_age if raw_age is not None and math.isfinite(raw_age) else None
            cpu_finite = math.isfinite(p.cpu_utilization_percent)
            avail_cpu = p.cpu_cores_total * (1.0 - p.cpu_utilization_percent / 100.0)
            checks: Dict[str, Dict[str, object]] = {
                "status": {"actual": p.status.value, "result": p.status == WorkerStatus.ACTIVE},
                "health": {"actual": p.status.value, "result": p.status in (WorkerStatus.ACTIVE, WorkerStatus.DEGRADED)},
                "can_execute_workloads": {"actual": p.can_execute_workloads, "result": p.can_execute_workloads},
                "cpu_telemetry_finite": {"actual": p.cpu_utilization_percent if cpu_finite else None,
                                         "result": cpu_finite},
                "telemetry": {"age_seconds": age, "maximum_age_seconds": self.STALE_PROFILE_SECONDS,
                              "result": age is not None and p.timestamp_utc is not None and math.isfinite(p.timestamp_utc)
                              and 0 <= age <= self.STALE_PROFILE_SECONDS},
                "cpu": {"required": spec.min_cpu_cores,
                        "available": avail_cpu if math.isfinite(avail_cpu) else None,
                        "result": cpu_finite and avail_cpu >= spec.min_cpu_cores},
                "ram": {"required": spec.min_ram_bytes, "available": p.ram_available_bytes,
                        "result": p.ram_available_bytes <= p.ram_total_bytes and p.ram_available_bytes >= spec.min_ram_bytes},
                "ram_telemetry_consistency": {"available": p.ram_available_bytes, "total": p.ram_total_bytes,
                                              "result": p.ram_available_bytes <= p.ram_total_bytes},
                "gpu": {"required": spec.requires_gpu or spec.min_vram_bytes > 0 or bool(spec.required_gpu_vendor or spec.required_gpu_model or spec.min_compute_capability or spec.required_driver_version or spec.required_runtime_compatibility),
                        "available": p.gpu_available, "result": not (spec.requires_gpu or spec.min_vram_bytes > 0 or spec.required_gpu_vendor or spec.required_gpu_model or spec.min_compute_capability or spec.required_driver_version or spec.required_runtime_compatibility) or p.gpu_available},
                "vram": {"required": spec.min_vram_bytes, "available": p.vram_available_bytes,
                         "result": p.vram_available_bytes <= p.vram_total_bytes and p.vram_available_bytes >= spec.min_vram_bytes},
                "vram_telemetry_consistency": {"available": p.vram_available_bytes, "total": p.vram_total_bytes,
                                               "result": p.vram_available_bytes <= p.vram_total_bytes},
                "concurrency": {"active": p.active_workload_count, "maximum": p.max_concurrent_workloads,
                                "result": p.active_workload_count < p.max_concurrent_workloads},
            }
            tags = worker_tags.get(p.worker_id, {})
            if spec.required_gpu_vendor:
                checks["gpu_vendor"] = {"required": spec.required_gpu_vendor, "actual": p.gpu_vendor,
                                        "result": p.gpu_vendor is not None and p.gpu_vendor.casefold() == spec.required_gpu_vendor.casefold()}
            if spec.required_gpu_model:
                checks["gpu_model"] = {"required": spec.required_gpu_model, "actual": p.gpu_model or p.gpu_device_name,
                                       "result": (p.gpu_model or p.gpu_device_name) is not None and
                                       (p.gpu_model or p.gpu_device_name).casefold() == spec.required_gpu_model.casefold()}
            if spec.min_compute_capability:
                actual_cap = self._capability_tuple(p.gpu_compute_capability)
                required_cap = self._capability_tuple(spec.min_compute_capability)
                checks["compute_capability"] = {"required": spec.min_compute_capability,
                    "actual": p.gpu_compute_capability, "result": actual_cap is not None and required_cap is not None and actual_cap >= required_cap}
            if spec.required_driver_version:
                checks["driver_version"] = {"required": spec.required_driver_version, "actual": p.gpu_driver_version,
                    "result": p.gpu_driver_version == spec.required_driver_version}
            if spec.required_runtime_compatibility:
                checks["runtime_compatibility"] = {"required": spec.required_runtime_compatibility, "actual": None, "result": False}
            if spec.required_worker_tags:
                checks["required_worker_tags"] = {"required": spec.required_worker_tags,
                    "actual": {k: tags.get(k) for k in spec.required_worker_tags},
                    "result": self._tag_result(tags, spec.required_worker_tags)}
            previous_workers = group_workers.get(spec.affinity_group_id or "", set())
            affinity_match = True
            if spec.affinity_mode == "same_worker" and previous_workers:
                affinity_match = p.worker_id in previous_workers
            elif spec.affinity_mode == "different_worker" and previous_workers:
                affinity_match = p.worker_id not in previous_workers
            elif spec.affinity_mode in ("same_tag", "different_tag"):
                actual_tag = tags.get(spec.affinity_tag_key or "")
                prior_tags = {worker_tags.get(worker_id, {}).get(spec.affinity_tag_key or "")
                              for worker_id in previous_workers}
                prior_tags.discard(None)
                if spec.affinity_mode == "same_tag" and previous_workers:
                    affinity_match = (actual_tag is not None and len(prior_tags) == len(previous_workers)
                                     and len(prior_tags) == 1 and actual_tag in prior_tags)
                elif spec.affinity_mode == "different_tag" and previous_workers:
                    affinity_match = (actual_tag is not None and len(prior_tags) == len(previous_workers)
                                     and actual_tag not in prior_tags)
            if spec.affinity_mode != "none":
                checks["affinity"] = {"group_id": spec.affinity_group_id, "mode": spec.affinity_mode,
                    "tag_key": spec.affinity_tag_key, "hard": spec.affinity_hard,
                    "prior_group_workers": sorted(previous_workers), "result": affinity_match}
                if not affinity_match and spec.affinity_hard:
                    reasons.append("AFFINITY_CONSTRAINT_NOT_MET")
            if spec.preferred_worker_tags:
                checks["preferred_worker_tags"] = {"preferred": spec.preferred_worker_tags,
                    "actual": {k: tags.get(k) for k in spec.preferred_worker_tags},
                    "result": self._tag_result(tags, spec.preferred_worker_tags)}

            reason_map = {
                "status": "WORKER_NOT_ACTIVE", "health": "WORKER_UNHEALTHY",
                "can_execute_workloads": "WORKER_CANNOT_EXECUTE", "telemetry": "STALE_TELEMETRY",
                "cpu_telemetry_finite": "INVALID_CPU_TELEMETRY",
                "cpu": "INSUFFICIENT_CPU", "ram": "INSUFFICIENT_RAM",
                "gpu": "REQUIRED_GPU_UNAVAILABLE", "vram": "INSUFFICIENT_VRAM",
                "concurrency": "CONCURRENCY_LIMIT_REACHED", "gpu_vendor": "GPU_VENDOR_MISMATCH",
                "gpu_model": "GPU_MODEL_MISMATCH", "compute_capability": "GPU_COMPUTE_CAPABILITY_UNKNOWN_OR_TOO_LOW",
                "driver_version": "GPU_DRIVER_VERSION_MISMATCH_OR_UNKNOWN",
                "runtime_compatibility": "GPU_RUNTIME_COMPATIBILITY_UNKNOWN",
                "required_worker_tags": "REQUIRED_WORKER_TAG_MISMATCH",
            }
            for name, check in checks.items():
                if name in ("affinity", "preferred_worker_tags"):
                    # Affinity has an explicit hard/soft policy and preferred
                    # tags are ranking-only; neither is a generic hard gate.
                    continue
                # ACTIVE status is the existing M6 rule. Health is reported separately,
                # not used to loosen that rule for DEGRADED workers.
                if name in ("ram_telemetry_consistency", "vram_telemetry_consistency"):
                    if not check["result"]:
                        reasons.append("INCONSISTENT_" + name.removesuffix("_consistency").upper() + "_TELEMETRY")
                    continue
                if name == "ram" and p.ram_available_bytes > p.ram_total_bytes:
                    continue
                if name == "vram" and p.vram_available_bytes > p.vram_total_bytes:
                    continue
                if name == "health":
                    if not check["result"]:
                        reasons.append("WORKER_UNHEALTHY")
                    continue
                if not check["result"]:
                    reason = reason_map.get(name, name.upper() + "_FAILED")
                    if name == "telemetry" and p.timestamp_utc is None:
                        reason = "MISSING_TELEMETRY"
                    reasons.append(reason)
            locality = self._locality(spec, p, asset_sizes_bytes)
            localities[p.worker_id] = locality
            candidate_explanation: Dict[str, object] = {
                "worker_id": p.worker_id, "worker_status": p.status.value,
                "health_eligible": bool(checks["health"]["result"]),
                "can_execute_workloads": p.can_execute_workloads,
                "checks": checks, "locality": locality,
                "eligible": not reasons, "rejection_reasons": reasons,
            }
            explanations.append(candidate_explanation)
            if reasons:
                continue

            c_w = avail_cpu / spec.min_cpu_cores
            m_w = p.ram_available_bytes / max(1, spec.min_ram_bytes)
            # Keep the verified M6 score formula intact; generic M11 GPU
            # constraints affect hard eligibility, not the historical M6 term.
            g_w = (1.0 + p.vram_available_bytes / max(1, spec.min_vram_bytes)
                   if spec.requires_gpu and p.gpu_available else 1.0 if not spec.requires_gpu else 0.0)
            # Preserve the verified M6 count-fraction locality contribution.
            d_w = float(locality["asset_count_fraction"])
            n_w = self._get_network_latency_penalty(p.worker_id)
            l_w = float(p.active_workload_count)
            m6_score = self.w_c*c_w + self.w_m*m_w + self.w_g*g_w + self.w_d*d_w + self.w_n*n_w - self.w_l*l_w
            m11_delta = 0.0
            m11_rank_factors: Dict[str, float] = {}
            if asset_sizes_bytes:
                m11_rank_factors["locality_weighting_adjustment"] = (float(locality["ranking_fraction"]) - d_w) * self.w_d
            if spec.preferred_worker_tags:
                m11_rank_factors["preferred_worker_tags"] = 0.25 if checks["preferred_worker_tags"]["result"] else 0.0
            if spec.affinity_mode != "none" and not spec.affinity_hard:
                m11_rank_factors["preferred_affinity"] = 0.25 if affinity_match else -0.25
            m11_delta = sum(m11_rank_factors.values())
            eligible.append(p)
            scores[p.worker_id] = m6_score
            rank_scores[p.worker_id] = m6_score + m11_delta
            breakdowns[p.worker_id] = {"compute": c_w, "memory": m_w, "gpu": g_w,
                                       "locality": d_w, "latency": n_w, "queue": l_w}
            candidate_explanation.update({"m6_score": m6_score, "m11_ranking_adjustment": m11_delta,
                                          "m11_ranking_factors": m11_rank_factors,
                                          "m6_score_breakdown": breakdowns[p.worker_id],
                                          "m11_rank_score": rank_scores[p.worker_id]})

        self.last_evaluation = {"workload_id": spec.workload_id, "evaluated_at_utc": now,
                                "eligible_worker_ids": sorted(p.worker_id for p in eligible),
                                "candidates": explanations}
        if not eligible:
            return None

        ranked = sorted(eligible, key=lambda p: (-rank_scores[p.worker_id], p.worker_id))
        m7_adjustments: Dict[str, float] = {}
        m7_risks: Dict[str, float] = {}
        if self.m7_bridge:
            intelligence = self.m7_bridge.evaluate_candidates(spec, ranked)
            before_m7 = {p.worker_id: index for index, p in enumerate(ranked, 1)}
            m7_risks = {worker_id: float(risk.total_risk) for worker_id, risk in intelligence.items()}
            ordered_ids = self.m7_bridge.adjust_ranking(rank_scores, intelligence, risk_weight=0.5)
            positions = {wid: index for index, wid in enumerate(ordered_ids, 1)}
            m7_adjustments = {wid: float(before_m7.get(wid, positions.get(wid, 0)) - positions.get(wid, 0))
                              for wid in positions}
            ranked.sort(key=lambda p: (positions.get(p.worker_id, len(positions) + 1), p.worker_id))
        selected = ranked[0]
        deadline_state = None
        predicted_duration = predicted_duration_seconds or spec.estimated_duration_seconds
        deadline_slack = None
        if spec.deadline_at_utc is not None:
            remaining = spec.deadline_at_utc - now
            deadline_slack = remaining - predicted_duration
            deadline_state = "expired" if remaining < 0 else "approaching" if deadline_slack <= 0 else "future"
        for explanation in explanations:
            if explanation["eligible"]:
                wid = str(explanation["worker_id"])
                explanation["m7_risk_score"] = m7_risks.get(wid)
                explanation["m7_rank_delta"] = m7_adjustments.get(wid, 0.0)
                explanation["final_rank"] = next(i for i, p in enumerate(ranked, 1) if p.worker_id == wid)
                explanation["selected"] = wid == selected.worker_id
        missing = spec.input_asset_hashes - selected.local_cached_hashes
        return PlacementDecision(
            workload_id=spec.workload_id, selected_worker_id=selected.worker_id,
            placement_score=scores[selected.worker_id], score_breakdown=breakdowns[selected.worker_id],
            missing_assets_on_worker=missing, execution_tier=self._get_tier(selected.worker_id),
            decision_timestamp_utc=now, candidate_explanations=explanations,
            m7_ranking_adjustments=m7_adjustments, locality_details=localities[selected.worker_id],
            deadline_details={"deadline_at_utc": spec.deadline_at_utc, "state": deadline_state,
                              "predicted_duration_seconds": predicted_duration_seconds,
                              "fallback_duration_seconds": spec.estimated_duration_seconds,
                              "duration_estimate_source": "m7_history" if predicted_duration_seconds is not None else "workload_estimate_fallback",
                              "remaining_slack_seconds": deadline_slack,
                              "predicted_miss": (deadline_slack < 0) if deadline_slack is not None and predicted_duration_seconds is not None else None,
                              "fallback_estimated_miss": (deadline_slack < 0) if deadline_slack is not None and predicted_duration_seconds is None else None,
                              "ranking_scope": "workload_admission_only"} if spec.deadline_at_utc is not None else {},
        )
