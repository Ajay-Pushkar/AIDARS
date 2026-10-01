"""Read-only M12 views over durable M10 attempts and existing M7 memory."""
from __future__ import annotations

import hashlib
import statistics
import time
from collections import defaultdict
from typing import Iterable, List, Optional

from aidars.distributed.attempt import AttemptRecord, TERMINAL_ATTEMPT_STATUSES
from aidars.distributed.models import WorkerInfo
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadState
from aidars.m12.history import HistoricalObservationProjector
from aidars.m12.models import (
    AnomalyRecord, BehaviorClassification, CapacityForecast, ConfidenceLevel,
    DataQualityState, EstimateKind, FailurePrediction, HistoricalObservation,
    OutcomeSemantics, PolicyRecommendation, PredictionEstimate,
    ResourcePrediction, WorkloadIntelligence, WorkerIntelligence,
)
from aidars.m7.telemetry import TelemetryMemory
from aidars.m7.policy import AdaptivePolicyEngine
from aidars.m7.anomaly import AnomalyDetector


class DistributedIntelligence:
    """Analytics facade; it cannot make placement, admission, or worker-state decisions."""

    MIN_SAMPLES = 3
    ANOMALY_BASELINE = 5

    def __init__(self, memory: TelemetryMemory, worker_registry) -> None:
        self.memory = memory
        self.worker_registry = worker_registry
        self.projector = HistoricalObservationProjector()
        self.observations: List[HistoricalObservation] = []
        self._by_attempt = {}
        self._anomalies: List[AnomalyRecord] = []

    def rebuild_history(self, workloads: Iterable[WorkloadRecord], attempts: Iterable[AttemptRecord]) -> None:
        """Deterministic startup replay; no source records or control state are mutated."""
        workloads = list(workloads)
        attempts = list(attempts)
        self.memory.reset_workload_history()
        self.observations = self.projector.project(workloads, attempts).observations
        self._by_attempt = {o.attempt_id: o for o in self.observations if o.attempt_id}
        by_id = {w.spec.workload_id: w for w in workloads}
        ordered = sorted(self.observations, key=lambda o: (o.finished_at_utc or 0, o.attempt_id or ""))
        for o in ordered:
            self._replay_one(o, by_id.get(o.workload_id or ""))
        self._anomalies = self._find_anomalies(ordered)

    def observe_attempt(self, attempt: AttemptRecord, workload: Optional[WorkloadRecord], all_attempts) -> None:
        """Live idempotent observation from AttemptRegistry's lifecycle observer."""
        if workload is None or attempt.status not in TERMINAL_ATTEMPT_STATUSES:
            return
        # Re-project the ledger so duplicate identity validation remains consistent.
        projection = self.projector.project([workload], all_attempts)
        observation = next((o for o in projection.observations if o.attempt_id == attempt.attempt_id), None)
        if observation is None:
            return
        previous = self._by_attempt.get(observation.attempt_id)
        if previous is not None:
            if previous.model_dump(mode="json") == observation.model_dump(mode="json"):
                return
            observation.data_quality = DataQualityState.INVALID
            observation.validation_errors.append("CONFLICTING_REPLAYED_ATTEMPT_IDENTITY")
        self._by_attempt[observation.attempt_id] = observation
        self.observations.append(observation)
        self._replay_one(observation, workload)
        self._anomalies = self._find_anomalies(sorted(self.observations, key=lambda o: (o.finished_at_utc or 0, o.attempt_id or "")))

    def _replay_one(self, o: HistoricalObservation, workload: Optional[WorkloadRecord]) -> None:
        if o.data_quality != DataQualityState.VALID or o.outcome not in (OutcomeSemantics.SUCCESS, OutcomeSemantics.FAILURE):
            return
        self.memory.ingest_workload_result(
            workload_type=o.task_type or "unknown",
            duration=o.duration_seconds if o.outcome == OutcomeSemantics.SUCCESS else None,
            ram_peak=None,
            failed=o.outcome == OutcomeSemantics.FAILURE,
            event_id=o.attempt_id,
        )

    def _usable(self, *, task_type=None, worker_id=None):
        return [o for o in self.observations
                if o.data_quality == DataQualityState.VALID
                and o.outcome in (OutcomeSemantics.SUCCESS, OutcomeSemantics.FAILURE)
                and (task_type is None or o.task_type == task_type)
                and (worker_id is None or o.worker_id == worker_id)]

    @staticmethod
    def _confidence(n: int) -> ConfidenceLevel:
        if n >= 10: return ConfidenceLevel.HIGH
        if n >= 5: return ConfidenceLevel.MEDIUM
        if n >= 3: return ConfidenceLevel.LOW
        return ConfidenceLevel.UNKNOWN

    def _duration(self, spec) -> PredictionEstimate:
        rows = self._usable(task_type=spec.task_type)
        def resource_class(req):
            if req is None:
                return None
            cpu_cores = getattr(req, "cpu_cores", getattr(req, "min_cpu_cores", 1))
            ram_bytes = getattr(req, "ram_bytes", getattr(req, "min_ram_bytes", 0))
            requires_gpu = getattr(req, "requires_gpu", False)
            cpu = "small" if cpu_cores <= 2 else "medium" if cpu_cores <= 8 else "large"
            ram_gib = ram_bytes / (1024 ** 3)
            ram = "small" if ram_gib <= 4 else "medium" if ram_gib <= 16 else "large"
            return (requires_gpu, cpu, ram)
        target_signature = resource_class(spec)
        signature_rows = [o for o in rows if resource_class(o.requested_resources) == target_signature]
        use_signature = len(signature_rows) >= self.MIN_SAMPLES
        selected_rows = signature_rows if use_signature else rows
        successes = [o.duration_seconds for o in selected_rows
                     if o.outcome == OutcomeSemantics.SUCCESS and o.duration_seconds]
        now = time.time()
        if len(successes) >= self.MIN_SAMPLES:
            return PredictionEstimate(value=statistics.median(successes), unit="seconds",
                kind=EstimateKind.PREDICTED, confidence=self._confidence(len(successes)),
                sample_count=len(successes), prediction_version="m12-duration-median/v1",
                generated_at_utc=now, explanation=["Median of valid observed successful attempt durations.",
                "History segmentation: " + ("task type plus coarse GPU/CPU/RAM request class." if use_signature else "task type (sparse request classes fall back to task type)."),
                "LOST and checkpointed attempts are censored and excluded."])
        declared = spec.estimated_duration_seconds
        return PredictionEstimate(value=None, fallback_value=declared, fallback_reason=(
                "Declared WorkloadSpec estimate; not a learned prediction." if declared is not None else None),
            unit="seconds", kind=EstimateKind.DECLARED_FALLBACK if declared is not None else EstimateKind.INSUFFICIENT_DATA,
            confidence=ConfidenceLevel.UNKNOWN, sample_count=len(successes),
            prediction_version="m12-duration-median/v1", generated_at_utc=now,
            explanation=[f"Requires at least {self.MIN_SAMPLES} valid successful observations."])

    def _failure(self, rows, *, scope_reason: str) -> FailurePrediction:
        failures = sum(o.outcome == OutcomeSemantics.FAILURE for o in rows)
        n = len(rows)
        return FailurePrediction(probability=(failures / n if n >= self.MIN_SAMPLES else None),
            confidence=self._confidence(n), sample_count=n, failure_count=failures,
            generated_at_utc=time.time(), contributing_factors=[scope_reason] if n else [],
            reason=None if n >= self.MIN_SAMPLES else f"Insufficient valid outcomes; requires {self.MIN_SAMPLES}.")

    def _behavior(self, rows, *, task_type=None) -> BehaviorClassification:
        successes = [o.duration_seconds for o in rows if o.outcome == OutcomeSemantics.SUCCESS and o.duration_seconds]
        failures = sum(o.outcome == OutcomeSemantics.FAILURE for o in rows)
        labels, evidence, unavailable = [], [], {
            "CPU_BOUND": "Per-attempt CPU use is not recorded.",
            "MEMORY_BOUND": "Per-attempt RAM use is not recorded.",
            "GPU_BOUND": "Per-attempt GPU utilization is not recorded.",
            "TRANSFER_HEAVY": "Transfer metrics are not persisted per attempt.",
        }
        if len(rows) >= 5:
            median = statistics.median(successes) if len(successes) >= 5 else None
            if median is not None and median >= 60:
                labels.append("LONG_RUNNING"); evidence.append(f"Median observed duration is {median:.2f}s.")
            elif median is not None and median <= 5:
                labels.append("SHORT_RUNNING"); evidence.append(f"Median observed duration is {median:.2f}s.")
            rate = failures / len(rows)
            if rate >= 0.3:
                labels.append("FAILURE_PRONE"); evidence.append(f"Observed explicit failure rate is {rate:.1%}.")
            elif len(rows) >= 10 and rate <= 0.05 and median is not None and median > 0:
                mad = statistics.median(abs(value - median) for value in successes)
                if mad / median <= 0.2:
                    labels.append("STABLE")
                    evidence.append(f"At least 10 outcomes, failure rate <=5%, and duration MAD/median={mad / median:.2f}.")
        return BehaviorClassification(labels=labels, confidence=self._confidence(len(rows)),
            sample_count=len(rows), generated_at_utc=time.time(), evidence=evidence,
            unavailable_labels=unavailable)

    def _find_anomalies(self, rows) -> List[AnomalyRecord]:
        by_task = defaultdict(list)
        found = []
        for o in rows:
            if o.data_quality != DataQualityState.VALID or o.outcome != OutcomeSemantics.SUCCESS or not o.duration_seconds:
                if o.data_quality == DataQualityState.VALID and o.outcome == OutcomeSemantics.FAILURE:
                    hist = by_task[o.task_type or ""]
                    if len(hist) >= 10 and all(x[1] == OutcomeSemantics.SUCCESS for x in hist):
                        found.append(self._anomaly("RARE_FAILURE", o, 1.0, 0.0, len(hist), "An explicit failure followed at least ten prior successes."))
                    if o.data_quality == DataQualityState.VALID:
                        hist.append((None, OutcomeSemantics.FAILURE))
                continue
            hist = [d for d, _ in by_task[o.task_type or ""] if d is not None]
            if len(hist) >= self.ANOMALY_BASELINE:
                expected = statistics.median(hist)
                ratio = AnomalyDetector.duration_ratio(expected, o.duration_seconds)
                if ratio >= 1.5 or ratio <= 0.5:
                    score = min(1.0, abs(ratio - 1.0))
                    found.append(self._anomaly("DURATION_DEVIATION", o, o.duration_seconds, expected, len(hist),
                        f"Observed duration ratio {ratio:.2f}; anomaly boundary is >=1.5x or <=0.5x prior median.", score))
            by_task[o.task_type or ""].append((o.duration_seconds, o.outcome))
        return found

    @staticmethod
    def _anomaly(category, o, observed, expected, support, explanation, score=1.0):
        identity = f"{o.attempt_id}:{category}:{observed}:{expected}"
        return AnomalyRecord(anomaly_id=hashlib.sha256(identity.encode()).hexdigest()[:24],
            category=category, score=max(0, min(1, score)), observed_value=float(observed),
            expected_value=float(expected), unit="seconds" if category == "DURATION_DEVIATION" else "failure-rate",
            threshold_basis="prior valid same-task attempts", confidence=DistributedIntelligence._confidence(support),
            support_count=support, workload_id=o.workload_id, attempt_id=o.attempt_id, worker_id=o.worker_id,
            observed_at_utc=o.observed_at_utc or time.time(), explanation=[explanation])

    def workload_view(self, record: WorkloadRecord) -> WorkloadIntelligence:
        rows = [o for o in self.observations if o.workload_id == record.spec.workload_id]
        valid = [o for o in rows if o.data_quality == DataQualityState.VALID]
        return WorkloadIntelligence(workload_id=record.spec.workload_id, task_type=record.spec.task_type,
            data_quality=DataQualityState.VALID if valid else DataQualityState.INSUFFICIENT_HISTORY,
            observation_count=len(valid), invalid_observation_count=sum(o.data_quality == DataQualityState.INVALID for o in rows),
            duration=self._duration(record.spec),
            failure=self._failure(self._usable(task_type=record.spec.task_type), scope_reason="same task_type"),
            resources=ResourcePrediction(generated_at_utc=time.time()),
            behavior=self._behavior(self._usable(task_type=record.spec.task_type), task_type=record.spec.task_type),
            anomalies=[a for a in self._anomalies if a.workload_id == record.spec.workload_id],
            generated_at_utc=time.time())

    def worker_view(self, worker_id: str) -> WorkerIntelligence:
        profile: Optional[WorkerInfo] = self.worker_registry.get_worker(worker_id)
        rows = self._usable(worker_id=worker_id)
        profile_data = {}
        if profile is not None and profile.resource_profile is not None:
            resource_profile = profile.resource_profile
            profile_data = {k: getattr(resource_profile, k) for k in (
                "status", "cpu_cores_total", "cpu_utilization_percent", "ram_total_bytes",
                "ram_available_bytes", "gpu_available", "gpu_vendor", "gpu_model",
                "vram_total_bytes", "vram_available_bytes", "active_workload_count", "timestamp_utc")
                if hasattr(resource_profile, k)}
            profile_data["worker_status"] = profile.status.value
            profile_data["telemetry_age_seconds"] = (
                max(0.0, time.time() - resource_profile.timestamp_utc)
                if resource_profile.timestamp_utc is not None else None
            )
        return WorkerIntelligence(worker_id=worker_id, current_profile=profile_data,
            data_quality=DataQualityState.VALID if rows else DataQualityState.INSUFFICIENT_HISTORY,
            observation_count=len(rows), failure=self._failure(rows, scope_reason="same worker_id"),
            behavior=self._behavior(rows), anomalies=[a for a in self._anomalies if a.worker_id == worker_id],
            generated_at_utc=time.time())

    def capacity_view(self, workloads: Iterable[WorkloadRecord]) -> CapacityForecast:
        now = time.time(); window = 3600.0
        records = list(workloads)
        pending = [r for r in records if r.state == WorkloadState.SUBMITTED]
        workers = self.worker_registry.list_workers(active_only=True)
        requested = {"cpu_cores": float(sum(r.spec.min_cpu_cores for r in pending)),
                     "ram_bytes": float(sum(r.spec.min_ram_bytes for r in pending)),
                     "gpu_workloads": float(sum(1 for r in pending if r.spec.requires_gpu)),
                     "vram_bytes": float(sum(r.spec.min_vram_bytes for r in pending))}
        available = {"cpu_cores": 0.0, "ram_bytes": 0.0, "gpu_workers": 0.0, "vram_bytes": 0.0}
        fresh_profile_count = 0
        for w in workers:
            p = w.resource_profile
            if (w.can_execute_workloads and p is not None and p.timestamp_utc is not None
                    and 0 <= now - p.timestamp_utc <= PlacementEngine.STALE_PROFILE_SECONDS):
                fresh_profile_count += 1
                available["cpu_cores"] += max(0, p.cpu_cores_total * (1 - p.cpu_utilization_percent / 100))
                available["ram_bytes"] += p.ram_available_bytes
                available["gpu_workers"] += int(p.gpu_available)
                available["vram_bytes"] += p.vram_available_bytes
        recent_submissions = [r for r in records if now-window <= r.submitted_at <= now]
        recent_completions = [r for r in records if r.completed_at is not None and now-window <= r.completed_at <= now]
        valid_rows = [o for o in self._usable() if o.observed_at_utc and now-window <= o.observed_at_utc <= now]
        fail_n = len(valid_rows); failures = sum(o.outcome == OutcomeSemantics.FAILURE for o in valid_rows)
        enough = len(recent_submissions) + len(recent_completions) >= self.MIN_SAMPLES and fail_n >= self.MIN_SAMPLES
        pressure = ("UNKNOWN" if fresh_profile_count == 0 else
            "HIGH" if (requested["cpu_cores"] > available["cpu_cores"] or requested["ram_bytes"] > available["ram_bytes"] or requested["gpu_workloads"] > available["gpu_workers"])
            else "NORMAL")
        arrivals = len(recent_submissions); completions = len(recent_completions)
        growth = float(arrivals-completions)
        recommendation = "ADD_CAPACITY" if pressure == "HIGH" or (enough and growth > 0) else ("MONITOR" if not enough or pressure == "UNKNOWN" else "STABLE")
        quality = (DataQualityState.STALE if workers and fresh_profile_count == 0
                   else DataQualityState.VALID if enough else DataQualityState.INSUFFICIENT_HISTORY)
        return CapacityForecast(data_quality=quality,
            generated_at_utc=now, window_seconds=window, queue_depth=len(pending),
            arrivals_per_hour=float(arrivals) if len(recent_submissions) >= self.MIN_SAMPLES else None,
            completions_per_hour=float(completions) if len(recent_completions) >= self.MIN_SAMPLES else None,
            projected_queue_growth_per_hour=growth if enough else None, eligible_attempt_count=fail_n,
            failure_rate=failures/fail_n if fail_n >= self.MIN_SAMPLES else None, requested_demand=requested,
            current_available=available, pressure=pressure, confidence=self._confidence(fail_n),
            recommendation=recommendation, basis=["Current queue and worker telemetry are observed.",
                f"Fresh authoritative resource profiles: {fresh_profile_count}.",
                "Demand is declared workload requirements, not measured usage.",
                "One-hour event counts are descriptive; no worker state is changed."])

    def anomalies(self) -> List[AnomalyRecord]:
        return list(self._anomalies)

    def recommendations(self, workloads: Iterable[WorkloadRecord]) -> List[PolicyRecommendation]:
        forecast = self.capacity_view(workloads)
        output = [PolicyRecommendation(recommendation_id="capacity-" + str(int(forecast.generated_at_utc)),
            recommendation_type="CAPACITY", target="cluster", proposed_change=forecast.recommendation,
            rationale=forecast.basis, supporting_observations=[f"queue_depth={forecast.queue_depth}",
                f"pressure={forecast.pressure}"], confidence=forecast.confidence,
            generated_at_utc=forecast.generated_at_utc)]
        if self.memory.workers:
            weights = AdaptivePolicyEngine.evaluate_environment(list(self.memory.workers.values()))
            values = weights.model_dump() if hasattr(weights, "model_dump") else vars(weights)
            output.append(PolicyRecommendation(
                recommendation_id="policy-weights-" + str(int(forecast.generated_at_utc)),
                recommendation_type="PLACEMENT_WEIGHTS", target="existing M7 advisory weights",
                proposed_change=str(values),
                rationale=["Read-only output from the existing M7 AdaptivePolicyEngine.",
                    "No placement policy or M6 eligibility state is modified."],
                supporting_observations=[f"worker_telemetry_count={len(self.memory.workers)}"],
                confidence=ConfidenceLevel.LOW, generated_at_utc=forecast.generated_at_utc))
        return output
