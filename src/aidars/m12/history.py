"""Validated projection from AIDAR's existing durable workload/attempt ledgers."""
from __future__ import annotations

import math
import time
from collections import Counter
from typing import Dict, Iterable, List, Optional, Tuple

from aidars.distributed.attempt import AttemptRecord, AttemptStatus, make_attempt_id
from aidars.distributed.workload_registry import WorkloadRecord
from aidars.m12.models import (
    DataQualityState,
    HistoricalObservation,
    HistoricalProjection,
    OutcomeSemantics,
    RequestedResources,
)


def _valid_timestamp(value: Optional[float], now_utc: float) -> bool:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and 0 < numeric <= now_utc


def _timestamp_or_none(value: Optional[float], now_utc: float) -> Optional[float]:
    return float(value) if _valid_timestamp(value, now_utc) else None


class HistoricalObservationProjector:
    """Projects immutable attempt identities without mutating source records."""

    @staticmethod
    def project(
        workloads: Iterable[WorkloadRecord],
        attempts: Iterable[AttemptRecord],
        *,
        now_utc: Optional[float] = None,
    ) -> HistoricalProjection:
        now = time.time() if now_utc is None else float(now_utc)
        workload_by_id = {r.spec.workload_id: r for r in workloads}
        attempt_list = list(attempts)
        counts = Counter(a.attempt_id for a in attempt_list)
        observations: List[HistoricalObservation] = []
        duplicate_count = sum(n - 1 for n in counts.values() if n > 1)
        duplicate_ids = {attempt_id for attempt_id, n in counts.items() if n > 1}

        for attempt in sorted(attempt_list, key=lambda a: (
            float(a.queued_at) if _valid_timestamp(a.queued_at, now) else float("-inf"),
            str(a.attempt_id or ""),
        )):
            record = workload_by_id.get(attempt.workload_id)
            spec = record.spec if record is not None else None
            errors: List[str] = []
            if not attempt.workload_id:
                errors.append("MISSING_WORKLOAD_ID")
            if not attempt.attempt_id:
                errors.append("MISSING_ATTEMPT_ID")
            if attempt.attempt_number < 1:
                errors.append("INVALID_ATTEMPT_NUMBER")
            elif attempt.attempt_id != make_attempt_id(attempt.workload_id, attempt.attempt_number):
                errors.append("ATTEMPT_IDENTITY_MISMATCH")
            if attempt.attempt_id in duplicate_ids:
                errors.append("DUPLICATE_ATTEMPT_IDENTITY")
            if spec is None:
                errors.append("MISSING_WORKLOAD_SPEC")
            elif not spec.task_type.strip():
                errors.append("MISSING_TASK_TYPE")
            if not _valid_timestamp(attempt.queued_at, now):
                errors.append("INVALID_QUEUED_TIMESTAMP")
            if record is not None and not _valid_timestamp(record.submitted_at, now):
                errors.append("INVALID_SUBMITTED_TIMESTAMP")

            timeline = [
                ("assigned", attempt.assigned_at),
                ("started", attempt.started_at),
                ("finished", attempt.finished_at),
            ]
            previous = float(attempt.queued_at) if _valid_timestamp(attempt.queued_at, now) else float("-inf")
            for label, value in timeline:
                if value is None:
                    continue
                if not _valid_timestamp(value, now):
                    errors.append(f"INVALID_{label.upper()}_TIMESTAMP")
                elif float(value) < previous:
                    errors.append("TIMESTAMP_ORDER_INVALID")
                else:
                    previous = float(value)

            status = attempt.status
            result = attempt.execution_result
            outcome = OutcomeSemantics.INCOMPLETE
            quality = DataQualityState.CENSORED
            duration = None
            if status in (AttemptStatus.SUCCEEDED, AttemptStatus.FAILED, AttemptStatus.LOST):
                if attempt.finished_at is None:
                    errors.append("TERMINAL_ATTEMPT_MISSING_FINISHED_TIMESTAMP")
                if attempt.worker_id is None:
                    errors.append("TERMINAL_ATTEMPT_MISSING_WORKER_ID")
            if result is not None:
                if not math.isfinite(result.execution_duration_seconds) or result.execution_duration_seconds < 0:
                    errors.append("INVALID_EXECUTION_DURATION")
                else:
                    duration = float(result.execution_duration_seconds)
                if result.workload_id != attempt.workload_id:
                    errors.append("RESULT_WORKLOAD_ID_MISMATCH")
                if attempt.worker_id and result.worker_id != attempt.worker_id:
                    errors.append("RESULT_WORKER_ID_MISMATCH")

            if status == AttemptStatus.LOST:
                if result is not None:
                    errors.append("LOST_ATTEMPT_HAS_OBSERVED_RESULT")
                outcome = OutcomeSemantics.LOST
                quality = DataQualityState.CENSORED
            elif status in (AttemptStatus.QUEUED, AttemptStatus.ASSIGNED, AttemptStatus.RUNNING):
                outcome = OutcomeSemantics.INCOMPLETE
                quality = DataQualityState.CENSORED
            elif status == AttemptStatus.SUCCEEDED:
                if result is None:
                    errors.append("SUCCEEDED_ATTEMPT_MISSING_RESULT")
                elif not result.success:
                    errors.append("SUCCEEDED_ATTEMPT_HAS_FAILED_RESULT")
                elif result.was_checkpointed or attempt.checkpoint_hash:
                    outcome = OutcomeSemantics.CHECKPOINTED
                    quality = DataQualityState.CENSORED
                else:
                    outcome = OutcomeSemantics.SUCCESS
                    quality = DataQualityState.VALID
                    if duration is None or duration <= 0:
                        errors.append("SUCCESS_DURATION_NOT_POSITIVE")
            elif status == AttemptStatus.FAILED:
                if result is not None and result.success:
                    errors.append("FAILED_ATTEMPT_HAS_SUCCESS_RESULT")
                if attempt.failure_category is None and not attempt.failure_reason and result is None:
                    errors.append("FAILURE_WITHOUT_EXPLICIT_EVIDENCE")
                outcome = OutcomeSemantics.FAILURE
                quality = DataQualityState.VALID
            else:
                errors.append("UNKNOWN_ATTEMPT_STATE")

            if errors:
                quality = DataQualityState.INVALID
            context: Dict[str, object] = {}
            decision = getattr(attempt, "placement_decision", None)
            if decision is not None:
                selected = next((c for c in decision.candidate_explanations
                                 if c.get("worker_id") == decision.selected_worker_id), {})
                context = {
                    "decision_timestamp_utc": decision.decision_timestamp_utc,
                    "selected_worker_id": decision.selected_worker_id,
                    "placement_score": decision.placement_score,
                    "score_breakdown": decision.score_breakdown,
                    "m7_ranking_adjustments": decision.m7_ranking_adjustments,
                    "selected_candidate_explanation": selected,
                    "locality_details": decision.locality_details,
                    "deadline_details": decision.deadline_details,
                }
            policy_context = {}
            if spec is not None:
                policy_context = {
                    "priority": spec.priority,
                    "deadline_at_utc": spec.deadline_at_utc,
                    "affinity_group_id": spec.affinity_group_id,
                    "affinity_mode": spec.affinity_mode,
                    "affinity_hard": spec.affinity_hard,
                }
            observations.append(HistoricalObservation(
                observation_id=attempt.attempt_id or None,
                data_quality=quality,
                validation_errors=sorted(set(errors)),
                observed_at_utc=_timestamp_or_none(attempt.finished_at, now) or _timestamp_or_none(attempt.queued_at, now),
                workload_id=attempt.workload_id or None,
                attempt_id=attempt.attempt_id or None,
                attempt_number=attempt.attempt_number if attempt.attempt_number > 0 else None,
                task_type=spec.task_type if spec is not None else None,
                worker_id=attempt.worker_id,
                requested_resources=(RequestedResources(
                    cpu_cores=spec.min_cpu_cores,
                    ram_bytes=spec.min_ram_bytes,
                    requires_gpu=spec.requires_gpu,
                    vram_bytes=spec.min_vram_bytes,
                    gpu_vendor=spec.required_gpu_vendor,
                    gpu_model=spec.required_gpu_model,
                ) if spec is not None else None),
                submitted_at_utc=_timestamp_or_none(record.submitted_at, now) if record is not None else None,
                queued_at_utc=_timestamp_or_none(attempt.queued_at, now),
                assigned_at_utc=_timestamp_or_none(attempt.assigned_at, now),
                started_at_utc=_timestamp_or_none(attempt.started_at, now),
                finished_at_utc=_timestamp_or_none(attempt.finished_at, now),
                duration_seconds=duration,
                outcome=outcome,
                failure_category=(attempt.failure_category.value if attempt.failure_category else
                                  result.failure_category.value if result and result.failure_category else None),
                failure_reason=attempt.failure_reason or (result.error_message if result else None),
                retry_count=max(0, attempt.attempt_number - 1),
                was_checkpointed=outcome == OutcomeSemantics.CHECKPOINTED,
                policy_context=policy_context,
                placement_context=context,
            ))

        return HistoricalProjection(
            observations=observations,
            valid_count=sum(o.data_quality == DataQualityState.VALID for o in observations),
            invalid_count=sum(o.data_quality == DataQualityState.INVALID for o in observations),
            censored_count=sum(o.data_quality == DataQualityState.CENSORED for o in observations),
            duplicate_count=duplicate_count,
            generated_at_utc=now,
        )
