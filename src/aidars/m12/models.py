"""Typed, versioned contracts for M12 analytical outputs.

These models describe observations and recommendations. They do not grant
authority to place, execute, drain, or resize workers.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


OBSERVATION_SCHEMA_VERSION = "m12-observation/v1"
FEATURE_SCHEMA_VERSION = "m12-features/v1"
DURATION_PREDICTION_VERSION = "m12-duration-median/v1"
FAILURE_PREDICTION_VERSION = "m12-failure-rate/v1"
RESOURCE_PREDICTION_VERSION = "m12-resource-unknown/v1"
BEHAVIOR_MODEL_VERSION = "m12-observed-behavior/v1"
ANOMALY_VERSION = "m12-duration-failure-anomaly/v1"
CAPACITY_FORECAST_VERSION = "m12-capacity-window/v1"
RECOMMENDATION_VERSION = "m12-read-only-recommendation/v1"


class DataQualityState(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    CENSORED = "CENSORED"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


class OutcomeSemantics(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    LOST = "LOST"
    CHECKPOINTED = "CHECKPOINTED"
    INCOMPLETE = "INCOMPLETE"


class ConfidenceLevel(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNKNOWN = "UNKNOWN"


class EstimateKind(str, Enum):
    PREDICTED = "PREDICTED"
    DECLARED_FALLBACK = "DECLARED_FALLBACK"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class RequestedResources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpu_cores: int
    ram_bytes: int
    requires_gpu: bool
    vram_bytes: int
    gpu_vendor: Optional[str] = None
    gpu_model: Optional[str] = None


class ActualResourceUsage(BaseModel):
    """Only authoritative per-attempt measurements belong here."""
    model_config = ConfigDict(extra="forbid")

    cpu_cores: Optional[float] = None
    ram_peak_bytes: Optional[int] = None
    gpu_utilization_percent: Optional[float] = None
    vram_peak_bytes: Optional[int] = None


class HistoricalObservation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = OBSERVATION_SCHEMA_VERSION
    observation_id: Optional[str] = None
    data_quality: DataQualityState
    validation_errors: List[str] = Field(default_factory=list)
    source: str = "CoordinatorStateStore/WorkloadRecord+AttemptRecord"
    observed_at_utc: Optional[float] = None
    workload_id: Optional[str] = None
    attempt_id: Optional[str] = None
    attempt_number: Optional[int] = None
    task_type: Optional[str] = None
    worker_id: Optional[str] = None
    requested_resources: Optional[RequestedResources] = None
    actual_resources: ActualResourceUsage = Field(default_factory=ActualResourceUsage)
    actual_resources_quality: DataQualityState = DataQualityState.UNKNOWN
    submitted_at_utc: Optional[float] = None
    queued_at_utc: Optional[float] = None
    assigned_at_utc: Optional[float] = None
    started_at_utc: Optional[float] = None
    finished_at_utc: Optional[float] = None
    duration_seconds: Optional[float] = None
    outcome: OutcomeSemantics = OutcomeSemantics.INCOMPLETE
    failure_category: Optional[str] = None
    failure_reason: Optional[str] = None
    retry_count: Optional[int] = None
    was_checkpointed: bool = False
    policy_context: Dict[str, Any] = Field(default_factory=dict)
    placement_context: Dict[str, Any] = Field(default_factory=dict)


class HistoricalProjection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = OBSERVATION_SCHEMA_VERSION
    observations: List[HistoricalObservation] = Field(default_factory=list)
    valid_count: int = 0
    invalid_count: int = 0
    censored_count: int = 0
    duplicate_count: int = 0
    generated_at_utc: float


class PredictionEstimate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: Optional[float] = None
    unit: str
    kind: EstimateKind
    confidence: ConfidenceLevel
    sample_count: int = 0
    source: str = "durable_attempt_history"
    feature_version: str = FEATURE_SCHEMA_VERSION
    prediction_version: str
    generated_at_utc: float
    explanation: List[str] = Field(default_factory=list)
    fallback_value: Optional[float] = None
    fallback_reason: Optional[str] = None


class FailurePrediction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    probability: Optional[float] = None
    confidence: ConfidenceLevel = ConfidenceLevel.UNKNOWN
    sample_count: int = 0
    failure_count: int = 0
    source: str = "durable_attempt_history"
    feature_version: str = FEATURE_SCHEMA_VERSION
    prediction_version: str = FAILURE_PREDICTION_VERSION
    generated_at_utc: float
    contributing_factors: List[str] = Field(default_factory=list)
    reason: Optional[str] = None


class ResourcePrediction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    predictions: Dict[str, Optional[float]] = Field(default_factory=lambda: {
        "cpu_cores": None,
        "ram_peak_bytes": None,
        "gpu_utilization_percent": None,
        "vram_peak_bytes": None,
    })
    confidence: ConfidenceLevel = ConfidenceLevel.UNKNOWN
    sample_count: int = 0
    source: str = "no_authoritative_per_attempt_resource_measurements"
    feature_version: str = FEATURE_SCHEMA_VERSION
    prediction_version: str = RESOURCE_PREDICTION_VERSION
    generated_at_utc: float
    reason: str = "NO_AUTHORITATIVE_PER_ATTEMPT_RESOURCE_HISTORY"
    requested_resources_are_not_actual_usage: bool = True


class BehaviorClassification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    labels: List[str] = Field(default_factory=list)
    confidence: ConfidenceLevel = ConfidenceLevel.UNKNOWN
    sample_count: int = 0
    feature_version: str = FEATURE_SCHEMA_VERSION
    model_version: str = BEHAVIOR_MODEL_VERSION
    generated_at_utc: float
    evidence: List[str] = Field(default_factory=list)
    unavailable_labels: Dict[str, str] = Field(default_factory=dict)


class AnomalyRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    anomaly_id: str
    category: str
    score: float = Field(ge=0.0, le=1.0)
    observed_value: float
    expected_value: float
    unit: str
    threshold_basis: str
    confidence: ConfidenceLevel
    support_count: int = 0
    workload_id: Optional[str] = None
    attempt_id: Optional[str] = None
    worker_id: Optional[str] = None
    observed_at_utc: float
    source: str = "validated_durable_attempt_history"
    anomaly_version: str = ANOMALY_VERSION
    explanation: List[str] = Field(default_factory=list)


class CapacityForecast(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data_quality: DataQualityState
    generated_at_utc: float
    window_seconds: float
    forecast_version: str = CAPACITY_FORECAST_VERSION
    queue_depth: int
    queue_depth_kind: str = "OBSERVED"
    arrivals_per_hour: Optional[float] = None
    arrivals_kind: str = "DERIVED"
    completions_per_hour: Optional[float] = None
    completions_kind: str = "DERIVED"
    projected_queue_growth_per_hour: Optional[float] = None
    projected_growth_kind: str = "PREDICTED"
    eligible_attempt_count: int = 0
    failure_rate: Optional[float] = None
    requested_demand: Dict[str, float] = Field(default_factory=dict)
    current_available: Dict[str, float] = Field(default_factory=dict)
    pressure: str = "UNKNOWN"
    confidence: ConfidenceLevel = ConfidenceLevel.UNKNOWN
    recommendation: str = "INSUFFICIENT_HISTORY"
    recommendation_kind: str = "RECOMMENDED"
    basis: List[str] = Field(default_factory=list)
    read_only: bool = True


class PolicyRecommendation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recommendation_id: str
    recommendation_type: str
    target: str
    proposed_change: str
    rationale: List[str] = Field(default_factory=list)
    supporting_observations: List[str] = Field(default_factory=list)
    confidence: ConfidenceLevel = ConfidenceLevel.UNKNOWN
    generated_at_utc: float
    recommendation_version: str = RECOMMENDATION_VERSION
    read_only: bool = True


class WorkloadIntelligence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workload_id: str
    task_type: str
    data_quality: DataQualityState
    observation_count: int = 0
    invalid_observation_count: int = 0
    duration: PredictionEstimate
    failure: FailurePrediction
    resources: ResourcePrediction
    behavior: BehaviorClassification
    anomalies: List[AnomalyRecord] = Field(default_factory=list)
    generated_at_utc: float


class WorkerIntelligence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str
    current_profile: Dict[str, Any] = Field(default_factory=dict)
    data_quality: DataQualityState
    observation_count: int = 0
    failure: FailurePrediction
    behavior: BehaviorClassification
    anomalies: List[AnomalyRecord] = Field(default_factory=list)
    generated_at_utc: float
