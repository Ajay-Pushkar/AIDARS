"""Distributed asset layer data models, wire schemas, and validation logic.

This module defines the foundational type contracts for AIDAR Milestone 1 through 5,
including worker registration, heartbeat telemetry, asset location candidates,
chunked binary transfer state, and metrics.
"""
from __future__ import annotations

import ipaddress
import json
import re
import time
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Set
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ============================================================================
# Validation Utilities
# ============================================================================

SHA256_HEX_REGEX = re.compile(r"^[a-fA-F0-9]{64}$")

# M9: explicit, documented, deterministic bound on WorkloadSpec.parameters'
# encoded JSON size. See WorkloadSpec.validate_parameters_size.
MAX_WORKLOAD_PARAMETERS_BYTES = 65536

_SENSITIVE_PARAMETER_KEY = re.compile(
    r"(?:^|[^a-z0-9])(?:secret|password|passwd|token|credential|authorization|"
    r"api[_-]?key|access[_-]?key|private[_-]?key)(?:$|[^a-z0-9])",
    re.IGNORECASE,
)
_SENSITIVE_COMMAND_VALUE = re.compile(
    r"(?i)(?P<prefix>(?:--?)(?:password|passwd|token|secret|api[-_]?key|"
    r"access[-_]?key|authorization)(?:=|\s+)|"
    r"(?:password|passwd|token|secret|api[-_]?key|access[-_]?key|authorization)"
    r"\s*=\s*)(?P<value>\"[^\"]*\"|'[^']*'|[^\s;&|]+)"
)
_URL_USERINFO = re.compile(r"(?i)(https?://)[^/@\s]+:[^/@\s]+@")
REDACTED_PARAMETER_VALUE = "[REDACTED]"


def redact_workload_parameters(value: Any, key: Optional[str] = None) -> Any:
    """Return metadata-safe parameter data without changing execution inputs.

    Secret references are not part of the current workload contract, so
    values under credential-shaped keys are redacted in persisted metadata.
    Common inline command credentials and URL userinfo are redacted as well.
    Ordinary non-sensitive parameter values pass through unchanged.
    """
    if key is not None and _SENSITIVE_PARAMETER_KEY.search(key.replace(".", "_")):
        return REDACTED_PARAMETER_VALUE
    if isinstance(value, dict):
        return {
            str(child_key): redact_workload_parameters(child_value, str(child_key))
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [redact_workload_parameters(item) for item in value]
    if isinstance(value, str):
        value = _URL_USERINFO.sub(r"\1[REDACTED]@", value)
        return _SENSITIVE_COMMAND_VALUE.sub(
            lambda match: match.group("prefix") + REDACTED_PARAMETER_VALUE,
            value,
        )
    return value


def validate_sha256_hex(val: str) -> str:
    """Validate that a string is a 64-character hexadecimal SHA-256 digest.

    Normalizes to lowercase. Rejects directory traversal tokens, null bytes,
    and non-hex characters.
    """
    if not isinstance(val, str):
        raise ValueError("SHA-256 digest must be a string")
    cleaned = val.strip().lower()
    if not SHA256_HEX_REGEX.match(cleaned):
        raise ValueError(
            f"Invalid SHA-256 hash format (must be 64 hex characters): {val!r}"
        )
    return cleaned


def validate_ip_address(val: str) -> str:
    """Validate that a string is a valid IPv4 or IPv6 address."""
    if not isinstance(val, str):
        raise ValueError("IP address must be a string")
    cleaned = val.strip()
    # Handle loopback hostnames / bracketed IPv6 defensively
    if cleaned.lower() in ("localhost", "127.0.0.1"):
        return "127.0.0.1"
    if cleaned.lower() in ("ip6-localhost", "ip6-loopback", "::1"):
        return "::1"
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    try:
        ip = ipaddress.ip_address(cleaned)
        return str(ip)
    except ValueError as exc:
        raise ValueError(f"Invalid IP address: {val!r}") from exc


def validate_endpoint_url(val: str) -> str:
    """Validate that an endpoint URL has an HTTP/HTTPS scheme and valid netloc."""
    if not isinstance(val, str):
        raise ValueError("Endpoint URL must be a string")
    cleaned = val.strip().rstrip("/")
    parsed = urlparse(cleaned)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"Endpoint URL scheme must be http or https, got {parsed.scheme!r}"
        )
    if not parsed.netloc:
        raise ValueError(f"Endpoint URL missing host/netloc: {val!r}")
    return cleaned


# ============================================================================
# Enums
# ============================================================================


class WorkerStatus(str, Enum):
    """Lifecycle status of a distributed worker node."""

    ACTIVE = "active"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"
    OFFLINE = "offline"


class LocalityTier(str, Enum):
    """Network proximity classification between requester and candidate."""

    LOOPBACK = "loopback"
    SUBNET = "subnet"
    LAN = "lan"
    WAN = "wan"


class FailureCategory(str, Enum):
    """M10.5: explicit, intentional retry classification for a failed
    execution attempt. Populated at the specific point in execution.py
    (or workload.py, for dispatch-level failures the worker never saw)
    where the failure actually occurred -- never inferred after the fact
    from string matching. See retry.py for which categories are
    retryable and retry.is_retryable()'s "unrecognized -> non-retryable"
    default.

    Retryable (environment/transient -- a new attempt has a genuine
    chance of succeeding):
      WORKER_UNAVAILABLE, TEMPORARY_CAS_FAILURE, RESOURCE_EXHAUSTION

    Non-retryable (about the workload/request itself -- retrying without
    changing anything will deterministically fail again):
      INVALID_INPUT, INVALID_DEPENDENCY, MISSING_EXECUTABLE,
      INVALID_WORKLOAD, APPLICATION_ERROR, AUTHORIZATION_FAILURE,
      MALFORMED_REQUEST
    """

    WORKER_UNAVAILABLE = "worker_unavailable"
    TEMPORARY_CAS_FAILURE = "temporary_cas_failure"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    EXECUTION_TIMEOUT = "execution_timeout"
    ASSET_TRANSFER_FAILURE = "asset_transfer_failure"
    ASSET_STAGING_FAILURE = "asset_staging_failure"

    INVALID_INPUT = "invalid_input"
    INVALID_DEPENDENCY = "invalid_dependency"
    MISSING_EXECUTABLE = "missing_executable"
    INVALID_WORKLOAD = "invalid_workload"
    APPLICATION_ERROR = "application_error"
    AUTHORIZATION_FAILURE = "authorization_failure"
    MALFORMED_REQUEST = "malformed_request"
    ARTIFACT_VERIFICATION_FAILURE = "artifact_verification_failure"
    EXECUTION_FAILURE = "execution_failure"


class TransferState(str, Enum):
    """State of an in-flight asset transfer."""

    IDLE = "idle"
    LOCATING = "locating"
    CONNECTING = "connecting"
    STREAMING = "streaming"
    VERIFYING = "verifying"
    COMMITTING = "committing"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


# ============================================================================
# Worker Capabilities & Metrics
# ============================================================================


class WorkerCapabilities(BaseModel):
    """Capabilities and limits advertised by a worker node."""

    model_config = ConfigDict(extra="ignore")

    can_serve_cas: bool = Field(
        default=True,
        description="Whether this worker can serve binary CAS chunks to peers.",
    )
    can_receive_cas: bool = Field(
        default=True,
        description="Whether this worker can receive and store CAS assets.",
    )
    max_concurrent_streams: int = Field(
        default=16,
        ge=1,
        le=256,
        description="Maximum concurrent HTTP streaming transfers supported.",
    )
    bandwidth_limit_mbps: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Optional upload rate limit in megabits per second.",
    )
    chunk_size_bytes: int = Field(
        default=1048576,  # 1 MiB
        ge=65536,         # 64 KiB min
        le=16777216,      # 16 MiB max
        description="Default chunk size for streaming binary transfers.",
    )
    supports_range_requests: bool = Field(
        default=True,
        description="Whether the worker supports HTTP Range resumption.",
    )
    supported_protocols: List[str] = Field(
        default_factory=lambda: ["http/1.1"],
        description="List of supported wire protocols.",
    )
    extra: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary extension capabilities.",
    )


class WorkerMetrics(BaseModel):
    """Real-time performance and resource utilization metrics."""

    model_config = ConfigDict(extra="ignore")

    active_transfers: int = Field(default=0, ge=0)
    active_uploads: int = Field(default=0, ge=0)
    active_downloads: int = Field(default=0, ge=0)
    cpu_percent: float = Field(default=0.0, ge=0.0, le=100.0)
    ram_percent: float = Field(default=0.0, ge=0.0, le=100.0)
    used_bytes: int = Field(default=0, ge=0)
    available_bytes: int = Field(default=0, ge=0)
    total_bytes_sent: int = Field(default=0, ge=0)
    total_bytes_received: int = Field(default=0, ge=0)
    transfer_error_count: int = Field(default=0, ge=0)
    uptime_seconds: float = Field(default=0.0, ge=0.0)


# ============================================================================
# Worker Registration & Registry Node State
# ============================================================================


class WorkerRegistrationPayload(BaseModel):
    """Payload sent by a worker to register with the coordinator."""

    model_config = ConfigDict(extra="ignore")

    worker_id: str = Field(..., min_length=1, max_length=128)
    endpoint_url: str = Field(...)
    ip_address: str = Field(...)
    port: int = Field(..., ge=1, le=65535)
    hostname: Optional[str] = Field(default=None, max_length=256)
    capacity_bytes: int = Field(default=0, ge=0)
    used_bytes: int = Field(default=0, ge=0)
    resource_profile: Optional[WorkerResourceProfile] = Field(
        default=None,
        description="Latest worker hardware telemetry. Optional for compatibility; absent telemetry cannot be used for compute placement.",
    )
    capabilities: WorkerCapabilities = Field(default_factory=WorkerCapabilities)
    inventory_hashes: Set[str] = Field(default_factory=set)
    tags: Dict[str, str] = Field(default_factory=dict)
    cost_per_hour: float = Field(
        default=0.0, ge=0.0,
        description="M17.8: Nominal cost rate of this worker per hour."
    )
    can_execute_workloads: bool = Field(
        default=True,
        description="Whether this node accepts compute workload placement, or exists purely as a CAS/asset source.",
    )

    @field_validator("endpoint_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        return validate_endpoint_url(v)

    @field_validator("ip_address")
    @classmethod
    def validate_ip(cls, v: str) -> str:
        return validate_ip_address(v)

    @field_validator("inventory_hashes")
    @classmethod
    def validate_hashes(cls, v: Set[str]) -> Set[str]:
        return {validate_sha256_hex(h) for h in v}

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        data["inventory_hashes"] = sorted(list(self.inventory_hashes))
        return data


class WorkerRegistrationResponse(BaseModel):
    """Response returned by the coordinator upon successful registration."""

    model_config = ConfigDict(extra="ignore")

    status: str = Field(default="registered")
    worker_id: str = Field(...)
    coordinator_id: str = Field(...)
    heartbeat_interval_seconds: float = Field(default=5.0, ge=0.01)
    heartbeat_timeout_seconds: float = Field(default=15.0, ge=0.01)
    registered_at_utc: float = Field(default_factory=time.time)
    acknowledged_inventory_count: int = Field(default=0, ge=0)
    worker_credential: Optional[str] = Field(
        default=None,
        description="M9: a fresh, per-worker bearer credential issued at successful "
                    "registration, returned exactly once. The worker must present it "
                    "as 'Authorization: Bearer <token>' on every subsequent call bound "
                    "to its worker_id (heartbeat, unregister). None when the coordinator "
                    "has no CredentialStore configured (e.g. programmatic register_worker_sync "
                    "callers that construct this model directly in tests).",
    )


class WorkerInfo(BaseModel):
    """Full node state maintained inside WorkerRegistry on the coordinator."""

    model_config = ConfigDict(extra="ignore")

    worker_id: str = Field(..., min_length=1, max_length=128)
    endpoint_url: str = Field(...)
    ip_address: str = Field(...)
    port: int = Field(..., ge=1, le=65535)
    hostname: Optional[str] = None
    status: WorkerStatus = Field(default=WorkerStatus.ACTIVE)
    capacity_bytes: int = Field(default=0, ge=0)
    used_bytes: int = Field(default=0, ge=0)
    capabilities: WorkerCapabilities = Field(default_factory=WorkerCapabilities)
    inventory_hashes: Set[str] = Field(default_factory=set)
    last_heartbeat_utc: float = Field(default_factory=time.time)
    estimated_rtt_ms: float = Field(default=0.0, ge=0.0)
    active_transfers: int = Field(default=0, ge=0)
    consecutive_heartbeat_failures: int = Field(default=0, ge=0)
    penalty_score: float = Field(default=0.0, ge=0.0)
    registered_at_utc: float = Field(default_factory=time.time)
    last_metrics: Optional[WorkerMetrics] = None
    resource_profile: Optional[WorkerResourceProfile] = Field(
        default=None,
        description="Latest resource telemetry snapshot, timestamped at coordinator receipt and persisted with this worker record.",
    )
    tags: Dict[str, str] = Field(default_factory=dict)
    cost_per_hour: float = Field(default=0.0, ge=0.0)
    can_execute_workloads: bool = Field(
        default=True,
        description="Whether this node accepts compute workload placement, or exists purely as a CAS/asset source.",
    )

    @field_validator("endpoint_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        return validate_endpoint_url(v)

    @field_validator("ip_address")
    @classmethod
    def validate_ip(cls, v: str) -> str:
        return validate_ip_address(v)

    @field_validator("inventory_hashes")
    @classmethod
    def validate_hashes(cls, v: Set[str]) -> Set[str]:
        return {validate_sha256_hex(h) for h in v}

    @property
    def available_bytes(self) -> int:
        return max(0, self.capacity_bytes - self.used_bytes)

    @property
    def is_healthy(self) -> bool:
        return self.status in (WorkerStatus.ACTIVE, WorkerStatus.DEGRADED)

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        data["inventory_hashes"] = sorted(list(self.inventory_hashes))
        data["available_bytes"] = self.available_bytes
        data["is_healthy"] = self.is_healthy
        return data


# ============================================================================
# Heartbeat & Health Check Models
# ============================================================================


class HeartbeatPayload(BaseModel):
    """Periodic liveness and metric update sent by worker to coordinator."""

    model_config = ConfigDict(extra="ignore")

    worker_id: str = Field(..., min_length=1)
    timestamp_utc: float = Field(default_factory=time.time)
    metrics: Optional[WorkerMetrics] = None
    resource_profile: Optional[WorkerResourceProfile] = Field(
        default=None,
        description="Refreshed worker hardware telemetry. Missing telemetry is not treated as fresh.",
    )
    active_transfers: int = Field(default=0, ge=0)
    used_bytes: Optional[int] = Field(default=None, ge=0)
    available_bytes: Optional[int] = Field(default=None, ge=0)
    inventory_delta_added: Set[str] = Field(default_factory=set)
    inventory_delta_removed: Set[str] = Field(default_factory=set)

    @field_validator("inventory_delta_added", "inventory_delta_removed")
    @classmethod
    def validate_deltas(cls, v: Set[str]) -> Set[str]:
        return {validate_sha256_hex(h) for h in v}

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        data["inventory_delta_added"] = sorted(list(self.inventory_delta_added))
        data["inventory_delta_removed"] = sorted(list(self.inventory_delta_removed))
        return data


class HeartbeatResponse(BaseModel):
    """Coordinator acknowledgment for a heartbeat ping."""

    model_config = ConfigDict(extra="ignore")

    status: str = Field(default="healthy")
    acknowledged_at_utc: float = Field(default_factory=time.time)
    coordinator_time_utc: float = Field(default_factory=time.time)
    re_register_required: bool = Field(default=False)


class PingRequest(BaseModel):
    """Probing ping request payload for RTT estimation."""

    model_config = ConfigDict(extra="ignore")

    client_timestamp_utc: float = Field(default_factory=time.time)
    sequence_number: Optional[int] = None
    payload: Optional[str] = None


class PongResponse(BaseModel):
    """Pong response acknowledgment for RTT estimation."""

    model_config = ConfigDict(extra="ignore")

    worker_id: str = Field(...)
    client_timestamp_utc: float = Field(...)
    server_timestamp_utc: float = Field(default_factory=time.time)
    status: str = Field(default="pong")
    sequence_number: Optional[int] = None


# ============================================================================
# Asset Location Models
# ============================================================================


class CandidateSource(BaseModel):
    """Ranked peer source from which an asset can be streamed."""

    model_config = ConfigDict(extra="ignore")

    worker_id: str = Field(...)
    endpoint_url: str = Field(...)
    ip_address: str = Field(...)
    port: int = Field(..., ge=1, le=65535)
    locality_tier: str = Field(default=LocalityTier.LAN.value)
    estimated_rtt_ms: float = Field(default=0.0, ge=0.0)
    load_factor: float = Field(default=0.0, ge=0.0)
    penalty_score: float = Field(default=0.0, ge=0.0)
    priority_score: float = Field(default=0.0)
    can_serve: bool = Field(default=True)

    @field_validator("endpoint_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        return validate_endpoint_url(v)

    @field_validator("ip_address")
    @classmethod
    def validate_ip(cls, v: str) -> str:
        return validate_ip_address(v)


class LocateAssetsRequest(BaseModel):
    """Request sent by a worker to locate missing assets across the cluster."""

    model_config = ConfigDict(extra="ignore")

    requester_worker_id: str = Field(...)
    missing_hashes: List[str] = Field(...)
    requester_ip: Optional[str] = Field(default=None)
    max_candidates_per_asset: int = Field(default=5, ge=1, le=20)
    include_degraded: bool = Field(default=True)

    @field_validator("missing_hashes")
    @classmethod
    def validate_missing_hashes(cls, v: List[str]) -> List[str]:
        return [validate_sha256_hex(h) for h in v]

    @field_validator("requester_ip")
    @classmethod
    def validate_req_ip(cls, v: Optional[str]) -> Optional[str]:
        return validate_ip_address(v) if v else None


class LocateAssetsResponse(BaseModel):
    """Response returned by coordinator mapping hashes to candidate sources."""

    model_config = ConfigDict(extra="ignore")

    locations: Dict[str, List[CandidateSource]] = Field(default_factory=dict)
    unresolved_hashes: List[str] = Field(default_factory=list)

    @property
    def resolved_count(self) -> int:
        return len(self.locations)

    @property
    def unresolved_count(self) -> int:
        return len(self.unresolved_hashes)


# ============================================================================
# Binary Transfer & Stream Models
# ============================================================================


class StreamMetadataHeader(BaseModel):
    """Metadata describing a chunked binary stream served over HTTP."""

    model_config = ConfigDict(extra="ignore")

    sha256: str = Field(...)
    total_size_bytes: int = Field(..., ge=0)
    chunk_size_bytes: int = Field(default=1048576, ge=1024)
    offset_bytes: int = Field(default=0, ge=0)
    content_range: Optional[str] = Field(default=None)
    content_type: str = Field(default="application/octet-stream")

    @field_validator("sha256")
    @classmethod
    def validate_hash(cls, v: str) -> str:
        return validate_sha256_hex(v)


class TransferProgress(BaseModel):
    """Active streaming transfer progress."""

    model_config = ConfigDict(extra="ignore")

    transfer_id: str = Field(...)
    sha256: str = Field(...)
    bytes_transferred: int = Field(default=0, ge=0)
    total_bytes: int = Field(default=0, ge=0)
    throughput_bytes_per_sec: float = Field(default=0.0, ge=0.0)
    elapsed_seconds: float = Field(default=0.0, ge=0.0)
    resumed_from_offset: int = Field(default=0, ge=0)
    current_source: Optional[CandidateSource] = Field(default=None)
    state: TransferState = Field(default=TransferState.IDLE)
    error_message: Optional[str] = Field(default=None)

    @field_validator("sha256")
    @classmethod
    def validate_hash(cls, v: str) -> str:
        return validate_sha256_hex(v)

    @property
    def progress_fraction(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        return min(1.0, self.bytes_transferred / self.total_bytes)

    @property
    def throughput_mbps(self) -> float:
        return (self.throughput_bytes_per_sec * 8.0) / 1_000_000.0


class TransferResult(BaseModel):
    """Outcome of an asset transfer, SHA-256 verification, and CAS commit."""

    model_config = ConfigDict(extra="ignore")

    sha256: str = Field(...)
    success: bool = Field(...)
    bytes_transferred: int = Field(default=0, ge=0)
    total_bytes: int = Field(default=0, ge=0)
    verified_sha256: Optional[str] = Field(default=None)
    committed_path: Optional[str] = Field(default=None)
    source_worker_id: Optional[str] = Field(default=None)
    source_endpoint_url: Optional[str] = Field(default=None)
    resumed_bytes: int = Field(default=0, ge=0)
    retry_count: int = Field(default=0, ge=0)
    duration_seconds: float = Field(default=0.0, ge=0.0)
    error_message: Optional[str] = Field(default=None)

    @field_validator("sha256")
    @classmethod
    def validate_target_hash(cls, v: str) -> str:
        return validate_sha256_hex(v)

    @field_validator("verified_sha256")
    @classmethod
    def validate_verified_hash(cls, v: Optional[str]) -> Optional[str]:
        return validate_sha256_hex(v) if v else None

    @property
    def throughput_mbps(self) -> float:
        if self.duration_seconds <= 0 or self.bytes_transferred <= 0:
            return 0.0
        return (self.bytes_transferred * 8.0) / (self.duration_seconds * 1_000_000.0)


# ============================================================================
# Telemetry & Metrics Models
# ============================================================================


class TransferMetrics(BaseModel):
    """Telemetry recording Byte Hit Ratio (BHR), network savings, and throughput."""

    model_config = ConfigDict(extra="ignore")

    total_requested_assets: int = Field(default=0, ge=0)
    local_cache_hit_assets: int = Field(default=0, ge=0)
    network_transferred_assets: int = Field(default=0, ge=0)
    failed_transfers: int = Field(default=0, ge=0)
    total_requested_bytes: int = Field(default=0, ge=0)
    local_cache_hit_bytes: int = Field(default=0, ge=0)
    network_transferred_bytes: int = Field(default=0, ge=0)
    resumption_events: int = Field(default=0, ge=0)
    failover_events: int = Field(default=0, ge=0)
    average_throughput_mbps: float = Field(default=0.0, ge=0.0)

    @property
    def byte_hit_ratio(self) -> float:
        if self.total_requested_bytes <= 0:
            return 0.0
        return min(1.0, max(0.0, self.local_cache_hit_bytes / self.total_requested_bytes))

    @property
    def network_savings_percent(self) -> float:
        return round(self.byte_hit_ratio * 100.0, 2)

    def to_dict(self) -> Dict[str, Any]:
        data = self.model_dump()
        data["byte_hit_ratio"] = round(self.byte_hit_ratio, 4)
        data["network_savings_percent"] = self.network_savings_percent
        return data


class ClusterTelemetry(BaseModel):
    """Global cluster-wide health, inventory, and capacity statistics."""

    model_config = ConfigDict(extra="ignore")

    coordinator_id: str = Field(...)
    uptime_seconds: float = Field(default=0.0, ge=0.0)
    total_registered_workers: int = Field(default=0, ge=0)
    active_workers: int = Field(default=0, ge=0)
    degraded_workers: int = Field(default=0, ge=0)
    unhealthy_workers: int = Field(default=0, ge=0)
    offline_workers: int = Field(default=0, ge=0)
    unique_cached_assets_count: int = Field(default=0, ge=0)
    total_inventory_records: int = Field(default=0, ge=0)
    total_cluster_capacity_bytes: int = Field(default=0, ge=0)
    total_cluster_used_bytes: int = Field(default=0, ge=0)
    aggregate_active_transfers: int = Field(default=0, ge=0)


# ============================================================================
# M6 Workload & Execution Models
# ============================================================================


class ExecutionSpec(BaseModel):
    """Structured, secure execution parameters replacing shell commands."""
    
    model_config = ConfigDict(extra="ignore")
    
    executable: str = Field(..., min_length=1)
    args: List[str] = Field(default_factory=list)
    env: Dict[str, str] = Field(default_factory=dict)
    cwd: Optional[str] = Field(default=None)
    timeout_seconds: Optional[float] = Field(default=None, gt=0)


class OutputVerificationPolicy(BaseModel):
    """Declarative expectations for workload outputs independent of the runtime."""
    
    model_config = ConfigDict(extra="ignore")
    
    expected_output_count: Optional[int] = Field(default=None, ge=1)
    expected_extensions: Optional[List[str]] = Field(default=None)


class ExecutionShape(str, Enum):
    """Generic execution arrangement declared by an application submission."""

    SINGLE_MACHINE = "single_machine"
    TASK_SPLIT = "task_split"
    PIPELINE = "pipeline"
    DISTRIBUTED_NATIVE = "distributed_native"


class RuntimeExecutionContext(BaseModel):
    """Ephemeral, application-neutral view of a distributed execution group."""

    execution_id: str
    worker_id: str
    rank: int = Field(ge=0)
    world_size: int = Field(ge=1)
    workers: List[Dict[str, Any]] = Field(default_factory=list)


class WorkloadExecutionRequest(BaseModel):
    spec: "WorkloadSpec"
    execution_context: RuntimeExecutionContext


class ExecutionGroup(BaseModel):
    """Durable membership and lifecycle for one distributed-native attempt."""

    execution_id: str
    worker_ids: List[str]
    required_worker_count: int = Field(ge=2, le=64)
    state: Literal["assigned", "running", "completed", "failed", "lost"] = "assigned"
    unavailable_worker_ids: Set[str] = Field(default_factory=set)
    placement_decisions: List["PlacementDecision"] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_membership(self) -> "ExecutionGroup":
        if len(self.worker_ids) != self.required_worker_count or len(set(self.worker_ids)) != len(self.worker_ids):
            raise ValueError("execution group must contain the required number of distinct workers")
        if not self.unavailable_worker_ids.issubset(set(self.worker_ids)):
            raise ValueError("unavailable workers must belong to the execution group")
        if self.placement_decisions and [item.selected_worker_id for item in self.placement_decisions] != self.worker_ids:
            raise ValueError("execution group requires one placement decision per worker in membership order")
        return self


class ExecutionDescription(BaseModel):
    """Adapter output: execution shape plus the existing generic workloads."""

    model_config = ConfigDict(extra="forbid")

    execution_shape: ExecutionShape
    workloads: List["WorkloadSpec"] = Field(min_length=1)
    required_worker_count: int = Field(default=1, ge=1, le=64)

    @model_validator(mode="after")
    def validate_plan(self) -> "ExecutionDescription":
        if self.execution_shape == ExecutionShape.DISTRIBUTED_NATIVE:
            if len(self.workloads) != 1 or self.required_worker_count < 2:
                raise ValueError("DISTRIBUTED_NATIVE requires one workload and at least two workers")
        elif self.required_worker_count != 1:
            raise ValueError("required_worker_count is only valid for DISTRIBUTED_NATIVE")
        elif self.execution_shape == ExecutionShape.SINGLE_MACHINE and len(self.workloads) != 1:
            raise ValueError("SINGLE_MACHINE requires exactly one workload")
        if self.execution_shape == ExecutionShape.PIPELINE:
            if len(self.workloads) < 2 or not any(item.depends_on for item in self.workloads):
                raise ValueError("PIPELINE requires multiple workloads and at least one dependency")
            ids = {item.workload_id for item in self.workloads}
            if any(dep not in ids for item in self.workloads for dep in item.depends_on):
                raise ValueError("pipeline dependencies must refer to workloads in the same execution")
            _validate_dependency_graph({item.workload_id: item.depends_on for item in self.workloads})
        return self


def _validate_dependency_graph(dependencies: Dict[str, List[str]]) -> None:
    visiting: Set[str] = set()
    visited: Set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            raise ValueError("execution dependencies must not contain cycles")
        if node in visited:
            return
        visiting.add(node)
        for dependency in dependencies.get(node, []):
            if dependency not in dependencies:
                raise ValueError(f"unknown workload dependency: {dependency}")
            visit(dependency)
        visiting.remove(node)
        visited.add(node)

    for node in dependencies:
        visit(node)

class WorkloadSpec(BaseModel):
    """Declarative specification of a computational task."""
    model_config = ConfigDict(extra="ignore")

    workload_id: str = Field(..., min_length=1, max_length=128)
    job_id: Optional[str] = Field(
        default=None,
        max_length=128,
        description="M8: the Job this workload belongs to, if any. First-class field "
                    "(not hidden inside parameters) so it survives persistence/recovery "
                    "without adapter-specific convention. None for a standalone workload "
                    "submitted outside any Job.",
    )
    task_type: str = Field(..., min_length=1, max_length=64)  # e.g., "compute", "simulation", "render", "analysis"
    input_asset_hashes: Set[str] = Field(default_factory=set)

    min_cpu_cores: int = Field(default=1, ge=1)
    min_ram_bytes: int = Field(default=1024 * 1024 * 1024, ge=1)  # 1 GiB default

    requires_gpu: bool = Field(default=False)
    min_vram_bytes: int = Field(default=0, ge=0)

    estimated_duration_seconds: float = Field(default=10.0, gt=0.0)
    priority: int = Field(default=100, ge=0)
    deadline_at_utc: Optional[float] = Field(
        default=None, gt=0,
        description="Optional absolute UTC Unix timestamp. Affects admission urgency only; it is not an execution timeout.",
    )
    required_gpu_vendor: Optional[str] = None
    required_gpu_model: Optional[str] = None
    min_compute_capability: Optional[str] = None
    required_driver_version: Optional[str] = None
    required_runtime_compatibility: Optional[str] = None
    required_worker_tags: Dict[str, str] = Field(default_factory=dict)
    preferred_worker_tags: Dict[str, str] = Field(default_factory=dict)
    affinity_group_id: Optional[str] = Field(default=None, max_length=128)
    affinity_mode: Literal["none", "same_worker", "different_worker", "same_tag", "different_tag"] = "none"
    affinity_tag_key: Optional[str] = Field(default=None, max_length=128)
    affinity_hard: bool = True
    max_cost_per_hour: Optional[float] = Field(
        default=None, ge=0.0,
        description="M17.8: Hard constraint on worker cost per hour. Workers exceeding this are ineligible."
    )
    # Dependency completion is success-only: every listed workload must
    # complete before this workload becomes runnable.
    depends_on: List[str] = Field(default_factory=list, max_length=1000)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    execution_spec: Optional["ExecutionSpec"] = Field(default=None)

    @field_validator("input_asset_hashes")
    @classmethod
    def validate_hashes(cls, v: Set[str]) -> Set[str]:
        return {validate_sha256_hex(h) for h in v}

    @model_validator(mode="after")
    def validate_affinity(self) -> "WorkloadSpec":
        if self.affinity_mode != "none" and not self.affinity_group_id:
            raise ValueError("affinity_group_id is required when affinity_mode is not 'none'")
        if self.affinity_mode in ("same_tag", "different_tag") and not self.affinity_tag_key:
            raise ValueError("affinity_tag_key is required for tag-scoped affinity")
        if self.workload_id in self.depends_on or len(self.depends_on) != len(set(self.depends_on)):
            raise ValueError("dependencies must be unique and cannot include the workload itself")
        return self

    @field_validator("parameters")
    @classmethod
    def validate_parameters_size(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """M9: parameters is free-form and adapter-controlled, so it has no
        structural size limit otherwise -- bound it explicitly to prevent
        an oversized-metadata submission from a malicious or buggy caller.
        64 KiB is generous for genuine per-chunk metadata (frame ranges,
        chunk indices, expected-output counts) while still being a hard,
        documented, deterministic ceiling."""
        encoded_size = len(json.dumps(v, default=str))
        if encoded_size > MAX_WORKLOAD_PARAMETERS_BYTES:
            raise ValueError(
                f"parameters exceeds the maximum size of {MAX_WORKLOAD_PARAMETERS_BYTES} "
                f"bytes (encoded size: {encoded_size})"
            )
        return v

    def redacted_metadata_json(self, *, indent: Optional[int] = None) -> str:
        """Serialize this spec for metadata storage without mutating execution input."""
        safe_parameters = redact_workload_parameters(self.parameters)
        safe_spec = self.model_copy(update={"parameters": safe_parameters})
        return safe_spec.model_dump_json(indent=indent)


ExecutionDescription.model_rebuild()
WorkloadExecutionRequest.model_rebuild()


class WorkerResourceProfile(BaseModel):
    """Real-time compute and hardware state advertised by a worker."""
    model_config = ConfigDict(extra="ignore")

    worker_id: str = Field(...)
    endpoint_url: str = Field(...)
    ip_address: str = Field(...)

    cpu_cores_total: int = Field(..., ge=1)
    cpu_utilization_percent: float = Field(..., ge=0.0, le=100.0)

    ram_total_bytes: int = Field(..., ge=1)
    ram_available_bytes: int = Field(..., ge=0)

    gpu_available: bool = Field(default=False)
    gpu_device_name: Optional[str] = None
    gpu_vendor: Optional[str] = None
    gpu_model: Optional[str] = None
    gpu_compute_capability: Optional[str] = None
    gpu_driver_version: Optional[str] = None
    vram_total_bytes: int = Field(default=0, ge=0)
    vram_available_bytes: int = Field(default=0, ge=0)

    active_workload_count: int = Field(default=0, ge=0)
    max_concurrent_workloads: int = Field(default=10, ge=1)
    status: WorkerStatus = Field(default=WorkerStatus.ACTIVE)
    local_cached_hashes: Set[str] = Field(default_factory=set)
    cost_per_hour: float = Field(default=0.0, ge=0.0)
    timestamp_utc: Optional[float] = Field(
        default=None,
        description="Telemetry sample or coordinator receipt time; None means freshness is unknown.",
    )
    can_execute_workloads: bool = Field(
        default=True,
        description="Whether this node accepts compute workload placement, or exists purely as a CAS/asset source.",
    )


# WorkerResourceProfile is declared after the wire and persistence models to
# keep the existing model organization. Resolve these annotations once the
# complete module has loaded.
WorkerRegistrationPayload.model_rebuild()
WorkerInfo.model_rebuild()
HeartbeatPayload.model_rebuild()


class PlacementDecision(BaseModel):
    """Explainable output of the multi-attribute placement decision engine."""
    model_config = ConfigDict(extra="ignore")

    workload_id: str
    selected_worker_id: str
    placement_score: float
    score_breakdown: Dict[str, float]  # e.g., {"compute": 0.85, "locality": 1.0, "latency": -0.05}
    missing_assets_on_worker: Set[str]
    execution_tier: str  # "local", "subnet", "lan"
    decision_timestamp_utc: float = Field(default_factory=time.time)
    candidate_explanations: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Additive per-candidate hard eligibility and soft ranking facts; absent on legacy records.",
    )
    m7_ranking_adjustments: Dict[str, float] = Field(
        default_factory=dict,
        description="Per-worker ordinal rank movement after M7 advisory reordering; not part of placement_score.",
    )
    locality_details: Dict[str, Any] = Field(default_factory=dict)
    deadline_details: Dict[str, Any] = Field(default_factory=dict)


ExecutionGroup.model_rebuild()


class WorkloadExecutionResult(BaseModel):
    """Immutable result metadata returned after workload completion."""
    model_config = ConfigDict(extra="ignore")

    workload_id: str
    worker_id: str
    success: bool
    output_asset_hashes: Set[str]  # SHA-256 artifacts committed to CAS
    output_asset_sizes: Dict[str, int] = Field(
        default_factory=dict,
        description="M8.8: byte size per output_asset_hashes entry, keyed by hash. "
                     "Populated by the worker at ingestion time (already reading the "
                     "bytes to hash them, so the size is free) for the Artifact model's "
                     "size field. Optional/additive -- absent or missing keys mean size "
                     "is simply unknown for that hash, not an error.",
    )
    execution_duration_seconds: float
    error_message: Optional[str] = None
    stdout_snippet: Optional[str] = None
    stderr_snippet: Optional[str] = None
    was_checkpointed: bool = Field(default=False)
    checkpoint_hash: Optional[str] = None

    # M10.5: explicit retry classification for a failed result. None for
    # a successful result, or for a failure predating this field's
    # introduction (backward-compatible: absent/None simply means
    # "unclassified", not an error).
    failure_category: Optional[FailureCategory] = Field(default=None)

    # M10.7 checkpoint capability metadata -- only meaningful when
    # was_checkpointed is True. checkpoint_runtime_type/format_version
    # let checkpoint.py validate a checkpoint without a second identity
    # scheme (content identity is still purely the CAS hash above).
    checkpoint_runtime_type: Optional[str] = Field(default=None)
    checkpoint_format_version: Optional[int] = Field(default=None)

    # M10.11: execution observability timeline. staging = dependency
    # staging into the sandbox (the asset-synchronization-equivalent
    # phase for this in-process dispatch path -- see docs/M10_ARCHITECTURE.md
    # for why a separate cross-worker asset-sync phase isn't separately
    # instrumented here). execution_duration_seconds above already is,
    # and remains, the pure runtime-execution phase duration (unchanged
    # from pre-M10 behavior). output_ingestion = writing produced files
    # into CAS. verification = the M8.6 expected-output-count check.
    staging_duration_seconds: float = Field(default=0.0, ge=0.0)
    output_ingestion_duration_seconds: float = Field(default=0.0, ge=0.0)
    verification_duration_seconds: float = Field(default=0.0, ge=0.0)
    transfer_duration_seconds: float = Field(default=0.0, ge=0.0)

    @property
    def total_duration_seconds(self) -> float:
        """Sum of every measured phase -- lets a caller/test distinguish
        which phase dominated without a second duplicated total field."""
        return (
            self.staging_duration_seconds
            + self.execution_duration_seconds
            + self.output_ingestion_duration_seconds
            + self.verification_duration_seconds
            + self.transfer_duration_seconds
        )
