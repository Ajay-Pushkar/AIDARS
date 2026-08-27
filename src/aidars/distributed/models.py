"""Distributed asset layer data models, wire schemas, and validation logic.

This module defines the foundational type contracts for AIDAR Milestone 1 through 5,
including worker registration, heartbeat telemetry, asset location candidates,
chunked binary transfer state, and metrics.
"""
from __future__ import annotations

import ipaddress
import re
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ============================================================================
# Validation Utilities
# ============================================================================

SHA256_HEX_REGEX = re.compile(r"^[a-fA-F0-9]{64}$")


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
    capabilities: WorkerCapabilities = Field(default_factory=WorkerCapabilities)
    inventory_hashes: Set[str] = Field(default_factory=set)
    tags: Dict[str, str] = Field(default_factory=dict)

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
    tags: Dict[str, str] = Field(default_factory=dict)

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

class WorkloadSpec(BaseModel):
    """Declarative specification of a computational task."""
    model_config = ConfigDict(extra="ignore")

    workload_id: str = Field(..., min_length=1, max_length=128)
    task_type: str = Field(..., min_length=1, max_length=64)  # e.g., "compute", "simulation", "render", "analysis"
    input_asset_hashes: Set[str] = Field(default_factory=set)

    min_cpu_cores: int = Field(default=1, ge=1)
    min_ram_bytes: int = Field(default=1024 * 1024 * 1024, ge=1)  # 1 GiB default

    requires_gpu: bool = Field(default=False)
    min_vram_bytes: int = Field(default=0, ge=0)

    estimated_duration_seconds: float = Field(default=10.0, gt=0.0)
    priority: int = Field(default=100, ge=0)
    parameters: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("input_asset_hashes")
    @classmethod
    def validate_hashes(cls, v: Set[str]) -> Set[str]:
        return {validate_sha256_hex(h) for h in v}


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
    vram_total_bytes: int = Field(default=0, ge=0)
    vram_available_bytes: int = Field(default=0, ge=0)

    active_workload_count: int = Field(default=0, ge=0)
    max_concurrent_workloads: int = Field(default=10, ge=1)
    status: WorkerStatus = Field(default=WorkerStatus.ACTIVE)
    local_cached_hashes: Set[str] = Field(default_factory=set)
    timestamp_utc: float = Field(default_factory=time.time)


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


class WorkloadExecutionResult(BaseModel):
    """Immutable result metadata returned after workload completion."""
    model_config = ConfigDict(extra="ignore")

    workload_id: str
    worker_id: str
    success: bool
    output_asset_hashes: Set[str]  # Verified SHA-256 artifacts committed to CAS
    execution_duration_seconds: float
    error_message: Optional[str] = None
    stdout_snippet: Optional[str] = None
    stderr_snippet: Optional[str] = None
    was_checkpointed: bool = Field(default=False)
    checkpoint_hash: Optional[str] = None

