"""AIDAR Distributed Asset Layer module.

Provides network-aware distributed asset caching, worker registry, peer discovery,
and resilient streaming transfers.
"""
from aidars.distributed.cas_adapter import (
    CASAdapter,
    LocalCASAdapter,
)
from aidars.distributed.client import DistributedClient
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.metrics import (
    TransferEvent,
    TransferMetricsTracker,
)
from aidars.distributed.models import (
    CandidateSource,
    ClusterTelemetry,
    HeartbeatPayload,
    HeartbeatResponse,
    LocalityTier,
    LocateAssetsRequest,
    LocateAssetsResponse,
    PingRequest,
    PongResponse,
    StreamMetadataHeader,
    TransferMetrics,
    TransferProgress,
    TransferResult,
    TransferState,
    WorkerCapabilities,
    WorkerInfo,
    WorkerMetrics,
    WorkerRegistrationPayload,
    WorkerRegistrationResponse,
    WorkerStatus,
    validate_endpoint_url,
    validate_ip_address,
    validate_sha256_hex,
)
from aidars.distributed.prioritizer import (
    CandidatePrioritizer,
    LatencyTracker,
    classify_locality,
    measure_ping_rtt,
    normalize_ip,
)
from aidars.distributed.registry import (
    ClusterStats,
    WorkerHealthRecord,
    WorkerHealthStatus,
    WorkerRegistry,
)
from aidars.distributed.server import WorkerServer, create_worker_app
from aidars.distributed.transfer import (
    DEFAULT_CHUNK_SIZE,
    CandidateExhaustedError,
    CASCommitError,
    IntegrityError,
    StagingContext,
    StreamAbortError,
    TransferError,
    WorkerHttpError,
    download_stream_to_staging,
    generate_bounded_chunks,
    parse_byte_range_header,
    transfer_asset_from_candidate,
    transfer_asset_with_failover,
)
from aidars.distributed.worker import DistributedWorker

__all__ = [
    # Models & Enums
    "WorkerStatus",
    "LocalityTier",
    "TransferState",
    "WorkerCapabilities",
    "WorkerMetrics",
    "WorkerRegistrationPayload",
    "WorkerRegistrationResponse",
    "WorkerInfo",
    "HeartbeatPayload",
    "HeartbeatResponse",
    "PingRequest",
    "PongResponse",
    "CandidateSource",
    "LocateAssetsRequest",
    "LocateAssetsResponse",
    "StreamMetadataHeader",
    "TransferProgress",
    "TransferResult",
    "TransferMetrics",
    "ClusterTelemetry",
    "validate_sha256_hex",
    "validate_ip_address",
    "validate_endpoint_url",
    # CAS Adapter
    "CASAdapter",
    "LocalCASAdapter",
    # Registry & Coordinator
    "WorkerHealthStatus",
    "WorkerHealthRecord",
    "ClusterStats",
    "WorkerRegistry",
    "CoordinatorService",
    # Prioritizer
    "normalize_ip",
    "classify_locality",
    "measure_ping_rtt",
    "LatencyTracker",
    "CandidatePrioritizer",
    # Server & Data Plane
    "WorkerServer",
    "create_worker_app",
    # Client
    "DistributedClient",
    # Worker Runtime
    "DistributedWorker",
    # Metrics
    "TransferEvent",
    "TransferMetricsTracker",
    # Transfer & Staging
    "DEFAULT_CHUNK_SIZE",
    "StagingContext",
    "parse_byte_range_header",
    "generate_bounded_chunks",
    "download_stream_to_staging",
    "transfer_asset_from_candidate",
    "transfer_asset_with_failover",
    # Exceptions
    "TransferError",
    "IntegrityError",
    "StreamAbortError",
    "WorkerHttpError",
    "CandidateExhaustedError",
    "CASCommitError",
]
