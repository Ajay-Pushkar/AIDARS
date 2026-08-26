"""Unit tests for distributed asset layer data models, wire schemas, and validation."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

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

VALID_HASH_1 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
VALID_HASH_2 = "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
VALID_HASH_UPPER = "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"


# ============================================================================
# Validation Utility Tests
# ============================================================================


def test_validate_sha256_hex_valid():
    assert validate_sha256_hex(VALID_HASH_1) == VALID_HASH_1
    assert validate_sha256_hex(VALID_HASH_UPPER) == VALID_HASH_1
    assert validate_sha256_hex(f"  {VALID_HASH_1}  ") == VALID_HASH_1


def test_validate_sha256_hex_invalid():
    # Length != 64
    with pytest.raises(ValueError, match="Invalid SHA-256 hash format"):
        validate_sha256_hex("abcd")

    # Non-hex characters
    with pytest.raises(ValueError, match="Invalid SHA-256 hash format"):
        validate_sha256_hex("g" * 64)

    # Path traversal attempts
    with pytest.raises(ValueError, match="Invalid SHA-256 hash format"):
        validate_sha256_hex("../../etc/passwd")

    with pytest.raises(ValueError, match="Invalid SHA-256 hash format"):
        validate_sha256_hex(VALID_HASH_1[:60] + "/../")

    # Non-string types
    with pytest.raises(ValueError, match="must be a string"):
        validate_sha256_hex(12345)  # type: ignore


def test_validate_ip_address_valid():
    assert validate_ip_address("192.168.1.100") == "192.168.1.100"
    assert validate_ip_address("127.0.0.1") == "127.0.0.1"
    assert validate_ip_address("::1") == "::1"
    assert validate_ip_address("localhost") == "127.0.0.1"
    assert validate_ip_address("[::1]") == "::1"
    assert validate_ip_address("  10.0.0.1  ") == "10.0.0.1"


def test_validate_ip_address_invalid():
    with pytest.raises(ValueError, match="Invalid IP address"):
        validate_ip_address("999.999.999.999")

    with pytest.raises(ValueError, match="Invalid IP address"):
        validate_ip_address("not_an_ip")

    with pytest.raises(ValueError, match="must be a string"):
        validate_ip_address(1234)  # type: ignore


def test_validate_endpoint_url_valid():
    assert validate_endpoint_url("http://192.168.1.100:8000") == "http://192.168.1.100:8000"
    assert validate_endpoint_url("https://worker.aidar.internal:8443/") == "https://worker.aidar.internal:8443"
    assert validate_endpoint_url("http://localhost:9000/api/v1") == "http://localhost:9000/api/v1"


def test_validate_endpoint_url_invalid():
    with pytest.raises(ValueError, match="scheme must be http or https"):
        validate_endpoint_url("ftp://192.168.1.100:21")

    with pytest.raises(ValueError, match="missing host/netloc"):
        validate_endpoint_url("http://")

    with pytest.raises(ValueError, match="must be a string"):
        validate_endpoint_url(None)  # type: ignore


# ============================================================================
# Capabilities & Metrics Model Tests
# ============================================================================


def test_worker_capabilities_defaults():
    caps = WorkerCapabilities()
    assert caps.can_serve_cas is True
    assert caps.can_receive_cas is True
    assert caps.max_concurrent_streams == 16
    assert caps.bandwidth_limit_mbps is None
    assert caps.chunk_size_bytes == 1048576
    assert caps.supports_range_requests is True
    assert "http/1.1" in caps.supported_protocols


def test_worker_capabilities_custom_and_bounds():
    caps = WorkerCapabilities(
        can_serve_cas=False,
        max_concurrent_streams=32,
        bandwidth_limit_mbps=100.5,
        chunk_size_bytes=2097152,
    )
    assert caps.max_concurrent_streams == 32
    assert caps.bandwidth_limit_mbps == 100.5
    assert caps.chunk_size_bytes == 2097152

    # Bounds check: max_concurrent_streams ge=1, le=256
    with pytest.raises(ValidationError):
        WorkerCapabilities(max_concurrent_streams=0)
    with pytest.raises(ValidationError):
        WorkerCapabilities(max_concurrent_streams=300)


def test_worker_metrics():
    metrics = WorkerMetrics(
        active_transfers=3,
        active_uploads=2,
        active_downloads=1,
        cpu_percent=45.2,
        ram_percent=60.1,
        used_bytes=1000000,
        available_bytes=5000000,
        total_bytes_sent=2000000,
        total_bytes_received=1500000,
        uptime_seconds=3600.0,
    )
    assert metrics.active_transfers == 3
    assert metrics.cpu_percent == 45.2

    with pytest.raises(ValidationError):
        WorkerMetrics(cpu_percent=-5.0)
    with pytest.raises(ValidationError):
        WorkerMetrics(ram_percent=150.0)


# ============================================================================
# Worker Registration & Info Tests
# ============================================================================


def test_worker_registration_payload():
    payload = WorkerRegistrationPayload(
        worker_id="worker-01",
        endpoint_url="http://192.168.1.50:8000",
        ip_address="192.168.1.50",
        port=8000,
        capacity_bytes=100000000,
        used_bytes=25000000,
        inventory_hashes={VALID_HASH_UPPER, VALID_HASH_2},
    )
    assert payload.worker_id == "worker-01"
    assert VALID_HASH_1 in payload.inventory_hashes
    assert len(payload.inventory_hashes) == 2

    dump = payload.to_dict()
    assert isinstance(dump["inventory_hashes"], list)
    assert dump["inventory_hashes"] == sorted(list(payload.inventory_hashes))


def test_worker_registration_payload_validation():
    with pytest.raises(ValidationError):
        WorkerRegistrationPayload(
            worker_id="",  # min_length=1
            endpoint_url="http://192.168.1.50:8000",
            ip_address="192.168.1.50",
            port=8000,
        )

    with pytest.raises(ValidationError):
        WorkerRegistrationPayload(
            worker_id="w-1",
            endpoint_url="http://192.168.1.50:8000",
            ip_address="invalid_ip",
            port=8000,
        )


def test_worker_registration_response():
    resp = WorkerRegistrationResponse(
        worker_id="w-1",
        coordinator_id="coord-01",
        acknowledged_inventory_count=42,
    )
    assert resp.status == "registered"
    assert resp.worker_id == "w-1"
    assert resp.coordinator_id == "coord-01"
    assert resp.acknowledged_inventory_count == 42
    assert resp.registered_at_utc > 0


def test_worker_info_properties():
    worker = WorkerInfo(
        worker_id="worker-alpha",
        endpoint_url="http://10.0.0.5:8000",
        ip_address="10.0.0.5",
        port=8000,
        capacity_bytes=1000,
        used_bytes=400,
        status=WorkerStatus.ACTIVE,
    )
    assert worker.available_bytes == 600
    assert worker.is_healthy is True

    worker.status = WorkerStatus.DEGRADED
    assert worker.is_healthy is True

    worker.status = WorkerStatus.UNHEALTHY
    assert worker.is_healthy is False

    worker.status = WorkerStatus.OFFLINE
    assert worker.is_healthy is False

    # Check to_dict() helper
    d = worker.to_dict()
    assert d["available_bytes"] == 600
    assert d["is_healthy"] is False


# ============================================================================
# Heartbeat & Ping/Pong Tests
# ============================================================================


def test_heartbeat_payload():
    payload = HeartbeatPayload(
        worker_id="w-1",
        active_transfers=2,
        used_bytes=5000,
        inventory_delta_added={VALID_HASH_1},
        inventory_delta_removed={VALID_HASH_2},
    )
    assert payload.worker_id == "w-1"
    assert VALID_HASH_1 in payload.inventory_delta_added
    d = payload.to_dict()
    assert d["inventory_delta_added"] == [VALID_HASH_1]


def test_heartbeat_response():
    resp = HeartbeatResponse(status="healthy", re_register_required=False)
    assert resp.status == "healthy"
    assert resp.acknowledged_at_utc > 0


def test_ping_pong_models():
    ping = PingRequest(client_timestamp_utc=100.0, sequence_number=1)
    assert ping.client_timestamp_utc == 100.0
    assert ping.sequence_number == 1

    pong = PongResponse(
        worker_id="w-1",
        client_timestamp_utc=100.0,
        server_timestamp_utc=100.005,
        sequence_number=1,
    )
    assert pong.worker_id == "w-1"
    assert pong.status == "pong"


# ============================================================================
# Candidate & Location Request/Response Tests
# ============================================================================


def test_candidate_source_model():
    cand = CandidateSource(
        worker_id="w-1",
        endpoint_url="http://192.168.1.10:8000",
        ip_address="192.168.1.10",
        port=8000,
        locality_tier=LocalityTier.SUBNET.value,
        estimated_rtt_ms=1.5,
        load_factor=0.25,
        priority_score=4800.0,
    )
    assert cand.worker_id == "w-1"
    assert cand.locality_tier == "subnet"
    assert cand.estimated_rtt_ms == 1.5
    assert cand.can_serve is True


def test_locate_assets_request_response():
    req = LocateAssetsRequest(
        requester_worker_id="req-1",
        missing_hashes=[VALID_HASH_1, VALID_HASH_2],
        requester_ip="192.168.1.20",
        max_candidates_per_asset=3,
    )
    assert req.requester_worker_id == "req-1"
    assert len(req.missing_hashes) == 2

    cand = CandidateSource(
        worker_id="w-1",
        endpoint_url="http://192.168.1.10:8000",
        ip_address="192.168.1.10",
        port=8000,
    )
    resp = LocateAssetsResponse(
        locations={VALID_HASH_1: [cand]},
        unresolved_hashes=[VALID_HASH_2],
    )
    assert resp.resolved_count == 1
    assert resp.unresolved_count == 1


# ============================================================================
# Streaming & Transfer Models
# ============================================================================


def test_stream_metadata_header():
    header = StreamMetadataHeader(
        sha256=VALID_HASH_1,
        total_size_bytes=5242880,
        chunk_size_bytes=1048576,
        offset_bytes=0,
    )
    assert header.sha256 == VALID_HASH_1
    assert header.total_size_bytes == 5242880
    assert header.content_type == "application/octet-stream"


def test_transfer_progress():
    progress = TransferProgress(
        transfer_id="tx-01",
        sha256=VALID_HASH_1,
        bytes_transferred=500000,
        total_bytes=1000000,
        throughput_bytes_per_sec=1000000.0,
        state=TransferState.STREAMING,
    )
    assert progress.progress_fraction == 0.5
    assert progress.throughput_mbps == 8.0


def test_transfer_result():
    res = TransferResult(
        sha256=VALID_HASH_1,
        success=True,
        bytes_transferred=1000000,
        total_bytes=1000000,
        verified_sha256=VALID_HASH_1,
        duration_seconds=0.5,
        source_worker_id="w-1",
    )
    assert res.success is True
    assert res.throughput_mbps == 16.0


# ============================================================================
# Telemetry & Metrics Models
# ============================================================================


def test_transfer_metrics():
    metrics = TransferMetrics(
        total_requested_assets=10,
        local_cache_hit_assets=6,
        network_transferred_assets=4,
        total_requested_bytes=1000,
        local_cache_hit_bytes=600,
        network_transferred_bytes=400,
    )
    assert metrics.byte_hit_ratio == 0.6
    assert metrics.network_savings_percent == 60.0

    d = metrics.to_dict()
    assert d["byte_hit_ratio"] == 0.6
    assert d["network_savings_percent"] == 60.0


def test_transfer_metrics_zero_denominator():
    metrics = TransferMetrics()
    assert metrics.byte_hit_ratio == 0.0
    assert metrics.network_savings_percent == 0.0


def test_cluster_telemetry():
    telemetry = ClusterTelemetry(
        coordinator_id="coord-main",
        uptime_seconds=120.0,
        total_registered_workers=5,
        active_workers=4,
        degraded_workers=1,
        unique_cached_assets_count=150,
    )
    assert telemetry.coordinator_id == "coord-main"
    assert telemetry.total_registered_workers == 5
    assert telemetry.active_workers == 4
