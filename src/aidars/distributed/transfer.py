"""AIDAR Distributed Binary Transfer and Stream Chunking Utilities.

Provides memory-bounded streaming data plane operations, incremental SHA-256
verification, disk-based staging lifecycle management, and atomic CAS commit pipelines.
"""
from __future__ import annotations

import hashlib
import io
import logging
import os
import re
import time
import uuid
from pathlib import Path
from typing import BinaryIO, Callable, Iterator, List, Optional, Tuple, Union

import httpx

from aidars.distributed.cas_adapter import CASAdapter
from aidars.distributed.models import (
    CandidateSource,
    TransferResult,
    validate_sha256_hex,
)

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB (1,048,576 bytes)
RANGE_HEADER_REGEX = re.compile(r"^bytes=(\d+)-(\d+)?$")


# ============================================================================
# Transfer Exceptions
# ============================================================================


class TransferError(Exception):
    """Base exception for all distributed asset transfer errors."""

    def __init__(
        self,
        message: str,
        sha256: Optional[str] = None,
        source_worker_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.sha256 = sha256
        self.source_worker_id = source_worker_id


class IntegrityError(TransferError):
    """Raised when incremental SHA-256 verification of received bytes fails."""

    def __init__(
        self,
        message: str,
        expected_sha256: str,
        actual_sha256: str,
        source_worker_id: Optional[str] = None,
    ) -> None:
        super().__init__(message, sha256=expected_sha256, source_worker_id=source_worker_id)
        self.expected_sha256 = expected_sha256
        self.actual_sha256 = actual_sha256


class StreamAbortError(TransferError):
    """Raised when a stream is unexpectedly aborted, truncated, or disconnected."""

    pass


class WorkerHttpError(TransferError):
    """Raised when a worker node returns an HTTP error code (e.g. 404, 500, 503)."""

    def __init__(
        self,
        message: str,
        status_code: int,
        sha256: Optional[str] = None,
        source_worker_id: Optional[str] = None,
    ) -> None:
        super().__init__(message, sha256=sha256, source_worker_id=source_worker_id)
        self.status_code = status_code


class CandidateExhaustedError(TransferError):
    """Raised when all available candidate sources fail for a requested asset."""

    def __init__(self, message: str, sha256: str, candidate_count: int) -> None:
        super().__init__(message, sha256=sha256)
        self.candidate_count = candidate_count


class CASCommitError(TransferError):
    """Raised when atomic CAS insertion fails after transfer."""

    pass


# ============================================================================
# Range Parsing & Chunk Generator
# ============================================================================


def parse_byte_range_header(
    range_header: Optional[str],
    total_size: int,
) -> Tuple[int, int, int, bool]:
    """Parse HTTP Range header and calculate byte boundaries.

    Args:
        range_header: Raw HTTP Range header string (e.g. 'bytes=1048576-' or 'bytes=0-100').
        total_size: Total file size in bytes.

    Returns:
        Tuple of (start_offset, end_offset, content_length, is_partial).

    Raises:
        ValueError: If Range header format is syntactically invalid.
        IndexError: If requested range is unsatisfiable (HTTP 416).
    """
    if not range_header or not range_header.strip():
        # Full content request
        end = max(0, total_size - 1) if total_size > 0 else 0
        return (0, end, total_size, False)

    cleaned = range_header.strip()
    match = RANGE_HEADER_REGEX.match(cleaned)
    if not match:
        raise ValueError(f"Invalid Range header syntax: {range_header!r}")

    start_str, end_str = match.groups()
    start = int(start_str)

    if total_size == 0:
        if start == 0:
            return (0, 0, 0, True)
        raise IndexError("Range start exceeds 0-byte file size")

    if start < 0 or start >= total_size:
        raise IndexError(f"Range start {start} out of bounds for size {total_size}")

    if end_str is not None:
        end = int(end_str)
        if end < start:
            raise IndexError(f"Range end {end} cannot be less than start {start}")
        end = min(end, total_size - 1)
    else:
        end = total_size - 1

    content_length = max(0, end - start + 1)
    return (start, end, content_length, True)


def generate_bounded_chunks(
    stream: BinaryIO,
    bytes_to_send: int,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Iterator[bytes]:
    """Yield bounded binary chunks from an open stream with leak-free cleanup.

    Ensures the underlying file handle is closed upon normal completion, exception,
    or client disconnect/generator termination.
    """
    chunk_size = max(1024, int(chunk_size))
    bytes_remaining = max(0, int(bytes_to_send))

    try:
        while bytes_remaining > 0:
            read_size = min(chunk_size, bytes_remaining)
            chunk = stream.read(read_size)
            if not chunk:
                break
            bytes_remaining -= len(chunk)
            yield chunk
    finally:
        try:
            stream.close()
        except Exception as exc:
            logger.debug("Error closing asset stream handle: %s", exc)


# ============================================================================
# Staging Context Manager
# ============================================================================


class StagingContext:
    """Context manager managing the lifecycle of a disk staging file.

    Guarantees immediate cleanup and deletion of uncommitted staging files on any
    exception, timeout, network error, bit corruption, or premature abort.
    """

    def __init__(
        self,
        staging_dir: Union[str, Path],
        sha256: str,
        prefix: str = "transfer",
    ) -> None:
        self.staging_dir = Path(staging_dir).resolve()
        self.sha256 = validate_sha256_hex(sha256)
        self.prefix = prefix
        self.path = self.staging_dir / f"{self.sha256}_{uuid.uuid4().hex}.tmp"
        self.committed = False

    def __enter__(self) -> StagingContext:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self.committed:
            try:
                if self.path.exists():
                    self.path.unlink(missing_ok=True)
            except OSError as exc:
                logger.debug("Failed to unlink uncommitted staging file %s: %s", self.path, exc)

    def mark_committed(self) -> None:
        """Mark the staging file as committed to prevent automatic deletion on block exit."""
        self.committed = True


# ============================================================================
# Streaming Download Engine
# ============================================================================


async def download_stream_to_staging(
    client: httpx.AsyncClient,
    endpoint_url: str,
    sha256: str,
    staging_dir: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    offset: int = 0,
    timeout_seconds: float = 30.0,
    progress_callback: Optional[Callable[[int, Optional[int]], None]] = None,
) -> Tuple[Path, str, int]:
    """Stream a binary asset from a peer endpoint directly to a disk staging file.

    Incrementally computes SHA-256 as chunks arrive. Verifies digest against
    the expected hash before returning the staged file path.

    Returns:
        Tuple of (staging_file_path, computed_sha256_hex, total_bytes_received).

    Raises:
        WorkerHttpError: On non-200/206 HTTP status.
        StreamAbortError: On stream truncation or network disconnection.
        IntegrityError: On SHA-256 mismatch.
    """
    norm_sha256 = validate_sha256_hex(sha256)
    clean_url = endpoint_url.strip().rstrip("/")
    stream_url = f"{clean_url}/api/v1/assets/{norm_sha256}/stream"

    headers = {"Accept": "application/octet-stream"}
    if offset > 0:
        headers["Range"] = f"bytes={offset}-"

    with StagingContext(staging_dir, norm_sha256) as stg:
        try:
            async with client.stream(
                "GET", stream_url, headers=headers, timeout=timeout_seconds
            ) as response:
                if response.status_code == 404:
                    raise WorkerHttpError(
                        f"Asset {norm_sha256} not found on peer {endpoint_url}",
                        status_code=404,
                        sha256=norm_sha256,
                    )
                if response.status_code not in (200, 206):
                    raise WorkerHttpError(
                        f"Peer {endpoint_url} returned HTTP {response.status_code}",
                        status_code=response.status_code,
                        sha256=norm_sha256,
                    )

                expected_bytes: Optional[int] = None
                cl_header = response.headers.get("content-length")
                if cl_header:
                    try:
                        expected_bytes = int(cl_header)
                    except ValueError:
                        pass

                hasher = hashlib.sha256()
                bytes_received = 0

                with stg.path.open("wb") as f_out:
                    async for chunk in response.aiter_bytes(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        hasher.update(chunk)
                        f_out.write(chunk)
                        bytes_received += len(chunk)
                        if progress_callback:
                            try:
                                progress_callback(bytes_received, expected_bytes)
                            except Exception as cb_exc:
                                logger.debug("Error in progress callback: %s", cb_exc)

                if expected_bytes is not None and bytes_received != expected_bytes:
                    raise StreamAbortError(
                        f"Stream truncated: received {bytes_received} bytes, expected {expected_bytes} bytes",
                        sha256=norm_sha256,
                    )

                actual_hash = hasher.hexdigest().lower()

                # For full stream (offset == 0), verify SHA-256 matches expected
                if offset == 0 and actual_hash != norm_sha256:
                    raise IntegrityError(
                        f"SHA-256 checksum mismatch: expected {norm_sha256}, got {actual_hash}",
                        expected_sha256=norm_sha256,
                        actual_sha256=actual_hash,
                    )

                # Transfer succeeded and verified; preserve file for commit
                stg.mark_committed()
                return (stg.path, actual_hash, bytes_received)

        except (IntegrityError, WorkerHttpError, StreamAbortError):
            raise
        except (httpx.RequestError, httpx.TimeoutException) as exc:
            raise StreamAbortError(
                f"Network error streaming from {endpoint_url}: {exc}",
                sha256=norm_sha256,
            ) from exc
        except Exception as exc:
            raise StreamAbortError(
                f"Unexpected error streaming from {endpoint_url}: {exc}",
                sha256=norm_sha256,
            ) from exc


# ============================================================================
# Asset Transfer Pipeline Functions
# ============================================================================


async def transfer_asset_from_candidate(
    client: httpx.AsyncClient,
    candidate: CandidateSource,
    sha256: str,
    cas_adapter: CASAdapter,
    staging_dir: Optional[Path] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    timeout_seconds: float = 30.0,
    progress_callback: Optional[Callable[[int, Optional[int]], None]] = None,
) -> TransferResult:
    """Execute complete transfer pipeline for single candidate: download -> verify -> atomic CAS commit."""
    norm_sha256 = validate_sha256_hex(sha256)

    # 1. Local CAS shortcut
    if cas_adapter.has_asset(norm_sha256):
        asset_size = getattr(cas_adapter, "get_asset_size", lambda h: 0)(norm_sha256) or 0
        asset_path = getattr(cas_adapter, "get_asset_path", lambda h: None)(norm_sha256)
        return TransferResult(
            sha256=norm_sha256,
            success=True,
            bytes_transferred=0,
            total_bytes=asset_size,
            verified_sha256=norm_sha256,
            committed_path=str(asset_path) if asset_path else None,
            source_worker_id="local_cas",
            source_endpoint_url="local://cas",
            duration_seconds=0.0,
        )

    stg_dir = staging_dir or getattr(cas_adapter, "staging_dir", Path(".aidars_cas/staging"))
    t0 = time.time()
    stg_path: Optional[Path] = None

    try:
        stg_path, verified_hash, bytes_transferred = await download_stream_to_staging(
            client=client,
            endpoint_url=candidate.endpoint_url,
            sha256=norm_sha256,
            staging_dir=stg_dir,
            chunk_size=chunk_size,
            timeout_seconds=timeout_seconds,
            progress_callback=progress_callback,
        )

        # Atomic commit into local CAS
        committed = cas_adapter.commit_staged_file(stg_path, norm_sha256)
        if not committed:
            raise CASCommitError(
                f"CASAdapter failed to commit staged file for {norm_sha256}",
                sha256=norm_sha256,
                source_worker_id=candidate.worker_id,
            )

        duration = max(0.0001, time.time() - t0)
        committed_p = getattr(cas_adapter, "get_asset_path", lambda h: None)(norm_sha256)

        return TransferResult(
            sha256=norm_sha256,
            success=True,
            bytes_transferred=bytes_transferred,
            total_bytes=bytes_transferred,
            verified_sha256=verified_hash,
            committed_path=str(committed_p) if committed_p else None,
            source_worker_id=candidate.worker_id,
            source_endpoint_url=candidate.endpoint_url,
            duration_seconds=duration,
        )
    except Exception as exc:
        duration = max(0.0001, time.time() - t0)
        if stg_path and Path(stg_path).exists():
            try:
                Path(stg_path).unlink(missing_ok=True)
            except OSError:
                pass
        return TransferResult(
            sha256=norm_sha256,
            success=False,
            bytes_transferred=0,
            total_bytes=0,
            source_worker_id=candidate.worker_id,
            source_endpoint_url=candidate.endpoint_url,
            duration_seconds=duration,
            error_message=str(exc),
        )


async def transfer_asset_with_failover(
    client: httpx.AsyncClient,
    candidates: List[CandidateSource],
    sha256: str,
    cas_adapter: CASAdapter,
    staging_dir: Optional[Path] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    timeout_seconds: float = 30.0,
    on_candidate_error: Optional[Callable[[CandidateSource, Exception], None]] = None,
) -> TransferResult:
    """Attempt transfer across candidate sources in priority order with automatic fail-over."""
    norm_sha256 = validate_sha256_hex(sha256)

    # Fast shortcut if already cached locally
    if cas_adapter.has_asset(norm_sha256):
        asset_size = getattr(cas_adapter, "get_asset_size", lambda h: 0)(norm_sha256) or 0
        asset_path = getattr(cas_adapter, "get_asset_path", lambda h: None)(norm_sha256)
        return TransferResult(
            sha256=norm_sha256,
            success=True,
            bytes_transferred=0,
            total_bytes=asset_size,
            verified_sha256=norm_sha256,
            committed_path=str(asset_path) if asset_path else None,
            source_worker_id="local_cas",
            source_endpoint_url="local://cas",
            duration_seconds=0.0,
        )

    if not candidates:
        raise CandidateExhaustedError(
            f"No candidate sources provided for asset {norm_sha256}",
            sha256=norm_sha256,
            candidate_count=0,
        )

    last_error: Optional[str] = None
    for cand in candidates:
        result = await transfer_asset_from_candidate(
            client=client,
            candidate=cand,
            sha256=norm_sha256,
            cas_adapter=cas_adapter,
            staging_dir=staging_dir,
            chunk_size=chunk_size,
            timeout_seconds=timeout_seconds,
        )
        if result.success:
            return result

        last_error = result.error_message
        if on_candidate_error:
            try:
                on_candidate_error(
                    cand,
                    RuntimeError(result.error_message or f"Transfer failed for {norm_sha256}"),
                )
            except Exception as cb_exc:
                logger.debug("Error in on_candidate_error callback: %s", cb_exc)

    raise CandidateExhaustedError(
        f"All {len(candidates)} candidates failed for asset {norm_sha256}. Last error: {last_error}",
        sha256=norm_sha256,
        candidate_count=len(candidates),
    )
