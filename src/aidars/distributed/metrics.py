"""AIDAR Distributed Asset Layer Telemetry and Metrics Engine.

Provides thread-safe collection and calculation of Byte Hit Ratio (BHR),
network savings percentage, transfer throughput, and cluster resilience metrics.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TransferEvent:
    """Record of a single asset transfer or cache hit event."""

    sha256: str
    size_bytes: int
    is_cache_hit: bool
    success: bool
    bytes_transferred: int = 0
    duration_seconds: float = 0.0
    resumed: bool = False
    failed_over: bool = False
    source_worker_id: Optional[str] = None
    timestamp_utc: float = field(default_factory=time.time)

    @property
    def throughput_mbps(self) -> float:
        if self.duration_seconds <= 0 or self.bytes_transferred <= 0:
            return 0.0
        return (self.bytes_transferred * 8.0) / (self.duration_seconds * 1_000_000.0)


class TransferMetricsTracker:
    """Thread-safe telemetry collector tracking asset transfer and caching metrics."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.total_requested_assets: int = 0
        self.local_cache_hits: int = 0
        self.network_transferred_assets: int = 0
        self.failed_transfers: int = 0

        self.total_requested_bytes: int = 0
        self.local_cache_hit_bytes: int = 0
        self.network_transferred_bytes: int = 0

        self.resumption_events: int = 0
        self.failover_events: int = 0

        self._total_transfer_duration: float = 0.0
        self._events: List[TransferEvent] = []

    def record_cache_hit(self, sha256: str, size_bytes: int) -> None:
        """Record a local CAS cache hit avoiding network transfer."""
        with self._lock:
            self.total_requested_assets += 1
            self.local_cache_hits += 1
            self.total_requested_bytes += size_bytes
            self.local_cache_hit_bytes += size_bytes
            self._events.append(
                TransferEvent(
                    sha256=sha256,
                    size_bytes=size_bytes,
                    is_cache_hit=True,
                    success=True,
                )
            )

    def record_network_transfer(
        self,
        sha256: str,
        size_bytes: int,
        bytes_transferred: int,
        duration_seconds: float,
        source_worker_id: Optional[str] = None,
        resumed: bool = False,
        failed_over: bool = False,
    ) -> None:
        """Record a successful remote network streaming transfer."""
        with self._lock:
            self.total_requested_assets += 1
            self.network_transferred_assets += 1
            self.total_requested_bytes += size_bytes
            self.network_transferred_bytes += bytes_transferred
            self._total_transfer_duration += max(0.0, duration_seconds)

            if resumed:
                self.resumption_events += 1
            if failed_over:
                self.failover_events += 1

            self._events.append(
                TransferEvent(
                    sha256=sha256,
                    size_bytes=size_bytes,
                    is_cache_hit=False,
                    success=True,
                    bytes_transferred=bytes_transferred,
                    duration_seconds=duration_seconds,
                    resumed=resumed,
                    failed_over=failed_over,
                    source_worker_id=source_worker_id,
                )
            )

    def record_transfer_failure(
        self,
        sha256: str,
        size_bytes: int = 0,
        source_worker_id: Optional[str] = None,
    ) -> None:
        """Record a failed transfer attempt."""
        with self._lock:
            self.total_requested_assets += 1
            self.failed_transfers += 1
            if size_bytes > 0:
                self.total_requested_bytes += size_bytes
            self._events.append(
                TransferEvent(
                    sha256=sha256,
                    size_bytes=size_bytes,
                    is_cache_hit=False,
                    success=False,
                    source_worker_id=source_worker_id,
                )
            )

    def record_resumption(self) -> None:
        """Increment resumption event counter."""
        with self._lock:
            self.resumption_events += 1

    def record_failover(self) -> None:
        """Increment failover event counter."""
        with self._lock:
            self.failover_events += 1

    @property
    def byte_hit_ratio(self) -> float:
        """Fraction of requested bytes served from local CAS cache [0.0 - 1.0]."""
        with self._lock:
            if self.total_requested_bytes <= 0:
                return 1.0 if self.local_cache_hits > 0 else 0.0
            ratio = self.local_cache_hit_bytes / self.total_requested_bytes
            return round(min(1.0, max(0.0, ratio)), 4)

    @property
    def network_savings_percent(self) -> float:
        """Percentage of network bandwidth saved via local cache hits [0.0% - 100.0%]."""
        return round(self.byte_hit_ratio * 100.0, 2)

    @property
    def average_throughput_mbps(self) -> float:
        """Average network streaming throughput across all transfers in Mbps."""
        with self._lock:
            if self._total_transfer_duration <= 0 or self.network_transferred_bytes <= 0:
                return 0.0
            mbps = (self.network_transferred_bytes * 8.0) / (self._total_transfer_duration * 1_000_000.0)
            return round(mbps, 2)

    def compute_ratios(self) -> None:
        """Compatibility no-op (properties dynamically compute)."""
        pass

    def get_summary(self) -> Dict[str, Any]:
        """Return full telemetry summary dictionary."""
        with self._lock:
            return {
                "total_requested_assets": self.total_requested_assets,
                "local_cache_hits": self.local_cache_hits,
                "network_transferred_assets": self.network_transferred_assets,
                "failed_transfers": self.failed_transfers,
                "total_requested_bytes": self.total_requested_bytes,
                "local_cache_hit_bytes": self.local_cache_hit_bytes,
                "network_transferred_bytes": self.network_transferred_bytes,
                "byte_hit_ratio": self.byte_hit_ratio,
                "network_savings_percent": self.network_savings_percent,
                "average_throughput_mbps": self.average_throughput_mbps,
                "resumption_events": self.resumption_events,
                "failover_events": self.failover_events,
            }

    def reset(self) -> None:
        """Reset all tracked metrics."""
        with self._lock:
            self.total_requested_assets = 0
            self.local_cache_hits = 0
            self.network_transferred_assets = 0
            self.failed_transfers = 0
            self.total_requested_bytes = 0
            self.local_cache_hit_bytes = 0
            self.network_transferred_bytes = 0
            self.resumption_events = 0
            self.failover_events = 0
            self._total_transfer_duration = 0.0
            self._events.clear()
