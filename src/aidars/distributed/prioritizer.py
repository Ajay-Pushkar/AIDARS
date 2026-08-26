"""Network locality classification, latency EMA tracking, and candidate prioritizer."""
from __future__ import annotations

import functools
import ipaddress
import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

from aidars.distributed.models import (
    CandidateSource,
    LocalityTier,
    WorkerInfo,
    WorkerStatus,
)

logger = logging.getLogger(__name__)


# ============================================================================
# IP Normalization & Locality Classification
# ============================================================================


@functools.lru_cache(maxsize=2048)
def normalize_ip(ip_or_host: str) -> str:
    """Normalize IP address strings, handling localhost aliases, brackets, and whitespace."""
    if not ip_or_host:
        return "127.0.0.1"
    cleaned = ip_or_host.strip().lower()
    if cleaned in ("localhost", "127.0.0.1"):
        return "127.0.0.1"
    if cleaned in ("ip6-localhost", "ip6-loopback", "::1"):
        return "::1"
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    return cleaned


_IPV6_ULA_NET = ipaddress.ip_network("fc00::/7")


@functools.lru_cache(maxsize=8192)
def classify_locality(
    requester_ip: str,
    candidate_ip: str,
    ipv4_subnet_prefix: int = 24,
    ipv6_subnet_prefix: int = 64,
) -> LocalityTier:
    """Classify the 4-tier network locality between requester and candidate IP addresses.

    Tiers:
    - LOOPBACK: Same machine or loopback interface.
    - SUBNET: Same local subnet (default /24 IPv4 or /64 IPv6).
    - LAN: RFC-1918 private / campus address across different subnets.
    - WAN: Public routable or unroutable remote address.
    """
    req_str = normalize_ip(requester_ip)
    cand_str = normalize_ip(candidate_ip)

    try:
        req_addr = ipaddress.ip_address(req_str)
        cand_addr = ipaddress.ip_address(cand_str)
    except ValueError:
        # Fallback to WAN if IP is unparseable
        return LocalityTier.WAN

    # Must be same IP version family (IPv4 vs IPv6 cannot be loopback/subnet/LAN)
    if req_addr.version != cand_addr.version:
        return LocalityTier.WAN

    # Tier 0: Loopback or Identical IP
    if req_addr == cand_addr:
        return LocalityTier.LOOPBACK
    if req_addr.is_loopback and cand_addr.is_loopback and req_addr.version == cand_addr.version:
        return LocalityTier.LOOPBACK

    # Tier 1: Same Subnet check
    if req_addr.version == 4:
        net_req = ipaddress.ip_network(f"{req_str}/{ipv4_subnet_prefix}", strict=False)
        net_cand = ipaddress.ip_network(f"{cand_str}/{ipv4_subnet_prefix}", strict=False)
        if net_req == net_cand:
            return LocalityTier.SUBNET
    elif req_addr.version == 6:
        net_req = ipaddress.ip_network(f"{req_str}/{ipv6_subnet_prefix}", strict=False)
        net_cand = ipaddress.ip_network(f"{cand_str}/{ipv6_subnet_prefix}", strict=False)
        if net_req == net_cand:
            return LocalityTier.SUBNET

    # Tier 2: Private LAN (RFC 1918 IPv4 / RFC 4193 ULA IPv6 / Link-Local)
    if req_addr.version == 4:
        if req_addr.is_private and cand_addr.is_private:
            return LocalityTier.LAN
    elif req_addr.version == 6:
        is_req_lan = (req_addr in _IPV6_ULA_NET or req_addr.is_link_local)
        is_cand_lan = (cand_addr in _IPV6_ULA_NET or cand_addr.is_link_local)
        if is_req_lan and is_cand_lan:
            return LocalityTier.LAN

    # Tier 3: WAN
    return LocalityTier.WAN


def measure_ping_rtt(client_timestamp_utc: float, current_time: Optional[float] = None) -> float:
    """Calculate RTT in milliseconds from ping initiation timestamp."""
    now_utc = current_time if current_time is not None else time.time()
    rtt_ms = max(0.01, (now_utc - client_timestamp_utc) * 1000.0)
    return round(rtt_ms, 3)


# ============================================================================
# Latency Tracker with Exponential Moving Average (EMA)
# ============================================================================


class LatencyTracker:
    """Thread-safe latency tracker maintaining smoothed RTT per worker node using EMA."""

    def __init__(self, default_alpha: float = 0.3) -> None:
        self.alpha = float(default_alpha)
        self._rtt_map: Dict[str, float] = {}
        self._lock = threading.Lock()

    def update_rtt(
        self,
        worker_id: str,
        sample_rtt_ms: float,
        alpha: Optional[float] = None,
    ) -> float:
        """Update smoothed RTT for worker_id with a new sample in milliseconds."""
        smoothing = alpha if alpha is not None else self.alpha
        clamped_sample = max(0.01, float(sample_rtt_ms))

        with self._lock:
            current = self._rtt_map.get(worker_id, 0.0)
            if current <= 0.0:
                smoothed = clamped_sample
            else:
                smoothed = (smoothing * clamped_sample) + ((1.0 - smoothing) * current)
            smoothed = round(smoothed, 3)
            self._rtt_map[worker_id] = smoothed
            return smoothed

    def get_rtt(self, worker_id: str, default: float = 10.0) -> float:
        """Retrieve current smoothed RTT for worker_id."""
        with self._lock:
            return self._rtt_map.get(worker_id, default)

    def remove_worker(self, worker_id: str) -> None:
        """Remove worker from latency tracking."""
        with self._lock:
            self._rtt_map.pop(worker_id, None)

    def clear(self) -> None:
        """Clear all tracked latencies."""
        with self._lock:
            self._rtt_map.clear()


# ============================================================================
# Candidate Prioritizer
# ============================================================================


class CandidatePrioritizer:
    """Prioritizes and ranks candidate worker nodes for missing asset transfers."""

    # Base scores for locality tiers
    TIER_BASE_SCORES: Dict[LocalityTier, float] = {
        LocalityTier.LOOPBACK: 10000.0,
        LocalityTier.SUBNET: 5000.0,
        LocalityTier.LAN: 2000.0,
        LocalityTier.WAN: 500.0,
    }

    def __init__(
        self,
        latency_tracker: Optional[LatencyTracker] = None,
        weight_rtt: float = 1.0,
        weight_load: float = 500.0,
        weight_error: float = 1000.0,
    ) -> None:
        self.latency_tracker = latency_tracker or LatencyTracker()
        self.weight_rtt = float(weight_rtt)
        self.weight_load = float(weight_load)
        self.weight_error = float(weight_error)
        self._error_counts: Dict[str, int] = {}
        self._lock = threading.Lock()

    def record_error(self, worker_id: str) -> None:
        """Increment error count for a failing worker."""
        with self._lock:
            self._error_counts[worker_id] = self._error_counts.get(worker_id, 0) + 1

    def clear_errors(self, worker_id: str) -> None:
        """Reset error count for a worker upon successful transfer."""
        with self._lock:
            self._error_counts.pop(worker_id, None)

    def get_error_count(self, worker_id: str) -> int:
        """Get the current error count for a worker."""
        with self._lock:
            return self._error_counts.get(worker_id, 0)

    def evaluate_candidate(
        self,
        requester_ip: str,
        worker: WorkerInfo,
        error_snapshot: Optional[Dict[str, int]] = None,
    ) -> Tuple[float, CandidateSource]:
        """Compute priority score and construct CandidateSource for a single worker node."""
        tier = classify_locality(requester_ip, worker.ip_address)
        rtt_ms = self.latency_tracker.get_rtt(
            worker.worker_id,
            default=worker.estimated_rtt_ms if worker.estimated_rtt_ms > 0 else 5.0,
        )
        max_streams = max(worker.capabilities.max_concurrent_streams, 1)
        active = getattr(worker, "active_transfers", 0)
        load_factor = round(active / max_streams, 3)

        if error_snapshot is not None:
            errors = error_snapshot.get(worker.worker_id, 0)
        else:
            errors = self.get_error_count(worker.worker_id)

        penalty = getattr(worker, "penalty_score", 0.0)
        tier_base = self.TIER_BASE_SCORES.get(tier, 500.0)

        score = (
            tier_base
            - (rtt_ms * self.weight_rtt)
            - (load_factor * self.weight_load)
            - (errors * self.weight_error)
            - (penalty * 100.0)
        )

        cand_source = CandidateSource(
            worker_id=worker.worker_id,
            endpoint_url=worker.endpoint_url,
            ip_address=worker.ip_address,
            port=worker.port,
            locality_tier=tier.value,
            estimated_rtt_ms=rtt_ms,
            load_factor=load_factor,
            penalty_score=penalty,
            priority_score=round(score, 3),
            can_serve=worker.capabilities.can_serve_cas,
        )
        return (score, cand_source)

    def rank_candidates(
        self,
        requester_ip: str,
        candidates: List[WorkerInfo],
        exclude_worker_id: Optional[str] = None,
        include_degraded: bool = True,
        max_candidates: Optional[int] = None,
    ) -> List[CandidateSource]:
        """Rank candidates in descending priority order for a given requester IP.

        Ranking hierarchy:
        1. Locality Tier (Loopback > Subnet > LAN > WAN)
        2. RTT Latency (Lower smoothed RTT favored)
        3. Concurrency Load Factor (Lower load favored)
        4. Historical / Transient Errors (Penalized)
        """
        ranked_list: List[Tuple[float, CandidateSource]] = []

        with self._lock:
            error_snapshot = dict(self._error_counts)

        for worker in candidates:
            # Skip excluded worker (e.g. requester itself)
            if exclude_worker_id and worker.worker_id == exclude_worker_id:
                continue

            # Skip offline workers
            if worker.status == WorkerStatus.OFFLINE:
                continue

            # Skip degraded workers if requested
            if not include_degraded and worker.status in (WorkerStatus.DEGRADED, WorkerStatus.UNHEALTHY):
                continue

            score, candidate_source = self.evaluate_candidate(
                requester_ip=requester_ip,
                worker=worker,
                error_snapshot=error_snapshot,
            )
            ranked_list.append((score, candidate_source))

        # Sort descending by priority score
        ranked_list.sort(key=lambda item: item[0], reverse=True)

        ordered_candidates = [item[1] for item in ranked_list]
        if max_candidates is not None and max_candidates > 0:
            ordered_candidates = ordered_candidates[:max_candidates]

        return ordered_candidates
