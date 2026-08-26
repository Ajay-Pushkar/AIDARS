"""Milestone 5 Hit/Miss Resolver & Set Difference Engine.

Computes O(A) set difference (missing = required - cached) and transfer efficiency
metrics (byte_hit_ratio, network_saved_bytes) with duck-typed M4 PackagePlan support.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Set, Tuple

from aidars.cache.models import ResolutionResult


class HitMissResolver:
    """O(A) set-difference resolver for cache hit/miss queries."""

    @staticmethod
    def _extract_plan_assets(target: Any) -> Tuple[Dict[str, int], Set[str]]:
        """Duck-type extract required asset hashes and sizes from various input formats.

        Supports:
        - M4 PackagePlan objects (.all_assets, .deduplicated_assets)
        - Dictionaries representing package plans (e.g. {'assets': [...]})
        - Iterables of AssetRecord or dicts ({'sha256': ..., 'size_bytes': ...})
        - Iterables of SHA-256 strings
        """
        hash_sizes: Dict[str, int] = {}
        hashes: Set[str] = set()

        if target is None:
            return hash_sizes, hashes

        # Case 1: PackagePlan object or custom container
        if hasattr(target, "deduplicated_assets") and target.deduplicated_assets:
            items = target.deduplicated_assets
        elif hasattr(target, "all_assets") and target.all_assets:
            items = target.all_assets
        elif hasattr(target, "assets") and isinstance(target.assets, (list, tuple, set)):
            items = target.assets
        elif isinstance(target, dict) and "assets" in target and isinstance(target["assets"], list):
            items = target["assets"]
        elif isinstance(target, dict):
            # Mapping of hash -> size_bytes or hash -> dict/record
            for k, v in target.items():
                if isinstance(k, str) and len(k) == 64:
                    h = k.lower()
                    size = v if isinstance(v, (int, float)) else (getattr(v, "size_bytes", 0) or (v.get("size_bytes", 0) if isinstance(v, dict) else 0))
                    hash_sizes[h] = int(size)
                    hashes.add(h)
            return hash_sizes, hashes
        elif isinstance(target, (list, tuple, set)):
            items = target
        elif isinstance(target, Iterable):
            items = list(target)
        else:
            items = [target]

        for item in items:
            h: Optional[str] = None
            size: int = 0

            if isinstance(item, str):
                h = item.lower()
                size = 0
            elif isinstance(item, dict):
                h = item.get("sha256")
                size = int(item.get("size_bytes", 0))
            elif hasattr(item, "sha256"):
                h = getattr(item, "sha256")
                size = int(getattr(item, "size_bytes", 0) or 0)

            if h and isinstance(h, str) and len(h.strip()) > 0:
                norm_h = h.strip().lower()
                hashes.add(norm_h)
                # Keep largest size if multiple records specify the same hash
                hash_sizes[norm_h] = max(hash_sizes.get(norm_h, 0), size)

        return hash_sizes, hashes

    def resolve_hashes(
        self,
        arg1: Optional[Any] = None,
        arg2: Optional[Any] = None,
        hash_sizes: Optional[Dict[str, int]] = None,
        cached_hashes: Optional[Set[str]] = None,
        required_hashes: Optional[Iterable[str]] = None,
    ) -> ResolutionResult:
        """Resolve cache hits and misses for a collection of required hashes.

        Performs O(A) average-time set difference.
        """
        if cached_hashes is not None:
            norm_cached = {h.lower() for h in cached_hashes}
            req_source = required_hashes if required_hashes is not None else (arg1 if arg1 is not None else [])
            req_set = {h.lower() for h in req_source}
        elif arg2 is not None:
            # Check if arg1 is cached set and arg2 is required list
            if isinstance(arg1, (set, frozenset)) and isinstance(arg2, (list, tuple, set, dict)):
                norm_cached = {h.lower() for h in arg1}
                req_set = {h.lower() for h in arg2}
            else:
                req_set = {h.lower() for h in (arg1 if arg1 is not None else [])}
                norm_cached = {h.lower() for h in arg2}
        else:
            req_source = required_hashes if required_hashes is not None else (arg1 if arg1 is not None else [])
            req_set = {h.lower() for h in req_source}
            norm_cached = set()


        hits = req_set.intersection(norm_cached)
        misses = req_set.difference(norm_cached)

        sizes = hash_sizes or {}
        hit_bytes = sum(sizes.get(h, 0) for h in hits)
        miss_bytes = sum(sizes.get(h, 0) for h in misses)
        total_requested_bytes = hit_bytes + miss_bytes

        if total_requested_bytes > 0:
            byte_hit_ratio = hit_bytes / total_requested_bytes
        else:
            # When total requested bytes is 0 or empty request, ratio is 1.0 if no misses
            byte_hit_ratio = 1.0 if not misses else 0.0

        return ResolutionResult(
            hits=hits,
            misses=misses,
            total_requested_bytes=total_requested_bytes,
            hit_bytes=hit_bytes,
            miss_bytes=miss_bytes,
            byte_hit_ratio=byte_hit_ratio,
            network_saved_bytes=hit_bytes,
        )

    def resolve_plan(
        self,
        plan: Any,
        cached_hashes: Set[str],
        index: Optional[Any] = None,
    ) -> ResolutionResult:
        """Resolve required assets from an M4 PackagePlan or duck-typed plan."""
        hash_sizes, req_hashes = self._extract_plan_assets(plan)

        # If size_bytes was 0 in plan and index is available, look up cached sizes for hits
        if index is not None and any(hash_sizes.get(h, 0) == 0 for h in req_hashes):
            cached_entries = index.get_entries_by_hashes(req_hashes)
            for h, entry in cached_entries.items():
                if hash_sizes.get(h, 0) == 0:
                    hash_sizes[h] = entry.size_bytes

        return self.resolve_hashes(
            required_hashes=req_hashes,
            cached_hashes=cached_hashes,
            hash_sizes=hash_sizes,
        )
