"""Scene caching foundation for Phase 2 (caching, incremental scanning, change detection).

This module intentionally does one thing well: given a scene source (a JSON
payload or a .blend file) and its previously computed content hash, tell the
caller whether re-analysis can be skipped, and where to find the cached
outputs if so. It does not decide *when* to invalidate beyond content
identity - that policy stays in the caller (e.g. the CLI).

Design notes for the next phase increment:
- Hashing is content-based (sha256), not mtime-based, so moving/copying a
  .blend file without changing its bytes is still a cache hit, and a hash
  is stable across machines/timezones.
- The cache index is a single small JSON file rather than one file per
  entry, since Phase 2's initial scope is single-machine/single-user; a
  shared/distributed cache backend can replace SceneCache's storage without
  changing its public interface.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

DEFAULT_CACHE_DIR = Path(".aidars_cache")
INDEX_FILENAME = "index.json"
_HASH_CHUNK_SIZE = 1024 * 1024  # 1 MiB


def hash_json_payload(payload: dict[str, Any]) -> str:
    """Compute a stable content hash for a JSON-like scene payload.

    Keys are sorted so semantically identical payloads with different key
    ordering hash identically.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def hash_blend_file(path: str | Path) -> str:
    """Compute a stable content hash for a .blend file by streaming its bytes.

    Streams in fixed-size chunks so this stays memory-safe for large scene
    files instead of reading the whole file into memory at once.
    """
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while chunk := handle.read(_HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def hash_source(source: str | Path | dict[str, Any]) -> str:
    """Hash either a JSON payload (dict) or a path to a .blend/.json file."""
    if isinstance(source, dict):
        return hash_json_payload(source)

    path = Path(source)
    if path.suffix.lower() == ".blend":
        return hash_blend_file(path)

    with path.open("r", encoding="utf-8-sig") as handle:
        return hash_json_payload(json.load(handle))


@dataclass(slots=True)
class SceneCacheEntry:
    """A single cache record: what a source hashed to, and where its outputs live."""

    source_hash: str
    scene_output: str
    graph_output: Optional[str] = None
    cached_at: float = 0.0


class SceneCache:
    """A small, file-backed cache keyed by scene source content hash.

    Example:
        cache = SceneCache()
        source_hash = hash_source(input_path)
        entry = cache.get(str(input_path))
        if entry is not None and entry.source_hash == source_hash:
            # unchanged since last run - skip re-analysis, reuse entry.scene_output
            ...
        else:
            # analyze, then:
            cache.put(str(input_path), SceneCacheEntry(
                source_hash=source_hash,
                scene_output=str(scene_output_path),
                graph_output=str(graph_output_path),
            ))
    """

    def __init__(self, cache_dir: str | Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = Path(cache_dir)
        self.index_path = self.cache_dir / INDEX_FILENAME

    def _load_index(self) -> dict[str, dict[str, Any]]:
        if not self.index_path.exists():
            return {}
        try:
            with self.index_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, OSError):
            # A corrupt or unreadable cache is treated as an empty cache
            # (safe: worst case is redundant work, never stale/wrong output).
            return {}
        return data if isinstance(data, dict) else {}

    def _write_index(self, index: dict[str, dict[str, Any]]) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        with self.index_path.open("w", encoding="utf-8") as handle:
            json.dump(index, handle, indent=2)

    def get(self, source_key: str) -> Optional[SceneCacheEntry]:
        """Look up the most recent cache entry for a source key (e.g. a file path)."""
        index = self._load_index()
        record = index.get(source_key)
        if record is None:
            return None
        return SceneCacheEntry(**record)

    def put(self, source_key: str, entry: SceneCacheEntry) -> None:
        """Record (or overwrite) the cache entry for a source key."""
        entry.cached_at = entry.cached_at or time.time()
        index = self._load_index()
        index[source_key] = asdict(entry)
        self._write_index(index)

    def has_changed(self, source_key: str, current_hash: str) -> bool:
        """Return True if the source's content hash differs from what's cached.

        A source that has never been cached counts as changed (nothing to
        reuse yet).
        """
        cached = self.get(source_key)
        return cached is None or cached.source_hash != current_hash

    def invalidate(self, source_key: str) -> None:
        """Remove a cache entry, forcing the next lookup to report a change."""
        index = self._load_index()
        if source_key in index:
            del index[source_key]
            self._write_index(index)
