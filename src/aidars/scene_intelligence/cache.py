"""Scene caching foundation for Phase 2 (caching, incremental scanning, change detection).

This module provides request-aware and content-based caching for scene intelligence
and downstream pipeline artifacts.
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
    """Compute a stable content hash for a JSON-like scene payload."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def hash_blend_file(path: str | Path) -> str:
    """Compute a stable content hash for a .blend file by streaming its bytes."""
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
    """A request-aware cache record: content hash, request configuration hash, and outputs."""

    source_hash: str
    scene_output: str
    request_hash: str = ""
    graph_output: Optional[str] = None
    package_output: Optional[str] = None
    build_graph: bool = True
    build_package: bool = False
    optimize_package_by_visibility: bool = False
    frame_start: int = 1
    frame_end: int = 24
    camera_id: str = ""
    cached_at: float = 0.0


class SceneCache:
    """A file-backed, request-aware cache keyed by scene source and request configuration."""

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
            return {}
        return data if isinstance(data, dict) else {}

    def _write_index(self, index: dict[str, dict[str, Any]]) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        with self.index_path.open("w", encoding="utf-8") as handle:
            json.dump(index, handle, indent=2)

    def _make_key(self, source_key: str, request_hash: str = "") -> str:
        if request_hash:
            return f"{source_key}::{request_hash}"
        return source_key

    def get(self, source_key: str | Path, request_hash: str = "", verify_artifacts: bool = False) -> Optional[SceneCacheEntry]:
        """Look up the cache entry for a source key, verifying request-match and optional disk artifacts."""
        s_key = str(source_key)
        index = self._load_index()
        key = self._make_key(s_key, request_hash)
        record = index.get(key)
        if record is None and request_hash:
            # Fallback to check legacy un-suffixed key ONLY if its request_hash strictly matches
            fallback = index.get(s_key)
            if fallback and fallback.get("request_hash") == request_hash:
                record = fallback

        if record is None:
            return None

        valid_fields = set(SceneCacheEntry.__dataclass_fields__)
        filtered_record = {k: v for k, v in record.items() if k in valid_fields}
        entry = SceneCacheEntry(**filtered_record)

        if verify_artifacts:
            if entry.scene_output and not Path(entry.scene_output).exists():
                return None
            if entry.build_graph and entry.graph_output and not Path(entry.graph_output).exists():
                return None
            if entry.build_package and entry.package_output and not Path(entry.package_output).exists():
                return None

        return entry

    def put(self, source_key: str | Path, entry: SceneCacheEntry) -> None:
        """Record (or overwrite) the cache entry for a source key and request configuration."""
        s_key = str(source_key)
        entry.cached_at = entry.cached_at or time.time()
        index = self._load_index()

        # Store keyed by specific request configuration as well as source
        key = self._make_key(s_key, entry.request_hash)
        entry_dict = asdict(entry)
        index[key] = entry_dict
        index[s_key] = entry_dict

        self._write_index(index)

    def has_changed(self, source_key: str | Path, current_hash: str, request_hash: str = "", verify_artifacts: bool = False) -> bool:
        """Return True if the source's content hash or request hash differs from what's cached."""
        cached = self.get(source_key, request_hash=request_hash, verify_artifacts=verify_artifacts)
        if cached is None:
            return True
        if cached.source_hash != current_hash:
            return True
        if request_hash and cached.request_hash != request_hash:
            return True
        return False

    def invalidate(self, source_key: str | Path) -> None:
        """Remove cache entries for a source key."""
        s_key = str(source_key)
        index = self._load_index()
        keys_to_delete = [k for k in index if k == s_key or k.startswith(f"{s_key}::")]
        for k in keys_to_delete:
            del index[k]
        if keys_to_delete:
            self._write_index(index)
