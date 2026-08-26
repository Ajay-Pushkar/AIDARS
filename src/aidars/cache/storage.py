"""Milestone 5 Content-Addressed Storage Layer.

Implements split-hash 2-level directory structure (objects/aa/bb...),
atomic staging via tmp/ and os.replace, and chunked 64 KiB I/O streaming.
"""
from __future__ import annotations

import hashlib
import io
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import BinaryIO, Iterator, Optional, Tuple, Callable

from aidars.cache.models import (
    CacheStorageError,
    HashMismatchError,
    InvalidHashError,
    SHA256_HEX_PATTERN,
)

DEFAULT_CHUNK_SIZE: int = 64 * 1024  # 64 KiB


class SplitHashStorage:
    """Low-level content-addressed filesystem storage driver.

    Organizes stored objects using a 2-level split-hash directory hierarchy:
        <cache_root>/objects/<h[:2]>/<h[2:]>
    """

    def __init__(self, cache_root: Path | str) -> None:
        self.cache_root = Path(cache_root).resolve()
        self.objects_dir = self.cache_root / "objects"
        self.tmp_dir = self.cache_root / "tmp"
        self.metadata_dir = self.cache_root / "metadata"

        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Create base cache directory structure if not already present."""
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def validate_hash(sha256: str) -> str:
        """Validate and normalize a SHA-256 hash string.

        Returns lowercase normalized 64-hex string.
        Raises InvalidHashError if hash is malformed.
        """
        if not isinstance(sha256, str) or not SHA256_HEX_PATTERN.match(sha256):
            raise InvalidHashError(
                f"Invalid SHA-256 digest format: {sha256!r}. Expected 64 hexadecimal characters."
            )
        return sha256.lower()

    def get_relative_path(self, sha256: str) -> Path:
        """Return the relative path from cache root for the given SHA-256 hash.

        Format: objects/<h[:2]>/<h[2:]>
        """
        norm_hash = self.validate_hash(sha256)
        return Path("objects") / norm_hash[:2] / norm_hash[2:]

    def get_absolute_path(self, sha256: str) -> Path:
        """Return the absolute filesystem path for the given SHA-256 hash."""
        norm_hash = self.validate_hash(sha256)
        return self.objects_dir / norm_hash[:2] / norm_hash[2:]

    def exists(self, sha256: str) -> bool:
        """Check if physical object exists."""
        try:
            return self.get_absolute_path(sha256).is_file()
        except InvalidHashError:
            return False

    def get_all_hashes(self) -> Set[str]:
        """Iterate over the objects directory and yield all physically present hashes."""
        hashes = set()
        if not self.objects_dir.exists():
            return hashes
        for prefix_dir in self.objects_dir.iterdir():
            if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
                continue
            for obj_file in prefix_dir.iterdir():
                if obj_file.is_file():
                    hashes.add(prefix_dir.name + obj_file.name)
        return hashes

    def get_size(self, sha256: str) -> int:
        """Return the exact size in bytes of a cached object on disk."""
        path = self.get_absolute_path(sha256)
        if not path.is_file():
            raise CacheStorageError(f"Object {sha256} not found in storage at {path}")
        return path.stat().st_size

    def delete(self, sha256: str) -> bool:
        """Delete an object file from disk.

        Cleans up empty shard directory if possible.
        Returns True if deleted, False if file did not exist.
        """
        path = self.get_absolute_path(sha256)
        if not path.exists():
            return False

        try:
            path.unlink(missing_ok=True)
        except PermissionError as e:
            # Under Windows, locked files raise PermissionError
            raise CacheStorageError(f"Cannot delete locked cache file {path}: {e}") from e

        # Attempt to clean up parent bucket directory if empty
        parent_bucket = path.parent
        try:
            parent_bucket.rmdir()
        except OSError:
            pass  # Non-empty or in-use; ignore safely

        return True

    @staticmethod
    def compute_stream_sha256(
        stream: BinaryIO,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> Tuple[str, int]:
        """Compute SHA-256 digest and byte length for a binary stream."""
        hasher = hashlib.sha256()
        total_bytes = 0
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
            total_bytes += len(chunk)
        return hasher.hexdigest().lower(), total_bytes

    def compute_file_sha256(
        self,
        file_path: Path | str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> Tuple[str, int]:
        """Compute SHA-256 digest and byte length for a file on disk."""
        path = Path(file_path).resolve()
        if not path.is_file():
            raise CacheStorageError(f"File not found: {path}")
        with open(path, "rb") as f:
            return self.compute_stream_sha256(f, chunk_size=chunk_size)

    def write_stream(
        self,
        stream: BinaryIO,
        expected_sha256: Optional[str] = None,
        expected_size: Optional[int] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_bytes: Optional[int] = None,
        on_validated: Optional[Callable[[int, str], None]] = None,
    ) -> Tuple[Path, int, str]:
        """Atomically write stream data to content-addressed storage.

        Streams data to a staging file in tmp/, computes hash and byte count,
        verifies expectations if provided, and calls on_validated(size, hash) before
        atomically moving to objects/<h[:2]>/<h[2:]>.

        Returns: (final_path, size_bytes, computed_sha256)
        """
        self._ensure_directories()
        tmp_filename = f"{uuid.uuid4().hex}.tmp"
        tmp_path = self.tmp_dir / tmp_filename

        hasher = hashlib.sha256()
        total_bytes = 0

        try:
            with open(tmp_path, "wb") as out_f:
                while True:
                    chunk = stream.read(chunk_size)
                    if not chunk:
                        break
                    
                    if max_bytes is not None and (total_bytes + len(chunk)) > max_bytes:
                        raise CacheStorageError(f"Stream exceeds maximum allowed size of {max_bytes} bytes.")
                        
                    out_f.write(chunk)
                    hasher.update(chunk)
                    total_bytes += len(chunk)

            computed_hash = hasher.hexdigest().lower()

            # Hash verification
            if expected_sha256 is not None:
                norm_expected = self.validate_hash(expected_sha256)
                if computed_hash != norm_expected:
                    raise HashMismatchError(
                        f"Computed SHA-256 ({computed_hash}) does not match expected ({norm_expected})"
                    )

            # Size verification
            if expected_size is not None and total_bytes != expected_size:
                raise CacheStorageError(
                    f"Written stream size ({total_bytes} bytes) does not match expected size ({expected_size} bytes)"
                )
                
            if on_validated is not None:
                on_validated(total_bytes, computed_hash)

            dest_path = self.get_absolute_path(computed_hash)
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Atomic rename on the same filesystem
            os.replace(tmp_path, dest_path)
            return dest_path, total_bytes, computed_hash

        except Exception:
            if tmp_path.exists():
                try:
                    tmp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise

    def write_bytes(
        self,
        data: bytes,
        expected_sha256: Optional[str] = None,
        expected_size: Optional[int] = None,
    ) -> Tuple[Path, int, str]:
        """Store in-memory bytes into storage atomically."""
        stream = io.BytesIO(data)
        return self.write_stream(
            stream,
            expected_sha256=expected_sha256,
            expected_size=expected_size or len(data),
        )

    def put_bytes(
        self,
        data: bytes,
        sha256: Optional[str] = None,
    ) -> Tuple[str, int, Path]:
        """Store bytes into storage returning (sha256, size_bytes, path)."""
        path, size, h = self.write_bytes(data, expected_sha256=sha256)
        return h, size, path

    def write_file(
        self,
        source_path: Path | str,
        expected_sha256: Optional[str] = None,
        expected_size: Optional[int] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        on_validated: Optional[Callable[[int, str], None]] = None,
    ) -> Tuple[Path, int, str]:
        """Ingest an existing local file into content-addressed storage atomically."""
        path = Path(source_path).resolve()
        if not path.is_file():
            raise CacheStorageError(f"Source file does not exist: {path}")
        with open(path, "rb") as f:
            return self.write_stream(
                f,
                expected_sha256=expected_sha256,
                expected_size=expected_size or path.stat().st_size,
                chunk_size=chunk_size,
                on_validated=on_validated,
            )

    def put_file(
        self,
        source_path: Path | str,
        sha256: Optional[str] = None,
    ) -> Tuple[str, int, Path]:
        """Store file into storage returning (sha256, size_bytes, path)."""
        path, size, h = self.write_file(source_path, expected_sha256=sha256)
        return h, size, path

    def put_stream(
        self,
        stream: BinaryIO,
        size_bytes: Optional[int] = None,
        sha256: Optional[str] = None,
    ) -> Tuple[str, int, Path]:
        """Store stream into storage returning (sha256, size_bytes, path)."""
        path, size, h = self.write_stream(stream, expected_sha256=sha256, expected_size=size_bytes)
        return h, size, path

    def read_stream(
        self,
        sha256: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> Iterator[bytes]:
        """Yield chunks of bytes from a stored object."""
        path = self.get_absolute_path(sha256)
        if not path.is_file():
            raise CacheStorageError(f"Object {sha256} not found in storage at {path}")
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def read_bytes(self, sha256: str) -> bytes:
        """Read and return complete byte contents of a cached object."""
        path = self.get_absolute_path(sha256)
        if not path.is_file():
            raise CacheStorageError(f"Object {sha256} not found in storage at {path}")
        return path.read_bytes()

    def clean_tmp(self, max_age_seconds: float = 3600.0) -> int:
        """Remove stale .tmp staging files older than max_age_seconds."""
        cleaned = 0
        now = time.time()
        for tmp_file in self.tmp_dir.glob("*.tmp"):
            try:
                if max_age_seconds <= 0 or (now - tmp_file.stat().st_mtime) > max_age_seconds:
                    tmp_file.unlink(missing_ok=True)
                    cleaned += 1
            except OSError:
                pass
        return cleaned
