"""Public CAS Adapter interface and local implementation for AIDAR distributed layer.

This module provides strict isolation between the distributed networking layer
and local Content Addressed Storage (CAS), ensuring memory-bounded streaming,
streaming SHA-256 verification, and atomic commit semantics.
"""
from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    Union,
    runtime_checkable,
)

from aidars.distributed.models import validate_sha256_hex

logger = logging.getLogger(__name__)

DEFAULT_CAS_DIR = Path(".aidars_cas")
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB


@runtime_checkable
class CASAdapter(Protocol):
    """Protocol interface defining the contract between distributed networking and local CAS."""

    def has_asset(self, sha256_hex: str) -> bool:
        """Check if asset exists in local CAS."""
        ...

    def get_missing_hashes(self, required_hashes: Iterable[str]) -> Set[str]:
        """Return subset of required_hashes not in local CAS."""
        ...

    def open_asset_stream(self, sha256_hex: str, offset: int = 0) -> BinaryIO:
        """Open a readable binary stream for an asset starting at byte offset."""
        ...

    def commit_staged_file(self, staged_file_path: Path, expected_sha256: str) -> bool:
        """Atomically insert a staged file into local CAS, verifying SHA-256."""
        ...

    def get_inventory_hashes(self) -> Set[str]:
        """Enumerate all cached hashes in local CAS."""
        ...


class LocalCASAdapter:
    """Thread-safe local filesystem Content Addressed Storage (CAS) adapter.

    Manages a sharded storage directory layout (objects/XX/<hash>) and an isolated
    staging directory (staging/) for atomic commit and streaming verification.
    """

    def __init__(
        self,
        cas_dir: Union[str, Path] = DEFAULT_CAS_DIR,
        staging_dir: Optional[Union[str, Path]] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        self.cas_dir = Path(cas_dir).resolve()
        self.objects_dir = self.cas_dir / "objects"
        self.staging_dir = Path(staging_dir).resolve() if staging_dir else (self.cas_dir / "staging")
        self.chunk_size = max(1024, int(chunk_size))
        self._lock = threading.RLock()

        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    def _get_path_for_hash(self, sha256_hex: str) -> Path:
        """Resolve content-addressed path for a 64-hex SHA-256 digest with 2-char sharding."""
        normalized = validate_sha256_hex(sha256_hex)
        shard = normalized[:2]
        return self.objects_dir / shard / normalized

    def has_asset(self, sha256_hex: str) -> bool:
        """Check if asset exists in local CAS storage.

        Returns False if the hash is invalid or the file is missing.
        """
        try:
            path = self._get_path_for_hash(sha256_hex)
            return path.is_file()
        except ValueError:
            return False

    def get_missing_hashes(self, required_hashes: Iterable[str]) -> Set[str]:
        """Calculate set difference `required \\ cached` against local CAS without network calls."""
        missing: Set[str] = set()
        valid_candidates: Set[str] = set()

        for raw_h in required_hashes:
            try:
                norm_h = validate_sha256_hex(raw_h)
                valid_candidates.add(norm_h)
            except (ValueError, TypeError):
                # Invalid hashes are treated as missing / unresolvable
                missing.add(str(raw_h).strip().lower() if raw_h is not None else "none")

        if not valid_candidates:
            return missing

        # Optimization: for large query sets, enumerate inventory in memory to avoid
        # thousands of individual disk stat syscalls on Windows.
        if len(valid_candidates) > 32:
            cached_inventory = self.get_inventory_hashes()
            missing.update(valid_candidates - cached_inventory)
        else:
            for norm_h in valid_candidates:
                shard = norm_h[:2]
                asset_path = self.objects_dir / shard / norm_h
                try:
                    if not asset_path.is_file():
                        missing.add(norm_h)
                except OSError:
                    missing.add(norm_h)

        return missing

    def get_asset_path(self, sha256_hex: str) -> Optional[Path]:
        """Return the Path to the asset file if it exists, otherwise None."""
        try:
            path = self._get_path_for_hash(sha256_hex)
            return path if path.is_file() else None
        except ValueError:
            return None

    def get_asset_size(self, sha256_hex: str) -> Optional[int]:
        """Return file size in bytes for a cached asset, or None if not found."""
        path = self.get_asset_path(sha256_hex)
        if path is not None:
            try:
                return path.stat().st_size
            except OSError:
                return None
        return None

    def get_inventory_hashes(self) -> Set[str]:
        """Enumerate all valid cached SHA-256 hashes across shard directories."""
        with self._lock:
            hashes: Set[str] = set()
            if not self.objects_dir.exists():
                return hashes
            for shard_dir in self.objects_dir.iterdir():
                if shard_dir.is_dir():
                    for obj_file in shard_dir.iterdir():
                        if obj_file.is_file():
                            try:
                                norm = validate_sha256_hex(obj_file.name)
                                hashes.add(norm)
                            except ValueError:
                                continue
            return hashes

    def list_cached_hashes(self) -> Set[str]:
        """Alias for get_inventory_hashes for compatibility."""
        return self.get_inventory_hashes()

    def open_asset_stream(self, sha256_hex: str, offset: int = 0) -> BinaryIO:
        """Open a readable binary stream for an asset starting at byte offset.

        Raises:
            FileNotFoundError: If asset does not exist in local CAS.
            ValueError: If offset is negative.
            IndexError: If offset exceeds the asset's total size.
        """
        path = self.get_asset_path(sha256_hex)
        if not path:
            raise FileNotFoundError(f"Asset {sha256_hex} not found in local CAS")

        total_size = path.stat().st_size
        if offset < 0:
            raise ValueError(f"Offset cannot be negative: {offset}")
        if offset > total_size:
            raise IndexError(f"Offset {offset} exceeds total file size {total_size}")

        handle = path.open("rb")
        if offset > 0:
            handle.seek(offset)
        return handle

    def create_staging_file(self, sha256_hex: Optional[str] = None, prefix: str = "transfer") -> Path:
        """Generate a unique staging file path in the staging directory."""
        tag = ""
        if sha256_hex:
            try:
                tag = f"_{validate_sha256_hex(sha256_hex)[:16]}"
            except ValueError:
                tag = "_obj"
        else:
            tag = "_obj"
        unique_name = f"{prefix}{tag}_{uuid.uuid4().hex}.tmp"
        return self.staging_dir / unique_name

    def commit_staged_file(self, staged_file_path: Union[str, Path], expected_sha256: str) -> bool:
        """Atomically insert a staged file into local CAS after incremental SHA-256 verification.

        If checksum matches, moves file to objects/XX/<hash> via atomic `os.replace`.
        If checksum fails or expected_sha256 is invalid, deletes staged file and returns False.
        """
        staged_p = Path(staged_file_path).resolve()
        if not staged_p.exists() or not staged_p.is_file():
            logger.warning("Staged file does not exist or is not a file: %s", staged_p)
            return False

        try:
            norm_expected = validate_sha256_hex(expected_sha256)
        except ValueError as exc:
            logger.warning("Invalid expected SHA-256 hash %r: %s", expected_sha256, exc)
            if staged_p.exists():
                staged_p.unlink(missing_ok=True)
            return False

        hasher = hashlib.sha256()
        try:
            with staged_p.open("rb") as f:
                while chunk := f.read(self.chunk_size):
                    hasher.update(chunk)
            actual_hash = hasher.hexdigest().lower()
        except OSError as exc:
            logger.error("Failed to read staged file %s for hashing: %s", staged_p, exc)
            if staged_p.exists():
                staged_p.unlink(missing_ok=True)
            return False

        if actual_hash != norm_expected:
            logger.warning(
                "SHA-256 checksum mismatch for staged file %s: expected %s, got %s",
                staged_p.name,
                norm_expected,
                actual_hash,
            )
            if staged_p.exists():
                staged_p.unlink(missing_ok=True)
            return False

        target_path = self._get_path_for_hash(norm_expected)

        try:
            with self._lock:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                if target_path.is_file():
                    # CAS is content-addressed: identical verified content is already present
                    staged_p.unlink(missing_ok=True)
                    return True
                os.replace(staged_p, target_path)
            logger.info("Successfully committed asset %s to CAS", norm_expected)
            return True
        except OSError as exc:
            logger.error("Failed to commit staged file %s to %s: %s", staged_p, target_path, exc)
            if staged_p.exists():
                staged_p.unlink(missing_ok=True)
            return False

    def store_bytes(self, data: bytes) -> str:
        """Store in-memory bytes into local CAS, returning the SHA-256 digest."""
        sha256_hex = hashlib.sha256(data).hexdigest()
        staging_file = self.create_staging_file(sha256_hex, prefix="store")
        staging_file.write_bytes(data)
        success = self.commit_staged_file(staging_file, sha256_hex)
        if not success:
            raise RuntimeError(f"Failed to commit {len(data)} bytes to CAS under hash {sha256_hex}")
        return sha256_hex

    def store_file(self, source_path: Union[str, Path], expected_sha256: Optional[str] = None) -> str:
        """Import an external file into local CAS using chunked copy and verification."""
        src = Path(source_path).resolve()
        if not src.exists() or not src.is_file():
            raise FileNotFoundError(f"Source file not found: {src}")

        staging_file = self.create_staging_file(expected_sha256, prefix="import")
        hasher = hashlib.sha256()
        try:
            with src.open("rb") as f_in, staging_file.open("wb") as f_out:
                while chunk := f_in.read(self.chunk_size):
                    hasher.update(chunk)
                    f_out.write(chunk)
        except Exception:
            staging_file.unlink(missing_ok=True)
            raise

        computed_hash = hasher.hexdigest().lower()
        target_hash = expected_sha256.lower() if expected_sha256 else computed_hash

        if expected_sha256 and computed_hash != target_hash:
            staging_file.unlink(missing_ok=True)
            raise ValueError(
                f"Source file checksum mismatch: expected {expected_sha256}, got {computed_hash}"
            )

        success = self.commit_staged_file(staging_file, target_hash)
        if not success:
            raise RuntimeError(f"Failed to commit file {src} to CAS under hash {target_hash}")
        return target_hash

    def prune_staging(self, max_age_seconds: Optional[float] = None) -> int:
        """Delete temporary staging files, optionally filtering by maximum age in seconds."""
        with self._lock:
            count = 0
            if not self.staging_dir.exists():
                return 0
            now = time.time()
            for tmp_file in self.staging_dir.iterdir():
                if tmp_file.is_file():
                    try:
                        if max_age_seconds is not None:
                            mtime = tmp_file.stat().st_mtime
                            if now - mtime < max_age_seconds:
                                continue
                        tmp_file.unlink(missing_ok=True)
                        count += 1
                    except OSError:
                        pass
            return count

    def delete_asset(self, sha256_hex: str) -> bool:
        """Delete an asset from local CAS and prune empty shard folder if applicable."""
        with self._lock:
            try:
                path = self._get_path_for_hash(sha256_hex)
                if path.is_file():
                    try:
                        path.unlink()
                    except OSError as exc:
                        logger.warning("Failed to unlink asset %s (in use or locked): %s", sha256_hex, exc)
                        return False

                    shard_dir = path.parent
                    if shard_dir.exists() and not any(shard_dir.iterdir()):
                        try:
                            shard_dir.rmdir()
                        except OSError:
                            pass
                    return True
                return False
            except (ValueError, OSError):
                return False

    def get_cas_stats(self) -> Dict[str, Any]:
        """Compute aggregated asset count and byte volume statistics."""
        with self._lock:
            total_assets = 0
            total_bytes = 0
            if self.objects_dir.exists():
                for shard in self.objects_dir.iterdir():
                    if shard.is_dir():
                        for obj in shard.iterdir():
                            if obj.is_file():
                                total_assets += 1
                                total_bytes += obj.stat().st_size

            return {
                "total_assets": total_assets,
                "total_bytes": total_bytes,
                "cas_dir": str(self.cas_dir),
                "objects_dir": str(self.objects_dir),
                "staging_dir": str(self.staging_dir),
            }
