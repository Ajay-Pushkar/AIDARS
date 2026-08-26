# Task Assignment: Milestone 5 Core Implementation

**Working Directory**: C:\AIDAR\.agents\teamwork_preview_worker_1
**Original Request**: C:\AIDAR\ORIGINAL_REQUEST.md
**Project Plan**: C:\AIDAR\PROJECT.md
**Project Root**: C:\AIDAR

## Write Ownership
You exclusively own:
- `src/aidars/cache/__init__.py`
- `src/aidars/cache/base.py`
- `src/aidars/cache/models.py`
- `src/aidars/cache/storage.py`
- `src/aidars/cache/index.py`
- `src/aidars/cache/resolver.py`
- `src/aidars/cache/eviction.py`
- `src/aidars/cache/verifier.py`
- `src/aidars/cache/store.py`

Do NOT modify any files outside `src/aidars/cache/`.

## Mandatory Integrity Warning
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A teamwork_preview_auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.

## Mission & Architecture
Implement Milestone 5 Core (Local Content-Addressed Asset Cache) in `src/aidars/cache/`:
1. `models.py`:
   - `CacheEntry`: `sha256`, `size_bytes`, `asset_type`, `original_name`, `source_path`, `created_at`, `last_accessed_at`, `access_count`, `verification_status`, `relative_path`, `metadata`.
   - `ResolutionResult`: `hits: set[str]`, `misses: set[str]`, `total_requested_bytes: int`, `hit_bytes: int`, `miss_bytes: int`, `byte_hit_ratio: float`, `network_saved_bytes: int`.
   - `VerificationReport`: `verified_count`, `corrupted_count`, `missing_count`, `corrupted_hashes`, `missing_hashes`, `is_healthy`.
   - Exceptions: `CacheError`, `CacheQuotaExceededError`, `HashMismatchError`, `InvalidHashError`, `CacheStorageError`.
2. `storage.py` (`SplitHashStorage`):
   - SHA-256 validation (64 hex lowercase).
   - Split-hash relative path: `objects/<h[:2]>/<h[2:]>`.
   - Atomic ingestion via `<cache_root>/tmp/<uuid>.tmp` and `os.replace`.
   - 64 KiB chunked read/write stream methods.
3. `index.py` (`SQLiteMetadataIndex`):
   - SQLite db at `<cache_root>/metadata/index.db`.
   - Schema with WAL mode (`PRAGMA journal_mode=WAL`), `PRAGMA busy_timeout=10000`.
   - Indices on `last_accessed_at`, `size_bytes`, `verification_status`.
   - Thread-safe connection context, CRUD, touch timestamp update, and LRU eviction candidate queries (`ORDER BY last_accessed_at ASC`).
4. `resolver.py` (`HitMissResolver`):
   - O(A) set difference (`missing = required - cached`) using Python sets.
   - Duck-typed M4 `PackagePlan` bridge (extracts `.sha256` and `.size_bytes` from assets).
   - Accurate metrics calculation: `byte_hit_ratio` ($BHR = \frac{hit\_bytes}{total\_bytes}$ or 1.0 if empty), `network_saved_bytes = hit_bytes`.
5. `eviction.py` (`LRUEvictor`):
   - Quota enforcement (`max_cache_size_bytes`).
   - Evicts oldest `last_accessed_at` entries from disk and DB until target free bytes met.
   - Handles Windows file locking exceptions (`PermissionError`) gracefully without crashing.
6. `verifier.py` (`IntegrityVerifier`):
   - Fast metadata check + deep streaming SHA-256 digest check.
   - Auto-eviction and self-healing of corrupted files (marking DB record corrupt and deleting bad file).
   - Batch verification scrubber (`verify_all`).
7. `base.py` & `store.py` (`DiskCacheStore` implementing `CacheStore` ABC):
   - High-level facade uniting storage, index, resolver, eviction, and verifier.
   - Implements `contains`, `get_path`, `get_stream`, `get_bytes`, `put_bytes`, `put_file`, `put_stream`, `verify`, `verify_all`, `remove`, `evict_lru`, `resolve_hashes`, `resolve_plan`, `get_stats`.
   - Decoupled from Blender (`bpy`).
8. `__init__.py`: Clean exports of all classes and functions.

When implementation is complete, run existing tests and new tests with pytest to ensure 100% pass and no regressions.
Write your handoff report to `C:\AIDAR\.agents\teamwork_preview_worker_1\handoff.md` and report back when finished.
