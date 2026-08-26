# Milestone 5 Core Codebase Investigation & Survey Report

## 1. Observation

### 1.1 Project Structure & Package Hierarchy
- **Project Root**: `C:\AIDAR`
- **Source Root**: `C:\AIDAR\src\aidars`
  - `scene_intelligence/`: Scene inspection, canonical schema extraction, graph extraction, M1 request-aware scene caching (`cache.py`), facade orchestrator (`scene_engine.py`), and CLI (`cli.py`).
  - `visibility/`: Milestone 3 3D render requirement analysis, visibility determination, camera projection math, frustum culling, raycast occlusion, influence analysis, and dependency closure (`models.py`, `analyzer.py`, `camera.py`, `eligibility.py`, `geometry.py`, `influence.py`, `resolver.py`).
  - `smart_package/`: Milestone 4 smart packaging pipeline (`models.py`, `resolver.py`, `builder.py`, `validator.py`, and blender scripts `remap_paths.py`, `verify_package.py`).
  - `scheduler/`: Milestone 7 frame scheduler (`frame_scheduler.py`, `queue.py`).
  - `cache/`: **Currently does not exist.** Milestone 5 will create this package (`src/aidars/cache/`).

### 1.2 M4 Packaging Architecture & Data Models
From `C:\AIDAR\src\aidars\smart_package\models.py`:
- `AssetRecord` (lines 50–91): Represents an individual asset with:
  - `asset_id`: `str`
  - `asset_type`: `AssetType` (enum: `SCENE`, `MESH`, `MATERIAL`, `TEXTURE`, `IMAGE`, `HDRI`, `LIGHT`, `CAMERA`, `LIBRARY`, `SIMULATION_CACHE`, `GENERATED`, `ACTION`, `MODIFIER`, `COLLECTION`, `UNKNOWN`)
  - `selection_reason`: `SelectionReason` (`RENDER_REQUIRED`, `DEPENDENCY`, `UNKNOWN_DEPENDENCY`)
  - `source_path`: `Optional[str]`
  - `relative_path`: `Optional[str]`
  - `package_path`: `Optional[str]` (e.g., `assets/{sha256[:8]}_{filename}`)
  - `status`: `AssetStatus` (`RESOLVED`, `EMBEDDED`, `MISSING`, `GENERATED`)
  - `sha256`: `Optional[str]` (64-character lowercase hex string)
  - `size_bytes`: `int` (exact byte count)
  - `embedded`: `bool`
  - `conservative`: `bool`
  - `dependencies`: `List[str]`
- `PackagePlan` (lines 120–156):
  - `package_id`: `str`
  - `scene_name`: `str`, `camera`: `str`, `frame_start`: `int`, `frame_end`: `int`
  - `all_assets`: `List[AssetRecord]`
  - `deduplicated_assets`: `List[AssetRecord]`
  - `missing_assets`: `List[AssetRecord]`
  - `statistics`: `PackageStatistics` (`total_assets`, `resolved_assets`, `embedded_assets`, `missing_assets`, `duplicate_assets`, `original_size_bytes`, `package_size_bytes`, `reduction_percent`)
- `PackageIntegrityReport` (lines 159–176):
  - `verified`: `bool`, `asset_count`: `int`, `verified_count`: `int`, `failed_assets`: `List[str]`, `missing_assets`: `List[str]`

From `C:\AIDAR\src\aidars\smart_package\resolver.py` (lines 248–255):
```python
@staticmethod
def compute_sha256(file_path: Union[str, Path]) -> str:
    """Compute SHA-256 hash of a file incrementally."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()
```

### 1.3 Existing Cache & Storage Code
- `C:\AIDAR\src\aidars\scene_intelligence\cache.py`:
  - Implements `SceneCache` and `SceneCacheEntry` (lines 53–154).
  - Used for Phase 2 / Milestone 1 scene intelligence caching: caches whole pipeline output JSON files (`scene.json`, `graph.json`, `package.json`) keyed by `source_key::request_hash` inside `.aidars_cache/index.json`.
  - This is a high-level JSON index for pipeline runs, NOT a content-addressed chunked object cache.
- There is currently no content-addressed asset cache or SQLite-backed metadata store in the repository.

### 1.4 Dependencies & Environment Configuration
- `pyproject.toml`:
  - `name = "aidars"`, `version = "0.1.0"`, `requires-python = ">=3.10"`.
  - Build system: `setuptools>=68`, `setuptools.build_meta`.
  - Package dir: `package-dir = {"" = "src"}`.
  - Optional dev dependencies: `pytest`, `ruff`, `black`, `mypy`, `fake-bpy-module-4.5==20260128`.
- `requirements.txt`:
  - Zero third-party runtime dependencies required. Standard library (`hashlib`, `sqlite3`, `pathlib`, `shutil`, `tempfile`, `dataclasses`, `typing`, `json`, `os`, `sys`, `time`) is used across core subsystems.
- `sitecustomize.py`:
  - Resolves `src` and prepends to `sys.path`.
- System Environment:
  - Python: `3.14.5` (64-bit Windows).
  - SQLite C Library: `3.50.4` via standard library `sqlite3`.
  - Blender: `Blender 4.5.11 LTS` available on PATH.
  - Pytest: `pytest-9.1.1` with `anyio-4.14.2`, `asyncio-1.4.0`.

### 1.5 Test Suite Baseline
- Command: `pytest`
- Result: `171 passed in 3.94s` across 14 test files (`test_blender_scripts_extractors.py`, `test_cache_adversarial.py`, `test_cli.py`, `test_frame_scheduler.py`, `test_m4_adversarial_challenger_1.py`, `test_m4_blender_integration.py`, `test_m4_smart_packaging.py`, `test_package_imports.py`, `test_render_requirements.py`, `test_scene_cache.py`, `test_scene_engine.py`, `test_scene_engine_facade.py`, `test_visibility.py`, `test_visibility_adversarial.py`).

---

## 2. Logic Chain

1. **Subsystem Boundary & Decoupling (R5)**:
   - Observation: In `AIDAR_AGENT_SKILL.md` (lines 31–48) and `ORIGINAL_REQUEST.md` (lines 34–36), architecture mandates that Master caching belongs to Orchestration (`src/aidars/cache/`) and must NOT reach into or depend on Scene Intelligence / Blender structures (`bpy`, `SceneSnapshot`, `RenderRequirementReport`, or Blender geometry).
   - Inference: `src/aidars/cache/` should only deal with raw data types: cryptographic SHA-256 strings (`str`), byte counts (`int`), file paths / streams (`Path`, `BinaryIO`, `bytes`), metadata dictionaries / records (`CacheEntry`), and sets of hashes (`Set[str]`).

2. **Content-Addressed Storage & Split-Hash Directory Mechanics (R1)**:
   - Observation: Assets in M4 are keyed by standard 64-character SHA-256 hex strings (e.g. `a91f3e...`).
   - Inference: A split-hash structure should use prefix subdirectories (e.g., `objects/{hash[:2]}/{hash[2:]}` or `objects/{hash[:2]}/{hash}`) inside the storage directory (e.g., `<cache_root>/objects/`). This ensures thousands of cached assets avoid single-directory inode/filesystem degradation on Windows and Linux.

3. **Metadata Index Design (R2)**:
   - Observation: SQLite C library 3.50.4 is available in the Python 3.14 standard library.
   - Inference: `cache/metadata/index.db` can be managed using `sqlite3` with an ACID table schema tracking:
     - `hash` (TEXT PRIMARY KEY)
     - `size_bytes` (INTEGER NOT NULL)
     - `asset_type` (TEXT)
     - `original_name` (TEXT)
     - `created_at` (REAL NOT NULL)
     - `last_accessed` (REAL NOT NULL)
     - `verified` (INTEGER DEFAULT 1)
     - `relative_path` (TEXT)
   - Using WAL (Write-Ahead Logging) mode and connection isolation ensures thread safety and high throughput.

4. **Hit/Miss Resolution & Set Difference (R3)**:
   - Observation: A render package plan contains $A$ requested asset hashes. The worker's local cache index contains $C$ cached asset hashes.
   - Inference: Querying cached hashes into a Python `set` and performing `missing_hashes = requested_hashes - cached_hashes` provides strict $O(A)$ average-time resolution.
   - For transfer simulation metrics:
     - `byte_hit_ratio = (cached_bytes / total_requested_bytes)` if total_bytes > 0 else `1.0`
     - `network_saved = cached_bytes` (in bytes).

5. **Integrity, LRU Eviction, and Chunked Streaming (R4)**:
   - Observation: Large texture/mesh files can exceed hundreds of megabytes.
   - Inference:
     - `put()` operations must use buffered streaming (e.g. 64 KiB or 1 MiB chunk size) to stream from file/source to temporary staging file, calculating SHA-256 on the fly, before atomically renaming to the split-hash destination path.
     - `LRU Eviction`: When cache total size exceeds `max_bytes` quota, entries ordered by `last_accessed ASC` must be evicted (deleted from disk and index) until total size is within quota.
     - `Corruption Recovery`: `verify(hash)` checks disk existence and re-computes SHA-256. If disk hash differs from authoritative index hash or file is missing, the entry is flagged corrupted, evicted from disk, and removed/marked invalid in SQLite index.

---

## 3. Caveats

- **No Caveats**: All 171 existing test suites pass cleanly. Standard library `sqlite3` and `hashlib` are fully compatible with Python 3.14.5.
- Note on Python 3.14: `sqlite3.version` attribute was removed in Python 3.14, but `sqlite3.sqlite_version` and the core `sqlite3` module operate completely standardly.

---

## 4. Conclusion

The AIDAR codebase is in a clean, highly structured state ready for Milestone 5 Core implementation:
1. **Module Location**: `src/aidars/cache/` should be created as a standalone package containing:
   - `models.py`: `CacheEntry`, `CacheStatistics`, `CacheQueryResult`, `EvictionPolicy` enum.
   - `interfaces.py` / `base.py`: `CacheStore` ABC defining `contains`, `get`, `put`, `verify`, `remove`, `evict`, `resolve_missing`, `get_stats`.
   - `index.py`: SQLite-backed metadata index (`cache/metadata/index.db`) managing metadata schema, timestamps, verification flags, and atomic transactions.
   - `store.py`: Filesystem content-addressed store implementing split-hash directory layout (`objects/ab/cdef...`), atomic write-via-staging, chunked streaming, and LRU quota enforcement.
   - `resolver.py`: $O(A)$ set-difference hit/miss resolver calculating `byte_hit_ratio` and `network_saved`.
   - `__init__.py`: Clean exports of all public interfaces.
2. **Decoupling**: `src/aidars/cache/` must NOT import Blender or `aidars.scene_intelligence` / `aidars.visibility` models directly.
3. **Testing Strategy**: A dedicated test suite in `tests/test_cache_store.py`, `tests/test_cache_index.py`, `tests/test_cache_lru.py`, and `tests/test_cache_resolver.py` will verify split-hash storage, SQLite indexing, $O(A)$ set differences, LRU eviction, chunked transfers, and corruption resilience.

---

## 5. Verification Method

To verify the codebase status and baseline:
1. Run full test suite:
   ```bash
   pytest -v
   ```
2. Verify Python runtime and SQLite availability:
   ```bash
   python -c "import sqlite3, hashlib; print('SQLite:', sqlite3.sqlite_version); print('SHA-256:', hashlib.sha256(b'test').hexdigest())"
   ```
3. Inspect key source and model files:
   - `src/aidars/smart_package/models.py`
   - `src/aidars/smart_package/resolver.py`
   - `src/aidars/scene_intelligence/cache.py`
