# Milestone 5 Core: Technical Architecture & Storage Design Specification

## 1. Observation

### 1.1 Requirements & Context Observations
- **Source**: `C:\AIDAR\ORIGINAL_REQUEST.md` (lines 21–42):
  - **R1 Content-Addressed Storage & Hashing**: "Implement a local filesystem-backed cache store where asset identity is strictly defined by its SHA-256 hash, not its filename or path. Store objects using a split-hash directory structure (e.g., `objects/a9/1f3e...`) to prevent directory bloat."
  - **R2 Cache Metadata Index**: "Implement a SQLite-backed index (`cache/metadata/index.db`) to track cache entries. It must store the authoritative hash, size, type, original name, creation time, last accessed time, and verification status."
  - **R3 Hit/Miss Resolver & Set Difference**: "Given a requested set of asset hashes (from an M4 PackagePlan), the cache must compute the set difference (`missing = required - cached`) in O(A) average time using standard Sets to definitively identify which assets need to be transferred."
  - **R4 Integrity, Eviction, and Interfaces**: "Implement an LRU eviction policy based on `last_accessed` tracking to enforce a maximum cache quota. Define a clean `CacheStore` interface (`contains`, `get`, `put`, `verify`, `remove`) that abstracts the local implementation. Implement chunked transfer logic to bound memory usage when caching large assets."
  - **R5 Subsystem Independence**: "The M5 cache logic (in `src/aidars/cache/`) must be entirely decoupled from M4. M5 consumes the asset hashes and sizes provided by M4, but must not depend on Blender objects, materials, cameras, or visibility logic."
  - **Acceptance Criteria**: Comprehensive `pytest` suite, `byte_hit_ratio` and `network_saved` calculations, simulated cache corruption detection & eviction, and zero imports from Blender-specific graph modules.

### 1.2 System Rules & Architectural Observations
- **Source**: `C:\AIDAR\AIDAR_AGENT_SKILL.md` (lines 66–69, 115–136):
  - Milestone order: "5. **M5 Cache Engine** — layered (RAM / SSD / metadata DB), content-addressed via SHA-256, not filenames. Big assets live on disk/filesystem; the DB only stores metadata. Each worker reports its cache inventory (`sha256:...` list) so Master only transfers what's missing."
  - Tech stack: Python core, SQLite for metadata only ("never store large assets in the DB"), local filesystem for assets/cache.
  - Three layers: Orchestration layer owns Cache Engine; execution layer owns worker Cache Manager. Decoupled from Intelligence layer.

### 1.3 Codebase Layout & Data Model Observations
- **Source**: `C:\AIDAR\src\aidars\smart_package\models.py` (lines 49–91, 120–156):
  - `AssetRecord` provides `sha256: Optional[str]`, `size_bytes: int`, `asset_type: AssetType`, `source_path: Optional[str]`.
  - `PackagePlan` holds `all_assets: List[AssetRecord]`, `deduplicated_assets: List[AssetRecord]`, `missing_assets: List[AssetRecord]`.
- **Source**: `C:\AIDAR\tests/` (171 passing tests across M1–M4):
  - No existing `src/aidars/cache/` directory exists yet (currently legacy scene caching lives in `src/aidars/scene_intelligence/cache.py`).
  - M5 requires a dedicated new package in `src/aidars/cache/` completely separate from `scene_intelligence`.

---

## 2. Logic Chain

### 2.1 Split-Hash Storage Hierarchy & Atomic File Operations
1. **Hash Normalization & Namespace Isolation**:
   - Every asset is identified by a 64-character lowercase SHA-256 hex digest (`[0-9a-f]{64}`).
   - Validation must enforce `re.match(r"^[0-9a-f]{64}$", hash_str)` or raise `ValueError("Invalid SHA-256 digest")`.
2. **Directory Partitioning (`objects/{prefix}/{suffix}`)**:
   - Partitioning by 2-character prefix (e.g. `hash[:2]`) gives 256 subdirectories under `<cache_root>/objects/`.
   - Object path formula: `<cache_root>/objects/<hash[0:2]>/<hash[2:64]>`.
   - Why 2 characters? 256 subdirectories provides the optimal fanout for POSIX and NTFS filesystems up to $10^6$ entries, avoiding NTFS linear search bottlenecks on giant flat directories while keeping directory tree depth shallow (1 level).
3. **Atomic Write Protocol (Tempfile-to-Rename)**:
   - To guarantee zero half-written or corrupt files on crash/power loss or concurrent reads:
     1. Stage incoming data in a temp file within `<cache_root>/tmp/` on the *same physical disk volume* (e.g. `<cache_root>/tmp/.tmp_<hash>_<uuid4>.tmp`).
     2. Stream payload in 64 KiB chunks (`65,536 bytes`), updating an in-flight `hashlib.sha256()` hasher and tracking exact bytes written.
     3. Verify `computed_hash == expected_hash` (if expected hash was passed). If mismatch, delete temp file and raise `IntegrityError`.
     4. Flush buffers (`f.flush()`) and sync to storage (`os.fsync(f.fileno())`).
     5. Atomic Move: Call `os.replace(temp_path, final_target_path)`. On Windows and POSIX, `os.replace` atomically renames/overwrites on the same filesystem.
     6. Insert metadata into SQLite index in an atomic transaction.
4. **Collision & Deduplication Property**:
   - If `final_target_path` already exists: since SHA-256 is collision-resistant ($2^{128}$ collision bounds), the existing file is identical in content. The store can perform a fast-path touch (`UPDATE cache_entries SET last_accessed_at = ?, access_count = access_count + 1 WHERE hash = ?`) and skip redundant disk I/O.

### 2.2 SQLite Metadata Index Schema & Concurrency
1. **Schema Design**:
   - Stored at `<cache_root>/metadata/index.db`.
   ```sql
   CREATE TABLE IF NOT EXISTS cache_entries (
       hash TEXT PRIMARY KEY,
       size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
       asset_type TEXT NOT NULL DEFAULT 'unknown',
       original_name TEXT DEFAULT '',
       source_path TEXT DEFAULT '',
       created_at REAL NOT NULL,
       last_accessed_at REAL NOT NULL,
       access_count INTEGER NOT NULL DEFAULT 1,
       verification_status TEXT NOT NULL DEFAULT 'verified',
       metadata_json TEXT DEFAULT '{}'
   );

   CREATE INDEX IF NOT EXISTS idx_cache_last_accessed 
       ON cache_entries (last_accessed_at ASC);

   CREATE INDEX IF NOT EXISTS idx_cache_size 
       ON cache_entries (size_bytes);

   CREATE INDEX IF NOT EXISTS idx_cache_verification 
       ON cache_entries (verification_status);
   ```
2. **SQLite Performance & Concurrency Pragmas**:
   - `PRAGMA journal_mode = WAL;`: Write-Ahead Logging allows unlimited concurrent readers alongside a writer, eliminating reader-writer lock contention.
   - `PRAGMA synchronous = NORMAL;`: Guarantees database integrity across application crashes with minimal fsync overhead in WAL mode.
   - `PRAGMA busy_timeout = 10000;`: Waits up to 10 seconds for locks before raising `sqlite3.OperationalError`.
   - `PRAGMA temp_store = MEMORY;`: Stores temporary tables and indices in RAM.
3. **Thread Safety & Connection Strategy**:
   - Maintain thread-safe connection pooling or a reentrant connection context manager (`with self._connect() as conn:`), setting `isolation_level=None` (autocommit mode) with explicit `BEGIN IMMEDIATE` / `COMMIT` blocks for atomic mutations.

### 2.3 Hit/Miss Resolver & $O(1)$ Set-Difference Mathematics
1. **Set Difference Algorithm**:
   - Input: Requested assets $R = \{A_1, A_2, \dots, A_n\}$ or requested hashes $H_{req} = \{h_1, h_2, \dots, h_n\}$.
   - Cached hashes $H_{cached} = \text{hashes present in SQLite with verification\_status='verified' and valid disk file}$.
   - Set operations:
     $$H_{missing} = H_{req} \setminus H_{cached}$$
     $$H_{hit} = H_{req} \cap H_{cached}$$
   - Computational complexity: Set initialization is $O(A)$, set subtraction is $O(|H_{req}|)$ average time using Python hash tables (`set`).
2. **Batched Index Lookup**:
   - For a batch of $N$ hashes, query SQLite using chunked `IN` clauses of up to 500 parameters (`SELECT hash, size_bytes FROM cache_entries WHERE hash IN (?, ?, ...) AND verification_status = 'verified'`).
   - Complexity: $O(N)$ total lookup time indexed by Primary Key B-Tree.
3. **Transfer & Bandwidth Metrics**:
   - Total Requested Bytes: $B_{req} = \sum_{a \in R} a.\text{size\_bytes}$
   - Hit Bytes (Saved): $B_{hit} = \sum_{a \in R, a.hash \in H_{hit}} a.\text{size\_bytes}$
   - Missing Bytes (To Transfer): $B_{missing} = \sum_{a \in R, a.hash \in H_{missing}} a.\text{size\_bytes}$
   - Byte Hit Ratio:
     $$\text{byte\_hit\_ratio} = \begin{cases} \frac{B_{hit}}{B_{req}} & \text{if } B_{req} > 0 \\ 1.0 & \text{if } B_{req} = 0 \end{cases}$$
   - Network Saved:
     $$\text{network\_saved\_bytes} = B_{hit}$$
   - Reduction Percent: $\text{byte\_hit\_ratio} \times 100.0$

### 2.4 LRU Eviction & Quota Management
1. **Quota Model**:
   - Configurable `max_size_bytes: int` (e.g. 50 GB default).
   - Optional low/high watermark: when cache exceeds `max_size_bytes`, evict down to `target_free_bytes` (or `max_size_bytes - incoming_size_bytes`).
2. **Eviction Algorithm**:
   1. Query current cache size: `SELECT COALESCE(SUM(size_bytes), 0) FROM cache_entries`.
   2. If `current_size + incoming_bytes > max_size_bytes`, compute deficit:
      $$\Delta_{free} = (current\_size + incoming\_bytes) - max\_size\_bytes$$
   3. Fetch candidate entries in strict LRU order:
      `SELECT hash, size_bytes FROM cache_entries ORDER BY last_accessed_at ASC LIMIT 100;`
   4. For each candidate $h$:
      - Construct path `objects/<h[:2]>/<h[2:]>`.
      - Delete physical file (`os.unlink(path)`). Catch `PermissionError` (e.g., file open by active render subprocess on Windows), log warning, and skip to next candidate without breaking eviction loop.
      - Delete SQLite record (`DELETE FROM cache_entries WHERE hash = ?`).
      - Accumulate freed bytes: `freed += candidate.size_bytes`.
      - Stop when `freed >= \Delta_{free}`.
3. **Touch Semantics**:
   - Every read (`get()`, `get_stream()`, `get_bytes()`) updates `last_accessed_at = time.time()` and increments `access_count += 1`.

### 2.5 Chunked Streaming, Hash Verification, & Self-Healing Corruption Recovery
1. **Chunked Streaming (Bounded RAM)**:
   - Chunk size: `DEFAULT_CHUNK_SIZE = 64 * 1024` (64 KiB).
   - Streaming reader `get_stream(hash, chunk_size=65536) -> Iterator[bytes]`.
   - Streaming writer `put_stream(stream, expected_hash, ...) -> CacheEntry`.
   - RAM footprint remains bounded at $\le 64 \text{ KiB}$ per stream, regardless of whether the asset is a 10 KB script or a 50 GB simulation cache.
2. **Two-Tier Verification**:
   - **Tier 1 (Fast / Metadata check)**: Checks SQLite row exists, physical file exists on disk, and `os.path.getsize(path) == row.size_bytes`.
   - **Tier 2 (Deep / Cryptographic check)**: Streams entire file through `hashlib.sha256()`, calculates SHA-256 hex digest, and asserts exact match against `hash`.
3. **Corruption Detection & Self-Healing Auto-Eviction**:
   - If a corrupted file is detected during `get()`, `verify()`, or `verify_all()`:
     1. Unlink corrupted physical file from disk (if present).
     2. Delete or mark record as `corrupted` in SQLite index.
     3. Log warning with expected vs actual digest / size.
     4. Return `None` (cache miss), forcing the caller / orchestrator to re-transfer the valid asset.
   - Comprehensive cache scrubbing: `verify_all() -> VerificationReport` sweeps all entries, validates digests, and purges invalid or orphaned records.

### 2.6 Clean `CacheStore` Interface & Subsystem Decoupling
1. **Decoupled Architecture (`src/aidars/cache/`)**:
   - Zero imports of Blender (`bpy`), `aidars.scene_intelligence`, `aidars.visibility`, or `aidars.smart_package`.
   - Polymorphic input handling: `resolve_plan(plan)` accepts:
     - Any object with `.all_assets` containing objects with `.sha256` and `.size_bytes` (e.g. M4 `PackagePlan`)
     - Dictionaries with `"assets"` keys
     - Raw iterables of hash strings or `(hash, size_bytes)` tuples.
2. **Module Layout**:
   ```
   src/aidars/cache/
   ├── __init__.py           # Public exports (CacheStore, DiskCacheStore, CacheEntry, etc.)
   ├── base.py               # Abstract Base Class CacheStore & protocols
   ├── models.py             # Dataclasses & Enums (CacheEntry, CacheStats, ResolutionResult, etc.)
   ├── storage.py            # SplitHashStorage (split paths, atomic writes, streaming)
   ├── index.py              # SQLiteMetadataIndex (schema, WAL, queries, transactions)
   ├── resolver.py           # HitMissResolver (O(1) set differences, metric calculation)
   ├── eviction.py           # LRUEvictor (quota management, candidate ordering, lock handling)
   ├── verifier.py           # IntegrityVerifier (fast/deep verification, corruption scrubbing)
   └── store.py              # Concrete DiskCacheStore facade uniting all components
   ```

---

## 3. Caveats

1. **Windows File Locking Concurrency**: On Windows (NTFS), files opened by another process cannot be deleted or renamed (`PermissionError: [WinError 32]`). The eviction and write engines must catch this exception and skip/retry rather than crash.
2. **Filesystem Boundaries for Atomic Rename**: `os.replace` is only atomic if the source temp file and destination object reside on the same filesystem/drive. Staging files must always be created inside `<cache_root>/tmp/`, never in the system `/tmp` (which might be a separate tmpfs/RAM disk or different drive).
3. **SQLite Busy Timeout Under High Thread Concurrency**: Under extreme multithreaded writes, SQLite may return `SQLITE_BUSY`. Setting WAL mode, `PRAGMA synchronous = NORMAL`, and `PRAGMA busy_timeout = 10000` mitigates this, but high-concurrency writes should use transaction retries.

---

## 4. Conclusion

The Milestone 5 Core Cache architecture fulfills all requirements (R1–R5) with rigorous mathematical, cryptographic, and systems guarantees:
1. **Storage**: Content-addressed SHA-256 identity with 2-level split hierarchy (`objects/{prefix}/{suffix}`), atomic tempfile staging on the same filesystem, and `os.replace`.
2. **Metadata**: SQLite index with WAL mode, indices on `last_accessed_at` and `size_bytes`, and complete audit columns.
3. **Hit/Miss Resolution**: Pure $O(A)$ average set difference using Python `set` and batched SQL lookups, delivering `byte_hit_ratio` and `network_saved` transfer metrics.
4. **Eviction**: Strict LRU quota enforcement, ordering by `last_accessed_at ASC`, atomic disk and DB deletion, and Windows sharing violation resilience.
5. **Streaming & Integrity**: 64 KiB memory-bounded chunk streaming, two-tier verification, and automatic self-healing eviction of corrupted/tampered entries.
6. **Decoupling**: Isolated in `src/aidars/cache/` with zero Blender dependencies and duck-typed M4 integration.

---

## 5. Verification Method

### 5.1 Independent Verification Plan
Once implemented by the downstream implementation team, verify with the following commands and checks:

1. **Unit & Integration Test Suite Execution**:
   ```bash
   pytest tests/test_cache_store.py -v
   pytest tests/test_cache_adversarial.py -v
   pytest tests/ -v
   ```
2. **Verification Test Cases to Execute**:
   - `test_split_hash_hierarchy`: Assert object stored at `objects/<hash[:2]>/<hash[2:]>`.
   - `test_atomic_write_durability`: Simulate process kill/interruption during write; verify no corrupt partial file in `objects/`.
   - `test_set_difference_hit_miss`: Provide required hashes $\{A, B, C\}$, cache $\{B\}$, verify missing is $\{A, C\}$, hit is $\{B\}$, and verify $O(A)$ scaling.
   - `test_transfer_metrics_calculation`: Verify `byte_hit_ratio` and `network_saved` match exact expected arithmetic for mixed hit/miss batches.
   - `test_lru_eviction_quota`: Set cache quota to 1 MB, insert entries totaling 1.5 MB, assert oldest entry evicted, newest retained, total size $\le 1\text{ MB}$.
   - `test_simulated_corruption_eviction`: Tamper with cached file bytes on disk; assert `verify()` returns `False`, corrupted entry is purged from DB and disk, and subsequent request triggers a cache miss.
   - `test_memory_bounded_streaming`: Stream a large mock file (e.g. 50 MB) through `put_stream()` and `get_stream()`; verify process RSS memory does not increase by 50 MB (stays bounded to 64 KB buffer).
   - `test_subsystem_isolation`: Run `import sys; assert 'bpy' not in sys.modules` and verify `src/aidars/cache/` has no imports from `aidars.visibility`, `aidars.scheduler`, or `bpy`.

3. **Invalidation Conditions**:
   - Any dependency on `bpy` or Blender scene graph modules inside `src/aidars/cache/`.
   - Missing atomic write staging (writing directly to destination path).
   - Non-WAL SQLite mode or lack of index on `last_accessed_at`.
   - Memory usage proportional to total asset size rather than chunk size during streaming.
