# Task Assignment: Specification & Test Matrix Mining

**Working Directory**: C:\AIDAR\.agents\teamwork_preview_spec_miner_1
**Original Request**: C:\AIDAR\ORIGINAL_REQUEST.md
**Project Root**: C:\AIDAR

## Mission
Probe all authoritative requirements from `ORIGINAL_REQUEST.md` and project context to construct a complete specification and test matrix for Milestone 5 Core:
1. Enumerate all user-facing requirements R1, R2, R3, R4, R5 and acceptance criteria.
2. Build a comprehensive 4-tier testing specification:
   - Tier 1: Feature Coverage (>=5 tests per feature: storage, index, resolver, LRU eviction, chunking, verification).
   - Tier 2: Boundary & Corner Cases (empty assets, zero-byte files, huge assets, quota = 0 or exact fit, empty hash sets, invalid hashes, corrupt disk files, read-only paths).
   - Tier 3: Cross-Feature Combinations (e.g. Put -> Evict -> Resolve; Put -> Corrupt -> Verify -> Evict; Concurrent hit/miss under quota pressure).
   - Tier 4: Real-World Workload Scenarios (Simulated M4 package distribution workflows, multi-asset transfers, byte hit ratio & network saved calculations).
3. Specify exact corruption scenarios (e.g., bit flips, truncated files, deleted physical files with dangling DB records, modified timestamps).
4. Specify decoupling requirements to guarantee no Blender `bpy` dependencies.

Write your specification and test matrix to `C:\AIDAR\.agents\teamwork_preview_spec_miner_1\handoff.md` and `progress.md`.

## 2026-08-23T12:52:03Z
Read your task description in C:\AIDAR\.agents\teamwork_preview_spec_miner_1\DISPATCH.md and the project request at C:\AIDAR\ORIGINAL_REQUEST.md.
Extract all requirements, boundary conditions, corruption scenarios, metrics definitions, and design the complete 4-tier test matrix for M5 cache.
Update your progress.md periodically and write your specification report to C:\AIDAR\.agents\teamwork_preview_spec_miner_1\handoff.md. Report back with send_message when complete.

---

# MILESTONE 5 SPECIFICATION & TEST MATRIX REPORT

**Subsystem**: Milestone 5 Core (Local Content-Addressed Asset Cache Engine)  
**Target Path**: `src/aidars/cache/`  
**Test Suite**: `tests/test_m5_cache.py`, `tests/test_m5_cache_adversarial.py`  
**Miner Archetype**: Teamwork Specification Miner  
**Date**: 2026-08-23T12:55:00Z  

---

## 1. Features Discovered

| # | Category | Feature | Description | Inputs | Outputs | Error Behavior | Discovered Via |
|---|----------|---------|-------------|--------|---------|----------------|----------------|
| F1 | Storage | Content-Addressed Storage (CAS) | Local filesystem store where asset identity is strictly defined by SHA-256 hash. | `sha256: str`, `data: bytes \| BinaryIO \| Path` | `Path` to stored object `<root>/objects/<h[:2]>/<h[2:]>` | `InvalidHashError` on invalid SHA-256 hex; `HashMismatchError` on payload mismatch | `ORIGINAL_REQUEST.md` R1 |
| F2 | Storage | Split-Hash 2-Level Directory Hierarchy | Distributes objects into 256 subdirectories (`objects/aa/bbcc...`) to eliminate directory entry bloat and maintain high filesystem I/O performance on NTFS/EXT4. | `sha256: str` | `relative_path: Path` ("objects/aa/bbcc...") | Enforces lowercase 64-char hex string | `ORIGINAL_REQUEST.md` R1 |
| F3 | Storage | Atomic File Ingestion | Writes incoming asset streams/files to a temporary file (`tmp/<uuid>.tmp`) before atomic rename (`os.replace`) to prevent half-written / corrupt cache entries upon crash. | `stream / bytes` | Final object path on disk | Unlinks temporary file on write failure / hash mismatch | `PROJECT.md` & POSIX/NTFS atomic patterns |
| F4 | Metadata | SQLite-Backed Cache Index | Persistent metadata store (`cache/metadata/index.db`) using WAL mode, storing authoritative hash, size, type, original name, creation time, last accessed, access count, and status. | `CacheEntry` record data | SQLite database records | `sqlite3.DatabaseError` on corruption; WAL mode prevents locking contention | `ORIGINAL_REQUEST.md` R2 |
| F5 | Metadata | Touch & Access Tracking | Updates `last_accessed` timestamp and increments `access_count` on every `get()` / `touch()` invocation to maintain accurate LRU temporal ordering. | `sha256: str`, `timestamp: Optional[float]` | None / updated record | `KeyError` / returns False if asset not in index | `ORIGINAL_REQUEST.md` R2, R4 |
| F6 | Resolver | O(A) Set-Difference Hit/Miss Resolver | Computes set difference (`missing = required - cached`) in $O(A)$ average time using standard Python Sets to identify assets needing network transfer. | `requested: Set[str]` (or `PackagePlan`) | `ResolutionResult` (`hits: Set[str]`, `misses: Set[str]`, `byte_hit_ratio: float`, `network_saved: int`) | Handles empty sets, missing hashes, duplicate requested hashes | `ORIGINAL_REQUEST.md` R3 |
| F7 | Resolver | M4 PackagePlan Resolution Bridge | High-level resolver accepting an M4 `PackagePlan` or JSON manifest, resolving cached assets by hash, and calculating total required, hit, and missed byte metrics. | `plan: PackagePlan \| dict` | `PackageResolutionResult` with byte hit ratio and transfer savings | Gracefully handles embedded/generated assets without hashes | `ORIGINAL_REQUEST.md` R3, `src/aidars/smart_package/models.py` |
| F8 | Eviction | LRU Cache Quota Enforcer | Enforces maximum cache capacity (`max_cache_size_bytes`) by evicting entries ordered by `last_accessed ASC` when incoming asset exceeds available quota. | `incoming_bytes: int`, `target_quota: int` | `evicted_count: int`, `evicted_bytes: int` | Raises `CacheQuotaExceededError` if asset exceeds total quota | `ORIGINAL_REQUEST.md` R4 |
| F9 | Streaming | Chunked Stream Transfer & Memory Bounding | Streams assets in bounded chunks (e.g. 1 MiB or 64 KiB) to enforce $O(\text{chunk\_size})$ constant memory footprint during ingest and read of multi-GB assets. | `stream: BinaryIO`, `chunk_size: int` | Stream generator / atomic file write | Raises `HashMismatchError` if computed SHA-256 != claimed hash | `ORIGINAL_REQUEST.md` R4 |
| F10 | Integrity | Single & Batch Integrity Verification | Recomputes SHA-256 from disk file chunk-by-chunk and validates against SQLite authoritative record. Auto-quarantines or evicts corrupted files. | `sha256: str`, `auto_heal: bool` | `bool` (verified) / `VerificationReport` | Detects bit flips, truncations, missing disk files; updates status to 'corrupt' | `ORIGINAL_REQUEST.md` R4, Acceptance Criteria |
| F11 | Architecture | Decoupled CacheStore Interface | Clean abstract base class (`CacheStore`) abstracting local filesystem + SQLite implementation from higher-order AIDAR Master/Worker pipelines. | Methods: `contains`, `get`, `put`, `verify`, `remove`, `evict`, `stats` | Concrete `LocalCacheStore` instance | Zero dependencies on Blender `bpy` or `aidars.visibility` | `ORIGINAL_REQUEST.md` R4, R5 |
| F12 | Analytics | Cache Efficiency Metrics Reporter | Calculates `byte_hit_ratio`, `network_saved_bytes`, `asset_hit_ratio`, `cache_utilization_percent`, and total lifetime transferred vs saved metrics. | Resolution events / Cache state | `CacheMetricsReport` dataclass / dict | Safe division by zero guards on empty requests | `ORIGINAL_REQUEST.md` Acceptance Criteria |

---

## 2. Edge Cases

| # | Feature | Input | Observed Behavior |
|---|---------|-------|-------------------|
| E1 | CAS Storage | Zero-byte asset (empty file) | Computed SHA-256 is `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`. Stored at `objects/e3/b0c44...`, size = 0. Resolves and verifies cleanly. |
| E2 | CAS Storage | Mixed-case or uppercase SHA-256 string (e.g. `A91F...`) | Normalized automatically to lowercase `a91f...`. Maps to exact same physical object. |
| E3 | CAS Storage | Multiple assets with identical bytes but distinct original filenames | Deduplicated to single physical file in `objects/`. SQLite index updates original_name / metadata or tracks single canonical entry without file duplication. |
| E4 | Metadata Index | Concurrent reader and writer threads | SQLite WAL (`journal_mode=WAL`) and `busy_timeout=5000` allow concurrent non-blocking reads during active writes without `database locked` errors. |
| E5 | Resolver | Empty requested hash set (`set()`) | Returns `hits = set()`, `misses = set()`, `total_requested_bytes = 0`, `byte_hit_ratio = 1.0`, `network_saved_bytes = 0`. |
| E6 | Resolver | Requested set containing duplicate asset hashes | Duplicate hashes collapsed by Set semantics; byte counts calculated on deduplicated asset set. |
| E7 | Resolver | Requested set where all assets are cached | `misses = set()`, `byte_hit_ratio = 1.0`, `network_saved = total_bytes`. |
| E8 | Resolver | Requested set where no assets are cached | `hits = set()`, `byte_hit_ratio = 0.0`, `network_saved = 0`. |
| E9 | LRU Eviction | Cache quota = 0 bytes | Any `put()` operation fails with `CacheQuotaExceededError` or triggers immediate eviction. |
| E10 | LRU Eviction | Asset size exactly equals max cache quota | Evicts all existing entries to 0 bytes; successfully stores the new asset at 100% quota utilization. |
| E11 | LRU Eviction | Asset size strictly exceeds max cache quota | Refuses caching, rolls back write, raises `CacheQuotaExceededError` without evicting existing valid entries. |
| E12 | LRU Eviction | Multiple assets with identical `last_accessed` timestamps | Secondary sort key `access_count ASC` or FIFO primary key ID breaks ties deterministically. |
| E13 | Chunking | Asset size is exact multiple of chunk size (e.g. 2 MiB with 1 MiB chunk) | Final chunk reads empty bytes `b""`, closes stream cleanly, verifies full byte count. |
| E14 | Chunking | Stream interrupted / disconnected midway | Catches `IOError` / EOF before expected length, unlinks temporary file from `tmp/`, index not updated. |
| E15 | Integrity | Bit flip (1 byte altered in physical file) | `verify()` re-hashes file, fails checksum match against index, marks `status='corrupt'`, returns `False`. |
| E16 | Integrity | Physical file deleted externally from disk | `contains()` or `get()` detects file absence on disk, purges dangling index record or flags miss. |
| E17 | Boundary | Filenames containing unicode, emojis, or Windows invalid chars (`aux.png`, `nul.exr`, `../traversal`) | CAS strictly indexes by hash; original name stored as unicode text field in SQLite without path injection vulnerability. |
| E18 | Boundary | Read-only storage directory / permission denied | `put()` raises `CacheStorageError`; read operations (`get`, `contains`, `resolve`) continue functioning. |

---

## 3. Detailed Requirements Breakdown

### R1. Content-Addressed Storage & Hashing
- **Hash Algorithm**: SHA-256 (`hashlib.sha256()`). Output must be 64-character lowercase hex string (`^[0-9a-f]{64}$`).
- **Split-Hash Directory Structure**:
  - `objects/<prefix2>/<hash[2:]>` (e.g., hash `c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2` -> `objects/c3/ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2`).
  - Distributes millions of assets across 256 subdirectories, preventing OS directory inode exhaustion and file lookup latency.
- **Atomic Ingestion Protocol**:
  1. Generate UUID-based temp file in `<cache_root>/tmp/<uuid>.tmp` on the same filesystem.
  2. Stream incoming bytes in chunks into temp file while updating SHA-256 digest and byte counter.
  3. Validate computed SHA-256 against expected hash. If mismatched, unlink temp file and raise `HashMismatchError`.
  4. Ensure target directory `<cache_root>/objects/<prefix2>/` exists (`os.makedirs(..., exist_ok=True)`).
  5. Atomically move/replace temp file to final object destination (`os.replace(temp_path, final_path)`).
  6. Register or update SQLite metadata index.

### R2. Cache Metadata Index
- **Database Engine**: SQLite 3 at `<cache_root>/metadata/index.db`.
- **Connection Configuration**:
  ```sql
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  PRAGMA foreign_keys = ON;
  PRAGMA busy_timeout = 5000;
  ```
- **Schema Specification**:
  ```sql
  CREATE TABLE IF NOT EXISTS cache_entries (
      sha256 TEXT PRIMARY KEY NOT NULL,
      size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
      asset_type TEXT NOT NULL DEFAULT 'unknown',
      original_name TEXT DEFAULT '',
      created_at REAL NOT NULL,
      last_accessed REAL NOT NULL,
      access_count INTEGER NOT NULL DEFAULT 1,
      verification_status TEXT NOT NULL DEFAULT 'unverified',
      relative_path TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_last_accessed ON cache_entries(last_accessed ASC);
  CREATE INDEX IF NOT EXISTS idx_asset_type ON cache_entries(asset_type);
  CREATE INDEX IF NOT EXISTS idx_status ON cache_entries(verification_status);
  ```
- **Index Operations**:
  - `upsert_entry(entry: CacheEntry) -> None`
  - `get_entry(sha256: str) -> Optional[CacheEntry]`
  - `touch_entry(sha256: str, timestamp: Optional[float] = None) -> bool`
  - `remove_entry(sha256: str) -> bool`
  - `get_total_bytes() -> int`
  - `get_entries_for_eviction(target_bytes: int) -> List[CacheEntry]` (ordered by `last_accessed ASC`)
  - `update_status(sha256: str, status: str) -> bool`

### R3. Hit/Miss Resolver & Set Difference
- **Mathematical Model**:
  - Let $R$ be the requested set of SHA-256 hashes $\{h_1, h_2, \dots, h_m\}$.
  - Let $C$ be the active set of cached SHA-256 hashes $\{h \in \text{DB} \mid \text{exists}(h) \land \text{status}(h) \neq \text{'corrupt'}\}$.
  - Missing Assets: $M = R \setminus C = \{h \in R \mid h \notin C\}$.
  - Hit Assets: $H = R \cap C = \{h \in R \mid h \in C\}$.
- **Complexity**: $O(|R|)$ average time using Python hash sets.
- **Resolution Result Dataclass**:
  ```python
  @dataclass(slots=True)
  class ResolutionResult:
      hits: set[str]
      misses: set[str]
      total_requested_bytes: int
      hit_bytes: int
      miss_bytes: int
      byte_hit_ratio: float
      network_saved_bytes: int
  ```

### R4. LRU Eviction Policy, Interfaces & Chunking
- **LRU Eviction Engine**:
  - Parameter: `max_cache_size_bytes: int`.
  - Condition: Triggered when `current_total_bytes + new_asset_bytes > max_cache_size_bytes`.
  - Algorithm: Query SQLite for entries ordered by `last_accessed ASC, access_count ASC`. For each entry:
    1. Unlink physical file at `objects/<prefix2>/<hash[2:]>`.
    2. Remove SQLite record.
    3. Decrement `current_total_bytes` by `entry.size_bytes`.
    4. Terminate loop when `current_total_bytes + new_asset_bytes <= max_cache_size_bytes`.
- **Chunked Transfer Protocol**:
  - `CHUNK_SIZE = 1024 * 1024` (1 MiB default, configurable).
  - Streams read/write in fixed-size buffers, bounding peak memory consumption to $O(\text{chunk\_size})$ regardless of whether asset is 10 KB or 100 GB.
- **CacheStore Public Interface**:
  ```python
  class CacheStore(ABC):
      @abstractmethod
      def contains(self, sha256: str) -> bool: ...
      @abstractmethod
      def get_path(self, sha256: str) -> Optional[Path]: ...
      @abstractmethod
      def get_stream(self, sha256: str) -> Iterator[bytes]: ...
      @abstractmethod
      def put_bytes(self, data: bytes, sha256: Optional[str] = None, metadata: Optional[dict] = None) -> CacheEntry: ...
      @abstractmethod
      def put_file(self, file_path: Path, sha256: Optional[str] = None, metadata: Optional[dict] = None) -> CacheEntry: ...
      @abstractmethod
      def put_stream(self, stream: BinaryIO, sha256: str, size_bytes: int, metadata: Optional[dict] = None) -> CacheEntry: ...
      @abstractmethod
      def verify(self, sha256: str, deep_check: bool = True) -> bool: ...
      @abstractmethod
      def verify_all() -> VerificationReport: ...
      @abstractmethod
      def remove(self, sha256: str) -> bool: ...
      @abstractmethod
      def evict_to_fit(self, required_bytes: int) -> int: ...
      @abstractmethod
      def get_stats() -> CacheStats: ...
  ```

### R5. Subsystem Independence & Decoupling
- Strict isolation of `src/aidars/cache/`:
  - Zero imports of `bpy`, `bmesh`, `mathutils`.
  - Zero imports of `aidars.visibility`, `aidars.scene_intelligence`, `aidars.scheduler`.
  - Interfaces with M4 via standard DTOs or duck-typed `PackagePlan` objects where assets expose `.sha256` and `.size_bytes`.

---

## 4. Metrics Definitions & Mathematical Formulas

1. **Byte Hit Ratio ($BHR$)**:
   $$BHR = \begin{cases} 1.0 & \text{if } \text{Total Requested Bytes} = 0 \text{ and } |R| = 0 \\ \frac{\sum_{h \in H} \text{size}(h)}{\sum_{r \in R} \text{size}(r)} & \text{otherwise} \end{cases}$$
   - Measures the fraction of requested payload volume fulfilled from local cache. Range: $[0.0, 1.0]$.

2. **Network Saved ($NS$)**:
   $$NS = \sum_{h \in H} \text{size}(h)$$
   - Absolute volume of bytes preserved, avoiding network transfer.

3. **Asset Hit Ratio ($AHR$)**:
   $$AHR = \frac{|H|}{|R|} = \frac{\text{Count of Cached Hit Assets}}{\text{Count of Total Requested Assets}}$$

4. **Cache Space Utilization ($CSU$)**:
   $$CSU = \left( \frac{\text{Total Stored Valid Bytes}}{\text{Max Quota Bytes}} \right) \times 100\%$$

---

## 5. Corruption Scenarios Catalog

| ID | Scenario | Fault Injection Method | Detection Mechanism | Recovery & System Action |
|---|----------|------------------------|---------------------|--------------------------|
| C1 | Payload Bit Flip | Modify random byte at offset $k$ in `objects/aa/bb...` | `verify()` recomputes SHA-256 chunk-by-chunk and detects mismatch against SQLite record. | Mark SQLite record `verification_status = 'corrupt'`; delete corrupt file; return `False` on verify; trigger miss on subsequent queries. |
| C2 | Truncated Object | Truncate file on disk to half its length | `os.path.getsize(f) != record.size_bytes` detected immediately or during hash verification. | Mark `status = 'corrupt'`; unlink truncated file; delete or update DB record. |
| C3 | Dangling DB Record | Manually `os.remove()` object file on disk while keeping DB row | `Path.exists()` check in `get()` or `verify()` fails. | Clean up dangling SQLite row; return `None` on `get()`; report as missing. |
| C4 | Orphan Disk Object | Create file in `objects/` without creating SQLite row | Reconciliation audit scans disk objects and queries DB. | Hash file from disk; if valid, insert into DB; if corrupt or orphaned beyond retention, unlink. |
| C5 | Database Lock Contention | Spawn 20 concurrent writer threads | SQLite connection with WAL mode and 5000ms busy timeout. | Transactions serialize cleanly via WAL; zero locked errors. |
| C6 | Concurrent Ingestion Race | Two threads ingest identical asset hash simultaneously | Both write to unique `tmp/<uuid>.tmp`, then perform atomic `os.replace` + SQLite `INSERT OR REPLACE`. | Thread-safe & process-safe; single canonical file preserved; consistent metadata. |
| C7 | Mid-Stream Ingest Interruption | Stream throws exception or cuts off before completion | Exception handler catches error before atomic rename. | Unlink `tmp/<uuid>.tmp`; SQLite untouched; no partial artifacts in `objects/`. |
| C8 | Ingest Hash Mismatch | Ingest data claiming hash $H_1$, but content hashes to $H_2$ | Incremental SHA-256 calculation compares final digest with expected hash. | Discard temp file; raise `HashMismatchError`; index not updated. |
| C9 | Read-Only Directory | Set `objects/` permissions to read-only (0o444) | `put()` catches `PermissionError` / `OSError`. | Raise `CacheStorageError`; read operations continue cleanly without crashing. |
| C10 | Disk Full (ENOSPC) | Simulate `OSError: No space left on device` during chunk write | Catch `OSError` during chunk write to `tmp/`. | Clean up temp file; trigger aggressive LRU eviction if quota-driven or raise `CacheDiskFullError`. |

---

## 6. Complete 4-Tier Test Matrix

### Tier 1: Feature Coverage (36 Tests: 6 tests $\times$ 6 features)
- **Feature 1: Content-Addressed Storage (CAS)**
  - `test_cas_put_and_get_path`: Stores asset bytes, verifies path `<root>/objects/<h[:2]>/<h[2:]>`.
  - `test_cas_content_deduplication`: Stores identical data with 2 different names, asserts single file on disk.
  - `test_cas_atomic_write_durability`: Verifies temp file write + atomic replace mechanics.
  - `test_cas_case_insensitive_hash_normalization`: Verifies uppercase/lowercase hex normalization.
  - `test_cas_remove_cleans_split_directory`: Verifies `remove()` deletes object and empty prefix dir.
  - `test_cas_object_retrieval_stream_and_bytes`: Verifies `get_bytes()` and `get_stream()` return exact bytes.
- **Feature 2: SQLite Metadata Index**
  - `test_index_record_insertion_and_lookup`: Inserts entry, validates all schema columns match.
  - `test_index_last_accessed_and_touch_update`: Validates `touch()` updates timestamp & increments `access_count`.
  - `test_index_wal_mode_and_concurrency`: Validates `PRAGMA journal_mode=WAL` and concurrent transactions.
  - `test_index_aggregate_size_query`: Validates `get_total_bytes()` accurately sums active entries.
  - `test_index_status_transitions`: Validates `unverified` -> `verified` -> `corrupt` lifecycle.
  - `test_index_query_by_type_and_status`: Validates queries filtered by `asset_type` and `status`.
- **Feature 3: Hit/Miss Resolver & Set Difference**
  - `test_resolver_all_hits`: All requested hashes cached -> 0 misses, $BHR = 1.0$.
  - `test_resolver_all_misses`: No requested hashes cached -> all misses, $BHR = 0.0$.
  - `test_resolver_partial_hits_and_misses`: 50% hit/miss split -> exact set difference in $O(A)$ time.
  - `test_resolver_package_plan_integration`: Resolves M4 `PackagePlan` schema dict/model.
  - `test_resolver_empty_request_handling`: Empty requested set -> 0 hits, 0 misses, $BHR = 1.0$.
  - `test_resolver_duplicate_hashes_in_request`: Deduplicates requested hashes prior to set difference.
- **Feature 4: LRU Eviction Engine**
  - `test_lru_eviction_strictly_orders_by_last_accessed`: Oldest accessed asset evicted first.
  - `test_lru_touch_prevents_eviction`: Touching old asset moves it to MRU, evicting second-oldest.
  - `test_lru_quota_exact_fit_no_eviction`: Cache filled to 100% does not evict until exceeded.
  - `test_lru_multi_asset_cascade_eviction`: Large asset triggers eviction of multiple smaller assets.
  - `test_lru_eviction_cleans_both_disk_and_index`: Unlinks disk file and removes SQLite row.
  - `test_lru_eviction_with_oversized_asset`: Asset larger than total quota raises `CacheQuotaExceededError`.
- **Feature 5: Chunked Streaming & Bounded Memory**
  - `test_chunked_streaming_memory_bound`: Ingests 20 MB asset with 64 KB chunks, verifying constant memory.
  - `test_chunked_streaming_incremental_sha256`: Stream interrupted -> temp file cleaned, error raised.
  - `test_chunked_streaming_exact_byte_boundary`: Stream asset with size exact multiple of chunk size.
  - `test_chunked_read_generator`: Verifies generator yields exact byte stream.
  - `test_chunked_transfer_cancellation_cleanup`: Verifies aborted stream cleans up temporary files.
  - `test_chunked_streaming_huge_payload`: Validates streaming 100+ MB payload through cache store.
- **Feature 6: Cache Verification & Integrity**
  - `test_verify_valid_asset`: Valid file returns `True` and status updated to `verified`.
  - `test_verify_detects_corrupted_payload`: Modified file returns `False` and status marked `corrupt`.
  - `test_verify_detects_truncated_file`: Truncated file returns `False`.
  - `test_verify_detects_dangling_missing_file`: Missing file detected and handled.
  - `test_verify_all_batch_scan`: Batch scan returns typed report with valid, corrupt, and missing lists.
  - `test_verify_auto_evicts_corrupted_entries`: Auto-heal unlinks corrupt file and cleans DB.

### Tier 2: Boundary & Corner Cases (10 Tests)
- `test_boundary_empty_zero_byte_asset`: Ingestion and resolution of 0-byte file (SHA-256 `e3b0c44...`).
- `test_boundary_huge_multi_gigabyte_asset`: Simulated multi-GB stream chunking without OOM.
- `test_boundary_cache_quota_zero`: Quota = 0 bytes, any `put()` triggers immediate eviction or quota error.
- `test_boundary_quota_exact_size_match`: Ingest asset of size $S$ into cache with quota $S$, verify exact 100% capacity fit.
- `test_boundary_empty_hash_set_resolution`: Set difference with $\emptyset$ required assets returns 0 hits, 0 misses, $0$ bytes.
- `test_boundary_invalid_hash_strings`: Hashes with invalid lengths (32, 65), invalid characters (`g-z`), SQL injection payloads.
- `test_boundary_read_only_filesystem`: Read-only cache directory gracefully handles `put()` failure with `CacheStorageError`.
- `test_boundary_deep_nested_special_character_names`: Unicode, spaces, and Windows reserved filenames in metadata index.
- `test_boundary_extreme_clock_skew`: Negative or future `last_accessed` timestamps in LRU sorting.
- `test_boundary_simultaneous_empty_and_huge_assets`: Ingestion of mixed 0-byte and large assets under tight quota.

### Tier 3: Cross-Feature Combinations (8 Tests)
- `test_combo_put_evict_resolve`: Put asset A -> fill quota with B, C causing A eviction -> resolve $\{A, B, C\}$ -> correctly identifies $A$ as miss, $B, C$ as hits.
- `test_combo_put_corrupt_verify_evict`: Put asset -> alter file bytes -> run `verify()` -> verify marked corrupt -> run `evict()` -> corrupt asset purged -> resolve shows miss.
- `test_combo_concurrent_ingest_and_eviction`: 10 parallel threads ingesting assets while background cleaner enforces tight quota -> no race condition, total cache size stays strictly $\le \text{quota}$.
- `test_combo_streaming_ingest_during_verification`: Verify one asset while streaming another -> SQLite WAL allows concurrent operations without blocking.
- `test_combo_dedup_ingest_then_single_evict`: Ingest asset from path 1, then ingest same bytes from path 2 (dedup) -> single LRU record tracked -> evict removes the single physical file cleanly.
- `test_combo_resolve_plan_cache_misses_re_resolve_plan`: Resolve M4 plan -> 3 misses -> ingest the 3 missing assets -> re-resolve plan -> 0 misses, 100% hit ratio.
- `test_combo_eviction_under_high_concurrency`: Concurrent reads, writes, and evictions across multiple threads.
- `test_combo_rebuild_index_from_storage_scan`: Rebuilds corrupted SQLite database from on-disk `objects/` directory.

### Tier 4: Real-World Workload Scenarios (6 Tests)
- `test_workload_m4_scene_distribution_cold_start`: First worker node cold cache -> receives M4 PackagePlan (10 assets, 150 MB) -> 100% misses -> downloads all 10 -> caches -> `byte_hit_ratio = 0.0`.
- `test_workload_m4_scene_distribution_warm_start_incremental_frames`: Second render job shares 8 assets and adds 2 new assets -> resolves 8 hits (140 MB), 2 misses (10 MB) -> `byte_hit_ratio = 93.3%`, `network_saved = 140 MB`.
- `test_workload_multi_worker_cache_inventory_exchange`: Master queries 5 workers for cached hashes -> determines optimal dispatch target based on highest cache locality.
- `test_workload_high_throughput_texture_stream_churn`: Stream 1000 textures through a 500 MB cache -> continuous LRU turnover -> validates zero leaks, correct metrics accumulation.
- `test_workload_simulated_network_interruption_recovery`: Ingest 5 assets, 3rd asset drops mid-stream -> retry completes -> cache state completely consistent.
- `test_workload_multi_camera_scene_package_deduplication`: Packages from 3 different camera views share 85% geometry/materials -> validates cross-package storage dedup and hit ratios.

---

## 7. Decoupling & Independence Specification

- **Blender `bpy` Isolation**:
  - `src/aidars/cache/` contains ZERO imports of `bpy`, `bmesh`, `mathutils`, `gpu`.
  - Enforced via AST unit test `test_cache_decoupling_no_bpy` which parses all `.py` files in `src/aidars/cache/`.
- **Subsystem Boundary Enforcement**:
  - `src/aidars/cache/` does NOT import `aidars.visibility`, `aidars.scene_intelligence`, or `aidars.scheduler`.
  - M5 is purely an Orchestration / Execution layer cache engine operating on raw content hashes, file streams, and metadata.

---

## 8. Handoff Report (5-Component Protocol)

### 1. Observation
- Inspected `ORIGINAL_REQUEST.md` (lines 1-44), `PROJECT.md` (lines 1-99), `AIDAR_AGENT_SKILL.md` (lines 1-184), and `src/aidars/smart_package/models.py`.
- Verified that Milestone 5 requires a standalone, local content-addressed asset cache using SHA-256, SQLite index (`index.db`), split-hash directory structure (`objects/<h[:2]>/<h[2:]>`), $O(A)$ set-difference resolver, LRU eviction under quota, chunked streaming, and corruption self-healing.
- Observed that M4 packaging produces `PackagePlan` with `AssetRecord` objects containing `sha256` and `size_bytes`.

### 2. Logic Chain
1. Asset identity must be determined by SHA-256 content hash (R1), eliminating duplicate transfers of identical files.
2. Storing objects in `objects/<h[:2]>/<h[2:]>` guarantees filesystem scalability by partitioning files into 256 buckets.
3. Tracking metadata in a WAL-enabled SQLite index (R2) allows fast LRU queries (`ORDER BY last_accessed ASC`) and concurrent reader/writer access.
4. Python `set` difference provides $O(A)$ average-time resolution (R3) between requested package plans and cached inventory.
5. Chunked streaming (R4) ensures bounded memory consumption ($O(\text{chunk\_size})$) during multi-gigabyte asset ingest.
6. Decoupling from `bpy` (R5) ensures M5 can run standalone on Master or lightweight worker nodes without Blender binaries.

### 3. Caveats
- Windows file locking: On Windows, open file handles prevent deletion or rename. Cache implementations must ensure all file handles/streams are closed in `finally` blocks before unlinking/renaming.
- SQLite busy timeouts: High concurrency requires setting `PRAGMA busy_timeout = 5000;` and WAL mode.

### 4. Conclusion
The specification and 4-tier test matrix for Milestone 5 Core are fully defined, mathematically formulated, and ready for architectural design and test-driven implementation.

### 5. Verification Method
- Execute pytest test suites:
  ```powershell
  pytest tests/test_m5_cache.py tests/test_m5_cache_adversarial.py -v
  ```
- Run AST decoupling test:
  ```powershell
  pytest tests/test_m5_cache.py -k "test_cache_decoupling_no_bpy"
  ```
