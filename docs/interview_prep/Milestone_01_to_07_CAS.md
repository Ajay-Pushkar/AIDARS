# AIDAR INTERVIEW PREPARATION
## Milestones 01 to 07: Content-Addressed Storage (CAS) Core

### 1. What was actually introduced or changed
Milestones 1 through 7 established the foundational data layer of AIDAR: a robust, high-performance, local Content-Addressed Storage (CAS) subsystem. It introduced:
- A filesystem-based binary blob store using a 2-level split-hash directory layout.
- Atomic file ingestion using a `tmp/` staging directory and `os.replace`.
- A SQLite metadata index (`index.db`) running in Write-Ahead Logging (WAL) mode for tracking asset metadata and access statistics.
- An O(A) set-difference hit/miss resolver for fast synchronization.
- An LRU (Least Recently Used) eviction engine to enforce disk quota limits.
- An integrity verifier that detects data corruption via deep SHA-256 streaming and performs self-healing (auto-eviction).
- A unified abstract `CacheStore` facade to isolate the application from the storage implementation.

### 2. Architecture and Implementation Decisions
- **Decoupling from Application**: The CAS was designed to have zero dependencies on application internals (e.g., Blender scene graphs). It operates purely on bytes and SHA-256 hashes.
- **Split-Hash Directory Layout**: Files are stored as `objects/<h[:2]>/<h[2:]>` to prevent filesystem directory bloat and performance degradation associated with storing thousands of files in a single flat directory.
- **Atomic Ingestion**: Files are written to a temporary location (`tmp/`) and then atomically moved to their final destination using `os.replace`. This guarantees that partial, corrupted, or incomplete files are never visible to the application.
- **SQLite WAL Mode**: Used for the metadata index to allow concurrent readers without blocking writers, crucial for a high-throughput multi-worker environment.

### 3. Algorithms and Data Structures
- **Cryptographic Hashing**: SHA-256 is used as the universal, deterministic identifier for all data, guaranteeing content immutability and automatic deduplication.
- **O(A) Set Difference**: The hit/miss resolver computes `missing = required - cached` using Python's native Hash Sets, yielding O(1) lookups per item and O(A) total time (where A is the number of required assets).
- **Least Recently Used (LRU)**: The eviction engine sorts records by `last_accessed_at ASC` to identify the oldest unused assets when the disk quota is exceeded.

### 4. Important APIs, Classes, Modules, Interfaces, and Execution Flows
- **Interfaces**: `CacheStore` (Abstract Base Class).
- **Concrete Classes**: `DiskCacheStore` (Facade), `SplitHashStorage` (Disk I/O), `SQLiteMetaIndex` (Metadata), `HitMissResolver` (Sync logic), `LRUEvictor` (Quota), `IntegrityVerifier` (Corruption checks).
- **Data Models**: `CacheEntry`, `ResolutionResult`, `VerificationReport`.
- **Execution Flow (Ingestion)**: Client -> `put_bytes()` -> `SplitHashStorage` (writes chunked to `tmp/` -> `os.replace` to `objects/`) -> `SQLiteMetaIndex` (inserts metadata) -> Returns `CacheEntry`.

### 5. Tests and Validation Performed
- **Tiers 1-4 Functional Tests**: `test_cache_store.py` covered all basic CRUD operations, metadata tracking, and quota enforcement.
- **Adversarial Hardening**: `test_cache_adversarial.py` subjected the system to deliberate boundary violations, file corruptions, concurrent access contention, and zero-byte file ingestions to prove structural resilience and self-healing.

### 6. Known Failures, Limitations, and Unfinished Work
- **Windows File Locking (WinError 32)**: Because AIDAR is validated on Windows, concurrent deletion (eviction) while another process is reading a file can throw `WinError 32`. The eviction engine must defensively catch OS errors and defer deletion, relying on the SQLite WAL to maintain consistency.

### 7. Design Alternatives Considered
- **Why not Redis or Memcached?** Rejected to fulfill the strict "zero external infrastructure dependencies" requirement. AIDAR must run on bare-metal workstations without requiring users to configure sidecar databases.
- **Why not a Flat Directory?** Rejected because NTFS/ext4 degrade severely when maintaining tens of thousands of directory entries in a single folder.
- **Why not store blobs directly in SQLite?** Rejected because SQLite is not optimized for streaming large multi-gigabyte binary assets (like video or 3D models), which are better served directly by the OS filesystem buffer cache.

---

### INTERVIEW QUESTIONS

#### A. BASIC UNDERSTANDING
- **Level 1**: What is Content-Addressed Storage (CAS) and how does AIDAR use it?
- **Level 1**: What components were introduced in Milestones 1-7?
- **Level 2**: How does AIDAR guarantee that an asset is never partially written?

#### B. WHY QUESTIONS
- **Level 2**: Why does AIDAR use a split-hash directory structure (`objects/aa/bbbb...`) instead of a flat folder?
- **Level 3**: Why is SQLite configured in WAL (Write-Ahead Logging) mode for the cache index?
- **Level 3**: Why is the CAS subsystem strictly isolated from the application execution layer?

#### C. WHY-NOT QUESTIONS
- **Level 3**: Why not use Redis to track cache metadata and blobs?
- **Level 4**: Why not store the binary blobs directly inside the SQLite database?

#### D. HOW QUESTIONS
- **Level 2**: How does the LRU eviction engine decide what to delete?
- **Level 3**: How does data flow through the system when a new asset is ingested?
- **Level 4**: How does the IntegrityVerifier handle a file that has been maliciously modified on disk?

#### E. ALGORITHM / DATA STRUCTURE
- **Level 3**: What algorithm is used to determine which assets a worker is missing before execution? What is its time complexity?
- **Level 4**: Why were Python sets chosen for the hit/miss resolver? What happens to space complexity when a workload requires 1,000,000 assets?

#### F. CODE-LEVEL QUESTIONS
- **Level 3**: Where is the filesystem I/O implemented? (Answer: `SplitHashStorage`).
- **Level 3**: What calls `HitMissResolver`? (Answer: The coordinator/worker during the workload admission phase to calculate `missing_assets_on_worker`).
- **Level 4**: What happens immediately before and after `os.replace` in the ingestion flow?

#### G. FAILURE / RECOVERY
- **Level 3**: What happens if the power fails while writing a 1GB asset to disk?
- **Level 4**: What happens if the disk quota is reached, but the files targeted for LRU eviction are currently locked/open by a running workload on Windows?
- **Level 5**: What happens if the SQLite index gets out of sync with the physical filesystem?

#### H. PERFORMANCE
- **Level 4**: What was the primary bottleneck considered when designing the ingestion API? (Answer: Memory bloat; solved via 64 KiB chunked streaming).
- **Level 5**: How does the SQLite `busy_timeout` impact high-concurrency ingestion?

#### I. DISTRIBUTED SYSTEMS
- **Level 3**: How does the deterministic nature of SHA-256 aid in distributed worker coordination?
- **Level 4**: How does the CAS design prevent duplicate work when two workers request the same file simultaneously?

#### J. SECURITY
- **Level 3**: How are inputs validated when querying the cache? (Answer: Strict regex `^[0-9a-f]{64}$` for SHA-256).
- **Level 4**: What attack surface exists if an adversarial worker submits a manipulated payload claiming a popular SHA-256 hash?

#### K. TESTING
- **Level 2**: What failure scenarios were deliberately tested in `test_cache_adversarial.py`?
- **Level 3**: Which tests prove the atomic ingestion works?

#### L. TRADE-OFFS
- **Level 4**: What did the split-hash design optimize for, and what did it sacrifice? (Optimized for OS inode lookup limits; sacrificed direct human browsability of the cache folder).
- **Level 5**: What would need to change in this CAS implementation to support 10,000 concurrent workers at petabyte scale?

#### M. SYSTEM DESIGN FOLLOW-UPS
- **Question**: Why does AIDAR use content-addressable storage?
- **Follow-up 1**: How does the hash help deduplication?
- **Follow-up 2**: What happens if two distinct files produce the same SHA-256 hash? (Collision).
- **Follow-up 3**: How would you scale the SQLite metadata index if the disk IOPS bottlenecked?

#### N. REAL-WORLD QUESTIONS
- **Level 3**: What unexpected failures occurred when testing file deletions on Windows during physical validation?

#### O. RESUME DEFENSE
- **Claim you can make**: "Designed and implemented a high-performance, atomic, zero-dependency Content-Addressed Storage engine in Python."
- **Evidence**: `src/aidars/cache/`, chunked streaming, atomic `os.replace`, O(A) set difference, LRU SQLite integration.
- **Do NOT claim**: "Built a distributed Redis-like cache cluster." (It is a local embedded cache).

#### P. PROJECT / HR QUESTIONS
- **Level 1**: Why did you build the CAS layer first before any execution logic?
- **Level 2**: What was the hardest part of ensuring cache integrity?
