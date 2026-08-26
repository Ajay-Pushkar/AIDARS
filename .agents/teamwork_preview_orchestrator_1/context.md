# Context & Technical Constraints — AIDAR Milestone 5 Core

## Project Context
AIDAR (Asset Ingestion, Distribution, and Asset Resolution) requires a robust, local content-addressed asset caching layer (Milestone 5 Core).

## Core Requirements & Invariants
1. **Content-Addressed Storage & Split-Hash**:
   - Identity is purely SHA-256 (64 hex characters lowercase).
   - Storage uses split-hash directory structure: `objects/{hash[:2]}/{hash[2:]}` (or similar 2-char prefix structure) to avoid directory bloat.
   - Asset names/paths do not define identity.
2. **Cache Metadata Index (SQLite)**:
   - File location: `cache/metadata/index.db`.
   - Fields: authoritative hash, size (bytes), type/mime, original filename, creation timestamp, last accessed timestamp, verification status (VALID, CORRUPT, PENDING, etc.).
   - Atomic / thread-safe transaction handling.
3. **Hit/Miss Resolver & Set Difference**:
   - Input: PackagePlan or list/set of required asset hashes (and metadata).
   - Operation: Computes set difference `missing = required - cached` in O(A) average time using standard Python `set`.
   - Returns missing assets, cached assets, and transfer plan metrics.
4. **LRU Eviction & Quota Management**:
   - Cache enforces max total bytes quota.
   - When quota exceeded on `put`, evicts least-recently-accessed items (`last_accessed` timestamp) until within budget.
   - Both metadata in SQLite and files on disk must be cleanly removed on eviction.
5. **Integrity & Chunked I/O**:
   - `verify(hash)` checks file existence and computes streaming SHA-256 against index.
   - Corrupted entries detected are marked / evicted cleanly.
   - Chunked transfer logic for read/write to bound memory consumption for large assets.
6. **Subsystem Isolation**:
   - Pure Python / standard library + pytest (no Blender `bpy`, Blender graphs, material/camera/mesh bindings in `src/aidars/cache/`).
   - Clean interface: `CacheStore` with methods `contains(hash) -> bool`, `get(hash) -> Path/Stream`, `put(stream/path, original_name, ...) -> CacheEntry`, `verify(hash) -> bool`, `remove(hash) -> bool`, `resolve(required_hashes) -> ResolutionResult`, `evict_lru(target_bytes) -> List[str]`.

## Test Requirements & Metrics
- Test suite with pytest.
- Metrics calculation: `byte_hit_ratio = cached_bytes / total_requested_bytes`, `network_saved = cached_bytes`.
- Simulated disk corruption tests.
- Zero Blender dependencies in cache module.
