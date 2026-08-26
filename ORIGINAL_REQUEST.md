# Original User Request

## Initial Request — 2026-08-23T12:50:55Z

You are the Project Orchestrator for the AIDAR project.

Your working directory is:
C:\AIDAR\.agents\teamwork_preview_orchestrator_1

Project root workspace:
C:\AIDAR

Original Request reference:
C:\AIDAR\ORIGINAL_REQUEST.md

Task Summary:
Implement Milestone 5 Core (Local Content-Addressed Asset Cache) for the AIDAR project. The cache must use SHA-256 for identity, feature a SQLite-backed index, implement strict O(1) set-difference for hit/miss resolution, and include an LRU eviction policy. Ensure this subsystem is cleanly isolated from the M4 packaging logic. Use a multi-agent team to explore, design, implement, test, and challenge the solution.

Integrity mode: development

Requirements:
### R1. Content-Addressed Storage & Hashing
Implement a local filesystem-backed cache store where asset identity is strictly defined by its SHA-256 hash, not its filename or path. Store objects using a split-hash directory structure (e.g., `objects/a9/1f3e...`) to prevent directory bloat.

### R2. Cache Metadata Index
Implement a SQLite-backed index (`cache/metadata/index.db`) to track cache entries. It must store the authoritative hash, size, type, original name, creation time, last accessed time, and verification status.

### R3. Hit/Miss Resolver & Set Difference
Given a requested set of asset hashes (from an M4 PackagePlan), the cache must compute the set difference (`missing = required - cached`) in O(A) average time using standard Sets to definitively identify which assets need to be transferred.

### R4. Integrity, Eviction, and Interfaces
Implement an LRU eviction policy based on `last_accessed` tracking to enforce a maximum cache quota. Define a clean `CacheStore` interface (`contains`, `get`, `put`, `verify`, `remove`) that abstracts the local implementation. Implement chunked transfer logic to bound memory usage when caching large assets.

### R5. Subsystem Independence
The M5 cache logic (in `src/aidars/cache/`) must be entirely decoupled from M4. M5 consumes the asset hashes and sizes provided by M4, but must not depend on Blender objects, materials, cameras, or visibility logic.

Acceptance Criteria:
- A comprehensive `pytest` suite is built from scratch that rigorously tests cache hit/miss set differences, LRU eviction limits, and split-hash storage mechanics.
- The test suite calculates and reports `byte_hit_ratio` and `network_saved` metrics for simulated transfer requests.
- Tests simulate cache corruption (e.g., modifying cached files on disk) and verify that corrupted entries are detected and evicted.
- The cache code is isolated in `src/aidars/cache/` and does not import or depend on Blender-specific graph modules.

Please maintain your plan.md, progress.md, and context.md in your working directory C:\AIDAR\.agents\teamwork_preview_orchestrator_1. Notify me when victory is ready to be audited.
