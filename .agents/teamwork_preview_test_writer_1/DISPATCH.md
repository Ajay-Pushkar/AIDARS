# Task Assignment: E2E Test Suite Creation

**Working Directory**: C:\AIDAR\.agents\teamwork_preview_test_writer_1
**Original Request**: C:\AIDAR\ORIGINAL_REQUEST.md
**Project Plan**: C:\AIDAR\PROJECT.md
**Test Infra Spec**: C:\AIDAR\TEST_INFRA.md
**Project Root**: C:\AIDAR

## Write Ownership
You exclusively own:
- `tests/test_cache_store.py`
- `tests/test_cache_adversarial.py`
- `C:\AIDAR\TEST_READY.md`

Do NOT modify any files inside `src/aidars/` or any other test files.

## Mission
Author the complete, rigorous pytest suite for Milestone 5 Core covering:
1. `tests/test_cache_store.py`:
   - Tier 1: Feature Coverage (Storage CAS, SQLite Index, Resolver O(A) set difference, LRU Eviction, Chunked Streaming, Integrity Verification).
   - Tier 2: Boundary & Corner cases (0-byte file, huge files, quota=0, exact fit, special chars, empty sets).
   - Tier 3: Cross-Feature Combinations (Put->Evict->Resolve, Put->Corrupt->Verify->Evict, concurrent ingest+eviction, dedup+evict, plan resolution).
   - Tier 4: Real-World Workload Scenarios (M4 scene distribution cold/warm starts, byte_hit_ratio & network_saved validation, texture stream churn, multi-camera dedup).
2. `tests/test_cache_adversarial.py`:
   - Simulated corruption suite: Bit flips, file truncations, dangling DB records, orphan disk files.
   - Bounded memory streaming verification.
   - Concurrency & WAL mode stress testing.
   - AST decoupling check: Verify that `src/aidars/cache/` contains ZERO imports of `bpy`, `bmesh`, `mathutils`, or `aidars.visibility`.
3. When the test suite files are written and verified, create `C:\AIDAR\TEST_READY.md` summarizing the test count and execution commands.

Write your handoff report to `C:\AIDAR\.agents\teamwork_preview_test_writer_1\handoff.md` and report back when finished.
