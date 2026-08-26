# TEST_READY — Milestone 5 Core Test Suite

## Overview
The end-to-end and adversarial test suite for AIDAR Milestone 5 Core (Local Content-Addressed Asset Cache) has been successfully created and compiled.

## Test Inventory & Coverage Summary

### 1. `tests/test_cache_store.py` (60 Tests)
- **Tier 1: Feature Coverage (36 tests)**
  - Feature 1: Content-Addressed Storage & Split-Hash Mechanics (6 tests)
  - Feature 2: SQLite Metadata Index & WAL Mode (6 tests)
  - Feature 3: O(A) Set-Difference Resolver & Metrics (6 tests)
  - Feature 4: LRU Quota Eviction Engine (6 tests)
  - Feature 5: Chunked Transfer & Bounded Memory (6 tests)
  - Feature 6: Integrity Verification & Self-Healing (6 tests)
- **Tier 2: Boundary & Corner Cases (10 tests)**
  - 0-byte asset ingestion, exact fit quota, empty requests, invalid SHA-256 formats, hash mismatch rejection, special character naming, 64-bit large size metadata, request deduplication.
- **Tier 3: Cross-Feature Combinations (8 tests)**
  - Put-Evict-Resolve lifecycle, Put-Corrupt-Verify-Evict-Resolve pipeline, concurrent ingest and eviction under tight quota, deduplication across aliases and eviction, mixed PackagePlan resolution, touch reordering under eviction pressure, re-ingest after eviction, full cache reconstruction from existing disk state.
- **Tier 4: Real-World Workload Scenarios (6 tests)**
  - Scenario 1: Cold start M4 scene distribution (50 assets, 100% miss -> 100% transfer).
  - Scenario 2: Warm start incremental frames distribution (50 assets -> 45 hits, 5 misses, 90% BHR).
  - Scenario 3: Multi-worker inventory locality optimization (disjoint cache resolution).
  - Scenario 4: High-throughput texture stream churn under quota constraint.
  - Scenario 5: Network drop & interrupted stream recovery without orphan objects.
  - Scenario 6: Multi-camera asset deduplication & combined network savings calculation.

### 2. `tests/test_cache_adversarial.py` (19 Tests)
- **Simulated Corruption Suite (C1 - C10)**
  - C1: Bit flip corruption detection and auto-eviction.
  - C2: File truncation detection.
  - C3: File expansion & zero-padding detection.
  - C4: Dangling SQLite record with missing disk file.
  - C5: Orphan disk file without SQLite index record.
  - C6: Zero-byte file corruption detection.
  - C7: Stale staging files in `tmp/` isolation.
  - C8: Permission error / file locking resilience during LRU eviction.
  - C9: Path traversal attack rejection in SHA-256 inputs.
  - C10: SQL injection mitigation via parameterized SQLite queries.
- **Bounded Memory Streaming Verification**
  - Multi-megabyte payload ingestion and stream extraction verifying bounded peak heap RAM ($O(1)$ memory delta using tracemalloc).
- **Concurrency & WAL Mode Stress**
  - Multi-threaded concurrent readers, writers, and touch operations.
  - Concurrent ingestion against background LRU eviction worker.
- **AST Decoupling Verification**
  - Strict AST static analysis proving zero imports of `bpy`, `bmesh`, `mathutils`, or `aidars.visibility` across all files in `src/aidars/cache/`.
  - Clean facade exports from `src/aidars/cache/__init__.py`.
- **Backward Compatibility**
  - Full regression coverage for `SceneCache` and `SceneEngine`.

## Total Test Count
- **79 Tests** across `tests/test_cache_store.py` and `tests/test_cache_adversarial.py`.

## Execution Commands
```bash
# Run complete M5 Core test suite
python -m pytest tests/test_cache_store.py tests/test_cache_adversarial.py -v

# Run with warnings treated as errors
python -m pytest tests/test_cache_store.py tests/test_cache_adversarial.py -v -W error
```
