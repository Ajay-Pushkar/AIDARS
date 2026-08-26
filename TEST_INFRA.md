# E2E Test Infra: AIDAR Milestone 5 Core

## Test Philosophy
- Opaque-box, requirement-driven. Derives test cases directly from `ORIGINAL_REQUEST.md`.
- Zero coupling to Blender runtime (`bpy`).
- Methodology: Category-Partition + Boundary Value Analysis (BVA) + Pairwise Combinatorial + Real-World M4 Workloads.

## Feature Inventory
| # | Feature | Source (Requirement) | Tier 1 | Tier 2 | Tier 3 | Tier 4 |
|---|---------|----------------------|:------:|:------:|:------:|:------:|
| 1 | Content-Addressed Storage & Split-Hash | ORIGINAL_REQUEST R1 | 6 | 2 | 2 | 1 |
| 2 | SQLite Metadata Index & WAL | ORIGINAL_REQUEST R2 | 6 | 2 | 2 | 1 |
| 3 | O(A) Set-Difference Resolver & Metrics | ORIGINAL_REQUEST R3 | 6 | 2 | 1 | 2 |
| 4 | LRU Eviction & Quota Enforcer | ORIGINAL_REQUEST R4 | 6 | 2 | 2 | 1 |
| 5 | Chunked Streaming & Bounded RAM | ORIGINAL_REQUEST R4 | 6 | 1 | 1 | 1 |
| 6 | Integrity Verification & Self-Healing | ORIGINAL_REQUEST R4 | 6 | 1 | 2 | 1 |

## Test Architecture
- **Test Runners**: `pytest tests/test_cache_store.py tests/test_cache_adversarial.py -v`
- **Pass/Fail Semantics**: All tests must exit with code 0. Zero warnings treated as fatal if configured with `-W error`.
- **Directory Layout**:
  - `tests/test_cache_store.py`: Tiers 1-4 functional and integration tests.
  - `tests/test_cache_adversarial.py`: Corruption simulation (C1-C10), concurrency, memory bounding, and AST decoupling tests.

## Real-World Application Scenarios (Tier 4)
| # | Scenario | Features Exercised | Complexity |
|---|----------|--------------------|------------|
| 1 | Cold Start M4 Scene Distribution | CAS, Index, Resolver, Metrics | Medium |
| 2 | Warm Start Incremental Frames Distribution | Resolver, Metrics, CAS Dedup | Medium |
| 3 | Multi-Worker Inventory Locality Optimization | Multi-Cache Resolver, Metrics | High |
| 4 | High-Throughput Texture Stream Churn | LRU Eviction, Chunking, Index | High |
| 5 | Network Drop & Interrupted Stream Recovery | Atomic Ingest, Self-Healing | High |
| 6 | Multi-Camera Asset Deduplication & Savings | Dedup, Hit/Miss Resolver, Metrics | Medium |

## Coverage Thresholds
- **Tier 1 (Feature Coverage)**: 36 test cases (6 per feature).
- **Tier 2 (Boundary & Corner)**: 10 test cases.
- **Tier 3 (Cross-Feature Combinations)**: 8 test cases.
- **Tier 4 (Real-World Workloads)**: 6 test cases.
- **Total Minimum Target**: 60 test cases across `test_cache_store.py` and `test_cache_adversarial.py`.
