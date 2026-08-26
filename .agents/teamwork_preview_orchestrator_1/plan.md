# Orchestration Plan — AIDAR Milestone 5 Core

## Objective
Implement Milestone 5 Core (Local Content-Addressed Asset Cache) for the AIDAR project, providing SHA-256 identity, SQLite index, O(1) set-difference resolver, LRU eviction, chunked transfers, and comprehensive pytest verification while ensuring zero coupling to Blender-specific graph/visibility logic.

## Workflow Phases

### Phase 0: Survey & Specification Mapping
1. Spawn 3 Survey Explorers / Spec Miners:
   - Explorer 1: Codebase structure survey (`src/aidars/`, M4 packaging models, imports, tests).
   - Explorer 2: Technical specification & interface analysis for M5 (storage layout, sqlite schema, resolver semantics, LRU eviction, chunking).
   - Spec Miner 1: Requirement derivation, test matrix, boundary cases, corruption scenarios, metrics definitions (`byte_hit_ratio`, `network_saved`).
2. Synthesize survey reports into `PROJECT.md` (Feature Inventory, Architecture, Interface Contracts, Code Layout).

### Phase 1: Dual Track Execution
- **Track A (E2E Testing Track)**:
  - Create `TEST_INFRA.md`.
  - Implement pytest suite covering Tier 1 (Feature Coverage), Tier 2 (Boundary & Corner Cases), Tier 3 (Cross-Feature Combinations), Tier 4 (Real-World Application Scenarios), plus corruption & metric tests.
  - Publish `TEST_READY.md`.
- **Track B (Implementation Track — Milestone 5 Core)**:
  - Sub-milestone 1: Cache Models, Interfaces, Split-Hash Storage, and SHA-256 Chunked Hashing/Transfer.
  - Sub-milestone 2: SQLite Metadata Index (`cache/metadata/index.db`) & Transactional State Management.
  - Sub-milestone 3: Hit/Miss Resolver (O(1) Set-Difference) & LRU Eviction Manager.
  - Sub-milestone 4: CacheStore Facade & Integration.

### Phase 2: Verification & Adversarial Hardening (Final Milestone)
- Phase 2.1: Run full E2E test suite (100% pass across Tiers 1-4).
- Phase 2.2: Adversarial Coverage Hardening (Tier 5) with Challengers & Forensic Auditor verification.

### Phase 3: Victory & Handoff
- Final Gate verification.
- Human report & notification to parent.
