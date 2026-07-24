# AIDARS Phase 2: Caching, Incremental Scanning, Change Detection

## 0. Status

Phase 1 is complete and verified. This document plans Phase 2 and describes
the small foundation already implemented ahead of time (`cache.py` +
`--cache-dir`) so Phase 2 work has somewhere to start from rather than a
blank page. For the broader multi-stage pipeline this fits into (Visibility
Analysis, Smart Packaging, Frame Scheduler, Workers, Frame Merger) and how
orchestration is structured, see `docs/scene_engine_architecture.md`.

## 1. Objective

Avoid redundant work. Today, every CLI invocation fully re-parses the input,
rebuilds the snapshot, rebuilds the dependency graph, and rewrites both JSON
files - even if nothing in the scene changed since the last run. For large
productions with hundreds of shots and frequent CI-style scene validation
runs, that's wasted time and wasted Blender launches (the external-Blender
path is the expensive one: it's a subprocess that has to boot the whole
application).

Phase 2 makes AIDARS **content-aware**: it should know when a scene hasn't
changed and skip work accordingly, and when only *part* of a scene changed,
it should eventually be able to avoid re-processing the parts that didn't.

## 2. What's already built (foundation, not the full phase)

`src/aidars/scene_intelligence/cache.py`:

- `hash_source(path_or_dict) -> str` - content hash (sha256) of a scene
  source. Works on a JSON payload dict, a `.json` file, or a `.blend` file
  (streamed in 1 MiB chunks, so large files don't blow up memory).
- `SceneCache` - a small JSON-file-backed cache keyed by source path, mapping
  to the last known content hash and where its outputs were written.
- CLI: `--cache-dir <dir>` (opt-in, off by default) makes the CLI hash the
  input *before* doing anything else and skip straight to "reusing cached
  outputs" when the hash matches the last run for that path.

This is deliberately the simplest possible correct implementation of
"change detection" - it answers yes/no to "did this exact source change,"
nothing more granular yet. It's tested in `tests/test_scene_cache.py` and
`tests/test_cli.py`.

## 3. What Phase 2 still needs to build

### 3.1 Incremental scanning (partial re-analysis)

Right now a single content hash covers the *entire* scene. The next step is
sub-scene hashing: hash each object/collection/material independently (their
already-typed dataclasses make this straightforward - hash each object's
`raw` dict, or a canonical subset of its fields) so that changing one object
in a 500-object scene doesn't require re-parsing the other 499.

Suggested approach:
- Add `object_hash(obj: SceneObject) -> str` and `collection_hash(...)` next
  to the existing `hash_json_payload`.
- `SceneCache` gains a second index level: `source_hash -> {object_id: object_hash}`.
- `SceneIntelligenceEngine` (or a new `IncrementalScanner` wrapping it) skips
  rebuilding a `SceneObject` whose hash is unchanged and reuses the
  previously-built instance instead.
- This only pays off once building a `SceneObject` is non-trivial (e.g. once
  Phase 4's visibility/render-cost analysis adds real computation per
  object). Until then, full re-parse is fast enough that this is a
  "build when needed" item, not a blocker.

### 3.2 Change detection reporting

A useful, cheap win before full incremental scanning: **diff two
snapshots**. Given `scene_v1.json` and `scene_v2.json` (or their SceneSnapshot
objects), report:
- Objects added / removed / renamed
- Objects whose material, transform, or mesh topology changed
- New or removed missing-target / unused-node entries in the dependency graph

This reuses the dependency graph's existing `find_missing_targets()` /
`find_unused_nodes()` and just needs a diff of two `DependencyGraph`
instances (`nodes`/`edges` are already plain dataclasses, so this is a
set-difference over identifiers). Good first Phase 2 ticket: self-contained,
testable without touching the engine/builders at all.

### 3.3 Cache backend beyond a single JSON file

`SceneCache`'s storage is intentionally swappable - `get`/`put`/`has_changed`/
`invalidate` are the whole public surface. A single JSON index file is fine
for one artist on one machine; a render farm with many workers will want a
shared backend (SQLite file, or a small HTTP service) behind the same
interface. Don't change the interface without a concrete multi-worker need;
premature distribution here is likely wasted effort until Phase 5
(distributed rendering) actually exists.

### 3.4 Cache invalidation triggers beyond content hash

Content hash catches "the scene file changed." It does *not* catch "an
external asset the scene references changed" (e.g. `/assets/chair.blend`
got a new version but the referencing scene's own bytes didn't change).
Once `referenced_assets` tracking (already in Phase 1) is paired with
Phase 3's asset packaging, cache entries should also invalidate when any
referenced asset's own hash changes. This needs the packaging layer to
expose per-asset hashes first, so it's blocked on Phase 3, not Phase 2.

## 4. Acceptance criteria for calling Phase 2 "done"

- [ ] Re-running the CLI on an unchanged `.blend`/`.json` input with
      `--cache-dir` set does zero Blender subprocess launches and zero
      snapshot/graph rewrites. **(done - see `cache.py`)**
- [ ] A changed scene invalidates the cache and produces fresh output.
      **(done)**
- [ ] Sub-scene (per-object) hashing exists and is used by at least one real
      consumer (not just unit-tested in isolation).
- [ ] A `scene-diff` CLI command (or `--diff <previous_scene.json>` flag)
      reports what changed between two snapshots in human-readable form.
- [ ] Cache behavior is documented in `docs/` the same way
      `blender_export_usage.md` documents the export paths.

## 5. Suggested order of work

1. Snapshot diffing (3.2) - fully decoupled, immediately useful, easy to test.
2. Per-object hashing (3.1) - depends on nothing else, but its payoff is
   currently mostly theoretical; sequence it based on whether a real
   performance problem shows up first.
3. Alternate cache backends (3.3) - defer until there's a concrete
   multi-worker use case.
4. Asset-hash-aware invalidation (3.4) - after Phase 3 packaging lands.
