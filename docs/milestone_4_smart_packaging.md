# Milestone 4: Smart Packaging, Path Remapping & Atomic Verification

**Module:** `src/aidars/smart_package/`  
**Status:** Completed & Validated (M4.1, M4.2, M4.3)  

---

## 1. Overview
Milestone 4 transforms the semantic visibility analysis from Milestone 3 into an isolated, self-contained, and verified physical render package.

```text
RenderRequirementReport (M3)
             │
             ▼
    RequirementResolver
             │
             ▼
    PhysicalAssetResolver ──► Resolves //relative and absolute paths
             │
             ▼
       PackagePlanner     ──► Content-addressed deduplication (SHA-256)
             │
             ▼
       PackageBuilder     ──► Staging in tempfile.mkdtemp -> Atomic .bak swap
             │
             ▼
     Blender Path Remap   ──► Rewrites internal paths to //assets/<hash>_<name>
             │
             ▼
      PackageValidator    ──► Checks .blend existence & re-verifies all hashes
```

---

## 2. Sub-Milestones & Capabilities

### Milestone 4.1: Logical Packaging & Physical Resolution
- `RequirementResolver`: Maps semantic entities to canonical dependency graph nodes.
- `DependencyClosureResolver`: Computes cycle-safe transitive closures over required nodes.
- `PhysicalAssetResolver`: Resolves Blender `//relative` paths and absolute paths; calculates streaming SHA-256 digests in 64 KiB chunks; categorizes files as `RESOLVED`, `MISSING`, or `EMBEDDED`. Missing assets are never silently dropped.

### Milestone 4.2: Senior Hardening & Atomic Construction
- **Path Traversal Defense**: Enforces `is_relative_to` checks against package roots to prevent path traversal attacks.
- **Symlink Escape Prevention**: Resolves source symlinks via `src.resolve()` before copying.
- **Collision Avoidance**: Prefixes filenames with SHA-256 hashes to prevent basename collisions across subdirectories.
- **Atomic 3-Step Directory Publication**: Builds inside a temporary directory (`tempfile.mkdtemp`) and performs atomic `.bak` directory swaps to guarantee no partial packages exist.

### Milestone 4.3: Legacy Cleanup, Headless Remapping & Smoke Testing
- **Headless Blender Path Remapping**: Invokes headless Blender to remap all external asset paths (textures, VDB volumes, fonts, sound files) to relative `//assets/` directories.
- **Package Manifest**: Emits deterministic `manifest.json` (`v1.0.0`) with sorted keys and content-addressed package fingerprints (`SHA-256(request.fingerprint())`).
- **Secondary Headless Smoke Test**: Validates the packaged `.blend` scene by opening it in a clean Blender process to ensure zero broken asset links.
