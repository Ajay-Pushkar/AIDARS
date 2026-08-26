# Project: AIDAR Milestone 5 Core (Local Content-Addressed Asset Cache)

## Architecture
Milestone 5 Core provides a high-performance, robust, content-addressed asset caching subsystem isolated in `src/aidars/cache/`.
- **Identity**: Strictly defined by cryptographic SHA-256 content hashes (`^[0-9a-f]{64}$`).
- **Storage Layer (`storage.py`)**: Split-hash 2-level directory structure (`objects/<h[:2]>/<h[2:]>`), atomic writes via staging in `<cache_root>/tmp/<uuid>.tmp` on the same filesystem followed by `os.replace`, and chunked memory-bounded streaming (64 KiB buffers).
- **Metadata Index (`index.py`)**: SQLite 3 database (`cache/metadata/index.db`) running in WAL mode with busy timeout, tracking `hash`, `size_bytes`, `asset_type`, `original_name`, `source_path`, `created_at`, `last_accessed_at`, `access_count`, and `verification_status`.
- **Hit/Miss Resolver (`resolver.py`)**: $O(A)$ average-time set difference (`missing = required - cached`) using Python hash sets, with duck-typed M4 `PackagePlan` support and transfer efficiency metrics (`byte_hit_ratio`, `network_saved_bytes`).
- **LRU Eviction Engine (`eviction.py`)**: Enforces max cache quota by querying entries ordered by `last_accessed_at ASC`, unlinking physical files and deleting SQLite records atomically with Windows file locking resilience.
- **Integrity & Verification (`verifier.py`)**: Fast metadata checks and deep chunked SHA-256 validation; auto-eviction and self-healing of corrupted files.
- **Facade (`store.py`, `base.py`)**: Abstract `CacheStore` interface and concrete `DiskCacheStore` integrating all components with zero dependencies on Blender (`bpy`) or scene graph modules.

```
+-------------------------------------------------------------------------------+
|                             AIDAR Workflows / M4                              |
|          (PackagePlan / AssetRecord / Requested Hashes / Streams)             |
+---------------------------------------+---------------------------------------+
                                        | (duck-typed hashes & sizes)
                                        v
+-------------------------------------------------------------------------------+
|                       DiskCacheStore (CacheStore ABC)                         |
|  - contains(h)    - get_path(h)   - put_bytes/file/stream(h) - verify(h)      |
|  - get_stream(h)  - remove(h)     - resolve(hashes/plan)     - evict(bytes)   |
+-----------+-------------------+-------------------+-------------------+-------+
            |                   |                   |                   |
            v                   v                   v                   v
+-------------------+ +-------------------+ +-------------------+ +-------------------+
| SplitHashStorage  | | SQLiteMetaIndex   | | HitMissResolver   | | LRUEvictor &      |
| - objects/aa/bb.. | | - index.db (WAL)  | | - O(A) set diff   | | IntegrityVerifier |
| - tmp staging     | | - last_accessed   | | - byte_hit_ratio  | | - quota enforce   |
| - atomic replace  | | - size_bytes      | | - network_saved   | | - self-healing    |
| - 64KB chunking   | | - status          | | - PackagePlan res | | - deep SHA256     |
+-------------------+ +-------------------+ +-------------------+ +-------------------+
```

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | Content-Addressed Storage | Filesystem store where identity is purely SHA-256 | M1-Storage | ORIGINAL_REQUEST R1 |
| 2 | Split-Hash 2-Level Fanout | `objects/<h[:2]>/<h[2:]>` layout avoiding directory bloat | M1-Storage | ORIGINAL_REQUEST R1 |
| 3 | Atomic File Ingestion | Staging in `tmp/` + `os.replace` preventing partial corruption | M1-Storage | Survey / POSIX/NTFS |
| 4 | Chunked Transfer & Bounded RAM | 64 KiB streaming reading/writing bounding memory to O(1) | M1-Storage | ORIGINAL_REQUEST R4 |
| 5 | SQLite Metadata Index | SQLite database at `cache/metadata/index.db` in WAL mode | M2-Index | ORIGINAL_REQUEST R2 |
| 6 | Touch & Access Tracking | Updates `last_accessed_at` and `access_count` on access | M2-Index | ORIGINAL_REQUEST R2 |
| 7 | O(A) Set-Difference Resolver | `missing = required - cached` in O(A) average time | M3-Resolver | ORIGINAL_REQUEST R3 |
| 8 | Transfer Efficiency Metrics | Computes `byte_hit_ratio` and `network_saved` | M3-Resolver | ORIGINAL_REQUEST R3 |
| 9 | M4 PackagePlan Bridge | Duck-typed resolver accepting M4 plans or dicts | M3-Resolver | ORIGINAL_REQUEST R3, R5 |
| 10 | LRU Quota Eviction | Evicts entries by `last_accessed_at ASC` when quota exceeded | M4-Eviction | ORIGINAL_REQUEST R4 |
| 11 | Integrity Verification | Fast and deep SHA-256 verification and corruption self-healing | M4-Eviction | ORIGINAL_REQUEST R4 |
| 12 | CacheStore Public Interface | ABC `CacheStore` & `DiskCacheStore` facade | M5-Facade | ORIGINAL_REQUEST R4 |
| 13 | Subsystem Independence | Zero imports of `bpy`, `bmesh`, or scene graph modules | M5-Facade | ORIGINAL_REQUEST R5 |
| 14 | 4-Tier Test Suite | Comprehensive pytest suite covering Tiers 1-4 + corruption | M6-Tests | Acceptance Criteria |
| 15 | Adversarial Hardening | Challenger edge cases & Forensic Audit integrity check | M7-Hardening | Project Quality Gate |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Storage & Models | `models.py`, `storage.py`, split-hash CAS, atomic staging, 64 KiB chunking | None | DONE |
| M2 | SQLite Metadata Index | `index.py`, schema, WAL pragmas, touch/access tracking, query methods | M1 | DONE |
| M3 | Hit/Miss Resolver | `resolver.py`, O(A) set difference, M4 PackagePlan bridge, metrics | M1, M2 | DONE |
| M4 | LRU Eviction & Verifier | `eviction.py`, `verifier.py`, quota enforcement, deep verification, self-healing | M1, M2 | DONE |
| M5 | CacheStore Facade | `base.py`, `store.py`, `__init__.py`, unifying all components | M1, M2, M3, M4 | DONE |
| M6 | Test Suite (Tiers 1-4) | `tests/test_cache_store.py`, `tests/test_cache_adversarial.py` | M5 | DONE |
| M7 | Final Hardening & Audit | E2E 100% Pass, Challenger verification, Forensic Audit | M6 | DONE |

## Interface Contracts

### Data Models (`src/aidars/cache/models.py`)
```python
@dataclass(slots=True)
class CacheEntry:
    sha256: str
    size_bytes: int
    asset_type: str = "unknown"
    original_name: str = ""
    source_path: str = ""
    created_at: float = field(default_factory=time.time)
    last_accessed_at: float = field(default_factory=time.time)
    access_count: int = 1
    verification_status: str = "verified"
    relative_path: str = ""
    metadata: dict = field(default_factory=dict)

@dataclass(slots=True)
class ResolutionResult:
    hits: set[str]
    misses: set[str]
    total_requested_bytes: int
    hit_bytes: int
    miss_bytes: int
    byte_hit_ratio: float
    network_saved_bytes: int

@dataclass(slots=True)
class VerificationReport:
    verified_count: int
    corrupted_count: int
    missing_count: int
    corrupted_hashes: list[str]
    missing_hashes: list[str]
    is_healthy: bool
```

### CacheStore ABC (`src/aidars/cache/base.py`)
```python
class CacheStore(ABC):
    @abstractmethod
    def contains(self, sha256: str) -> bool: ...
    @abstractmethod
    def get_path(self, sha256: str) -> Optional[Path]: ...
    @abstractmethod
    def get_stream(self, sha256: str, chunk_size: int = 65536) -> Iterator[bytes]: ...
    @abstractmethod
    def get_bytes(self, sha256: str) -> Optional[bytes]: ...
    @abstractmethod
    def put_bytes(self, data: bytes, sha256: Optional[str] = None, original_name: str = "", asset_type: str = "unknown") -> CacheEntry: ...
    @abstractmethod
    def put_file(self, file_path: Path, sha256: Optional[str] = None, original_name: str = "", asset_type: str = "unknown") -> CacheEntry: ...
    @abstractmethod
    def put_stream(self, stream: BinaryIO, size_bytes: int, sha256: str, original_name: str = "", asset_type: str = "unknown") -> CacheEntry: ...
    @abstractmethod
    def verify(self, sha256: str, deep_check: bool = True) -> bool: ...
    @abstractmethod
    def verify_all(self, auto_evict: bool = True) -> VerificationReport: ...
    @abstractmethod
    def remove(self, sha256: str) -> bool: ...
    @abstractmethod
    def evict_lru(self, target_bytes_to_free: int) -> int: ...
    @abstractmethod
    def resolve_hashes(self, required_hashes: Iterable[str]) -> ResolutionResult: ...
    @abstractmethod
    def resolve_plan(self, plan: Any) -> ResolutionResult: ...
    @abstractmethod
    def get_stats(self) -> dict: ...
```

## Code Layout
```
src/aidars/cache/
├── __init__.py           # Exports: CacheStore, DiskCacheStore, CacheEntry, ResolutionResult, etc.
├── base.py               # Abstract base class CacheStore
├── models.py             # Dataclasses, exceptions (CacheError, QuotaExceededError, HashMismatchError)
├── storage.py            # SplitHashStorage (filesystem operations, split paths, atomic rename, chunking)
├── index.py              # SQLiteMetadataIndex (schema, WAL mode, CRUD, access tracking, query indices)
├── resolver.py           # HitMissResolver (O(A) set difference, M4 bridge, metric calculation)
├── eviction.py           # LRUEvictor (quota enforcement, last_accessed_at sorting, atomic cleanup)
├── verifier.py           # IntegrityVerifier (fast metadata check, deep SHA-256 stream check, auto-evict)
└── store.py              # Concrete DiskCacheStore integrating all modules
tests/
├── test_cache_store.py         # Tiers 1-4 comprehensive unit & integration tests
└── test_cache_adversarial.py   # Boundary, corruption, concurrency, and decoupling stress tests
```
