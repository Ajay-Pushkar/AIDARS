# AIDAR CODE MASTERY: M1-M7 Content-Addressed Storage
## File: `src/aidars/cache/storage.py`

### 1. Purpose of the File
Provides the low-level physical disk operations for the CAS subsystem. It converts bytes into safely written files on the host OS.

### 2. Role in Architecture
It acts as the physical layer below the metadata index (`SQLiteMetaIndex`). It strictly enforces the split-hash layout (`objects/aa/bbbb...`) and atomic ingestion rules.

### 3. Important Classes/Functions
- `SplitHashStorage` (Class)
- `_get_split_path(sha256)` (Method)
- `put_stream(stream, sha256)` (Method)

### 4. Line-by-Line / Block-by-Block Explanation: `put_stream`

```python
def put_stream(self, stream: BinaryIO, size_bytes: int, expected_sha256: str) -> Path:
    # 1. Staging definition
    temp_id = uuid.uuid4().hex
    temp_path = self.tmp_dir / f"{temp_id}.tmp"
    
    # 2. Chunked ingestion and hashing
    hasher = hashlib.sha256()
    written_bytes = 0
    with open(temp_path, "wb") as f:
        while chunk := stream.read(65536):
            f.write(chunk)
            hasher.update(chunk)
            written_bytes += len(chunk)
```
**Explanation:** 
- **Staging**: We generate a UUID to ensure a unique temporary filename. We write to `self.tmp_dir` (which must be on the same physical drive as `objects/`) because `os.replace` is only atomic on the same mount point.
- **Chunking (65536)**: `stream.read(65536)` pulls 64 KiB at a time. This bounds memory to O(1) regardless of whether the file is 1MB or 50GB. 
- **Hashing**: `hasher.update(chunk)` computes the SHA-256 natively in Python while writing to disk in the exact same pass, halving I/O reads.

```python
    # 3. Integrity validation
    actual_sha256 = hasher.hexdigest()
    if actual_sha256 != expected_sha256:
        temp_path.unlink(missing_ok=True)
        raise HashMismatchError(f"Expected {expected_sha256}, got {actual_sha256}")
```
**Explanation:**
- **Integrity**: We compare the computed hash to the claimed hash. If a worker is malicious or a network glitch flipped a bit, the file is rejected.
- **Cleanup**: `temp_path.unlink` deletes the corrupted file immediately to avoid exhausting `tmp/` space.

```python
    # 4. Atomic placement
    final_path = self._get_split_path(actual_sha256)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(temp_path, final_path)
    return final_path
```
**Explanation:**
- **Split path**: `_get_split_path("aabbcc...")` returns `objects/aa/bbcc...`.
- **Atomic Move**: `os.replace` overwrites the destination if it exists and guarantees that partial files are never visible to concurrent readers. If a reader tries to access `final_path`, it either gets `FileNotFoundError` or the fully complete file.

### 5-17. Core Dimensions
- **Data Flow**: `BinaryIO` (in) -> chunked bytes -> `Path` (out).
- **State Changes**: Modifies the physical filesystem.
- **Error Paths**: OS permissions (`PermissionError`), disk space (`OSError`), or `HashMismatchError`.
- **Concurrency**: `os.replace` is atomic. If two threads ingest the same file concurrently, both will write to their own unique `temp_path`, and the last one to call `os.replace` "wins", but since the content is identical (content-addressed), there is no data corruption.
- **Resource Ownership**: It assumes the caller manages the `stream` lifecycle (closing it).

### 18. Code Interview Questions
- **Q1**: Why do we write to `tmp/` first instead of directly to `objects/aa/bb`?
- **Q2**: What happens if `os.replace` is called across two different mounted hard drives? (Answer: It falls back to a non-atomic copy, breaking our concurrency guarantees. This is why `tmp/` and `objects/` must share a parent root).
- **Q3**: Can this function block the event loop? (Yes, `stream.read` and `f.write` are blocking I/O, which is why M15 offloaded this to a `ThreadPoolExecutor`).

---
## TRACE: CAS Ingestion
User submits workload
    ↓
Adapter uploads missing assets
    ↓
**`CacheStore.put_stream(stream)`**
*Input*: Network HTTP stream of asset
*Output*: CacheEntry metadata
    ↓
**`SplitHashStorage.put_stream(stream)`**
*Action*: Writes chunks to `tmp/uuid.tmp`, computes SHA-256.
*Action*: `os.replace` to `objects/aa/bb..`
    ↓
**`SQLiteMetaIndex.insert()`**
*Action*: Writes `(sha256, size, time)` to `index.db`.
*Failure Path*: `sqlite3.IntegrityError` if primary key exists (handled gracefully).
