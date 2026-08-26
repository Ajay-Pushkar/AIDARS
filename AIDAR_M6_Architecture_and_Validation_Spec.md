# AIDAR M6 — Adaptive Computational Resource System
**Status:** M6 Design / Execution Blueprint  
**Base Architecture:** M5 Distributed Data Plane (Validated & Frozen)  

---

## 1. Mission

**Milestone 5 (M5)** established AIDAR's distributed content-addressed data plane and worker control plane.

**Milestone 6 (M6)** evolves that foundation from:
> **Asset-Centric:** *"Who has SHA-256 asset $X$?"*

to:
> **Workload-Centric:** *"Where should computational workload $T$ execute?"*

M6 must **consume M5 rather than rewrite it**:

```text
M5 (Data Layer)
Asset Discovery ──► Verified Streaming Transfer ──► Atomic CAS Commit

M6 (Compute Layer)
WorkloadSpec
   │
   ▼
Resource Profiling
   │
   ▼
Placement Decision
   │
   ▼
Dependency Synchronization (M5)
   │
   ▼
Execution Runtime
   │
   ▼
Output Hashing (SHA-256)
   │
   ▼
CAS Commit (M5)
   │
   ▼
WorkloadExecutionResult
```

---

## 2. Architecture Map

```text
                                     AIDAR M6
                                        │
        ┌───────────────────────────────┼───────────────────────────────┐
        ▼                               ▼                               ▼
  CONTROL PLANE                   COMPUTE PLANE                    DATA PLANE
        │                               │                               │
 Coordinator                     ExecutionManager                 M5 LocalCASAdapter
 WorkerRegistry                  RuntimeAdapter                   M5 Binary Streaming
 ResourceRegistry                ResourceMonitor                  M5 Hash Discovery
 PlacementEngine                 Workload Lifecycle               M5 SHA-256 Integrity
        │                               │                               │
        └───────────────────────────────┼───────────────────────────────┘
                                        ▼
                                PlacementDecision
                                        │
                                        ▼
                             Sync Missing Dependencies (M5)
                                        │
                                        ▼
                                  Execute Task
                                        │
                                        ▼
                              Hash + Commit Outputs (CAS)
                                        │
                                        ▼
                              WorkloadExecutionResult
```

---

## 3. M5 → M6 System Boundary

### Frozen M5 Components (Do Not Break)
- `src/aidars/distributed/coordinator.py`: Core cluster control-plane endpoints.
- `src/aidars/distributed/registry.py`: Inverted hash index, worker health & heartbeats.
- `src/aidars/distributed/prioritizer.py`: 4-tier network classification & RTT EMA tracking.
- `src/aidars/distributed/worker.py`: Node runtime, lifecycle management & background heartbeats.
- `src/aidars/distributed/server.py`: Binary streaming server (`/api/v1/assets/{hash}/stream`).
- `src/aidars/distributed/client.py`: Async RPC client & asset download coordination.
- `src/aidars/distributed/transfer.py`: Memory-bounded streaming, progressive SHA-256 & failover.
- `src/aidars/distributed/cas_adapter.py`: Content-Addressed Storage layout & atomic `os.replace`.
- `src/aidars/distributed/metrics.py`: Telemetry tracker & Byte Hit Ratio calculations.

### New M6 Components
- `src/aidars/distributed/models.py` (Additions): `WorkloadSpec`, `WorkerResourceProfile`, `PlacementDecision`, `WorkloadExecutionResult`.
- `src/aidars/distributed/workload.py`: Workload orchestration and state transition engine.
- `src/aidars/distributed/workload_registry.py`: Persistent/in-memory ledger of submitted, executing, and completed workloads.
- `src/aidars/distributed/resources.py`: CPU, RAM, GPU, VRAM, and thermal headroom telemetry collector.
- `src/aidars/distributed/placement.py`: Multi-attribute placement decision engine ($\mathcal{S}(w, \tau)$).
- `src/aidars/distributed/singleflight.py`: In-flight request deduplication & promise sharing.
- `src/aidars/distributed/runtime.py`: Pluggable execution abstraction (`RuntimeAdapter`).
- `src/aidars/distributed/execution.py`: Workload supervisor, isolated workspace builder & timeout enforcer.
- `src/aidars/distributed/workload_metrics.py`: Compute telemetry, queue latency & execution efficiency tracker.

---

## 4. Module Responsibility Matrix

| Module | Core Responsibility | State Owned | Dependencies |
|---|---|---|---|
| **`models.py`** | M6 wire schemas and validation rules | Immutable Pydantic models | None |
| **`workload.py`** | High-level workload submission & lifecycle | In-flight execution tasks | `models.py`, `placement.py` |
| **`workload_registry.py`** | Workload records, state history & audit logs | `_workloads: Dict[str, WorkloadRecord]` | `models.py`, `threading.RLock` |
| **`resources.py`** | Hardware profiling (CPU, RAM, GPU, VRAM) | Live hardware telemetry cache | `psutil`, `pynvml` / torch |
| **`placement.py`** | Multi-attribute scoring & node selection | Placement weights, evaluation cache | `resources.py`, `registry.py` |
| **`execution.py`** | Workspace isolation, execution & CAS commit | Active sub-processes, scratch paths | `runtime.py`, `cas_adapter.py` |
| **`runtime.py`** | Task execution engine abstraction | Process handles, stdout/stderr streams | `asyncio.subprocess` |
| **`singleflight.py`** | Coalescing duplicate concurrent transfers | `_in_flight: Dict[str, asyncio.Future]` | `asyncio.Lock` |
| **`workload_metrics.py`** | Scheduling latency & compute metrics | Cumulative timing arrays | `threading.RLock` |

---

## 5. Core M6 Contracts

### 5.1 `WorkloadSpec`
```python
from typing import Set, Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator
from aidars.distributed.models import validate_sha256_hex

class WorkloadSpec(BaseModel):
    """Declarative specification of a computational task."""
    workload_id: str = Field(..., min_length=1, max_length=128)
    task_type: str = Field(..., min_length=1, max_length=64)  # e.g., "blender_render", "scene_eval"
    input_asset_hashes: Set[str] = Field(default_factory=set)

    min_cpu_cores: int = Field(default=1, ge=1)
    min_ram_bytes: int = Field(default=1024 * 1024 * 1024, ge=1)  # 1 GiB default

    requires_gpu: bool = Field(default=False)
    min_vram_bytes: int = Field(default=0, ge=0)

    estimated_duration_seconds: float = Field(default=10.0, gt=0.0)
    priority: int = Field(default=100, ge=0)
    parameters: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("input_asset_hashes")
    @classmethod
    def validate_hashes(cls, v: Set[str]) -> Set[str]:
        return {validate_sha256_hex(h) for h in v}
```

### 5.2 `WorkerResourceProfile`
```python
class WorkerResourceProfile(BaseModel):
    """Real-time compute and hardware state advertised by a worker."""
    worker_id: str = Field(...)
    endpoint_url: str = Field(...)
    ip_address: str = Field(...)

    cpu_cores_total: int = Field(..., ge=1)
    cpu_utilization_percent: float = Field(..., ge=0.0, le=100.0)

    ram_total_bytes: int = Field(..., ge=1)
    ram_available_bytes: int = Field(..., ge=0)

    gpu_available: bool = Field(default=False)
    gpu_device_name: Optional[str] = None
    vram_total_bytes: int = Field(default=0, ge=0)
    vram_available_bytes: int = Field(default=0, ge=0)

    active_workload_count: int = Field(default=0, ge=0)
    local_cached_hashes: Set[str] = Field(default_factory=set)
    timestamp_utc: float = Field(...)
```

### 5.3 `PlacementDecision`
```python
class PlacementDecision(BaseModel):
    """Explainable output of the multi-attribute placement decision engine."""
    workload_id: str
    selected_worker_id: str
    placement_score: float
    score_breakdown: Dict[str, float]  # e.g., {"compute": 0.85, "locality": 1.0, "latency": -0.05}
    missing_assets_on_worker: Set[str]
    execution_tier: str  # "local", "subnet", "lan"
    decision_timestamp_utc: float
```

### 5.4 `WorkloadExecutionResult`
```python
class WorkloadExecutionResult(BaseModel):
    """Immutable result metadata returned after workload completion."""
    workload_id: str
    worker_id: str
    success: bool
    output_asset_hashes: Set[str]  # Verified SHA-256 artifacts committed to CAS
    execution_duration_seconds: float
    error_message: Optional[str] = None
    stdout_snippet: Optional[str] = None
    stderr_snippet: Optional[str] = None
```

---

## 6. Workload State Machine

```text
       [ SUBMITTED ]
             │
             ▼
       [ VALIDATING ] ──► (Invalid Spec) ──► [ FAILED ]
             │
             ▼
        [ PLACING ]   ──► (No Worker Fits) ──► [ UNSCHEDULABLE / PENDING ]
             │
             ▼
         [ PLACED ]
             │
             ▼
     [ SYNCING_ASSETS ] ──► (Sync M5 Fail) ──► [ FAILED / RETRY ]
             │
             ▼
         [ READY ]
             │
             ▼
       [ EXECUTING ]  ──► (Crash / Timeout) ──► [ FAILED / TIMEOUT ]
             │
             ▼
       [ INGESTING ]  ──► (Integrity Fail) ──► [ FAILED ]
             │
             ▼
       [ COMPLETED ]
```

---

## 7. Multi-Attribute Placement Formula

The placement engine chooses:
$$w^* = \arg\max_{w \in \mathcal{W}_{\text{eligible}}} \mathcal{S}(w, \tau)$$

Where $\mathcal{S}(w, \tau)$ is the normalized weighted score:

$$\mathcal{S}(w, \tau) = w_c \cdot \mathcal{C}(w) + w_m \cdot \mathcal{M}(w) + w_g \cdot \mathcal{G}(w) + w_d \cdot \mathcal{D}(w, \tau) + w_n \cdot \mathcal{N}(w, c) - w_l \cdot \mathcal{L}(w)$$

### Sub-Score Definitions:
1. **Compute Headroom $\mathcal{C}(w)$**:
   $$\mathcal{C}(w) = \frac{\text{CPU}_{\text{cores}}(w) \cdot (1 - \text{CPU}_{\text{util}}(w))}{\tau_{\text{min\_cores}}}$$
2. **Memory Headroom $\mathcal{M}(w)$**:
   $$\mathcal{M}(w) = \frac{\text{RAM}_{\text{avail}}(w)}{\tau_{\text{min\_ram}}}$$
3. **GPU Suitability $\mathcal{G}(w)$**:
   $$\mathcal{G}(w) = \begin{cases} 1.0 + \frac{\text{VRAM}_{\text{avail}}(w)}{\tau_{\text{min\_vram}}} & \text{if GPU required and available} \\ 1.0 & \text{if GPU not required} \\ 0.0 & \text{if GPU required but missing} \end{cases}$$
4. **Data Locality $\mathcal{D}(w, \tau)$**:
   $$\mathcal{D}(w, \tau) = \frac{|\tau_{\text{input\_hashes}} \cap w_{\text{cached\_hashes}}|}{|\tau_{\text{input\_hashes}}|}$$
5. **Network Latency Penalty $\mathcal{N}(w, c)$**:
   $$\mathcal{N}(w, c) = \text{TierBase}(\text{LocalityTier}) - \text{RTT}_{\text{EMA}}(w, c)$$
6. **Queue Load Penalty $\mathcal{L}(w)$**:
   $$\mathcal{L}(w) = w_{\text{active\_workloads}}$$

### Hard Constraint Filters:
A worker is **immediately disqualified** before scoring if:
- $\text{RAM}_{\text{avail}}(w) < \tau_{\text{min\_ram}}$
- $\tau_{\text{requires\_gpu}} = \text{True}$ and $w_{\text{gpu\_available}} = \text{False}$
- $\text{VRAM}_{\text{avail}}(w) < \tau_{\text{min\_vram}}$
- Worker status $\neq \text{ACTIVE}$
- Resource profile age $> 10.0\text{ seconds}$ (Stale profile)

---

## 8. Single-Flight Request Deduplication

### Invariant:
$$\forall N \text{ concurrent requests for } H \implies \text{NetworkTransfers}(H) = 1$$

```python
class SingleFlight:
    """Coalesces concurrent identical in-flight asynchronous operations."""
    def __init__(self) -> None:
        self._flights: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def run(self, key: str, operation: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            if key in self._flights:
                future = self._flights[key]
            else:
                loop = asyncio.get_running_loop()
                future = loop.create_future()
                self._flights[key] = future
                asyncio.create_task(self._execute(key, operation, future))

        return await asyncio.shield(future)

    async def _execute(self, key: str, operation: Callable[[], Awaitable[T]], future: asyncio.Future) -> None:
        try:
            result = await operation()
            future.set_result(result)
        except Exception as exc:
            future.set_exception(exc)
        finally:
            async with self._lock:
                self._flights.pop(key, None)
```

---

## 9. Execution Pipeline & Workspace Isolation

Workloads **never** execute directly inside the CAS object directories.

```text
C:\AIDAR-M6\workloads\<workload_id>\
├── inputs/           (Symlinks / hardlinks to CAS objects)
├── outputs/          (Freshly written result artifacts)
├── logs/
│   ├── stdout.log
│   └── stderr.log
└── metadata.json     (WorkloadSpec & timing records)
```

### Execution Steps:
1. `ExecutionManager` creates isolated working directory.
2. Symlinks verified input CAS objects into `inputs/`.
3. Invokes `RuntimeAdapter.execute(spec, workdir)`.
4. Enforces hard timeout (`spec.estimated_duration * 3.0`).
5. On completion, scans `outputs/` directory.
6. Computes SHA-256 for all generated files and commits them to CAS via `LocalCASAdapter.commit_staged_file()`.
7. Cleans working directory; returns `WorkloadExecutionResult(output_asset_hashes={...})`.

---

## 10. M6 Full Test Matrix (6.1 – 6.19)

| # | Test Name | Invariant to Prove | Target Result |
|---|---|---|---|
| **6.1** | Contract Validation | Reject invalid hashes, negative RAM/CPU, empty IDs | Validation Error |
| **6.2** | Hardware Profiler | Live CPU, RAM, GPU, and VRAM correctly measured | Verified |
| **6.3** | Placement Hard Filters | Disqualify workers lacking RAM/GPU/VRAM | Rejection |
| **6.4** | Placement Multi-Score | Select optimal worker matching compute & latency | Highest $\mathcal{S}(w)$ |
| **6.5** | Data Locality Bias | Prefer worker holding input assets over cold node | Locality win |
| **6.6** | Dependency Sync | Missing assets synchronized via M5 before execution | 0 missing |
| **6.7** | SingleFlight Stress | 100 concurrent requests for $H \implies 1$ HTTP stream | 1 transfer |
| **6.8** | Deterministic Execution | Input $X \implies$ Execute $\implies$ Output $Y \implies$ Commit CAS | Hash matches |
| **6.9** | Runtime Failure | Non-zero exit code caught $\implies$ Staging unlinked | `FAILED` status |
| **6.10** | Execution Timeout | Exceeding timeout kills sub-process and frees RAM | `TIMEOUT` status |
| **6.11** | Worker Crash Mid-Task | Dead executing node detected $\implies$ Rescheduled | Failover |
| **6.12** | Output Tamper Reject | Modified output file fails SHA-256 $\implies$ Zero commit | `IntegrityError` |
| **6.13** | Task Idempotency | Duplicate submission returns existing active task | Idempotent |
| **6.14** | Parallel Workloads | 10 concurrent workloads execute with zero cross-talk | All isolate |
| **6.15** | Resource Admission | High-load cluster queues tasks until RAM/GPU free | Queued |
| **6.16** | Pre-Exec Failover | Worker dies after placement but before execution $\implies$ Re-placed | Recovered |
| **6.17** | M5 Regression Suite | All M5 tests (5.1–5.15) pass without regression | 100% PASS |
| **6.18** | Physical Multi-LAN | Real PC A (Coord+Worker) ↔ Real PC B (Worker) | LAN validated |
| **6.19** | Full End-to-End | Submit $\implies$ Place $\implies$ Sync $\implies$ Run $\implies$ Output CAS | `SUCCESS` |

---

## 11. Core M6 Invariants

- **$\mathbf{I_1}$ (No Invalid Placement):** $\text{Requirements} > \text{Available Resources} \implies \text{Candidate Disqualified}$.
- **$\mathbf{I_2}$ (No Execution Without Dependencies):** $\text{Missing Dependencies} \neq \emptyset \implies \text{Execution Cannot Start}$.
- **$\mathbf{I_3}$ (No Unverified Output):** $\text{SHA256}(\text{Output}) \neq \text{Declared Hash} \implies \text{CAS Commit Rejected}$.
- **$\mathbf{I_4}$ (SingleFlight In-Flight Transfer):** $N \text{ concurrent}(H) \implies \text{NetworkTransfers}(H) = 1$.
- **$\mathbf{I_5}$ (No False Success):** $\text{Success} \iff \text{ExitCode}=0 \land \text{OutputsVerified} \land \text{OutputsCommitted}$.
- **$\mathbf{I_6}$ (M5 Zero Regression):** $\text{M6 Code Changes} \implies \text{M5 Test Suite Pass Rate} = 100\%$.

---

## 12. M6 Definition of Done

M6 is complete when AIDAR autonomously:
1. Ingests a `WorkloadSpec` declaring CPU, RAM, GPU, and input CAS dependencies.
2. Evaluates active worker resource profiles and calculates placement score $\mathcal{S}(w, \tau)$ incorporating data locality.
3. Dispatches the workload to the optimal node.
4. Synchronizes missing scene assets via M5 with SingleFlight request deduplication.
5. Executes the workload in an isolated sandbox.
6. Computes SHA-256 for all generated output artifacts and commits them atomically to the CAS.
7. Survives injected worker crashes and runtime timeouts without cluster corruption.
8. Passes all 19 M6 verification tests and the full 15-stage M5 regression suite across physical LAN machines.
