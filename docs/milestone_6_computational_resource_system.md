# Milestone 6: Adaptive Computational Resource System

**Status:** Design & Validation Blueprint Frozen (Ready for Implementation)  
**Specification:** [`AIDAR_M6_Architecture_and_Validation_Spec.md`](file:///c:/AIDAR/AIDAR_M6_Architecture_and_Validation_Spec.md)  

---

## 1. Executive Summary
Milestone 6 transitions AIDAR from an asset distribution network into an **Adaptive Computational Resource System**. It introduces workload contracts, hardware resource profiling, multi-attribute scheduling, and sandboxed task execution on top of the validated M5 data plane.

```text
M5 (Data Plane)           M6 (Compute Plane)
Distribute Raw Data  ──►  Distribute Computational Tasks
```

---

## 2. Core Modules & Contracts

### 2.1 Contracts (`src/aidars/distributed/models.py`)
- `WorkloadSpec`: Declarative task requirements (CPU, RAM, GPU, VRAM, input CAS hashes, priority).
- `WorkerResourceProfile`: Dynamic hardware telemetry (CPU/RAM availability, GPU device, VRAM headroom).
- `PlacementDecision`: Explainable scoring breakdown and required asset staging lists.
- `WorkloadExecutionResult`: Durable output artifact hashes committed to CAS.

### 2.2 Placement Engine (`src/aidars/distributed/placement.py`)
Multi-attribute normalized placement formula:
$$\mathcal{S}(w, \tau) = w_c \cdot \mathcal{C}(w) + w_m \cdot \mathcal{M}(w) + w_g \cdot \mathcal{G}(w) + w_d \cdot \mathcal{D}(w, \tau) + w_n \cdot \mathcal{N}(w, c) - w_l \cdot \mathcal{L}(w)$$

### 2.3 Single-Flight Coalescing (`src/aidars/distributed/singleflight.py`)
Enforces that $N$ simultaneous requests for the same missing asset hash coalesce into exactly 1 network streaming transfer.

### 2.4 Workspace Isolation & Execution Supervisor (`src/aidars/distributed/execution.py`)
Executes tasks inside isolated working sandboxes (`inputs/`, `outputs/`, `logs/`), captures stdout/stderr, enforces execution timeouts, hashes outputs, and commits them to the CAS.

---

## 3. Full Specification
For detailed state machine diagrams, failure taxonomies, and the complete 19-stage verification test plan, see the root specification:
👉 [`AIDAR_M6_Architecture_and_Validation_Spec.md`](file:///c:/AIDAR/AIDAR_M6_Architecture_and_Validation_Spec.md)
