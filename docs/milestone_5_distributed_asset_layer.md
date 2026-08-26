# Milestone 5: Distributed Content-Addressed Asset Layer & Resilient Mesh

**Modules:** `src/aidars/distributed/` & `src/aidars/cache/`  
**Status:** Completed, Validated & Frozen (Stages 5.1 – 5.15)  

---

## 1. Overview
Milestone 5 provides a high-throughput, fault-tolerant distributed asset storage and synchronization mesh across multi-node clusters.

```text
                               ┌───────────────────────────┐
                               │     AIDAR Coordinator     │
                               │  Registry / Heartbeats    │
                               └─────────────┬─────────────┘
                                             │
                       ┌─────────────────────┴─────────────────────┐
                       │                                           │
                       ▼                                           ▼
          ┌───────────────────────────┐               ┌───────────────────────────┐
          │         Worker A          │               │         Worker B          │
          │  LocalCASAdapter (CAS-A)  │               │  LocalCASAdapter (CAS-B)  │
          └─────────────┬─────────────┘               └─────────────┬─────────────┘
                        │                                           │
                        └──────── Data Plane: Binary Stream ────────┘
                                 GET /api/v1/assets/{sha256}/stream
```

---

## 2. Core Subsystems

### 2.1 Content-Addressed Storage (`LocalCASAdapter`)
- **Split-Hash 2-Level Fanout**: `objects/<h[:2]>/<h[2:]>` preventing filesystem inode saturation.
- **Atomic Publication**: Downloads land in `staging/<uuid>.tmp` and are committed via atomic `os.replace` under a concurrency lock.
- **Memory-Bounded Streaming**: 1 MiB chunked buffers ensure $O(1)$ memory usage during transfers.

### 2.2 Control Plane (`CoordinatorService` & `WorkerRegistry`)
- **Inverted Hash Index**: $O(1)$ reverse lookup mapping `SHA-256 -> Set[WorkerID]`.
- **4-Tier Network Locality Prioritizer**: Ranks candidate nodes by `LOOPBACK` (10,000 pts) > `SUBNET` (5,000 pts) > `LAN` (2,000 pts) > `WAN` (500 pts), adjusted for RTT EMA latency and worker load factor.
- **Heartbeat & Eviction Reaper**: Periodic 5.0s pulse; detects silent worker crashes and purges dead nodes from routing tables after 15.0s.

### 2.3 Data Plane & Streaming Engine (`transfer.py`, `server.py`, `client.py`)
- **Progressive SHA-256 Verification**: Incremental hashing during byte arrival guarantees corrupt streams are caught and deleted before CAS commit.
- **HTTP 206 Range Resumption**: Supports byte-range chunked downloads.
- **Candidate Failover**: Transparently falls over to secondary candidate nodes if the preferred node fails.
- **Single-Flight Preparation**: Architecture audit and integration contracts established for M6 request deduplication.

---

## 3. Verified Adversarial Test Matrix (Stages 5.1 – 5.15)

| Stage | Test Scenario | Verified Invariant | Status |
|---|---|---|---|
| **5.1–5.5** | Pre-Transfer Baseline & Ownership | Absent on A, present on B, advertised by Coordinator | **PASS** 🟢 |
| **5.6** | Full Transfer & Cache Hit Cycle | Transferred 30 bytes, verified SHA-256, second request used 0 network bytes | **PASS** 🟢 |
| **5.7** | Interrupted Transfer Protection | Mid-stream socket drop at 50% $\implies 0$ staging leaks, 0 CAS pollution | **PASS** 🟢 |
| **5.8** | Corruption / Tamper Rejection | Bit-flipped payload rejected via `IntegrityError`, unlinked from disk | **PASS** 🟢 |
| **5.8.5**| Production HTTP Corruption | Live rogue streaming server rejected over actual network transport | **PASS** 🟢 |
| **5.9** | Worker Disappearance | Hard kill $\implies$ Heartbeat reaper evicts dead node after 15s | **PASS** 🟢 |
| **5.10**| Candidate Failover | Dead candidate B bypassed $\implies$ Fallback to candidate C | **PASS** 🟢 |
| **5.11**| Stale Inventory Pruning | Dead worker removed from inverted hash index | **PASS** 🟢 |
| **5.12**| Self-Healing Recovery | Restarted node re-registers cached inventory and re-enters mesh | **PASS** 🟢 |
| **5.13**| Concurrency Stress | 10 parallel identical & 10 distinct requests execute with 0 race corruption | **PASS** 🟢 |
| **5.14**| 50 MiB Streaming Benchmark | 110 MiB/s transfer rate, memory strictly bounded to 4.69 MiB | **PASS** 🟢 |
| **5.15**| Observability & Telemetry | Live BHR (84.34%), throughput, failovers accurately recorded | **PASS** 🟢 |
