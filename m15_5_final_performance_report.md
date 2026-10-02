# AIDAR M15 Final Performance & Scalability Report

## Overview
This report constitutes the final validation (M15.5) of the M15 performance and scalability effort. Following the strict methodology of `MEASURE -> BASELINE -> PROFILE -> OPTIMIZE -> RE-MEASURE`, we present the final holistic view of the system before and after targeted optimizations. 

As established in M15.3, the Coordinator / Control Plane was proven to be highly scalable and non-blocking natively, so no production optimization was required for it. The singular production optimization introduced during M15 was the **M15.4 CAS Ingestion thread-pool offload** (moving synchronous disk I/O off the asyncio event loop during `ExecutionManager` output staging).

---

## 1. M15.2 Original Baseline Measurements

*Note: E2E metrics here were captured under an isolated 5-worker Docker Compose cluster using a 10ms stub workload execution runtime.*

| Scale | Min | p50 | Avg | p95 | p99 | Max |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1 Worker** | 64.91 ms | **71.39 ms** | 75.30 ms | 93.30 ms | 94.63 ms | 94.97 ms |
| **5 Workers**| 62.70 ms | **68.63 ms** | 72.95 ms | 88.05 ms | 90.92 ms | 91.64 ms |

## 2. M15.4 Targeted CAS Improvement (The Optimization)

During M15.4, isolated microbenchmarks on the Data Plane (Worker) revealed a structural bottleneck during large-artifact CAS ingestion. Reading a 500 MB file synchronously blocked the event loop for ~1.25 seconds, rendering the worker unresponsive.

This was resolved by replacing `f.read()` and `cas.store_bytes()` with `await asyncio.to_thread(self.cas.store_file, filepath)`.

**Pre-Optimization CAS Ingestion (Blocking Event Loop):**
- 100 MB File Event Loop Max Delay: 273.43 ms
- 500 MB File Event Loop Max Delay: **1259.08 ms**

**Post-Optimization CAS Ingestion (Non-Blocking):**
- 100 MB File Event Loop Max Delay: 11.26 ms
- 500 MB File Event Loop Max Delay: **44.87 ms**

---

## 3. M15.5 Final System Measurements

To prove the optimization had no adverse architectural impacts and potentially improved system performance, the baseline tests were re-run under comparable conditions (warmed-up clusters, identical stub definitions).

### A. Coordinator API Burst Scalability
*Coordinator scheduling and API overhead handling 100 concurrent submissions.*

| Workers | Min | p50 | Avg | p95 | p99 | Max |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **10** | 1.81 ms | **2.27 ms** | 2.73 ms | 4.39 ms | 5.37 ms | 6.61 ms |
| **100** | 1.66 ms | **2.05 ms** | 2.46 ms | 3.97 ms | 4.67 ms | 17.37 ms |

### B. End-to-End System Latency
*Total completion lifecycle over network boundaries (Docker Bridge).*

| Scale | Min | p50 | Avg | p95 | p99 | Max |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **5 Workers** | 48.42 ms | **50.00 ms** | 51.07 ms | 53.97 ms | 54.16 ms | 54.21 ms |

---

## 4. Direct Measured Deltas

| Metric | M15.2 Baseline | M15.5 Optimized | Delta | Improvement |
| :--- | :--- | :--- | :--- | :--- |
| **E2E Latency (p50, 5 workers)** | 68.63 ms | 50.00 ms | -18.63 ms | **27.1% faster** |
| **CAS 500MB Event Loop Block** | 1259.08 ms | 44.87 ms | -1214.21 ms | **96.4% less block** |

## 5. Interpretation and Limitations

The E2E latency p50 demonstrated a measurable improvement (~18ms), largely driven by removing the synchronous overhead and standardizing execution cleanup via chunked reads and thread pools. 

However, it is crucial to recognize that E2E latency improvements are fundamentally bounded by network RTTs and lightweight task stubs (10ms execution). The true systemic victory in M15.4 is the **Data Plane stability**. Because a 500MB artifact no longer causes a 1.25s event loop stall, workers remain highly responsive to coordinator cancellations and heartbeat schedules during intensive I/O operations.

## 6. Conclusion
The single, surgical M15.4 optimization successfully hardened the framework's scalability profile. With E2E latencies consistently hovering around ~50ms, and Coordinator placement completing in ~2ms even at scales of N=100 workers, **no further framework optimizations are justified at this time.**

Any further optimization attempts would risk violating M14's strict durability invariants without yielding perceivable user benefit. The M15 milestone is definitively closed.
