# AIDAR CODE MASTERY: M11-M13 Placement & Intelligence
## File: `src/aidars/distributed/placement.py`

### 1. Purpose of the File
Implements the `PlacementEngine`, which calculates the most optimal worker for a given workload by evaluating deterministic hard constraints (RAM, CPU, affinity) and soft constraints (CAS asset locality, intelligence API recommendations).

### 2. Role in Architecture
Acts as the central decision-maker. It is purely computational (no side effects, no HTTP calls). It takes the current state of the cluster and a workload, and returns a sorted ranking.

### 3. Important Classes/Functions
- `PlacementEngine` (Class)
- `evaluate_workload` (Method)
- `_check_hard_constraints` (Method)
- `_calculate_locality_score` (Method)

### 4. Line-by-Line / Block-by-Block Explanation: `evaluate_workload`

```python
    def evaluate_workload(
        self, workload: WorkloadSpec, eligible_worker_ids: Set[str]
    ) -> Optional[PlacementDecision]:
        
        # 1. Hard Constraints & Affinity
        valid_candidates = []
        for worker_id in eligible_worker_ids:
            worker = self.registry.get_worker(worker_id)
            if not self._check_hard_constraints(worker, workload):
                continue
```
**Explanation:**
- **Filtering**: We iterate over the explicitly eligible workers (which already excludes `exhausted_worker_ids`).
- **Hard Constraints**: `_check_hard_constraints` verifies `min_ram_bytes`, CPU requirements, and `affinity_group_id`. If `affinity_mode == "same_worker"` and this worker isn't the anchor, it is immediately discarded (`continue`).

```python
        # 2. Soft Scoring
        scored_candidates = []
        for worker in valid_candidates:
            # Memory headroom score (0.0 to 1.0)
            mem_score = (worker.capacity_bytes - worker.used_bytes) / worker.capacity_bytes
            
            # Locality score (O(A) missing asset diff)
            locality_score, missing = self._calculate_locality_score(worker, workload)
            
            # Weighted aggregation
            final_score = (mem_score * 0.4) + (locality_score * 0.6)
```
**Explanation:**
- **Memory Score**: Calculates available percentage. A completely idle worker scores `1.0`.
- **Locality Score**: Relies on the CAS index. If a worker already has all requested assets (e.g., from a previous job in the affinity group), `locality_score = 1.0`. If it's missing 50% of the bytes, it's `0.5`.
- **Weighting**: Locality is weighted higher (`0.6`) because network transfer of CAS assets across the physical LAN is significantly slower than CPU scheduling overhead.

```python
        # 3. Intelligence Advisory (M12)
        if self.intelligence_api:
            try:
                adjustments = self.intelligence_api.get_ranking_adjustments(workload)
                for sc in scored_candidates:
                    if sc.worker_id in adjustments:
                        sc.final_score += adjustments[sc.worker_id]
            except Exception as e:
                logger.warning(f"Intelligence API unavailable, degrading gracefully: {e}")
```
**Explanation:**
- **Advisory Boundary**: The Intelligence API is injected. It returns a dictionary of score modifiers (e.g., `-0.2` if it predicts failure, `+0.1` if it predicts fast execution). 
- **Graceful Degradation**: If the API crashes, the `try/except` swallows the error, logs a warning, and the engine falls back to the deterministic scores.

### 5-17. Core Dimensions
- **Data Flow**: `WorkloadSpec` + cluster state -> `PlacementDecision`.
- **State Changes**: None. This class is purely functional.
- **Error Paths**: If no valid candidates remain, returns `None`.
- **Complexity**: $O(W \times A)$ where W is workers and A is assets per workload, heavily optimized because `_calculate_locality_score` uses O(A) set intersection.

### 18. Code Interview Questions
- **Q1**: What happens if the `IntelligenceAPI` crashes and throws a `TimeoutError`?
- **Q2**: Why is the locality score weighted at `0.6` while memory is `0.4`?
- **Q3**: How would you modify this to support a worker that has a GPU? (Answer: Add a hard constraint check for `workload.execution_tags` matching `worker.resource_profile.tags`).

---
## TRACE: M11 Affinity Evaluation
Workload (affinity="same_worker", group="job-1") submitted
    ↓
**`PlacementEngine.evaluate_workload`**
    ↓
**`_check_hard_constraints`**
*Action*: Looks up "job-1" in affinity registry. Finds Anchor Worker A.
*Action*: Evaluating Worker B -> Returns False (fails affinity).
*Action*: Evaluating Worker A -> Returns True.
    ↓
**`_calculate_locality_score`**
*Action*: Worker A has 100% of assets. Locality = 1.0.
    ↓
**Placement Decision**
*Output*: Worker A selected.
