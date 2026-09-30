"""M8.7: Blender frame coverage detection.

Covers both the pure interval-math function (compute_frame_coverage) and
the JobRegistry/WorkloadRegistry-backed convenience wrapper
(compute_job_frame_coverage), including the honest limitation that
coverage is chunk-range granularity, not per-frame-number granularity
(see frame_coverage.py's module docstring for why).
"""
from __future__ import annotations

from aidars.adapters.blender.frame_coverage import (
    ChunkCoverage,
    compute_frame_coverage,
    compute_job_frame_coverage,
)
from aidars.distributed.job_registry import JobRegistry
from aidars.distributed.models import WorkloadExecutionResult, WorkloadSpec
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


# ============================================================================
# Pure interval math
# ============================================================================


def test_complete_chunk_coverage():
    report = compute_frame_coverage(1, 300, [
        ChunkCoverage("w1", 1, 100, True),
        ChunkCoverage("w2", 101, 200, True),
        ChunkCoverage("w3", 201, 300, True),
    ])
    assert report.is_complete
    assert report.missing_ranges == []


def test_missing_frame_range_detected():
    report = compute_frame_coverage(1, 300, [
        ChunkCoverage("w1", 1, 100, True),
        ChunkCoverage("w3", 201, 300, True),
    ])
    assert not report.is_complete
    assert report.missing_ranges == [(101, 200)]


def test_missing_at_both_edges():
    report = compute_frame_coverage(1, 300, [ChunkCoverage("w1", 50, 250, True)])
    assert report.missing_ranges == [(1, 49), (251, 300)]


def test_overlapping_duplicate_chunk_coverage_does_not_break_completeness():
    report = compute_frame_coverage(1, 300, [
        ChunkCoverage("w1", 1, 150, True),
        ChunkCoverage("w2", 100, 300, True),  # overlaps w1's tail
    ])
    assert report.is_complete
    assert report.overlapping_ranges == [(100, 150)]


def test_failed_chunk_makes_job_incomplete_even_with_full_range_covered_by_others():
    """A successful individual chunk must not be assumed to mean the whole
    job is valid -- here w2's range is separately covered by nothing, so
    even though w1/w3 succeeded, the job as a whole is incomplete."""
    report = compute_frame_coverage(1, 300, [
        ChunkCoverage("w1", 1, 100, True),
        ChunkCoverage("w2", 101, 200, False),  # failed
        ChunkCoverage("w3", 201, 300, True),
    ])
    assert not report.is_complete
    assert report.missing_ranges == [(101, 200)]
    assert report.incomplete_chunk_workload_ids == ["w2"]


def test_all_chunks_failed():
    report = compute_frame_coverage(1, 100, [ChunkCoverage("w1", 1, 100, False)])
    assert not report.is_complete
    assert report.missing_ranges == [(1, 100)]
    assert report.incomplete_chunk_workload_ids == ["w1"]


# ============================================================================
# WorkloadRegistry/JobRegistry-backed wrapper
# ============================================================================


def _blender_spec(workload_id, job_id, frame_start, frame_end, job_frame_start=1, job_frame_end=300):
    return WorkloadSpec(
        workload_id=workload_id, job_id=job_id, task_type="blender_render", min_ram_bytes=1024,
        parameters={
            "frame_start": frame_start, "frame_end": frame_end,
            "job_frame_start": job_frame_start, "job_frame_end": job_frame_end,
        },
    )


def test_job_frame_coverage_wrapper_complete():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    for wid, fs, fe in [("c0", 1, 100), ("c1", 101, 200), ("c2", 201, 300)]:
        wr.add_workload(_blender_spec(wid, "job-1", fs, fe))
        wr.set_result(wid, WorkloadExecutionResult(
            workload_id=wid, worker_id="w-x", success=True,
            output_asset_hashes=set(), execution_duration_seconds=1.0,
        ))
    jr.create_job("job-1", {"c0", "c1", "c2"})

    report = compute_job_frame_coverage("job-1", jr, wr)
    assert report.is_complete


def test_job_frame_coverage_wrapper_missing_chunk():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    wr.add_workload(_blender_spec("c0", "job-1", 1, 100))
    wr.set_result("c0", WorkloadExecutionResult(
        workload_id="c0", worker_id="w-x", success=True,
        output_asset_hashes=set(), execution_duration_seconds=1.0,
    ))
    # c1 (frames 101-300) was never even submitted -- job membership only
    # references c0, so the wrapper only sees what actually exists.
    jr.create_job("job-1", {"c0"})

    report = compute_job_frame_coverage("job-1", jr, wr)
    assert not report.is_complete
    # job_frame_start/end came from c0's own parameters (1, 300); the
    # wrapper has no way to know a whole second chunk was supposed to
    # exist beyond what the job's declared overall range says is missing.
    assert report.missing_ranges == [(101, 300)]


def test_job_frame_coverage_wrapper_unknown_job_returns_none():
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    assert compute_job_frame_coverage("nope", jr, wr) is None


def test_job_frame_coverage_wrapper_non_frame_job_returns_none():
    """A Job whose member workloads have no frame_start/frame_end
    parameters at all (not a Blender-style job) must not be
    misinterpreted -- returns None rather than a bogus report."""
    wr = WorkloadRegistry()
    jr = JobRegistry(wr)
    wr.add_workload(WorkloadSpec(workload_id="w1", job_id="job-1", task_type="llm_inference", min_ram_bytes=1024))
    jr.create_job("job-1", {"w1"})

    assert compute_job_frame_coverage("job-1", jr, wr) is None
