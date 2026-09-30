"""M8.7: Blender-specific frame coverage detection.

Deliberately lives in the Blender adapter package, not in
aidars.distributed (the generic orchestration core), per the PRD's
instruction that frame coverage "must remain generic enough that
non-Blender workloads do not inherit Blender-specific assumptions" --
concretely, it belongs in "an application-specific completion contract
... not hardcoded into the generic scheduler." JobRegistry/JobAggregate
know nothing about frames; this module is a separate, optional
consumer of the same durable facts (WorkloadSpec.parameters,
WorkloadRecord.state) that a caller who *knows* it's dealing with a
Blender job can invoke.

Known limitation (documented, not silently assumed): coverage here is
computed at chunk-range granularity, not individual frame-number
granularity within a chunk. ExecutionManager's output ingestion
(distributed/execution.py) hashes output files into CAS but does not
preserve their original filenames anywhere in WorkloadExecutionResult,
so there is no way to reconstruct "which specific frame numbers a
chunk's output_asset_hashes correspond to" from data the system
actually keeps. What IS fully verifiable from existing data: (a) does
the union of member chunks' declared frame_start/frame_end ranges cover
the job's whole requested range with no gaps, and (b) did every chunk
in that union actually reach WorkloadState.COMPLETED (already gated by
M8.6's expected_output_count check at execution time, so a COMPLETED
chunk is trusted to have produced its declared frame_count of output).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from aidars.distributed.job_registry import JobRegistry
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState


@dataclass
class ChunkCoverage:
    """One member chunk's declared frame range and completion status."""

    workload_id: str
    frame_start: int
    frame_end: int
    is_complete: bool


@dataclass
class FrameCoverageReport:
    job_frame_start: int
    job_frame_end: int
    missing_ranges: List[Tuple[int, int]] = field(default_factory=list)
    overlapping_ranges: List[Tuple[int, int]] = field(default_factory=list)
    incomplete_chunk_workload_ids: List[str] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return not self.missing_ranges and not self.incomplete_chunk_workload_ids


def compute_frame_coverage(
    job_frame_start: int, job_frame_end: int, chunks: List[ChunkCoverage],
) -> FrameCoverageReport:
    """Pure function: given the job's declared frame range and each
    member chunk's declared range + completion status, determine which
    sub-ranges of [job_frame_start, job_frame_end] are NOT covered by a
    COMPLETED chunk. A successful individual chunk never implies the
    complete job is covered -- this checks the union explicitly."""
    incomplete_ids = [c.workload_id for c in chunks if not c.is_complete]

    complete_ranges = sorted(
        (c.frame_start, c.frame_end) for c in chunks if c.is_complete
    )

    merged: List[Tuple[int, int]] = []
    overlaps: List[Tuple[int, int]] = []
    for start, end in complete_ranges:
        if merged and start <= merged[-1][1] + 1:
            if start <= merged[-1][1]:
                overlaps.append((start, min(end, merged[-1][1])))
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    missing: List[Tuple[int, int]] = []
    cursor = job_frame_start
    for start, end in merged:
        if start > cursor:
            missing.append((cursor, min(start - 1, job_frame_end)))
        cursor = max(cursor, end + 1)
        if cursor > job_frame_end:
            break
    if cursor <= job_frame_end:
        missing.append((cursor, job_frame_end))
    missing = [(s, e) for (s, e) in missing if s <= e]

    return FrameCoverageReport(
        job_frame_start=job_frame_start,
        job_frame_end=job_frame_end,
        missing_ranges=missing,
        overlapping_ranges=overlaps,
        incomplete_chunk_workload_ids=incomplete_ids,
    )


def compute_job_frame_coverage(
    job_id: str, job_registry: JobRegistry, workload_registry: WorkloadRegistry,
) -> Optional[FrameCoverageReport]:
    """Convenience wrapper: pull chunk coverage inputs for `job_id` out of
    the existing JobRegistry/WorkloadRegistry facts. Returns None if the
    job doesn't exist or none of its member workloads declare frame
    parameters (i.e. it isn't a frame-range-style job at all -- not every
    Job is a Blender render)."""
    job = job_registry.get_job(job_id)
    if job is None:
        return None

    chunks: List[ChunkCoverage] = []
    job_frame_start: Optional[int] = None
    job_frame_end: Optional[int] = None

    for workload_id in job.workload_ids:
        record = workload_registry.get_workload(workload_id)
        if record is None:
            continue
        params = record.spec.parameters
        frame_start = params.get("frame_start")
        frame_end = params.get("frame_end")
        if frame_start is None or frame_end is None:
            continue

        if job_frame_start is None:
            job_frame_start = params.get("job_frame_start", frame_start)
            job_frame_end = params.get("job_frame_end", frame_end)

        chunks.append(ChunkCoverage(
            workload_id=workload_id,
            frame_start=frame_start,
            frame_end=frame_end,
            is_complete=(record.state == WorkloadState.COMPLETED),
        ))

    if job_frame_start is None or job_frame_end is None:
        return None

    return compute_frame_coverage(job_frame_start, job_frame_end, chunks)
