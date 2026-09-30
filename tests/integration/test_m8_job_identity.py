"""M8.1: workload_id collision resistance and job_id propagation in
BlenderAdapter.evaluate_request().

Separate file from tests/integration/test_adapters.py (not modified) per
the audit's finding that the old workload_id = md5(input_path +
frame_start + frame_end) scheme collides across independently-submitted
requests for the same file/frame-range -- this proves the fix.
"""
from __future__ import annotations

from pathlib import Path

from aidars.adapters.blender.adapter import BlenderAdapter

BLENDER_FIXTURE_PATH = str(Path(__file__).resolve().parent.parent / "fixtures" / "blender_adapter_scene.json")


def _request(**overrides):
    defaults = dict(input_path=BLENDER_FIXTURE_PATH, frame_start=1, frame_end=250, requires_gpu=False)
    defaults.update(overrides)
    return defaults


def test_repeated_identical_requests_do_not_collide():
    """The old md5(input_path + frame_start + frame_end) scheme produced
    the exact same workload_id for two independent submissions of the
    same file/range -- a second submission would silently be treated as
    a re-registration of the first workload rather than a new one."""
    adapter = BlenderAdapter()
    specs_1 = adapter.evaluate_request(_request())
    specs_2 = adapter.evaluate_request(_request())

    ids_1 = {s.workload_id for s in specs_1}
    ids_2 = {s.workload_id for s in specs_2}
    assert ids_1.isdisjoint(ids_2)


def test_all_chunks_from_one_call_share_a_job_id():
    adapter = BlenderAdapter()
    specs = adapter.evaluate_request(_request(worker_count=3))

    job_ids = {s.job_id for s in specs}
    assert len(job_ids) == 1
    assert job_ids.pop() is not None


def test_two_separate_calls_get_different_job_ids():
    adapter = BlenderAdapter()
    specs_1 = adapter.evaluate_request(_request())
    specs_2 = adapter.evaluate_request(_request())

    assert specs_1[0].job_id != specs_2[0].job_id


def test_workload_id_is_prefixed_by_its_job_id():
    adapter = BlenderAdapter()
    specs = adapter.evaluate_request(_request(worker_count=2))
    for spec in specs:
        assert spec.workload_id.startswith(spec.job_id)


def test_expected_output_count_matches_chunk_frame_count():
    adapter = BlenderAdapter()
    specs = adapter.evaluate_request(_request(frame_start=1, frame_end=250, worker_count=2))

    for spec in specs:
        frame_start = spec.parameters["frame_start"]
        frame_end = spec.parameters["frame_end"]
        assert spec.parameters["expected_output_count"] == (frame_end - frame_start + 1)


def test_job_frame_range_matches_original_request_on_every_chunk():
    adapter = BlenderAdapter()
    specs = adapter.evaluate_request(_request(frame_start=10, frame_end=200, worker_count=3))

    for spec in specs:
        assert spec.parameters["job_frame_start"] == 10
        assert spec.parameters["job_frame_end"] == 200
