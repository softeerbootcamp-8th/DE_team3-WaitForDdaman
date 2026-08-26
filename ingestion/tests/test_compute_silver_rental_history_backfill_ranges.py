"""silver_rental_history 백필 청크 계획 계산 테스트 (#232).

경계 계산(build_ranges)과 완료 marker 재사용(select_pending_ranges/build_plan)을 나눠서
검증한다. 전자는 순수 계산이고, 후자는 "무엇을 다시 돌지 않아도 되는가"를 정하는 판단이라
틀렸을 때의 결과가 정반대다(재계산 낭비 vs 데이터 유실).
"""
import json
from datetime import date
from unittest import mock

import pytest
from moto import mock_aws

import config as config_module
import jobs.compute_silver_rental_history_backfill_ranges as compute_ranges
from common.s3_utils import ensure_bucket
from common.silver_rental_history_completion import (
    SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
    build_completion_marker,
    write_completion_marker,
)

BUCKET = "test-silver-plan-bucket"


@pytest.fixture
def s3_env(monkeypatch):
    monkeypatch.setattr(
        config_module,
        "SETTINGS",
        config_module.Settings(env="aws", raw_bucket=BUCKET, s3_region="ap-northeast-2"),
    )
    with mock_aws():
        ensure_bucket(BUCKET)
        yield


def _mark_complete(start: str, end: str, **overrides) -> None:
    marker = build_completion_marker(
        range_start=start,
        range_end=end,
        bronze_watermark_at_start="2017-03-29",
        bronze_row_count=10,
        silver_row_count=10,
        quarantine_row_count=0,
        dag_run_id="manual__old-run",
        processed_at="2026-08-26T00:00:00+00:00",
        **overrides,
    )
    write_completion_marker(BUCKET, marker)


def test_splits_into_chunk_days_windows():
    ranges = compute_ranges.build_ranges(
        silver_watermark=date(2017, 2, 26),
        bronze_watermark=date(2017, 3, 29),
        chunk_days=10,
        total_days_cap=3650,
    )
    assert ranges == [
        {"start": "2017-02-27", "end": "2017-03-08"},
        {"start": "2017-03-09", "end": "2017-03-18"},
        {"start": "2017-03-19", "end": "2017-03-28"},
        {"start": "2017-03-29", "end": "2017-03-29"},
    ]


def test_returns_empty_when_silver_caught_up():
    ranges = compute_ranges.build_ranges(
        silver_watermark=date(2017, 3, 29),
        bronze_watermark=date(2017, 3, 29),
        chunk_days=31,
        total_days_cap=3650,
    )
    assert ranges == []


def test_total_days_cap_limits_total_span():
    ranges = compute_ranges.build_ranges(
        silver_watermark=date(2017, 1, 1),
        bronze_watermark=date(2020, 1, 1),
        chunk_days=31,
        total_days_cap=31,
    )
    assert ranges == [{"start": "2017-01-02", "end": "2017-02-01"}]


def test_run_prints_plan_json(s3_env, capsys):
    with mock.patch.object(
        compute_ranges, "read_watermark", side_effect=[date(2017, 3, 29), date(2017, 2, 26)]
    ):
        compute_ranges.run(chunk_days=31, total_days_cap=3650)
    plan = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert plan["silver_watermark_before"] == "2017-02-26"
    assert plan["bronze_watermark_at_start"] == "2017-03-29"
    assert plan["contract_version"] == SILVER_RENTAL_HISTORY_CONTRACT_VERSION
    assert plan["all_ranges"] == [{"start": "2017-02-27", "end": "2017-03-29"}]
    assert plan["pending_ranges"] == plan["all_ranges"]


# ---------------------------------------------------------------- 완료 marker 재사용


def test_completed_ranges_are_dropped_from_pending(s3_env):
    _mark_complete("2017-02-27", "2017-03-08")
    plan = compute_ranges.build_plan(
        silver_watermark=date(2017, 2, 26),
        bronze_watermark=date(2017, 3, 29),
        chunk_days=10,
        total_days_cap=3650,
        bucket=BUCKET,
    )
    assert len(plan["all_ranges"]) == 4
    assert {"start": "2017-02-27", "end": "2017-03-08"} not in plan["pending_ranges"]
    assert len(plan["pending_ranges"]) == 3


def test_marker_from_another_dag_run_is_still_reused(s3_env):
    """dag_run_id는 감사 정보다 - 다르다고 재처리하면 marker가 아무 일도 못 한다."""
    _mark_complete("2017-02-27", "2017-03-08")
    pending = compute_ranges.select_pending_ranges(
        BUCKET, [{"start": "2017-02-27", "end": "2017-03-08"}],
        SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
    )
    assert pending == []


def test_other_contract_version_marker_is_not_reused(s3_env):
    _mark_complete("2017-02-27", "2017-03-08", contract_version=99)
    pending = compute_ranges.select_pending_ranges(
        BUCKET, [{"start": "2017-02-27", "end": "2017-03-08"}],
        SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
    )
    assert pending == [{"start": "2017-02-27", "end": "2017-03-08"}]


def test_changed_chunk_days_invalidates_old_markers(s3_env):
    """CHUNK_DAYS를 바꾸면 경계가 달라져 이전 marker가 매칭되지 않는다 - 전부 재처리한다."""
    _mark_complete("2017-02-27", "2017-03-08")  # chunk_days=10 시절 경계
    plan = compute_ranges.build_plan(
        silver_watermark=date(2017, 2, 26),
        bronze_watermark=date(2017, 3, 29),
        chunk_days=31,
        total_days_cap=3650,
        bucket=BUCKET,
    )
    assert plan["all_ranges"] == [{"start": "2017-02-27", "end": "2017-03-29"}]
    assert plan["pending_ranges"] == plan["all_ranges"]


def test_all_completed_leaves_pending_empty(s3_env):
    for start, end in [("2017-02-27", "2017-03-08"), ("2017-03-09", "2017-03-18"),
                       ("2017-03-19", "2017-03-28"), ("2017-03-29", "2017-03-29")]:
        _mark_complete(start, end)
    plan = compute_ranges.build_plan(
        silver_watermark=date(2017, 2, 26),
        bronze_watermark=date(2017, 3, 29),
        chunk_days=10,
        total_days_cap=3650,
        bucket=BUCKET,
    )
    assert plan["pending_ranges"] == []
    assert len(plan["all_ranges"]) == 4
