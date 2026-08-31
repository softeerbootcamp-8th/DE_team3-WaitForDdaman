"""Silver 대여이력 초기 적재 finalizer 테스트.

이 잡의 핵심 성질은 두 가지다.
  1. 워터마크는 연속 COMPLETE 구간을 절대 앞서가지 않는다 (앞서가면 처리되지 않은 날짜가
     "확정"으로 취급돼 Gold까지 조용히 비어버린다).
  2. 공백이 있어도 직전까지는 전진시킨 뒤 실패한다 (전진 자체를 포기하면 118청크 중
     117개 성공분이 다음 Run에서 통째로 재계산된다).
"""
import json
from datetime import date

import pytest
from moto import mock_aws

import config as config_module
import operations.advance_silver_rental_history_watermark as finalizer
from common.s3_utils import ensure_bucket
from common.silver_rental_history_completion import (
    SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
    build_completion_marker,
    write_completion_marker,
)
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import SILVER_RENTAL_HISTORY

BUCKET = "test-silver-finalizer-bucket"

RANGES = [
    {"start": "2015-01-01", "end": "2015-01-31"},
    {"start": "2015-02-01", "end": "2015-03-03"},
    {"start": "2015-03-04", "end": "2015-04-03"},
    {"start": "2015-04-04", "end": "2015-05-04"},
]


@pytest.fixture
def s3_env(monkeypatch):
    monkeypatch.setattr(
        config_module,
        "SETTINGS",
        config_module.Settings(env="aws", raw_bucket=BUCKET, s3_region="ap-northeast-2"),
    )
    with mock_aws():
        ensure_bucket(BUCKET)
        write_watermark(date(2014, 12, 31), watermark_key=SILVER_RENTAL_HISTORY)
        yield


def _mark_complete(chunk: dict, **overrides) -> None:
    write_completion_marker(
        BUCKET,
        build_completion_marker(
            range_start=chunk["start"],
            range_end=chunk["end"],
            bronze_watermark_at_start="2015-05-04",
            bronze_row_count=1,
            silver_row_count=1,
            quarantine_row_count=0,
            dag_run_id="manual__run",
            processed_at="2026-08-26T00:00:00+00:00",
            **overrides,
        ),
    )


def _set_plan(monkeypatch, all_ranges: list[dict]) -> None:
    monkeypatch.setenv(
        "SILVER_BACKFILL_PLAN",
        json.dumps(
            {
                "silver_watermark_before": "2014-12-31",
                "bronze_watermark_at_start": "2015-05-04",
                "contract_version": SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
                "all_ranges": all_ranges,
                "pending_ranges": all_ranges,
            }
        ),
    )


def _watermark() -> date:
    return read_watermark(watermark_key=SILVER_RENTAL_HISTORY)


def test_all_complete_advances_to_plan_upper_bound(s3_env, monkeypatch):
    for chunk in RANGES:
        _mark_complete(chunk)
    _set_plan(monkeypatch, RANGES)

    result = finalizer.run()

    assert _watermark() == date(2015, 5, 4)  # = bronze_watermark_at_start
    assert result["after"] == "2015-05-04"
    assert result["confirmed_range_count"] == 4
    assert result["first_incomplete_range"] is None
    assert result["noop"] is False


def test_gap_advances_only_to_previous_range_and_fails(s3_env, monkeypatch):
    """range 1,2 COMPLETE / 3 실패 / 4 COMPLETE -> 워터마크는 range 2 끝, 태스크는 실패."""
    _mark_complete(RANGES[0])
    _mark_complete(RANGES[1])
    _mark_complete(RANGES[3])
    _set_plan(monkeypatch, RANGES)

    with pytest.raises(finalizer.IncompleteBackfillError, match="2015-03-04~2015-04-03"):
        finalizer.run()

    assert _watermark() == date(2015, 3, 3)


def test_gap_preserves_markers_after_the_gap(s3_env, monkeypatch):
    """공백 뒤 COMPLETE marker는 지우지 않는다 - 다음 Run이 그대로 재사용한다."""
    _mark_complete(RANGES[0])
    _mark_complete(RANGES[1])
    _mark_complete(RANGES[3])
    _set_plan(monkeypatch, RANGES)
    with pytest.raises(finalizer.IncompleteBackfillError):
        finalizer.run()

    # 다음 실행: 공백이던 range 3만 처리되고 range 4는 marker 재사용으로 끝까지 전진한다.
    _mark_complete(RANGES[2])
    result = finalizer.run()

    assert _watermark() == date(2015, 5, 4)
    assert result["confirmed_range_count"] == 2  # 이미 워터마크가 덮은 앞 2개는 건너뛴다


def test_no_ranges_is_a_noop_success(s3_env, monkeypatch):
    _set_plan(monkeypatch, [])

    result = finalizer.run()

    assert result["noop"] is True
    assert _watermark() == date(2014, 12, 31)


def test_first_range_missing_marker_leaves_watermark_untouched(s3_env, monkeypatch):
    _mark_complete(RANGES[1])
    _set_plan(monkeypatch, RANGES)

    with pytest.raises(finalizer.IncompleteBackfillError):
        finalizer.run()

    assert _watermark() == date(2014, 12, 31)


def test_plan_upper_bound_is_used_even_if_bronze_watermark_moved(s3_env, monkeypatch):
    """DAG 실행 중 Bronze가 더 전진해도 이번 finalizer는 계획에 고정된 구간만 본다."""
    for chunk in RANGES:
        _mark_complete(chunk)
    write_watermark(date(2026, 6, 30))  # Bronze 워터마크(기본 키)가 더 전진한 상황
    _set_plan(monkeypatch, RANGES)

    finalizer.run()

    assert _watermark() == date(2015, 5, 4)


def test_other_contract_version_marker_is_not_confirmed(s3_env, monkeypatch):
    _mark_complete(RANGES[0], contract_version=99)
    _set_plan(monkeypatch, RANGES)

    with pytest.raises(finalizer.IncompleteBackfillError):
        finalizer.run()

    assert _watermark() == date(2014, 12, 31)


def test_missing_plan_env_is_a_clear_error(s3_env, monkeypatch):
    monkeypatch.delenv("SILVER_BACKFILL_PLAN", raising=False)
    with pytest.raises(ValueError, match="SILVER_BACKFILL_PLAN"):
        finalizer.run()


def test_non_json_plan_is_a_clear_error(s3_env, monkeypatch):
    """상류 planner가 실패해 XCom이 비면 'None' 같은 값이 그대로 렌더링돼 넘어온다."""
    monkeypatch.setenv("SILVER_BACKFILL_PLAN", "None")
    with pytest.raises(ValueError, match="JSON으로 읽을 수 없음"):
        finalizer.run()
