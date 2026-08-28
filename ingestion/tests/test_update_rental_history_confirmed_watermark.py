"""확정 워터마크 전진 규칙 테스트.

NOTE: test_watermark.py와 같은 이유로 config.SETTINGS.env를 "aws"로 교체해 moto의
가상 AWS를 쓴다 (moto는 커스텀 endpoint_url을 가로채지 못함).
"""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from moto import mock_aws

import config as config_module

KST = ZoneInfo("Asia/Seoul")
BUCKET = "test-watermark-bucket"
CUTOFF = "2026-08-22T06:00:00+09:00"
PROMOTION_ID = "20260822T060000+0900"
PROMOTION_KEY = (
    "_meta/promotion/bronze_rental_history/run_date=2026-08-22/"
    f"promotion_id={PROMOTION_ID}/promotion.json"
)


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        raw_bucket=BUCKET,
        s3_region="ap-northeast-2",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)
    monkeypatch.setenv("COLLECTION_CUTOFF_AT", CUTOFF)
    with mock_aws():
        from common.s3_utils import ensure_bucket

        ensure_bucket(BUCKET)
        yield


def _snapshot(target_date: str, snapshot_type: str, hours: list[int]) -> dict:
    return {
        "target_date": target_date,
        "snapshot_type": snapshot_type,
        "observed_at": CUTOFF,
        "payload_key": f"raw/rental_history/api/target_date={target_date}/x/payload.json",
        "requested_hours": list(hours),
        "row_count": 10,
        "fallback_reason": None,
    }


def _put_promotion(snapshots: list[dict], **overrides) -> dict:
    from common.s3_utils import put_json

    confirmed = [s["target_date"] for s in snapshots if len(s["requested_hours"]) == 24]
    promotion = {
        "dataset": "rental_history",
        "run_date": "2026-08-22",
        "promotion_id": PROMOTION_ID,
        "collection_cutoff_at": CUTOFF,
        "status": "COMPLETE",
        "mode": "NORMAL",
        "required_confirmed_dates": confirmed,
        "current_date_required": any(
            s["target_date"] == "2026-08-22" for s in snapshots
        ),
        "selected_snapshots": snapshots,
        "promoted_partitions": [s["target_date"] for s in snapshots],
        "bronze_row_count_by_partition": {s["target_date"]: 10 for s in snapshots},
        "confirmed_through_candidate": confirmed[-1] if confirmed else None,
        "promotion_reasons": {},
        "promoted_at": "2026-08-21T21:05:00+00:00",
    }
    promotion.update(overrides)
    put_json(BUCKET, PROMOTION_KEY, promotion)
    return promotion


def _set_watermark(watermark: date) -> None:
    from common.watermark import write_watermark

    write_watermark(watermark)


def _current_watermark() -> date:
    from common.watermark import read_watermark

    return read_watermark()


def test_full_day_preliminary_advances_confirmed_watermark(s3_env):
    from jobs import update_rental_history_confirmed_watermark as watermark_job

    _set_watermark(date(2026, 8, 20))
    _put_promotion([_snapshot("2026-08-21", "PRELIMINARY", list(range(24)))])

    result = watermark_job.run()

    assert _current_watermark() == date(2026, 8, 21)
    assert result["before"] == "2026-08-20"
    assert result["after"] == "2026-08-21"
    assert result["noop"] is False
    assert result["confirmed_partitions"] == ["2026-08-21"]


def test_t0_partial_partition_does_not_advance_watermark(s3_env):
    from jobs import update_rental_history_confirmed_watermark as watermark_job

    _set_watermark(date(2026, 8, 20))
    _put_promotion(
        [
            _snapshot("2026-08-21", "FINAL", list(range(24))),
            _snapshot("2026-08-22", "FINAL", [0, 1, 2, 3, 4, 5]),
        ]
    )

    result = watermark_job.run()

    assert _current_watermark() == date(2026, 8, 21)
    assert result["after"] == "2026-08-21"
    assert result["confirmed_partitions"] == ["2026-08-21"]


def test_gap_is_not_skipped(s3_env):
    from jobs import update_rental_history_confirmed_watermark as watermark_job

    _set_watermark(date(2026, 8, 18))
    _put_promotion([_snapshot("2026-08-21", "FINAL", list(range(24)))])

    result = watermark_job.run()

    assert _current_watermark() == date(2026, 8, 18)
    assert result["noop"] is True
    assert result["after"] == "2026-08-18"


def test_contiguous_backlog_advances_to_the_last_day(s3_env):
    from jobs import update_rental_history_confirmed_watermark as watermark_job

    _set_watermark(date(2026, 8, 17))
    _put_promotion(
        [
            _snapshot("2026-08-18", "FINAL", list(range(24))),
            _snapshot("2026-08-19", "FINAL", list(range(24))),
            _snapshot("2026-08-20", "FINAL", list(range(24))),
        ]
    )

    watermark_job.run()

    assert _current_watermark() == date(2026, 8, 20)


def test_watermark_never_regresses(s3_env):
    from jobs import update_rental_history_confirmed_watermark as watermark_job

    _set_watermark(date(2026, 8, 21))
    _put_promotion([_snapshot("2026-08-19", "FINAL", list(range(24)))])

    result = watermark_job.run()

    assert _current_watermark() == date(2026, 8, 21)
    assert result["noop"] is True


def test_empty_promotion_is_a_noop(s3_env):
    from jobs import update_rental_history_confirmed_watermark as watermark_job

    _set_watermark(date(2026, 8, 21))
    _put_promotion([])

    result = watermark_job.run()

    assert _current_watermark() == date(2026, 8, 21)
    assert result["noop"] is True


def test_missing_promotion_marker_is_rejected(s3_env):
    from jobs import update_rental_history_confirmed_watermark as watermark_job

    _set_watermark(date(2026, 8, 20))

    with pytest.raises(watermark_job.ConfirmedWatermarkError, match="promotion"):
        watermark_job.run()

    assert _current_watermark() == date(2026, 8, 20)


def test_incomplete_promotion_marker_is_rejected(s3_env):
    from jobs import update_rental_history_confirmed_watermark as watermark_job

    _set_watermark(date(2026, 8, 20))
    _put_promotion(
        [_snapshot("2026-08-21", "FINAL", list(range(24)))], status="IN_PROGRESS"
    )

    with pytest.raises(watermark_job.ConfirmedWatermarkError, match="COMPLETE"):
        watermark_job.run()

    assert _current_watermark() == date(2026, 8, 20)


def test_partition_missing_from_commit_marker_is_not_confirmed(s3_env):
    from jobs import update_rental_history_confirmed_watermark as watermark_job

    _set_watermark(date(2026, 8, 20))
    _put_promotion(
        [_snapshot("2026-08-21", "FINAL", list(range(24)))], promoted_partitions=[]
    )

    result = watermark_job.run()

    assert _current_watermark() == date(2026, 8, 20)
    assert result["noop"] is True
