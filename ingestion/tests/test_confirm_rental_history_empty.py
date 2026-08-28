"""검증된 대여이력 0행 날짜의 수동 승인 경로 테스트."""

from datetime import date

import pytest
from moto import mock_aws

import config as config_module


BUCKET = "test-confirm-empty-bucket"
TARGET_DATE = "2026-08-21"
MANIFEST_KEY = (
    "raw/rental_history/api/target_date=2026-08-21/"
    "observed_at=20260822T060000+0900/snapshot_type=FINAL/manifest.json"
)
PAYLOAD_KEY = MANIFEST_KEY.replace("manifest.json", "payload.json")


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        raw_bucket=BUCKET,
        s3_region="ap-northeast-2",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)

    with mock_aws():
        from common.s3_utils import ensure_bucket

        ensure_bucket(BUCKET)
        yield


def _put_manifest(
    *,
    observed_at="2026-08-22T06:00:00+09:00",
    status="COMPLETE_EMPTY",
    row_count=0,
    rows=None,
    completed_hours=None,
):
    from common.s3_utils import put_json

    hours = list(range(24))
    observed_key = observed_at.replace("-", "").replace(":", "").replace("+09:00", "+0900")
    manifest_key = (
        "raw/rental_history/api/target_date=2026-08-21/"
        f"observed_at={observed_key}/snapshot_type=FINAL/manifest.json"
    )
    payload_key = manifest_key.replace("manifest.json", "payload.json")
    payload = [] if rows is None else rows
    put_json(BUCKET, payload_key, payload)
    put_json(
        BUCKET,
        manifest_key,
        {
            "dataset": "rental_history",
            "target_date": TARGET_DATE,
            "observed_at": observed_at,
            "snapshot_type": "FINAL",
            "status": status,
            "requested_hours": hours,
            "completed_hours": hours if completed_hours is None else completed_hours,
            "page_count": 24,
            "row_count": row_count,
            "schema_valid": None if status == "COMPLETE_EMPTY" else True,
            "payload_key": payload_key,
            "error": None,
        },
    )
    return manifest_key


def _put_complete_empty_manifest(*, completed_hours=None, observed_at="2026-08-22T06:00:00+09:00"):
    return _put_manifest(
        observed_at=observed_at,
        completed_hours=completed_hours,
    )


def _set_previous_watermarks():
    from common.watermark import write_watermark
    from config.watermark_keys import SILVER_RENTAL_HISTORY

    previous = date(2026, 8, 20)
    write_watermark(previous)
    write_watermark(previous, watermark_key=SILVER_RENTAL_HISTORY)


def test_confirmed_empty_records_audit_marker_and_advances_contiguous_watermarks(s3_env):
    from common.s3_utils import get_json
    from common.watermark import read_watermark
    from config.watermark_keys import SILVER_RENTAL_HISTORY
    from jobs import confirm_rental_history_empty as job

    _put_complete_empty_manifest()
    _set_previous_watermarks()

    result = job.run(
        target_date_str=TARGET_DATE,
        confirmed_by="ezzkimm",
        reason="서울 API 24시간 조회 결과 실제 0행 확인",
    )

    assert read_watermark() == date(2026, 8, 21)
    assert read_watermark(watermark_key=SILVER_RENTAL_HISTORY) == date(2026, 8, 21)
    assert result["status"] == "MANUALLY_CONFIRMED_EMPTY"
    assert result["source_manifest_key"] == MANIFEST_KEY

    marker = get_json(BUCKET, job.completion_key(date(2026, 8, 21)))
    assert marker["target_date"] == TARGET_DATE
    assert marker["confirmed_by"] == "ezzkimm"
    assert marker["reason"] == "서울 API 24시간 조회 결과 실제 0행 확인"


def test_confirmed_empty_rejects_non_contiguous_watermark_jump(s3_env):
    from common.watermark import write_watermark
    from config.watermark_keys import SILVER_RENTAL_HISTORY
    from jobs import confirm_rental_history_empty as job

    _put_complete_empty_manifest()
    write_watermark(date(2026, 8, 19))
    write_watermark(date(2026, 8, 20), watermark_key=SILVER_RENTAL_HISTORY)

    with pytest.raises(job.EmptyConfirmationError, match="직전 날짜"):
        job.run(
            target_date_str=TARGET_DATE,
            confirmed_by="ezzkimm",
            reason="서울 API 24시간 조회 결과 실제 0행 확인",
        )


def test_confirmed_empty_rejects_incomplete_hour_range(s3_env):
    from jobs import confirm_rental_history_empty as job

    _put_complete_empty_manifest(completed_hours=list(range(23)))
    _set_previous_watermarks()

    with pytest.raises(job.EmptyConfirmationError, match="24시간"):
        job.run(
            target_date_str=TARGET_DATE,
            confirmed_by="ezzkimm",
            reason="서울 API 24시간 조회 결과 실제 0행 확인",
        )


def test_confirmed_empty_rejects_observation_before_target_day_closed(s3_env):
    from jobs import confirm_rental_history_empty as job

    _put_complete_empty_manifest(observed_at="2026-08-21T06:00:00+09:00")
    _set_previous_watermarks()

    with pytest.raises(job.EmptyConfirmationError, match="날짜가 끝난 뒤"):
        job.run(
            target_date_str=TARGET_DATE,
            confirmed_by="ezzkimm",
            reason="서울 API 24시간 조회 결과 실제 0행 확인",
        )


def test_confirmed_empty_rejects_when_newer_manifest_has_rows(s3_env):
    from jobs import confirm_rental_history_empty as job

    _put_complete_empty_manifest(observed_at="2026-08-22T06:00:00+09:00")
    _put_manifest(
        observed_at="2026-08-23T06:00:00+09:00",
        status="COMPLETE",
        row_count=10,
        rows=[{"BIKE_ID": "SPB-1"}] * 10,
    )
    _set_previous_watermarks()

    with pytest.raises(job.EmptyConfirmationError, match="최신"):
        job.run(
            target_date_str=TARGET_DATE,
            confirmed_by="ezzkimm",
            reason="서울 API 24시간 조회 결과 실제 0행 확인",
        )


def test_confirmed_empty_rejects_when_older_manifest_had_rows(s3_env):
    from jobs import confirm_rental_history_empty as job

    _put_manifest(
        observed_at="2026-08-22T06:00:00+09:00",
        status="COMPLETE",
        row_count=10,
        rows=[{"BIKE_ID": "SPB-1"}] * 10,
    )
    _put_complete_empty_manifest(observed_at="2026-08-23T06:00:00+09:00")
    _set_previous_watermarks()

    with pytest.raises(job.EmptyConfirmationError, match="이전에 데이터"):
        job.run(
            target_date_str=TARGET_DATE,
            confirmed_by="ezzkimm",
            reason="서울 API 24시간 조회 결과 실제 0행 확인",
        )
