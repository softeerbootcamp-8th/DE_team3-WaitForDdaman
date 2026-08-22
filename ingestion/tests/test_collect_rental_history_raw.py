import threading
import time
from datetime import date

import pytest

from jobs import collect_rental_history_raw as raw_job


VALID_ROW = {
    "BIKE_ID": "SPB-1",
    "RENT_DT": "2026-08-22 04:00:00",
    "RENT_ID": "101",
    "RTN_DT": "2026-08-22 04:10:00",
    "RTN_ID": "102",
    "USE_MIN": "10",
    "USE_DST": "1234.5",
    "START_INDEX": 1,
    "END_INDEX": 1,
    "RNUM": "1",
}


def _recording_writer():
    writes = []

    def write_json(key, payload):
        writes.append((key, payload))

    return writes, write_json


def test_parse_collection_cutoff_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone"):
        raw_job.parse_collection_cutoff("2026-08-22T05:00:00")


def test_collection_windows_include_previous_full_day_and_closed_current_hours():
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")

    assert raw_job.build_collection_windows(cutoff) == [
        (date(2026, 8, 21), list(range(24))),
        (date(2026, 8, 22), [0, 1, 2, 3, 4]),
    ]


def test_collection_windows_omit_empty_current_window_at_midnight():
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T00:00:00+09:00")

    assert raw_job.build_collection_windows(cutoff) == [
        (date(2026, 8, 21), list(range(24))),
    ]


def test_snapshot_keys_are_partitioned_by_target_observation_and_type():
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")

    payload_key, manifest_key = raw_job.snapshot_keys(
        date(2026, 8, 22), cutoff, "PRELIMINARY"
    )

    prefix = (
        "raw/rental_history/api/target_date=2026-08-22/"
        "observed_at=20260822T050000+0900/snapshot_type=PRELIMINARY/"
    )
    assert payload_key == f"{prefix}payload.json"
    assert manifest_key == f"{prefix}manifest.json"


def test_snapshot_keys_reject_unknown_snapshot_type():
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")

    with pytest.raises(ValueError, match="snapshot_type"):
        raw_job.snapshot_keys(date(2026, 8, 22), cutoff, "BACKUP")


def test_collect_snapshot_writes_unmodified_payload_before_complete_manifest():
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")
    writes, write_json = _recording_writer()

    def fetch_pages(target_date, hour):
        assert target_date == date(2026, 8, 22)
        yield [dict(VALID_ROW, REQUEST_HOUR=hour)]

    manifest = raw_job.collect_snapshot(
        target_date=date(2026, 8, 22),
        hours=[0, 1],
        observed_at=cutoff,
        snapshot_type="PRELIMINARY",
        fetch_pages=fetch_pages,
        write_json=write_json,
    )

    assert [key.rsplit("/", 1)[-1] for key, _ in writes] == [
        "payload.json",
        "manifest.json",
    ]
    assert writes[0][1] == [
        dict(VALID_ROW, REQUEST_HOUR=0),
        dict(VALID_ROW, REQUEST_HOUR=1),
    ]
    assert writes[0][1][0]["START_INDEX"] == 1
    assert manifest["status"] == "COMPLETE"
    assert manifest["requested_hours"] == [0, 1]
    assert manifest["completed_hours"] == [0, 1]
    assert manifest["page_count"] == 2
    assert manifest["row_count"] == 2
    assert manifest["schema_valid"] is True
    assert manifest["error"] is None
    assert writes[1][1] == manifest


def test_collect_snapshot_reuses_existing_complete_snapshot_without_rewriting():
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")
    payload_key, manifest_key = raw_job.snapshot_keys(
        date(2026, 8, 22), cutoff, "PRELIMINARY"
    )
    existing_manifest = {
        "dataset": "rental_history",
        "target_date": "2026-08-22",
        "observed_at": "2026-08-22T05:00:00+09:00",
        "snapshot_type": "PRELIMINARY",
        "status": "COMPLETE",
        "requested_hours": [0, 1],
        "completed_hours": [0, 1],
        "page_count": 2,
        "row_count": 2,
        "schema_valid": True,
        "payload_key": payload_key,
        "error": None,
    }

    def fetch_pages(target_date, hour):
        raise AssertionError("committed snapshot must not call the API again")

    def write_json(key, payload):
        raise AssertionError("committed snapshot must not be overwritten")

    manifest = raw_job.collect_snapshot(
        target_date=date(2026, 8, 22),
        hours=[0, 1],
        observed_at=cutoff,
        snapshot_type="PRELIMINARY",
        fetch_pages=fetch_pages,
        write_json=write_json,
        read_json=lambda key: existing_manifest if key == manifest_key else None,
    )

    assert manifest == existing_manifest


def test_collect_snapshot_does_not_reuse_complete_manifest_for_different_hours():
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")
    writes, write_json = _recording_writer()

    def fetch_pages(target_date, hour):
        yield [dict(VALID_ROW, REQUEST_HOUR=hour)]

    manifest = raw_job.collect_snapshot(
        target_date=date(2026, 8, 22),
        hours=[0, 1],
        observed_at=cutoff,
        snapshot_type="PRELIMINARY",
        fetch_pages=fetch_pages,
        write_json=write_json,
        read_json=lambda key: {
            "status": "COMPLETE",
            "schema_valid": True,
            "requested_hours": [0],
        },
    )

    assert manifest["requested_hours"] == [0, 1]
    assert [key.rsplit("/", 1)[-1] for key, _ in writes] == [
        "payload.json",
        "manifest.json",
    ]


def test_collect_snapshot_marks_successful_zero_rows_complete_empty():
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")
    writes, write_json = _recording_writer()

    def fetch_pages(target_date, hour):
        yield []

    manifest = raw_job.collect_snapshot(
        date(2026, 8, 22),
        [0, 1],
        cutoff,
        "PRELIMINARY",
        fetch_pages,
        write_json,
    )

    assert manifest["status"] == "COMPLETE_EMPTY"
    assert manifest["completed_hours"] == [0, 1]
    assert manifest["page_count"] == 2
    assert manifest["row_count"] == 0
    assert manifest["schema_valid"] is False


def test_collect_snapshot_marks_partial_api_failure_incomplete():
    """한 시간대 실패는 다른 시간대 처리를 막지 않고, 그 시간대만 completed_hours에서 빠진다."""
    from common.api_client import SeoulApiError

    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")
    writes, write_json = _recording_writer()

    def fetch_pages(target_date, hour):
        if hour == 1:
            raise SeoulApiError("hour 1 failed")
        yield [dict(VALID_ROW)]

    manifest = raw_job.collect_snapshot(
        date(2026, 8, 22),
        [0, 1, 2],
        cutoff,
        "PRELIMINARY",
        fetch_pages,
        write_json,
    )

    assert manifest["status"] == "INCOMPLETE"
    assert manifest["completed_hours"] == [0, 2]
    assert manifest["page_count"] == 2
    assert manifest["row_count"] == 2
    assert "hour 1 failed" in manifest["error"]
    assert writes[0][1] == [VALID_ROW, VALID_ROW]


def test_collect_snapshot_preserves_partial_pages_of_a_failing_hour():
    """실패한 시간대라도 실패 전에 성공한 페이지의 행은 payload에서 숨기지 않는다."""
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")
    writes, write_json = _recording_writer()

    def fetch_pages(target_date, hour):
        if hour == 1:
            yield [dict(VALID_ROW, REQUEST_HOUR=1, PAGE=1)]
            raise raw_job.SeoulApiError("hour 1 second page failed")
        yield [dict(VALID_ROW, REQUEST_HOUR=hour)]

    manifest = raw_job.collect_snapshot(
        date(2026, 8, 22),
        [0, 1],
        cutoff,
        "PRELIMINARY",
        fetch_pages,
        write_json,
    )

    assert manifest["status"] == "INCOMPLETE"
    assert manifest["completed_hours"] == [0]
    assert manifest["page_count"] == 2
    assert manifest["row_count"] == 2
    assert dict(VALID_ROW, REQUEST_HOUR=1, PAGE=1) in writes[0][1]


def test_collect_snapshot_runs_requested_hours_concurrently_up_to_max_eight():
    """요청된 시간대 API 호출이 최대 8개 동시성 이내에서 실제 병렬 실행된다."""
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")
    _, write_json = _recording_writer()

    lock = threading.Lock()
    active = 0
    max_active = 0

    def fetch_pages(target_date, hour):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.05)
            yield [dict(VALID_ROW, REQUEST_HOUR=hour)]
        finally:
            with lock:
                active -= 1

    manifest = raw_job.collect_snapshot(
        date(2026, 8, 22),
        list(range(12)),
        cutoff,
        "PRELIMINARY",
        fetch_pages,
        write_json,
    )

    assert manifest["status"] == "COMPLETE"
    assert manifest["completed_hours"] == list(range(12))
    assert 1 < max_active <= 8


def test_collect_snapshot_combines_hours_in_ascending_order_regardless_of_finish_order():
    """완료 순서가 뒤섞여도 payload/manifest는 항상 시간순으로 결정적으로 결합된다."""
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")
    writes, write_json = _recording_writer()

    # 시간대가 낮을수록 늦게 끝나도록 하여 완료 순서와 시간 순서를 어긋나게 만든다.
    def fetch_pages(target_date, hour):
        time.sleep(0.01 * (5 - hour))
        yield [dict(VALID_ROW, REQUEST_HOUR=hour)]

    manifest = raw_job.collect_snapshot(
        date(2026, 8, 22),
        [0, 1, 2, 3, 4],
        cutoff,
        "PRELIMINARY",
        fetch_pages,
        write_json,
    )

    assert manifest["completed_hours"] == [0, 1, 2, 3, 4]
    assert writes[0][1] == [dict(VALID_ROW, REQUEST_HOUR=h) for h in range(5)]


def test_collect_snapshot_records_schema_mismatch_separately_from_api_completeness():
    cutoff = raw_job.parse_collection_cutoff("2026-08-22T05:00:00+09:00")
    writes, write_json = _recording_writer()

    def fetch_pages(target_date, hour):
        yield [{"BIKE_ID": "SPB-1", "RENT_DT": "2026-08-22 04:00:00"}]

    manifest = raw_job.collect_snapshot(
        date(2026, 8, 22),
        [4],
        cutoff,
        "PRELIMINARY",
        fetch_pages,
        write_json,
    )

    assert manifest["status"] == "COMPLETE"
    assert manifest["schema_valid"] is False
    assert "필수 컬럼 누락" in manifest["error"]


def test_run_collects_previous_full_day_and_current_closed_hours(monkeypatch):
    calls = []

    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-22T05:00:00+09:00")
    monkeypatch.setenv("SNAPSHOT_TYPE", "PRELIMINARY")
    monkeypatch.setattr(raw_job, "ensure_bucket", lambda bucket: calls.append(("bucket", bucket)))

    def fake_collect_snapshot(
        target_date,
        hours,
        observed_at,
        snapshot_type,
        fetch_pages,
        write_json,
        read_json,
    ):
        calls.append((target_date, hours, observed_at.isoformat(), snapshot_type))
        return {"status": "COMPLETE", "schema_valid": True}

    monkeypatch.setattr(raw_job, "collect_snapshot", fake_collect_snapshot)

    manifests = raw_job.run()

    assert calls[1:] == [
        (date(2026, 8, 21), list(range(24)), "2026-08-22T05:00:00+09:00", "PRELIMINARY"),
        (date(2026, 8, 22), [0, 1, 2, 3, 4], "2026-08-22T05:00:00+09:00", "PRELIMINARY"),
    ]
    assert manifests == [
        {"status": "COMPLETE", "schema_valid": True},
        {"status": "COMPLETE", "schema_valid": True},
    ]


def test_run_fails_after_all_windows_when_any_snapshot_is_unusable(monkeypatch):
    processed_dates = []

    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-22T05:00:00+09:00")
    monkeypatch.setenv("SNAPSHOT_TYPE", "PRELIMINARY")
    monkeypatch.setattr(raw_job, "ensure_bucket", lambda bucket: None)

    def fake_collect_snapshot(
        target_date,
        hours,
        observed_at,
        snapshot_type,
        fetch_pages,
        write_json,
        read_json,
    ):
        processed_dates.append(target_date)
        if target_date == date(2026, 8, 21):
            return {"status": "INCOMPLETE", "schema_valid": True}
        return {"status": "COMPLETE", "schema_valid": True}

    monkeypatch.setattr(raw_job, "collect_snapshot", fake_collect_snapshot)

    with pytest.raises(raw_job.RawCollectionError, match="unusable"):
        raw_job.run()

    assert processed_dates == [date(2026, 8, 21), date(2026, 8, 22)]


def _fake_collect(calls, status_by_date=None):
    """collect_snapshot 호출 인자를 기록하고 날짜별로 미리 정한 manifest를 돌려준다."""

    def fake_collect_snapshot(
        target_date,
        hours,
        observed_at,
        snapshot_type,
        fetch_pages,
        write_json,
        read_json,
    ):
        calls.append((target_date, hours, observed_at.isoformat(), snapshot_type))
        status = (status_by_date or {}).get(target_date, "COMPLETE")
        return {
            "target_date": target_date.isoformat(),
            "status": status,
            "schema_valid": status == "COMPLETE",
        }

    return fake_collect_snapshot


def test_final_run_reads_confirmed_watermark_and_collects_oldest_capped_backlog(
    monkeypatch,
):
    calls = []

    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-22T06:00:00+09:00")
    monkeypatch.setenv("SNAPSHOT_TYPE", "FINAL")
    monkeypatch.setenv("MAX_DAYS_PER_RUN", "3")
    monkeypatch.delenv("RENTAL_HISTORY_T0_ENABLED", raising=False)
    monkeypatch.setattr(raw_job, "ensure_bucket", lambda bucket: None)
    monkeypatch.setattr(raw_job, "read_watermark", lambda: date(2026, 8, 15))
    monkeypatch.setattr(raw_job, "collect_snapshot", _fake_collect(calls))

    raw_job.run()

    assert [(target_date, hours) for target_date, hours, _, _ in calls] == [
        (date(2026, 8, 16), list(range(24))),
        (date(2026, 8, 17), list(range(24))),
        (date(2026, 8, 18), list(range(24))),
        (date(2026, 8, 22), [0, 1, 2, 3, 4, 5]),
    ]
    assert {snapshot_type for _, _, _, snapshot_type in calls} == {"FINAL"}


def test_final_current_failure_is_optional_when_t0_false(monkeypatch):
    calls = []

    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-22T06:00:00+09:00")
    monkeypatch.setenv("SNAPSHOT_TYPE", "FINAL")
    monkeypatch.setenv("MAX_DAYS_PER_RUN", "3")
    monkeypatch.setenv("RENTAL_HISTORY_T0_ENABLED", "false")
    monkeypatch.setattr(raw_job, "ensure_bucket", lambda bucket: None)
    monkeypatch.setattr(raw_job, "read_watermark", lambda: date(2026, 8, 20))
    monkeypatch.setattr(
        raw_job,
        "collect_snapshot",
        _fake_collect(calls, {date(2026, 8, 22): "INCOMPLETE"}),
    )

    manifests = raw_job.run()

    assert [target_date for target_date, _, _, _ in calls] == [
        date(2026, 8, 21),
        date(2026, 8, 22),
    ]
    assert [m["status"] for m in manifests] == ["COMPLETE", "INCOMPLETE"]


def test_final_current_failure_is_required_when_t0_true(monkeypatch):
    calls = []

    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-22T06:00:00+09:00")
    monkeypatch.setenv("SNAPSHOT_TYPE", "FINAL")
    monkeypatch.setenv("MAX_DAYS_PER_RUN", "3")
    monkeypatch.setenv("RENTAL_HISTORY_T0_ENABLED", "true")
    monkeypatch.setattr(raw_job, "ensure_bucket", lambda bucket: None)
    monkeypatch.setattr(raw_job, "read_watermark", lambda: date(2026, 8, 20))
    monkeypatch.setattr(
        raw_job,
        "collect_snapshot",
        _fake_collect(calls, {date(2026, 8, 22): "INCOMPLETE"}),
    )

    with pytest.raises(raw_job.RawCollectionError, match="unusable"):
        raw_job.run()


def test_final_run_rejects_unparsable_flag(monkeypatch):
    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-22T06:00:00+09:00")
    monkeypatch.setenv("SNAPSHOT_TYPE", "FINAL")
    monkeypatch.setenv("RENTAL_HISTORY_T0_ENABLED", "1")
    monkeypatch.setattr(raw_job, "ensure_bucket", lambda bucket: None)
    monkeypatch.setattr(raw_job, "read_watermark", lambda: date(2026, 8, 20))

    with pytest.raises(ValueError, match="boolean"):
        raw_job.run()


def test_preliminary_windows_remain_previous_full_day_and_current_closed_hours(
    monkeypatch,
):
    calls = []

    def forbidden_watermark():
        raise AssertionError("예비 수집은 확정 워터마크를 읽지 않는다")

    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-22T05:00:00+09:00")
    monkeypatch.setenv("SNAPSHOT_TYPE", "PRELIMINARY")
    monkeypatch.setenv("MAX_DAYS_PER_RUN", "3")
    monkeypatch.setattr(raw_job, "ensure_bucket", lambda bucket: None)
    monkeypatch.setattr(raw_job, "read_watermark", forbidden_watermark)
    monkeypatch.setattr(raw_job, "collect_snapshot", _fake_collect(calls))

    raw_job.run()

    assert [(target_date, hours) for target_date, hours, _, _ in calls] == [
        (date(2026, 8, 21), list(range(24))),
        (date(2026, 8, 22), [0, 1, 2, 3, 4]),
    ]
