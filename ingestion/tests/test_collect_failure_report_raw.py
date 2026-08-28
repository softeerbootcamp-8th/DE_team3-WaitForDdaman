"""collect_failure_report_raw 잡 단위 테스트."""

from datetime import date, datetime
from zoneinfo import ZoneInfo
from unittest.mock import patch

from jobs.collect_failure_report_raw import (
    collect_one_day,
    parse_collection_cutoff,
    snapshot_keys,
)

KST = ZoneInfo("Asia/Seoul")


def test_parse_collection_cutoff():
    cutoff = parse_collection_cutoff("2026-08-17T05:00:00+09:00")
    assert cutoff == datetime(2026, 8, 17, 5, 0, 0, tzinfo=KST)


def test_snapshot_keys():
    target = date(2026, 8, 17)
    observed = datetime(2026, 8, 17, 5, 0, 0, tzinfo=KST)
    payload_key, manifest_key = snapshot_keys(target, observed, "PRELIMINARY")

    assert "target_date=2026-08-17" in payload_key
    assert "snapshot_type=PRELIMINARY" in payload_key
    assert payload_key.endswith("payload.json")
    assert manifest_key.endswith("manifest.json")


def test_collect_one_day():
    saved_objects = {}

    def fake_put_json(bucket, key, data):
        saved_objects[key] = data

    fake_rows = [{"BIKE_NO": "SPB-12345", "REG_DTTM": "2026-08-17 01:23:45", "FAILURE_TYPE": "기타"}]

    with patch("jobs.collect_failure_report_raw.fetch_failure_reports_by_date", return_value=fake_rows), \
         patch("jobs.collect_failure_report_raw.put_json", side_effect=fake_put_json):
        cutoff = datetime(2026, 8, 17, 5, 0, 0, tzinfo=KST)
        count = collect_one_day(date(2026, 8, 17), cutoff, "PRELIMINARY", "test-bucket")

    assert count == 1
    assert any("manifest.json" in k for k in saved_objects)
    assert any("reg_dt=2026-08-17/payload.json" in k for k in saved_objects)
