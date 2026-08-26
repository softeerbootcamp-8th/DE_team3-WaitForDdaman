"""논리 기준일(COLLECTION_CUTOFF_AT) 기반 결정론적 배치 및 백필 회귀 테스트."""

from datetime import date, datetime
from zoneinfo import ZoneInfo
from unittest.mock import patch

from common.cutoff_utils import parse_collection_cutoff
from jobs.daily_batch_failure_report import run as run_failure_report
from jobs.daily_batch_bikeman_event import run as run_bikeman_event

KST = ZoneInfo("Asia/Seoul")


def test_parse_collection_cutoff_with_timezone():
    cutoff = parse_collection_cutoff("2026-08-15T06:00:00+09:00")
    assert cutoff == datetime(2026, 8, 15, 6, 0, 0, tzinfo=KST)
    assert cutoff.date() == date(2026, 8, 15)


def test_parse_collection_cutoff_default_to_now(monkeypatch):
    cutoff = parse_collection_cutoff(None)
    assert cutoff.tzinfo == KST


def test_failure_report_backfill_deterministic_date(monkeypatch):
    """과거 cutoff 주입 시 실제 물리 날짜와 무관하게 주입된 논리 날짜만 처리한다."""
    processed_dates = []

    def fake_process_one_day(target_date: date):
        processed_dates.append(target_date)
        return 1

    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-10T06:00:00+09:00")
    monkeypatch.setenv("FAILURE_REPORT_T0_ENABLED", "true")

    with patch("jobs.daily_batch_failure_report.ensure_bucket"), \
         patch("jobs.daily_batch_failure_report.read_watermark", return_value=date(2026, 8, 8)), \
         patch("jobs.daily_batch_failure_report.write_watermark"), \
         patch("jobs.daily_batch_failure_report._write_completion_marker"), \
         patch("jobs.daily_batch_failure_report._process_one_day", side_effect=fake_process_one_day):
        run_failure_report()

    # 확정 구간: 2026-08-09 (워터마크 08-08 다음날부터 cutoff 전날인 08-09)
    # T0 구간: 2026-08-10 (cutoff 당일)
    assert processed_dates == [date(2026, 8, 9), date(2026, 8, 10)]


def test_bikeman_event_backfill_deterministic_date(monkeypatch):
    """과거 cutoff 주입 시 cutoff.date() - 1일까지를 확정 상한선으로 처리한다."""
    processed_dates = []

    def fake_process_one_day(target_date: date):
        processed_dates.append(target_date)
        return 1

    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-10T06:00:00+09:00")

    with patch("jobs.daily_batch_bikeman_event.ensure_bucket"), \
         patch("jobs.daily_batch_bikeman_event.read_watermark", return_value=date(2026, 8, 7)), \
         patch("jobs.daily_batch_bikeman_event.write_watermark") as mock_write_wm, \
         patch("jobs.daily_batch_bikeman_event._process_one_day", side_effect=fake_process_one_day):
        run_bikeman_event()

    # 3일 lookback (워터마크 08-07 기준 08-05부터 08-09까지)
    assert processed_dates == [
        date(2026, 8, 5),
        date(2026, 8, 6),
        date(2026, 8, 7),
        date(2026, 8, 8),
        date(2026, 8, 9),
    ]
    mock_write_wm.assert_called_once_with(date(2026, 8, 9), watermark_key="_meta/watermark/bikeman_event.json")


def test_silver_bikeman_action_backfill_deterministic_date(monkeypatch):
    """Silver bikeman_action도 과거 cutoff 주입 시 cutoff.date() - 1일까지를 처리한다."""
    from jobs.silver_bikeman_action import run as run_silver_bikeman_action

    processed_dates = []

    def fake_process_one_day(catalog, silver_table, target_date: date):
        processed_dates.append(target_date)
        return 1, True

    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-10T06:00:00+09:00")

    with patch("jobs.silver_bikeman_action.ensure_bucket"), \
         patch("jobs.silver_bikeman_action.build_iceberg_catalog"), \
         patch("jobs.silver_bikeman_action._ensure_auxiliary_tables"), \
         patch("jobs.silver_bikeman_action._ensure_silver_table"), \
         patch("jobs.silver_bikeman_action.read_watermark", return_value=date(2026, 8, 7)), \
         patch("jobs.silver_bikeman_action.write_watermark") as mock_write_wm, \
         patch("jobs.silver_bikeman_action._process_one_day", side_effect=fake_process_one_day):
        run_silver_bikeman_action()

    # 3일 lookback (워터마크 08-07 다음날 08-08에서 3일 전인 08-05부터 08-09까지)
    assert processed_dates == [
        date(2026, 8, 5),
        date(2026, 8, 6),
        date(2026, 8, 7),
        date(2026, 8, 8),
        date(2026, 8, 9),
    ]
    # 신규 워터마크(08-08, 08-09)에 대해서만 갱신 호출
    assert mock_write_wm.call_count == 2

