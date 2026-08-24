"""Bronze Reconciliation gap/marker/워터마크 잡 테스트."""

from datetime import date

import pytest
from moto import mock_aws

import config as config_module


@pytest.fixture
def s3_env(monkeypatch):
    settings = config_module.Settings(env="aws", raw_bucket="test-reconciliation-bucket")
    monkeypatch.setattr(config_module, "SETTINGS", settings)
    with mock_aws():
        from common.s3_utils import ensure_bucket

        ensure_bucket(settings.raw_bucket)
        yield


def _write_marker(dataset: str, target_date: str, status: str):
    from common.s3_utils import put_json
    from jobs.check_bronze_gap import marker_key

    put_json(
        "test-reconciliation-bucket",
        marker_key(dataset, date.fromisoformat(target_date)),
        {"dataset": dataset, "target_date": target_date, "status": status},
    )


def test_gap_check_returns_only_dates_without_accepted_marker(s3_env, monkeypatch):
    from common.watermark import write_watermark
    from jobs import check_bronze_gap

    write_watermark(date(2026, 8, 20))
    _write_marker("rental_history", "2026-08-21", "COMPLETE")
    monkeypatch.setenv("DATASET", "rental_history")
    monkeypatch.setenv("RECONCILIATION_TARGET_DATE", "2026-08-22")

    assert check_bronze_gap.run() == ["2026-08-22"]


def test_advance_watermark_stops_at_first_gap(s3_env, monkeypatch):
    from common.watermark import read_watermark, write_watermark
    from jobs import advance_completion_watermark

    write_watermark(date(2026, 8, 20), watermark_key="_meta/watermark/failure_report.json")
    _write_marker("failure_report", "2026-08-21", "COMPLETE_EMPTY")
    _write_marker("failure_report", "2026-08-22", "COMPLETE")
    monkeypatch.setenv("DATASET", "failure_report")
    monkeypatch.setenv("RECONCILIATION_TARGET_DATE", "2026-08-22")

    result = advance_completion_watermark.run()

    assert result["after"] == "2026-08-22"
    assert read_watermark(watermark_key="_meta/watermark/failure_report.json") == date(2026, 8, 22)
