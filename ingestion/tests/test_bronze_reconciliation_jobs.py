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


def test_promote_and_marker_still_record_failure_when_prepare_left_no_selection(
    s3_env, monkeypatch
):
    """prepare가 실패해 selection.json을 못 남긴 날짜라도(#195 promote 매핑 보완),
    promote_rental_history_raw/write_rental_history_completion_marker는 실행되어
    FAILED marker를 남기고, downstream advance watermark는 그 지점에서 멈춰야 한다.

    promote_rental_history_date가 이제 prepare mapped task의 XCom이 아니라
    assign 단계의 rental_requests를 기준으로 expand되므로, prepare가 실패해 아무
    XCom도 못 남긴 날짜에 대해서도 이 두 잡은 반드시 호출된다 - 그 계약을 검증한다.
    """
    from common.watermark import read_watermark, write_watermark
    from jobs import advance_completion_watermark
    from jobs import promote_rental_history_raw
    from jobs import write_rental_history_completion_marker as marker_job

    write_watermark(date(2026, 8, 20))
    monkeypatch.setenv("COLLECTION_CUTOFF_AT", "2026-08-21T23:59:59+09:00")
    monkeypatch.setenv("BACKFILL_TARGET_DATE", "2026-08-21")
    monkeypatch.setenv("DAG_RUN_ID", "backfill__2026-08-21")

    # prepare 실패(또는 selection 없음)로 selection.json이 애초에 존재하지 않는 상태.
    with pytest.raises(promote_rental_history_raw.PromotionError):
        promote_rental_history_raw.run()

    # promote_rental_history_raw가 실패해도 completion marker 잡은 독립적으로 실행되어
    # 실제 S3 상태(manifest/promotion 둘 다 없음)를 근거로 FAILED를 남겨야 한다.
    marker = marker_job.run()
    assert marker["status"] == "FAILED"

    monkeypatch.setenv("DATASET", "rental_history")
    monkeypatch.setenv("RECONCILIATION_TARGET_DATE", "2026-08-21")
    monkeypatch.setenv("RECONCILIATION_FAIL_ON_INCOMPLETE", "true")

    with pytest.raises(RuntimeError):
        advance_completion_watermark.run()

    assert read_watermark() == date(2026, 8, 20)
