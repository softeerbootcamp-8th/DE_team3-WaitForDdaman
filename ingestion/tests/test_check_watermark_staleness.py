"""
전체 데이터셋 워터마크 정체 감지 테스트 (Issue #180)
"""
from datetime import date, timedelta

import pytest
from moto import mock_aws

import config as config_module


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        raw_bucket="test-raw-bucket",
        s3_region="ap-northeast-2",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)

    with mock_aws():
        from common.s3_utils import ensure_bucket

        ensure_bucket("test-raw-bucket")
        yield


def _write_all_fresh(as_of: date, offset_days: int = 1):
    from common.watermark import write_watermark
    from operations.check_watermark_staleness import WATERMARK_DATASETS

    for key in WATERMARK_DATASETS.values():
        write_watermark(as_of - timedelta(days=offset_days), watermark_key=key)


def test_all_fresh_watermarks_are_not_stale(s3_env):
    from operations.check_watermark_staleness import stale_datasets

    today = date.today()
    _write_all_fresh(today, offset_days=1)

    assert stale_datasets(today, max_stale_days=3) == []


def test_one_stale_dataset_is_detected(s3_env):
    from common.watermark import write_watermark
    from operations.check_watermark_staleness import stale_datasets
    from config.watermark_keys import BRONZE_RENTAL_HISTORY

    today = date.today()
    _write_all_fresh(today, offset_days=1)
    write_watermark(today - timedelta(days=10), watermark_key=BRONZE_RENTAL_HISTORY)

    stale = stale_datasets(today, max_stale_days=3)

    assert len(stale) == 1
    assert stale[0]["dataset"] == "rental_history"
    assert stale[0]["days_stale"] == 10


def test_missing_watermark_counts_as_stale(s3_env):
    """아무것도 안 쓴 데이터셋은 read_watermark의 backfill 기본값(2015-01-01)으로
    떨어지므로, 압도적으로 정체된 것으로 잡혀야 한다."""
    from operations.check_watermark_staleness import stale_datasets, WATERMARK_DATASETS

    today = date.today()
    stale = stale_datasets(today, max_stale_days=3)

    assert {s["dataset"] for s in stale} == set(WATERMARK_DATASETS.keys())


def test_run_passes_silently_when_fresh(s3_env):
    from operations.check_watermark_staleness import run

    _write_all_fresh(date.today(), offset_days=1)
    run()  # 예외가 나면 안 된다


def test_run_raises_when_stale(s3_env, monkeypatch):
    from operations.check_watermark_staleness import run, WatermarkStalenessError

    monkeypatch.setenv("MAX_STALE_DAYS", "3")
    # 아무 워터마크도 안 써서 전부 정체된 상태로 둔다.
    with pytest.raises(WatermarkStalenessError):
        run()


def test_max_stale_days_env_var_is_respected(s3_env, monkeypatch):
    from common.watermark import write_watermark
    from operations.check_watermark_staleness import run

    monkeypatch.setenv("MAX_STALE_DAYS", "10")
    _write_all_fresh(date.today(), offset_days=5)  # 5일 정체, 기준(10일)보다는 짧음

    run()  # 기준을 넘지 않았으므로 예외 없이 통과해야 한다
