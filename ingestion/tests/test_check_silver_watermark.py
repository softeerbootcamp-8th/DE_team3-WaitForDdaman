"""
check_silver_watermark 테스트

NOTE: test_watermark.py와 같은 이유로 config.SETTINGS.env를 "aws"로 바꿔 moto를 쓴다.
"""
from datetime import date

import pytest
from moto import mock_aws

import config as config_module

BUCKET = "test-raw-bucket"


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


def test_is_ready_true_when_watermark_covers_target(s3_env):
    from common.watermark import write_watermark
    from config.watermark_keys import SILVER_RENTAL_HISTORY
    from operations.check_silver_watermark import is_ready

    write_watermark(date(2026, 8, 17), watermark_key=SILVER_RENTAL_HISTORY)

    assert is_ready("rental_history", "2026-08-17") is True


def test_is_ready_false_when_watermark_behind_target(s3_env):
    from common.watermark import write_watermark
    from config.watermark_keys import SILVER_RENTAL_HISTORY
    from operations.check_silver_watermark import is_ready

    write_watermark(date(2026, 8, 16), watermark_key=SILVER_RENTAL_HISTORY)

    assert is_ready("rental_history", "2026-08-17") is False


def test_is_ready_applies_required_offset_days(s3_env):
    """T-1 구조(rental_history): 워터마크가 target보다 하루 늦어도 offset=1이면 통과."""
    from common.watermark import write_watermark
    from config.watermark_keys import SILVER_RENTAL_HISTORY
    from operations.check_silver_watermark import is_ready

    write_watermark(date(2026, 8, 16), watermark_key=SILVER_RENTAL_HISTORY)

    assert is_ready("rental_history", "2026-08-17", required_offset_days=1) is True


def test_is_ready_false_for_unknown_dataset(s3_env):
    from operations.check_silver_watermark import is_ready

    assert is_ready("unknown_dataset", "2026-08-17") is False
