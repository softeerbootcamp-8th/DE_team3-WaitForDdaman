"""
check_silver_bikeman_action_watermark 테스트

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
    from config.watermark_keys import SILVER_BIKEMAN_ACTION
    from jobs.check_silver_bikeman_action_watermark import is_ready

    write_watermark(date(2026, 8, 16), watermark_key=SILVER_BIKEMAN_ACTION)

    assert is_ready("2026-08-16") is True


def test_is_ready_false_when_watermark_behind_target(s3_env):
    from common.watermark import write_watermark
    from config.watermark_keys import SILVER_BIKEMAN_ACTION
    from jobs.check_silver_bikeman_action_watermark import is_ready

    write_watermark(date(2026, 8, 15), watermark_key=SILVER_BIKEMAN_ACTION)

    assert is_ready("2026-08-16") is False
