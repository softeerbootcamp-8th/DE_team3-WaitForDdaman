"""
NOTE: moto는 커스텀(비-AWS) endpoint_url을 가로채지 못하므로, 여기서는
config.SETTINGS.env = "aws" 로 교체해 moto의 가상 AWS로 로직만 검증한다.
실제 LocalStack 컨테이너 연동(엔드포인트 스위칭 자체)은 docker-compose.localstack.yml로
띄운 뒤 수동/통합 테스트로 별도 확인할 것.

config.SETTINGS를 직접 교체하는 이유: common 모듈들은 최상위 `config` 패키지를
`import config`로 참조하고 호출 시점에 config.SETTINGS를 조회하므로, 여기서 교체하면
이미 import된 s3_utils/watermark 모듈에도 즉시 반영된다.
"""
from datetime import date

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


def test_watermark_defaults_to_backfill_start_when_missing(s3_env):
    from common.watermark import read_watermark

    assert read_watermark() == date(2015, 1, 1)


def test_watermark_roundtrip(s3_env):
    from common.watermark import read_watermark, write_watermark

    write_watermark(date(2026, 8, 5))
    assert read_watermark() == date(2026, 8, 5)


def test_watermark_persisted_payload_shape(s3_env):
    import json

    from common.s3_utils import get_s3_client
    from common.watermark import write_watermark

    write_watermark(date(2026, 8, 5))
    s3 = get_s3_client()
    obj = s3.get_object(Bucket="test-raw-bucket", Key="_meta/watermark/rental_history.json")
    body = json.loads(obj["Body"].read())
    assert body["last_processed_date"] == "2026-08-05"
    assert "updated_at" in body
