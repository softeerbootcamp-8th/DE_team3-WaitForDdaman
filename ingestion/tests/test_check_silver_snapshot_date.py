"""
check_silver_snapshot_date 테스트

NOTE: test_watermark.py와 같은 이유로 config.SETTINGS.env를 "aws"로 바꿔 moto를 쓴다.
Iceberg Hadoop 카탈로그가 identity 파티션마다 만드는 Hive 스타일 디렉터리
(`{warehouse}/{namespace}/{table}/data/snapshot_date=YYYY-MM-DD/`)를 boto3로
나열하는 판정 로직만 검증한다 - 실제 Iceberg 테이블 생성/커밋은 검증 범위 밖.
"""
from datetime import date

import pytest
from moto import mock_aws

import config as config_module

BUCKET = "test-warehouse-bucket"


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        warehouse_bucket=BUCKET,
        iceberg_warehouse_path=f"s3a://{BUCKET}/warehouse",
        s3_region="ap-northeast-2",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)

    with mock_aws():
        from common.s3_utils import ensure_bucket

        ensure_bucket(BUCKET)
        yield


def _put_partition_file(namespace: str, table: str, snapshot_date: str) -> None:
    from common.s3_utils import get_s3_client

    key = f"warehouse/{namespace}/{table}/data/snapshot_date={snapshot_date}/00000-0-x.parquet"
    get_s3_client().put_object(Bucket=BUCKET, Key=key, Body=b"x")


def test_get_max_snapshot_date_picks_the_max_across_partitions(s3_env):
    from jobs.check_silver_snapshot_date import get_max_snapshot_date

    _put_partition_file("silver", "station_master", "2026-08-15")
    _put_partition_file("silver", "station_master", "2026-08-17")
    _put_partition_file("silver", "station_master", "2026-08-16")

    assert get_max_snapshot_date("silver", "station_master") == date(2026, 8, 17)


def test_get_max_snapshot_date_none_when_table_has_no_data_yet(s3_env):
    from jobs.check_silver_snapshot_date import get_max_snapshot_date

    assert get_max_snapshot_date("silver", "station_active") is None


def test_get_max_snapshot_date_none_when_warehouse_bucket_not_created_yet(monkeypatch):
    """완전 초기 환경(warehouse 버킷 자체가 아직 없음)도 '아직 준비 안 됨'으로 취급한다.

    실측: LocalStack을 새로 띄운 뒤 Silver ETL이 한 번도 안 돈 상태에서 재현됨
    (S3 NoSuchBucket이 그대로 올라와 태스크가 실패로 보였음, #145 검증 중 발견).
    """
    test_settings = config_module.Settings(
        env="aws",
        warehouse_bucket=BUCKET,
        iceberg_warehouse_path=f"s3a://{BUCKET}/warehouse",
        s3_region="ap-northeast-2",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)

    with mock_aws():
        from jobs.check_silver_snapshot_date import get_max_snapshot_date

        assert get_max_snapshot_date("silver", "station_master") is None


def test_is_ready_true_when_latest_partition_covers_target(s3_env):
    from jobs.check_silver_snapshot_date import is_ready

    _put_partition_file("silver", "station_master", "2026-08-17")

    assert is_ready("silver.station_master", "2026-08-17") is True


def test_is_ready_true_when_latest_partition_is_after_target(s3_env):
    from jobs.check_silver_snapshot_date import is_ready

    _put_partition_file("silver", "station_master", "2026-08-18")

    assert is_ready("silver.station_master", "2026-08-17") is True


def test_is_ready_false_when_no_partition_reaches_target(s3_env):
    from jobs.check_silver_snapshot_date import is_ready

    _put_partition_file("silver", "station_master", "2026-08-16")

    assert is_ready("silver.station_master", "2026-08-17") is False


def test_is_ready_false_when_table_has_no_data_yet(s3_env):
    from jobs.check_silver_snapshot_date import is_ready

    assert is_ready("silver.station_active", "2026-08-17") is False
