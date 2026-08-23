"""
register_tables_in_jdbc_catalog 단위 테스트 (Issue #139)
"""
from unittest.mock import MagicMock, patch

import pytest
from moto import mock_aws

import config as config_module
from common.s3_utils import get_s3_client, put_text
from jobs.register_tables_in_jdbc_catalog import _discover_tables, run

BUCKET = "test-register-catalog-bucket"


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        warehouse_bucket=BUCKET,
        iceberg_warehouse_path=f"s3a://{BUCKET}/warehouse",
        s3_region="ap-northeast-2",
        iceberg_catalog_type="jdbc",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)
    with mock_aws():
        s3 = get_s3_client()
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ap-northeast-2"},
        )
        yield s3


def test_discover_tables_finds_version_hint(s3_env):
    # Setup mock S3 metadata structure
    put_text(BUCKET, "warehouse/bronze/station_master/metadata/version-hint.text", "3")
    put_text(BUCKET, "warehouse/silver/station_active/metadata/version-hint.text", "1")

    tables = _discover_tables(BUCKET, "warehouse")
    assert len(tables) == 2

    table_map = {f"{db}.{tbl}": loc for db, tbl, loc in tables}
    assert "bronze.station_master" in table_map
    assert table_map["bronze.station_master"] == f"s3a://{BUCKET}/warehouse/bronze/station_master/metadata/v3.metadata.json"
    assert "silver.station_active" in table_map
    assert table_map["silver.station_active"] == f"s3a://{BUCKET}/warehouse/silver/station_active/metadata/v1.metadata.json"


def test_register_tables_run_success(s3_env):
    put_text(BUCKET, "warehouse/bronze/station_active/metadata/version-hint.text", "1")

    mock_spark = MagicMock()

    with patch("jobs.register_tables_in_jdbc_catalog.build_spark_session", return_value=mock_spark):
        run()

    mock_spark.sql.assert_called_once()
    sql_arg = mock_spark.sql.call_args[0][0]
    assert "CALL" in sql_arg
    assert "system.register_table" in sql_arg
    assert "bronze.station_active" in sql_arg
