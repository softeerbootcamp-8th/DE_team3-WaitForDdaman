"""
신설 공통 모듈 (partition_listing, iceberg_io, sql_assert) 단위 테스트 (Issue #140)
"""
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from moto import mock_aws

import config as config_module
from common.iceberg_io import append, overwrite_partition
from common.partition_listing import (
    get_table_data_prefix,
    list_partitions,
    max_partition,
    min_partition,
    partition_exists,
)
from common.s3_utils import get_s3_client, put_text
from common.sql_assert import QualityCheck, QualityCheckError

BUCKET = "test-common-modules-bucket"


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
        s3 = get_s3_client()
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ap-northeast-2"},
        )
        yield s3


# ------------------------------------------------------------------------------
# partition_listing Tests
# ------------------------------------------------------------------------------
def test_get_table_data_prefix():
    bucket, prefix = get_table_data_prefix("silver.station_master")
    assert prefix.endswith("silver/station_master/data/")


def test_partition_listing_empty(s3_env):
    parts = list_partitions("silver.station_master")
    assert parts == []
    assert max_partition("silver.station_master") is None
    assert min_partition("silver.station_master") is None
    assert not partition_exists("silver.station_master", "2026-08-22")


def test_partition_listing_with_data(s3_env):
    # Create fake partition files in S3
    table = "silver.station_master"
    bucket, prefix = get_table_data_prefix(table)

    put_text(bucket, f"{prefix}snapshot_date=2026-08-20/file1.parquet", "data")
    put_text(bucket, f"{prefix}snapshot_date=2026-08-21/file2.parquet", "data")
    put_text(bucket, f"{prefix}snapshot_date=2026-08-22/file3.parquet", "data")

    parts = list_partitions(table, "snapshot_date")
    assert parts == ["2026-08-20", "2026-08-21", "2026-08-22"]
    assert max_partition(table, "snapshot_date") == "2026-08-22"
    assert min_partition(table, "snapshot_date") == "2026-08-20"
    assert partition_exists(table, "2026-08-21", "snapshot_date")
    assert not partition_exists(table, "2026-08-25", "snapshot_date")


# ------------------------------------------------------------------------------
# iceberg_io Tests
# ------------------------------------------------------------------------------
def test_iceberg_io_overwrite_partition():
    mock_table = MagicMock()
    mock_table.name.return_value = "silver.station_active"

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    sample_arrow = pa.table({"station_id": ["ST-10"], "snapshot_date": ["2026-08-22"]})

    overwrite_partition("silver.station_active", sample_arrow, "snapshot_date", "2026-08-22", catalog=mock_catalog)

    mock_table.overwrite.assert_called_once()
    args, kwargs = mock_table.overwrite.call_args
    assert args[0] == sample_arrow
    assert "overwrite_filter" in kwargs


def test_iceberg_io_append():
    mock_table = MagicMock()
    mock_table.name.return_value = "bronze.quarantine"

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    sample_arrow = pa.table({"event_id": [1], "payload": ["{}"]})

    append("bronze.quarantine", sample_arrow, catalog=mock_catalog)
    mock_table.append.assert_called_once_with(sample_arrow)


# ------------------------------------------------------------------------------
# sql_assert (PyDeequ replacement) Tests
# ------------------------------------------------------------------------------
def test_sql_assert_all_pass():
    table = pa.table({
        "bike_id": ["SPB-001", "SPB-002", "SPB-003"],
        "event_type": ["COLLECT", "DEPLOY", "COLLECT"],
        "count": [10, 0, 5],
        "status": ["OK", "OK", "OK"],
    })

    result = (
        QualityCheck("bikeman_action_checks")
        .is_complete("bike_id")
        .is_complete("event_type")
        .is_contained_in("event_type", ["COLLECT", "DEPLOY"])
        .is_non_negative("count")
        .satisfies("status = 'OK'")
        .has_uniqueness("bike_id", threshold=0.99)
        .has_min_rows(1)
        .run(table)
    )

    assert result.is_success
    assert len(result.failed_constraints) == 0
    # Should not raise
    result.raise_if_failed()


def test_sql_assert_failures_raise_with_details():
    table = pa.table({
        "bike_id": ["SPB-001", "SPB-001", None],  # duplicate & null
        "event_type": ["INVALID", "DEPLOY", "COLLECT"],  # invalid value
        "count": [10, -5, 5],  # negative value
        "status": ["OK", "ERROR", "OK"],  # expression violation
    })

    result = (
        QualityCheck("failing_checks")
        .is_complete("bike_id")
        .is_contained_in("event_type", ["COLLECT", "DEPLOY"])
        .is_non_negative("count")
        .satisfies("status = 'OK'")
        .has_uniqueness("bike_id", threshold=1.0)
        .run(table)
    )

    assert not result.is_success
    assert len(result.failed_constraints) == 5

    with pytest.raises(QualityCheckError) as exc_info:
        result.raise_if_failed()

    err_text = str(exc_info.value)
    assert "isComplete(bike_id)" in err_text
    assert "isContainedIn(event_type" in err_text
    assert "isNonNegative(count)" in err_text
    assert "satisfies(status = 'OK')" in err_text
    assert "hasUniqueness(['bike_id'])" in err_text
