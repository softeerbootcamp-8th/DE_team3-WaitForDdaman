"""
신설 공통 모듈 (partition_listing, iceberg_io, sql_assert) 단위 테스트 (Issue #140)
"""
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from moto import mock_aws

import config as config_module
from common.duckdb_io import query_arrow
from common.iceberg_io import append, overwrite_all, overwrite_partition
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


def test_iceberg_io_overwrite_all():
    """전량 교체 - overwrite_filter를 안 넘겨서 테이블 전체가 대체되게 한다 (#143)."""
    mock_table = MagicMock()
    mock_table.name.return_value = "silver.failure_report"

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    sample_arrow = pa.table({"bike_no": ["SPB-1"], "reg_date_partition": ["2026-08-22"]})

    overwrite_all("silver.failure_report", sample_arrow, catalog=mock_catalog)

    mock_table.overwrite.assert_called_once_with(sample_arrow)


# ------------------------------------------------------------------------------
# duckdb_io Tests
# ------------------------------------------------------------------------------
def test_query_arrow_returns_materialized_table():
    """
    duckdb 버전에 따라 .arrow()가 RecordBatchReader를 돌려주는데, 그걸 같은 커넥션에
    다시 register하면 교착이 난다(실측). query_arrow는 항상 pa.Table을 준다.
    """
    import duckdb

    con = duckdb.connect(":memory:")
    con.register("src", pa.table({"a": [1, 2, 3]}))

    result = query_arrow(con, "SELECT a FROM src WHERE a > 1")

    assert isinstance(result, pa.Table)
    assert result.column("a").to_pylist() == [2, 3]

    # 결과를 같은 커넥션에 다시 등록해서 이어 질의해도 멈추지 않아야 한다
    con.register("step2", result)
    assert query_arrow(con, "SELECT count(*) AS n FROM step2").column("n").to_pylist() == [2]


def test_query_arrow_accepts_parameters():
    import duckdb

    con = duckdb.connect(":memory:")
    con.register("src", pa.table({"a": [1, 2, 3]}))

    result = query_arrow(con, "SELECT a FROM src WHERE a = ?", [2])
    assert result.column("a").to_pylist() == [2]


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


def test_is_contained_in_allows_null():
    """Deequ isContainedIn은 non-null 값만 검사한다 - null은 위반이 아니다."""
    table = pa.table({"risk_grade": ["Normal", None, "Critical"]})

    result = QualityCheck("t").is_contained_in("risk_grade", ["Normal", "Warning", "Critical"]).run(table)

    assert result.is_success


def test_satisfies_treats_null_condition_as_violation():
    """Deequ satisfies는 조건이 NULL로 평가되는 행(예: 컬럼 자체가 null)도 위반으로
    센다 (CASE WHEN <조건> THEN 1 ELSE 0 END로 컴파일하는 것과 동일)."""
    table = pa.table({"risk_score": [50.0, None, 150.0]})

    result = QualityCheck("t").satisfies("risk_score >= 0 AND risk_score <= 100", "range").run(table)

    assert not result.is_success
    assert result.failed_constraints[0].violation_count == 2  # null 1건 + 150 1건


def test_has_uniqueness_uses_deequ_definition_not_distinctness():
    """중복이 3개 몰린 키 하나(A) + 유일값 하나(B) - Deequ 원래 정의(정확히 1번만
    등장하는 행의 비율)로는 1/4=0.25인데, distinctness(COUNT(DISTINCT)/COUNT(*))로는
    2/4=0.5로 값이 갈린다. 회귀 비교를 위해 Deequ 정의를 재현해야 한다(#140/#146)."""
    table = pa.table({"bike_id": ["A", "A", "A", "B"]})

    result = QualityCheck("t").has_uniqueness("bike_id", threshold=0.99).run(table)

    assert not result.is_success
    assert abs(result.failed_constraints[0].metric_value - 0.25) < 1e-9
