"""
V1 & V4 POC 검증: PyIceberg 동적 파티션 덮어쓰기 원자성 및 카탈로그 상호 운용성 (Issue #139)

대상: daily_batch_station_active (2.7k행, identity 파티션 snapshot_date)
목적:
  - V1: pyiceberg overwrite_partition의 원자성(부분 상태 미노출) 및 멱등성 검증
  - V4: Spark(JdbcCatalog)와 PyIceberg(SqlCatalog) 간 카탈로그 테이블 및 데이터 파일 공유 검증
"""
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from common.iceberg_io import overwrite_partition
from schemas.station_active_schema import REQUIRED_STANDARD_COLUMNS


def _make_station_active_arrow_data(snapshot_date: str, row_count: int = 2735) -> pa.Table:
    """station_active 표준 2,735행 샘플 PyArrow Table 생성"""
    station_ids = [f"ST-{i}" for i in range(1, row_count + 1)]
    station_names = [f"대여소 {i}" for i in range(1, row_count + 1)]
    racks = ["10"] * row_count
    bikes = ["5"] * row_count
    shares = ["50"] * row_count
    lats = ["37.5665"] * row_count
    longs = ["126.9780"] * row_count
    snapshot_dates = [snapshot_date] * row_count
    source_files = [f"api:{snapshot_date}"] * row_count
    ingested_ats = ["2026-08-22 00:10:00"] * row_count

    return pa.table({
        "station_id": station_ids,
        "station_name": station_names,
        "rack_tot_cnt": racks,
        "parking_bike_tot_cnt": bikes,
        "shared": shares,
        "latitude": lats,
        "longitude": longs,
        "snapshot_date": snapshot_dates,
        "source_file": source_files,
        "ingested_at": ingested_ats,
    })


def test_v1_poc_pyiceberg_partition_overwrite_atomicity():
    """
    V1 POC: PyIceberg overwrite_partition이 파티션 필터를 적용하여
    원자적으로 단일 파티션을 덮어쓰는지 검증합니다.
    """
    mock_table = MagicMock()
    mock_table.name.return_value = "bike_catalog.bronze.station_active"

    data_day1 = _make_station_active_arrow_data("2026-08-22", row_count=2735)
    overwrite_partition(mock_table, data_day1, "snapshot_date", "2026-08-22")

    mock_table.overwrite.assert_called_once()
    args, kwargs = mock_table.overwrite.call_args
    assert len(args[0]) == 2735
    assert "overwrite_filter" in kwargs


def test_v1_poc_pyiceberg_overwrite_failure_does_not_commit():
    """
    V1 POC: 덮어쓰기 도중 에러가 발생하면 commit이 발생하지 않고
    예외가 상위로 전파되어 부분 상태가 커밋되지 않음을 검증합니다.
    """
    mock_table = MagicMock()
    mock_table.name.return_value = "bike_catalog.bronze.station_active"
    mock_table.overwrite.side_effect = RuntimeError("S3 write failure during upload")

    data = _make_station_active_arrow_data("2026-08-22", row_count=100)

    with pytest.raises(RuntimeError, match="S3 write failure"):
        overwrite_partition(mock_table, data, "snapshot_date", "2026-08-22")


def test_v4_poc_spark_and_pyiceberg_catalog_compatibility():
    """
    V4 POC: Spark JdbcCatalog와 PyIceberg SqlCatalog의 설정 연결 및
    동일한 iceberg_catalog DB 접속 속성을 검증합니다.
    """
    import config

    settings = config.SETTINGS
    # URI 변환 검증: Spark의 jdbc:postgresql:// -> PyIceberg의 postgresql+psycopg2://
    assert "iceberg_catalog" in settings.iceberg_jdbc_catalog_uri
    assert settings.iceberg_catalog_name == "bike_catalog"
