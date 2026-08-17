"""
silver.station_active 정제 로직 테스트

브론즈는 원본 보존을 위해 전부 STRING이다. station_active는 station_id +
snapshot_date만 남기는 필터 테이블이라, station_id null 드롭 / 중복 제거 두
가지만 검증하면 된다.

Iceberg/S3가 필요 없으므로 카탈로그 설정 없는 최소 SparkSession을 직접 만든다.
"""
from datetime import date

import pytest
from pyspark.sql import types as T

from jobs.silver_station_active import SILVER_COLUMNS, normalize

# bronze.station_active 컬럼 구성 (전부 STRING, daily_batch_station_active.py 참고)
BRONZE_SCHEMA = T.StructType([
    T.StructField(name, T.StringType())
    for name in [
        "station_id", "station_name", "rack_tot_cnt", "parking_bike_tot_cnt",
        "shared", "latitude", "longitude", "snapshot_date", "source_file",
    ]
])

DEFAULT_ROW = {
    "station_id": "ST-4",
    "station_name": "102. 망원역 1번출구 앞",
    "rack_tot_cnt": "15",
    "parking_bike_tot_cnt": "8",
    "shared": "53",
    "latitude": "37.55564880",
    "longitude": "126.91082764",
    "snapshot_date": "2026-08-14",
    "source_file": "api:2026-08-14",
}


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-silver-station-active")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def bronze_df(spark, *overrides):
    """브론즈 모양의 DataFrame을 만든다. 인자마다 한 행이 된다."""
    rows = []
    for over in overrides or [{}]:
        row = dict(DEFAULT_ROW)
        row.update(over)
        rows.append(tuple(row[f.name] for f in BRONZE_SCHEMA.fields))
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def test_keeps_only_station_id_and_snapshot_date(spark):
    df = normalize(bronze_df(spark))
    assert df.columns == SILVER_COLUMNS


def test_snapshot_date_becomes_date(spark):
    row = normalize(bronze_df(spark, {"snapshot_date": "2026-08-14"})).collect()[0]
    assert row["snapshot_date"] == date(2026, 8, 14)


def test_null_station_id_is_dropped(spark):
    df = normalize(bronze_df(
        spark,
        {"station_id": "ST-1"},
        {"station_id": None},
    ))
    assert df.count() == 1
    assert df.collect()[0]["station_id"] == "ST-1"


def test_duplicate_station_id_is_deduped(spark):
    df = normalize(bronze_df(
        spark,
        {"station_id": "ST-1", "snapshot_date": "2026-08-14"},
        {"station_id": "ST-1", "snapshot_date": "2026-08-14"},
    ))
    assert df.count() == 1


def test_distinct_station_ids_all_kept(spark):
    df = normalize(bronze_df(
        spark,
        {"station_id": "ST-1"},
        {"station_id": "ST-2"},
        {"station_id": "ST-3"},
    ))
    assert df.count() == 3
    assert {r["station_id"] for r in df.collect()} == {"ST-1", "ST-2", "ST-3"}
