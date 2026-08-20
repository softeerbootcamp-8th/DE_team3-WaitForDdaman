"""
gold.station_active 조인 로직 테스트

Iceberg 테이블을 직접 읽는 부분(_latest_snapshot)은 카탈로그가 필요해 여기서는
테스트하지 않는다. 대신 두 DataFrame만으로 동작하는 순수 함수인
_join_active_stations()만 검증한다 - staging/tests와 동일하게 카탈로그 설정
없는 최소 SparkSession을 만든다.
"""
from datetime import date

import pytest
from pyspark.sql import types as T

from jobs.build_station_active import _join_active_stations

SNAPSHOT_DATE = date(2026, 8, 17)

MASTER_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()),
    T.StructField("station_name", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("district", T.StringType()),
    T.StructField("hold_num", T.IntegerType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
])

ACTIVE_IDS_SCHEMA = T.StructType([T.StructField("station_id", T.StringType())])


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-build-station-active")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def by_station(df):
    return {r["station_id"]: r for r in df.collect()}


def test_inner_join_keeps_only_active_stations(spark):
    master = spark.createDataFrame(
        [
            ("ST-1", "1번 대여소", "강북", "마포구", 10, 37.5, 126.9),
            ("ST-2", "2번 대여소", "강남", "강남구", 20, 37.4, 127.0),
        ],
        MASTER_SCHEMA,
    )
    active_ids = spark.createDataFrame([("ST-1",)], ACTIVE_IDS_SCHEMA)

    result = by_station(_join_active_stations(master, active_ids, SNAPSHOT_DATE))

    assert set(result.keys()) == {"ST-1"}


def test_description_columns_come_from_master(spark):
    master = spark.createDataFrame(
        [("ST-1", "1번 대여소", "강북", "마포구", 10, 37.5, 126.9)], MASTER_SCHEMA
    )
    active_ids = spark.createDataFrame([("ST-1",)], ACTIVE_IDS_SCHEMA)

    result = by_station(_join_active_stations(master, active_ids, SNAPSHOT_DATE))

    assert result["ST-1"]["station_name"] == "1번 대여소"
    assert result["ST-1"]["hold_num"] == 10


def test_snapshot_date_is_set(spark):
    master = spark.createDataFrame(
        [("ST-1", "1번 대여소", "강북", "마포구", 10, 37.5, 126.9)], MASTER_SCHEMA
    )
    active_ids = spark.createDataFrame([("ST-1",)], ACTIVE_IDS_SCHEMA)

    result = by_station(_join_active_stations(master, active_ids, SNAPSHOT_DATE))

    assert result["ST-1"]["snapshot_date"] == SNAPSHOT_DATE
