"""
gold.fact_station_inventory 핵심 로직 테스트

Iceberg 테이블을 직접 읽는 부분(빌드 함수들의 spark.read.table 호출)은 카탈로그가
필요해 여기서는 테스트하지 않는다. 대신 DataFrame만으로 동작하는 순수 함수
셋(_merge_bike_last_action / _resolve_bike_station / _aggregate_station_inventory)만
검증한다 - staging/tests와 동일하게 카탈로그 설정 없는 최소 SparkSession을 만든다.
"""
from datetime import date, datetime

import pytest
from pyspark.sql import types as T

from jobs.build_fact_station_inventory import (
    _aggregate_station_inventory,
    _merge_bike_last_action,
    _resolve_bike_station,
)

SNAPSHOT_DATE = date(2026, 8, 17)

BIKE_LAST_ACTION_BASELINE_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("base_event_type", T.StringType()),
    T.StructField("base_station_id", T.StringType()),
    T.StructField("base_at", T.TimestampType()),
])

BIKE_LAST_ACTION_DELTA_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("delta_event_type", T.StringType()),
    T.StructField("delta_station_id", T.StringType()),
    T.StructField("delta_at", T.TimestampType()),
])

BIKE_LOCATION_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("last_station_id", T.StringType()),
    T.StructField("last_event_at", T.TimestampType()),
])

LATEST_ACTION_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("action_event_type", T.StringType()),
    T.StructField("action_station_id", T.StringType()),
    T.StructField("action_at", T.TimestampType()),
])

RESOLVED_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("effective_station_id", T.StringType()),
    T.StructField("excluded", T.BooleanType()),
])

STATION_ACTIVE_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()),
    T.StructField("hold_num", T.IntegerType()),
])


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-build-fact-station-inventory")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def by_bike(df, key="bike_id"):
    return {r[key]: r for r in df.collect()}


# ---------------------------------------------------------- _merge_bike_last_action


def test_bike_last_action_delta_wins_when_newer(spark):
    baseline = spark.createDataFrame(
        [("B1", "DEPLOY", "ST-1", datetime(2026, 8, 15, 9, 0))], BIKE_LAST_ACTION_BASELINE_SCHEMA
    )
    delta = spark.createDataFrame(
        [("B1", "COLLECT", "ST-2", datetime(2026, 8, 16, 9, 0))], BIKE_LAST_ACTION_DELTA_SCHEMA
    )

    result = by_bike(_merge_bike_last_action(baseline, delta, SNAPSHOT_DATE))

    assert result["B1"]["action_event_type"] == "COLLECT"
    assert result["B1"]["action_station_id"] == "ST-2"


def test_bike_last_action_baseline_wins_when_delta_not_newer(spark):
    baseline = spark.createDataFrame(
        [("B1", "DEPLOY", "ST-1", datetime(2026, 8, 16, 9, 0))], BIKE_LAST_ACTION_BASELINE_SCHEMA
    )
    delta = spark.createDataFrame(
        [("B1", "COLLECT", "ST-2", datetime(2026, 8, 15, 9, 0))], BIKE_LAST_ACTION_DELTA_SCHEMA
    )

    result = by_bike(_merge_bike_last_action(baseline, delta, SNAPSHOT_DATE))

    assert result["B1"]["action_event_type"] == "DEPLOY"
    assert result["B1"]["action_station_id"] == "ST-1"


def test_bike_last_action_carry_forward_without_delta(spark):
    baseline = spark.createDataFrame(
        [("B1", "DEPLOY", "ST-1", datetime(2026, 8, 15, 9, 0))], BIKE_LAST_ACTION_BASELINE_SCHEMA
    )
    delta = spark.createDataFrame([], BIKE_LAST_ACTION_DELTA_SCHEMA)

    result = by_bike(_merge_bike_last_action(baseline, delta, SNAPSHOT_DATE))

    assert result["B1"]["action_station_id"] == "ST-1"


# --------------------------------------------------------------- _resolve_bike_station


def test_resolve_uses_bike_location_when_no_action(spark):
    bike_location = spark.createDataFrame(
        [("B1", "ST-1", datetime(2026, 8, 15, 9, 0))], BIKE_LOCATION_SCHEMA
    )
    latest_action = spark.createDataFrame([], LATEST_ACTION_SCHEMA)

    result = by_bike(_resolve_bike_station(bike_location, latest_action))

    assert result["B1"]["effective_station_id"] == "ST-1"
    assert result["B1"]["excluded"] is False


def test_resolve_uses_bike_location_when_action_is_older(spark):
    bike_location = spark.createDataFrame(
        [("B1", "ST-1", datetime(2026, 8, 16, 9, 0))], BIKE_LOCATION_SCHEMA
    )
    latest_action = spark.createDataFrame(
        [("B1", "COLLECT", None, datetime(2026, 8, 15, 9, 0))], LATEST_ACTION_SCHEMA
    )

    result = by_bike(_resolve_bike_station(bike_location, latest_action))

    assert result["B1"]["effective_station_id"] == "ST-1"
    assert result["B1"]["excluded"] is False


def test_resolve_newest_collect_excludes_bike(spark):
    bike_location = spark.createDataFrame(
        [("B1", "ST-1", datetime(2026, 8, 15, 9, 0))], BIKE_LOCATION_SCHEMA
    )
    latest_action = spark.createDataFrame(
        [("B1", "COLLECT", None, datetime(2026, 8, 16, 9, 0))], LATEST_ACTION_SCHEMA
    )

    result = by_bike(_resolve_bike_station(bike_location, latest_action))

    assert result["B1"]["effective_station_id"] is None
    assert result["B1"]["excluded"] is True


def test_resolve_newest_deploy_overwrites_station(spark):
    bike_location = spark.createDataFrame(
        [("B1", "ST-1", datetime(2026, 8, 15, 9, 0))], BIKE_LOCATION_SCHEMA
    )
    latest_action = spark.createDataFrame(
        [("B1", "DEPLOY", "ST-9", datetime(2026, 8, 16, 9, 0))], LATEST_ACTION_SCHEMA
    )

    result = by_bike(_resolve_bike_station(bike_location, latest_action))

    assert result["B1"]["effective_station_id"] == "ST-9"
    assert result["B1"]["excluded"] is False


def test_resolve_deploy_without_station_falls_back_to_bike_location(spark):
    """DEPLOY인데 station_id가 없는 이례적 케이스 - bike_location 값으로 폴백."""
    bike_location = spark.createDataFrame(
        [("B1", "ST-1", datetime(2026, 8, 15, 9, 0))], BIKE_LOCATION_SCHEMA
    )
    latest_action = spark.createDataFrame(
        [("B1", "DEPLOY", None, datetime(2026, 8, 16, 9, 0))], LATEST_ACTION_SCHEMA
    )

    result = by_bike(_resolve_bike_station(bike_location, latest_action))

    assert result["B1"]["effective_station_id"] == "ST-1"
    assert result["B1"]["excluded"] is False


# --------------------------------------------------------- _aggregate_station_inventory


def test_aggregate_station_with_no_bikes_gets_zero(spark):
    resolved = spark.createDataFrame([], RESOLVED_SCHEMA)
    station_active = spark.createDataFrame([("ST-1", 10)], STATION_ACTIVE_SCHEMA)

    result = by_bike(_aggregate_station_inventory(resolved, station_active, SNAPSHOT_DATE), key="station_id")

    assert result["ST-1"]["bike_cnt"] == 0
    assert result["ST-1"]["target_bike_cnt"] == 10


def test_aggregate_counts_only_non_excluded_bikes_with_station(spark):
    resolved = spark.createDataFrame(
        [
            ("B1", "ST-1", False),
            ("B2", "ST-1", False),
            ("B3", "ST-1", True),  # excluded(COLLECT) - 집계 제외
            ("B4", None, False),  # 대여소 없음(노상 방치) - 집계 제외
        ],
        RESOLVED_SCHEMA,
    )
    station_active = spark.createDataFrame([("ST-1", 10)], STATION_ACTIVE_SCHEMA)

    result = by_bike(_aggregate_station_inventory(resolved, station_active, SNAPSHOT_DATE), key="station_id")

    assert result["ST-1"]["bike_cnt"] == 2


def test_aggregate_snapshot_date_is_set(spark):
    resolved = spark.createDataFrame([], RESOLVED_SCHEMA)
    station_active = spark.createDataFrame([("ST-1", 10)], STATION_ACTIVE_SCHEMA)

    result = by_bike(_aggregate_station_inventory(resolved, station_active, SNAPSHOT_DATE), key="station_id")

    assert result["ST-1"]["snapshot_date"] == SNAPSHOT_DATE
