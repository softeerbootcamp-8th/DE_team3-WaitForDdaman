"""build_mart_station_daily 조인/urgency 로직 테스트."""
from datetime import date

import pytest
from pyspark.sql import types as T

from build_mart_station_daily import build_mart_station_daily

STATION_ACTIVE_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()),
    T.StructField("station_name", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("district", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("hold_num", T.IntegerType()),
])
INVENTORY_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()),
    T.StructField("bike_cnt", T.IntegerType()),
    T.StructField("target_bike_cnt", T.IntegerType()),
])
STATION_RISK_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()),
    T.StructField("risk_cnt", T.IntegerType()),
    T.StructField("healthy_ratio", T.DoubleType()),
])


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-build-mart-station-daily")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def build(spark, station_active_rows, inventory_rows, station_risk_rows):
    station_active_df = spark.createDataFrame(station_active_rows, STATION_ACTIVE_SCHEMA)
    inventory_df = spark.createDataFrame(inventory_rows, INVENTORY_SCHEMA)
    station_risk_df = spark.createDataFrame(station_risk_rows, STATION_RISK_SCHEMA)
    return {
        r["station_id"]: r
        for r in build_mart_station_daily(
            station_active_df, inventory_df, station_risk_df, date(2026, 8, 18)
        ).collect()
    }


def test_latitude_longitude_pass_through_unchanged(spark):
    result = build(
        spark,
        [("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10)],
        [("ST-1", 5, 10)],
        [],
    )
    assert result["ST-1"]["latitude"] == pytest.approx(37.5)
    assert result["ST-1"]["longitude"] == pytest.approx(127.0)


def test_healthy_ratio_ge_70_is_sufficient(spark):
    result = build(
        spark,
        [("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10)],
        [("ST-1", 5, 10)],
        [("ST-1", 1, 80.0)],
    )
    assert result["ST-1"]["urgency"] == "여유있음"


def test_healthy_ratio_below_70_is_insufficient(spark):
    result = build(
        spark,
        [("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10)],
        [("ST-1", 5, 10)],
        [("ST-1", 4, 50.0)],
    )
    assert result["ST-1"]["urgency"] == "부족함"


def test_missing_inventory_defaults_bike_cnt_to_zero(spark):
    result = build(
        spark,
        [("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10)],
        [],
        [],
    )
    assert result["ST-1"]["bike_cnt"] == 0
    assert result["ST-1"]["risk_cnt"] == 0
    assert result["ST-1"]["healthy_ratio"] == pytest.approx(100.0)
    assert result["ST-1"]["urgency"] == "여유있음"
