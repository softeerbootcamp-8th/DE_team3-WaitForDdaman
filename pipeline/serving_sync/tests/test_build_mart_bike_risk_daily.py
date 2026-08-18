"""build_mart_bike_risk_daily 조인/집계 순수 로직 테스트."""
from datetime import date

import pytest
from pyspark.sql import types as T

from build_mart_bike_risk_daily import build_mart_bike_risk_daily

RISK_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("risk_score", T.DoubleType()),
    T.StructField("risk_grade", T.StringType()),
])
DECISION_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("action", T.StringType()),
])
LOCATION_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("last_station_id", T.StringType()),
])
STATION_ACTIVE_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()),
    T.StructField("station_name", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("district", T.StringType()),
])
DIM_BIKE_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("start_year", T.IntegerType()),
])
FEATURES_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("dist_km", T.DoubleType()),
])
STATION_RISK_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()),
    T.StructField("risk_cnt", T.IntegerType()),
    T.StructField("healthy_ratio", T.DoubleType()),
])
FAILURE_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("fail_history", T.ArrayType(T.StringType())),
])


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-build-mart-bike-risk-daily")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def build(spark, risk_rows, decision_rows, capacity=100):
    risk_df = spark.createDataFrame(risk_rows, RISK_SCHEMA)
    decision_df = spark.createDataFrame(decision_rows, DECISION_SCHEMA)
    location_df = spark.createDataFrame(
        [(r[0], "ST-1") for r in risk_rows], LOCATION_SCHEMA
    )
    station_active_df = spark.createDataFrame(
        [("ST-1", "테스트대여소", "강북", "마포구")], STATION_ACTIVE_SCHEMA
    )
    dim_bike_df = spark.createDataFrame([(r[0], 2020) for r in risk_rows], DIM_BIKE_SCHEMA)
    features_df = spark.createDataFrame([(r[0], 12.5) for r in risk_rows], FEATURES_SCHEMA)
    station_risk_df = spark.createDataFrame([], STATION_RISK_SCHEMA)
    failure_df = spark.createDataFrame([], FAILURE_SCHEMA)
    return {
        r["bike_id"]: r
        for r in build_mart_bike_risk_daily(
            risk_df, decision_df, location_df, station_active_df, dim_bike_df,
            features_df, station_risk_df, failure_df, date(2026, 8, 18), capacity=capacity,
        ).collect()
    }


def test_hold_becomes_no_action(spark):
    result = build(spark, [("B1", 10.0, "Normal")], [("B1", "보류")])
    assert result["B1"]["action"] == "조치없음"


def test_suspend_within_capacity_becomes_collect(spark):
    result = build(spark, [("B1", 90.0, "Critical")], [("B1", "대여중단")], capacity=100)
    assert result["B1"]["action"] == "수거"


def test_suspend_beyond_capacity_stays_suspend(spark):
    rows = [(f"B{i}", float(100 - i), "Critical") for i in range(5)]
    decisions = [(f"B{i}", "대여중단") for i in range(5)]
    result = build(spark, rows, decisions, capacity=3)
    collected = [bid for bid, r in result.items() if r["action"] == "수거"]
    suspended = [bid for bid, r in result.items() if r["action"] == "대여중단"]
    assert sorted(collected) == ["B0", "B1", "B2"]  # risk_score 상위 3대
    assert sorted(suspended) == ["B3", "B4"]


def test_aging_is_snapshot_year_minus_start_year(spark):
    result = build(spark, [("B1", 10.0, "Normal")], [("B1", "보류")])
    assert result["B1"]["aging"] == 2026 - 2020


def test_no_risk_scored_station_defaults_to_full_health(spark):
    result = build(spark, [("B1", 10.0, "Normal")], [("B1", "보류")])
    assert result["B1"]["healthy_ratio"] == pytest.approx(100.0)
