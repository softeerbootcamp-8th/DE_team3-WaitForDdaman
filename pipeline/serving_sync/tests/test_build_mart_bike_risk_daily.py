"""build_mart_bike_risk_daily 조인/집계 순수 로직 테스트."""
from datetime import date

import pytest
from pyspark.sql import types as T

from build_mart_bike_risk_daily import _fail_history_agg, build_mart_bike_risk_daily

RAW_FAILURE_SCHEMA = T.StructType([
    T.StructField("bike_no", T.StringType()),
    T.StructField("reg_dttm", T.StringType()),
    T.StructField("failure_type", T.StringType()),
])

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


def build(spark, risk_rows, decision_rows):
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
            features_df, station_risk_df, failure_df, date(2026, 8, 18),
        ).collect()
    }


def test_mart_omits_action_column(spark):
    result = build(spark, [("B1", 10.0, "Normal")], [("B1", "보류")])
    assert "action" not in result["B1"].asDict()


def test_mart_keeps_only_decided_bikes(spark):
    result = build(
        spark,
        [("B1", 90.0, "Critical"), ("B2", 10.0, "Normal")],
        [("B1", "대여중단")],
    )
    assert sorted(result) == ["B1"]


def test_aging_is_snapshot_year_minus_start_year(spark):
    result = build(spark, [("B1", 10.0, "Normal")], [("B1", "보류")])
    assert result["B1"]["aging"] == 2026 - 2020


def test_no_risk_scored_station_defaults_to_full_health(spark):
    result = build(spark, [("B1", 10.0, "Normal")], [("B1", "보류")])
    assert result["B1"]["healthy_ratio"] == pytest.approx(100.0)


def test_fail_history_agg_orders_most_recent_first(spark):
    raw = spark.createDataFrame(
        [
            ("B1", "2026-01-01 00:00:00", "펑크"),
            ("B1", "2026-03-01 00:00:00", "타이어마모"),
            ("B1", "2026-02-01 00:00:00", "체인끊김"),
        ],
        RAW_FAILURE_SCHEMA,
    )
    result = {r["bike_id"]: r for r in _fail_history_agg(raw, date(2026, 8, 18)).collect()}
    assert result["B1"]["fail_history"] == [
        "2026-03-01 타이어마모",
        "2026-02-01 체인끊김",
        "2026-01-01 펑크",
    ]


def test_fail_history_agg_respects_limit(spark):
    raw = spark.createDataFrame(
        [(f"B2", f"2026-01-0{i} 00:00:00", f"고장{i}") for i in range(1, 8)],
        RAW_FAILURE_SCHEMA,
    )
    result = {r["bike_id"]: r for r in _fail_history_agg(raw, date(2026, 8, 18), limit=5).collect()}
    assert result["B2"]["fail_history"] == [
        "2026-01-07 고장7",
        "2026-01-06 고장6",
        "2026-01-05 고장5",
        "2026-01-04 고장4",
        "2026-01-03 고장3",
    ]
