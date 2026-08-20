"""대여소별 위험도 집계(station_risk_agg) 순수 로직 테스트."""
import pytest
from pyspark.sql import types as T

from station_risk_shared import station_risk_agg

RISK_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("risk_grade", T.StringType()),
])
LOCATION_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("last_station_id", T.StringType()),
])


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-station-risk-shared")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def rows_by_station(spark, risk_rows, location_rows):
    risk_df = spark.createDataFrame(risk_rows, RISK_SCHEMA)
    location_df = spark.createDataFrame(location_rows, LOCATION_SCHEMA)
    return {r["station_id"]: r for r in station_risk_agg(risk_df, location_df).collect()}


def test_all_normal_bikes_have_full_healthy_ratio(spark):
    result = rows_by_station(
        spark,
        [("B1", "Normal"), ("B2", "Normal")],
        [("B1", "ST-1"), ("B2", "ST-1")],
    )
    assert result["ST-1"]["risk_cnt"] == 0
    assert result["ST-1"]["healthy_ratio"] == pytest.approx(100.0)


def test_warning_and_critical_count_toward_risk_cnt(spark):
    result = rows_by_station(
        spark,
        [("B1", "Normal"), ("B2", "Warning"), ("B3", "Critical")],
        [("B1", "ST-1"), ("B2", "ST-1"), ("B3", "ST-1")],
    )
    assert result["ST-1"]["risk_cnt"] == 2
    assert result["ST-1"]["healthy_ratio"] == pytest.approx(33.3, abs=0.1)


def test_bikes_with_no_station_are_excluded(spark):
    result = rows_by_station(spark, [("B1", "Critical")], [("B1", None)])
    assert result == {}


def test_stations_are_independent(spark):
    result = rows_by_station(
        spark,
        [("B1", "Critical"), ("B2", "Normal")],
        [("B1", "ST-1"), ("B2", "ST-2")],
    )
    assert result["ST-1"]["healthy_ratio"] == pytest.approx(0.0)
    assert result["ST-2"]["healthy_ratio"] == pytest.approx(100.0)
