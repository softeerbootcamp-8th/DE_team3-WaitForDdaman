"""
Gold - 자전거별 서빙 마트 (여러 gold 테이블 join -> gold.mart_bike_risk_daily,
{{ ds }} 파티션 단위 OVERWRITE) -> postgres.bike_risk_daily로 넘어갈 최종 모양.

gold.fact_bike_decision은 오늘자 위험 자전거 중 의사결정 대상 bike_id를 확정하는
상위 산출물이다. mart_bike_risk_daily는 서빙 화면에 필요한 위험도/위치/이력 컬럼만
노출하며, 의사결정 action은 이 mart와 postgres 서빙 테이블에 싣지 않는다.

### healthy_ratio 기본값
risk-scored bike가 하나도 없는 대여소(station_risk_shared.station_risk_agg 결과에
안 나타남)는 100.0(완전 정상)으로 취급한다 - "위험 신호 없음 = 정상"이라는 보수적 기본값.

사용법:
    python -m jobs.build_mart_bike_risk_daily
"""
import logging
import os
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session
from station_risk_shared import read_station_risk_agg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MART_COLUMNS = [
    "snapshot_date", "bike_id", "station_id", "station_name", "region", "district",
    "healthy_ratio", "risk_score", "risk_grade", "dist_km", "start_year", "aging",
    "fail_history",
]


def _fact_bike_risk_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_bike_risk"


def _fact_bike_decision_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_bike_decision"


def _bike_location_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_location"


def _station_active_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.station_active"


def _dim_bike_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.dim_bike"


def _bike_features_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_features_daily"


def _failure_report_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.failure_report"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.mart_bike_risk_daily"


def _ensure_mart_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            snapshot_date DATE,
            bike_id STRING,
            station_id STRING,
            station_name STRING,
            region STRING,
            district STRING,
            healthy_ratio DOUBLE,
            risk_score DOUBLE,
            risk_grade STRING,
            dist_km DOUBLE,
            start_year INT,
            aging INT,
            fail_history ARRAY<STRING>
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )
    try:
        spark.sql(f"ALTER TABLE {_gold_table()} DROP COLUMN action")
    except AnalysisException as e:
        message = str(e)
        missing_column = any(fragment in message for fragment in [
            "Cannot delete missing field",
            "Cannot find field",
            "Missing field",
        ])
        if not missing_column:
            raise


def _fail_history_agg(failure_df, as_of_date, limit: int = 5):
    """자전거별 최근 고장신고 limit건을 "YYYY-MM-DD 유형" 문자열 배열로 (최신순)."""
    hist = (
        failure_df.filter(F.col("reg_dttm") < F.lit(str(as_of_date)))
        .withColumnRenamed("bike_no", "bike_id")
        .withColumn("entry", F.concat_ws(" ", F.date_format("reg_dttm", "yyyy-MM-dd"), "failure_type"))
    )
    w = Window.partitionBy("bike_id").orderBy(F.col("reg_dttm").desc())
    ranked = hist.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") <= limit)
    ordered = ranked.groupBy("bike_id").agg(
        F.sort_array(F.collect_list(F.struct(F.col("reg_dttm").alias("d"), F.col("entry"))), asc=False).alias(
            "_hist"
        )
    )
    return ordered.select("bike_id", F.col("_hist.entry").alias("fail_history"))


def build_mart_bike_risk_daily(
    risk_df, decision_df, location_df, station_active_df, dim_bike_df,
    features_df, station_risk_df, failure_df, snapshot_date,
):
    base = (
        risk_df.select("bike_id", "risk_score", "risk_grade")
        .join(decision_df.select("bike_id").dropDuplicates(["bike_id"]), on="bike_id", how="inner")
        .join(
            location_df.select("bike_id", F.col("last_station_id").alias("station_id")),
            on="bike_id",
            how="left",
        )
        .join(
            station_active_df.select("station_id", "station_name", "region", "district"),
            on="station_id",
            how="left",
        )
        .join(dim_bike_df.select("bike_id", "start_year"), on="bike_id", how="left")
        .join(features_df.select("bike_id", "dist_km"), on="bike_id", how="left")
        .join(station_risk_df, on="station_id", how="left")
    )

    base = base.withColumn("healthy_ratio", F.coalesce(F.col("healthy_ratio"), F.lit(100.0)))
    base = base.withColumn("aging", (F.year(F.lit(str(snapshot_date))) - F.col("start_year")).cast("int"))

    base = base.join(failure_df, on="bike_id", how="left")
    base = base.withColumn(
        "fail_history", F.coalesce(F.col("fail_history"), F.array().cast("array<string>"))
    )

    return base.withColumn("snapshot_date", F.lit(str(snapshot_date)).cast("date")).select(*MART_COLUMNS)


def _read_and_build(spark, snapshot_date: date):
    date_str = snapshot_date.strftime("%Y-%m-%d")

    risk_df = spark.read.table(_fact_bike_risk_table()).filter(F.col("snapshot_date") == date_str)
    if risk_df.count() == 0:
        return None

    decision_df = spark.read.table(_fact_bike_decision_table()).filter(F.col("snapshot_date") == date_str)
    location_df = spark.read.table(_bike_location_table())
    station_active_df = spark.read.table(_station_active_table())
    dim_bike_df = spark.read.table(_dim_bike_table()).select("bike_id", "start_year").dropDuplicates(["bike_id"])
    features_df = spark.read.table(_bike_features_table()).filter(F.col("snapshot_date") == date_str)
    station_risk_df = read_station_risk_agg(spark, date_str)
    failure_df = _fail_history_agg(spark.read.table(_failure_report_table()), snapshot_date)

    return build_mart_bike_risk_daily(
        risk_df, decision_df, location_df, station_active_df, dim_bike_df,
        features_df, station_risk_df, failure_df, snapshot_date,
    )


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE")
    target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("gold-build-mart-bike-risk-daily")
    _ensure_mart_table(spark)

    out_df = _read_and_build(spark, target_date)
    if out_df is None:
        logger.info("%s: fact_bike_risk에 처리할 데이터 없음", target_date)
        return

    out_df = out_df.cache()
    row_count = out_df.count()
    out_df.writeTo(_gold_table()).overwritePartitions()
    out_df.unpersist()

    logger.info("%s: gold.mart_bike_risk_daily %d행 갱신 완료", target_date, row_count)


if __name__ == "__main__":
    run()
