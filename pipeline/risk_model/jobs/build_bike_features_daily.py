"""
dag_risk_decision의 추론 입력을 만드는 feature 엔지니어링 job -
silver.rental_history + silver.failure_report -> gold.bike_features_daily (추론용)

노트북(0812_risk_model_code_optimize.ipynb)의 build_features(as_of) 로직을 이식했다.
dur_h는 원래 노트북 FEATURE_COLS에 있었지만 silver.rental_history에 이용시간(use_min)이
없어서 만들 수 없다 - 모델도 이미 dur_h 제외 7-feature로 재학습해서 컬럼이 맞다.

기준일 이전 14일 rolling window라 dim_bike처럼 누적 처리하지 않는다 - 워터마크 없이
SNAPSHOT_DATE(기본값 오늘) 하루치를 매번 통째로 재계산해 OVERWRITE.
"""
import logging
import os
import sys
from datetime import date, timedelta

os.environ.setdefault("SPARK_VERSION", "3.5")

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import functions as F
from pyspark.sql import types as T

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

WINDOW_DAYS = 14
FAIL_LOOKBACK_DAYS = 150
NO_FAIL_SENTINEL = 9999

OUTPUT_COLUMNS = [
    "snapshot_date", "bike_id", "trips", "dist_km", "instant_ret",
    "fail_150d", "days_since_fail", "days_since_last_rent", "trend_ratio",
]


class GoldValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


def _rental_history_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.rental_history"


def _failure_report_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.failure_report"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_features_daily"


def _ensure_bike_features_daily_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            snapshot_date DATE,
            bike_id STRING,
            trips INT,
            dist_km DOUBLE,
            instant_ret INT,
            fail_150d INT,
            days_since_fail INT,
            days_since_last_rent INT,
            trend_ratio DOUBLE
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def _validate_bike_features_daily(spark, df) -> None:
    check = Check(spark, CheckLevel.Error, "bike_features_daily_check")
    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            check.isComplete("bike_id")
            .hasUniqueness(["bike_id"], lambda fraction: fraction > 0.99)
            .isComplete("trips")
            .isComplete("trend_ratio")
        )
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show(truncate=False)

    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise GoldValidationError(f"bike_features_daily 품질 검증 실패: {failed_constraints}")


def _build_features(spark, target_date: date):
    as_of = target_date.strftime("%Y-%m-%d")
    window_start = (target_date - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
    mid = (target_date - timedelta(days=WINDOW_DAYS // 2)).strftime("%Y-%m-%d")
    fail_lookback_start = (target_date - timedelta(days=FAIL_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    rent = (
        spark.read.table(_rental_history_table())
        .filter((F.col("rent_dt") >= F.lit(window_start)) & (F.col("rent_dt") < F.lit(as_of)))
    )

    usage = rent.groupBy("bike_id").agg(
        F.count("*").alias("trips"),
        (F.sum("use_distance_m") / 1000).alias("dist_km"),
        F.sum(
            F.when(
                (F.col("rent_station_id") == F.col("return_station_id"))
                & F.col("use_distance_m").between(0, 10),
                1,
            ).otherwise(0)
        ).alias("instant_ret"),
        F.sum(F.when(F.col("rent_dt") < F.lit(mid), 1).otherwise(0)).alias("trips_first_half"),
        F.sum(F.when(F.col("rent_dt") >= F.lit(mid), 1).otherwise(0)).alias("trips_second_half"),
        F.max("rent_dt").alias("last_rent_dt"),
    ).withColumn(
        "trend_ratio", (F.col("trips_second_half") + 1) / (F.col("trips_first_half") + 1)
    ).withColumn(
        "days_since_last_rent", F.datediff(F.lit(as_of), F.col("last_rent_dt"))
    )

    prior_fail = (
        spark.read.table(_failure_report_table())
        .filter(F.col("reg_dttm") < F.lit(as_of))
    )
    fail_agg = prior_fail.groupBy("bike_no").agg(
        F.sum(
            F.when(F.col("reg_dttm") >= F.lit(fail_lookback_start), 1).otherwise(0)
        ).alias("fail_150d"),
        F.max("reg_dttm").alias("last_fail_dttm"),
    ).withColumn(
        "days_since_fail", F.datediff(F.lit(as_of), F.col("last_fail_dttm"))
    ).withColumnRenamed("bike_no", "bike_id")

    feat = (
        usage.join(fail_agg.select("bike_id", "fail_150d", "days_since_fail"), on="bike_id", how="left")
        .fillna({"fail_150d": 0, "days_since_fail": NO_FAIL_SENTINEL})
        .withColumn("snapshot_date", F.lit(target_date))
        .select(*OUTPUT_COLUMNS)
    )
    return feat


def _process_date(spark, target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")

    feat_df = _build_features(spark, target_date)
    row_count = feat_df.count()
    if row_count == 0:
        logger.info("%s: 최근 %d일 내 대여 이력 없음", date_str, WINDOW_DAYS)
        return 0

    feat_df.writeTo(_gold_table()).overwritePartitions()

    written = spark.read.table(_gold_table()).filter(F.col("snapshot_date") == date_str)
    _validate_bike_features_daily(spark, written)  # 실패 시 GoldValidationError -> 배치 중단

    logger.info("%s: 자전거 %d대 feature 산출", date_str, row_count)
    return row_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session_with_deequ("gold-build-bike-features-daily")
    try:
        _ensure_bike_features_daily_table(spark)

        snapshot_date_str = os.getenv("SNAPSHOT_DATE")
        target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

        try:
            _process_date(spark, target_date)
        except GoldValidationError as e:
            logger.error("%s 처리 실패, 배치 중단: %s", target_date, e)
            sys.exit(1)
    finally:
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()
