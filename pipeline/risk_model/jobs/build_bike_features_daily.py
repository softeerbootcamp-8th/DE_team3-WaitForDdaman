"""
dag_risk_decision의 추론 입력을 만드는 feature 엔지니어링 job -
silver.rental_history + silver.failure_report -> gold.bike_features_daily (추론용)

피처 로직은 pipeline/train_risk_model/features.py의 build_features_for_inference()를
그대로 호출한다. train_risk_model README의 설계 원칙("피처 로직은 features.py 하나를
학습·추론이 공유 - 갈라지면 train-serving skew")에 맞춰, 이 파일에 자체 구현을 두지 않는다.

기준일 이전 14일 rolling window라 dim_bike처럼 누적 처리하지 않는다 - 워터마크 없이
SNAPSHOT_DATE(기본값 오늘) 하루치를 매번 통째로 재계산해 OVERWRITE.
"""
import logging
import os
import sys
from datetime import date

os.environ.setdefault("SPARK_VERSION", "3.5")

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import functions as F
from pyspark.sql import types as T

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ
from pipeline.train_risk_model.features import build_features_for_inference
from pipeline.train_risk_model.settings import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = [
    "snapshot_date", "bike_id", "trips", "dist_km", "instant_ret",
    "fail_150d", "days_since_fail", "days_since_last_rent", "trend_ratio",
]


class GoldValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


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


def _build_features(spark, cfg, target_date: date):
    return build_features_for_inference(spark, cfg, target_date).select(*OUTPUT_COLUMNS)


def _process_date(spark, cfg, target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")

    feat_df = _build_features(spark, cfg, target_date)
    row_count = feat_df.count()
    if row_count == 0:
        logger.info("%s: 최근 %d일 내 대여 이력 없음", date_str, int(cfg.get_path("run.window_days", 14)))
        return 0

    feat_df.writeTo(_gold_table()).overwritePartitions()

    written = spark.read.table(_gold_table()).filter(F.col("snapshot_date") == date_str)
    _validate_bike_features_daily(spark, written)  # 실패 시 GoldValidationError -> 배치 중단

    logger.info("%s: 자전거 %d대 feature 산출", date_str, row_count)
    return row_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    cfg = load_config()
    spark = build_spark_session_with_deequ("gold-build-bike-features-daily")
    try:
        _ensure_bike_features_daily_table(spark)

        snapshot_date_str = os.getenv("SNAPSHOT_DATE")
        target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

        try:
            _process_date(spark, cfg, target_date)
        except GoldValidationError as e:
            logger.error("%s 처리 실패, 배치 중단: %s", target_date, e)
            sys.exit(1)
    finally:
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()
