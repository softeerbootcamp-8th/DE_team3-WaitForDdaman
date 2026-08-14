import logging
import os
import sys
from datetime import date, timedelta

os.environ.setdefault("SPARK_VERSION", "3.5")

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import functions as F

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import GOLD_DIM_BIKE, SILVER_RENTAL_HISTORY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SILVER_WATERMARK_KEY = SILVER_RENTAL_HISTORY
GOLD_WATERMARK_KEY = GOLD_DIM_BIKE


def _silver_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.rental_history"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.dim_bike"


class GoldValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


def _ensure_dim_bike_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            snapshot_date DATE,
            bike_id STRING,
            first_seen_at TIMESTAMP,
            start_year INT
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def _validate_dim_bike(spark, dim_bike_df) -> None:
    check = Check(spark, CheckLevel.Error, "dim_bike_check")
    result = (
        VerificationSuite(spark)
        .onData(dim_bike_df)
        .addCheck(
            check.hasUniqueness(["bike_id"], lambda fraction: fraction > 0.99)
            .isComplete("first_seen_at")
            .isComplete("start_year")
        )
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show(truncate=False)

    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise GoldValidationError(f"dim_bike 품질 검증 실패: {failed_constraints}")


def _process_range(spark, start_date: date, end_date: date) -> int:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    range_label = start_str if start_date == end_date else f"{start_str}~{end_str}"

    silver_df = spark.read.table(_silver_table()).filter(
        F.col("rent_date_partition").between(start_str, end_str)
    )

    row_count = silver_df.count()
    if row_count == 0:
        logger.info("%s: Silver에 처리할 데이터 없음", range_label)
        return 0

    range_agg = silver_df.groupBy("bike_id").agg(F.min("rent_dt").alias("first_seen_at"))

    existing_bike_ids = spark.read.table(_gold_table()).select("bike_id").distinct()
    new_bikes = range_agg.join(existing_bike_ids, on="bike_id", how="left_anti").cache()
    new_count = new_bikes.count()

    if new_count == 0:
        logger.info("%s: 신규 등장 자전거 없음", range_label)
        new_bikes.unpersist()
        return 0


    out_df = (
        new_bikes.withColumn("snapshot_date", F.to_date("first_seen_at"))
        .withColumn("start_year", F.year("first_seen_at"))
        .select("snapshot_date", "bike_id", "first_seen_at", "start_year")
    )
    out_df.writeTo(_gold_table()).overwritePartitions()
    new_bikes.unpersist()

    _validate_dim_bike(spark, spark.read.table(_gold_table()))  # 실패 시 GoldValidationError -> 배치 중단

    logger.info("%s: 신규 자전거 %d대 dim_bike 추가", range_label, new_count)
    return new_count


def run() -> None:
    # raw_bucket에는 워터마크 JSON이 있음 - 이 잡만 단독 실행하는 경우에도 안전하도록 보장
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session_with_deequ("gold-build-dim-bike")
    try:
        _ensure_dim_bike_table(spark)

        # 상한선: Silver가 확정 승격한 날짜
        silver_watermark = read_watermark(watermark_key=SILVER_WATERMARK_KEY)
        # 하한선: Gold 전용 워터마크
        gold_watermark = read_watermark(watermark_key=GOLD_WATERMARK_KEY)

        start_date = gold_watermark + timedelta(days=1)
        end_date = silver_watermark

        max_days = os.getenv("MAX_DAYS_PER_RUN")
        if max_days:
            capped_end = start_date + timedelta(days=int(max_days) - 1)
            if capped_end < end_date:
                logger.info(
                    "MAX_DAYS_PER_RUN=%s 적용 - 이번 실행은 %s ~ %s까지만 처리 (원래 끝: %s)",
                    max_days, start_date, capped_end, end_date,
                )
                end_date = capped_end

        if start_date > end_date:
            logger.info(
                "처리할 신규 날짜 없음 (Gold 워터마크=%s, Silver 워터마크=%s)",
                gold_watermark, silver_watermark,
            )
            return

        try:
            _process_range(spark, start_date, end_date)
            write_watermark(end_date, watermark_key=GOLD_WATERMARK_KEY)
        except GoldValidationError as e:
            logger.error("%s~%s 처리 실패, 배치 중단: %s", start_date, end_date, e)
            sys.exit(1)
    finally:
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()
