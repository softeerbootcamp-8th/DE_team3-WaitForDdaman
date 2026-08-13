"""
Bronze 일 배치 잡 - 서울시 공공자전거 고장신고 내역 (OA-15644)

전략: 증분 기준 = REGDTTM(등록일시)
- tbCycleFailureReport는 대여이력과 달리 시간 단위 분할이 필요 없다 (날짜 단위로 충분).
- 워터마크 다음날부터 어제까지 날짜별로 순차 처리, 성공한 날짜만 커밋.
- 재실행 시 동일 날짜 파티션을 덮어써서 멱등성 보장.

사용법:
    python -m jobs.daily_batch_failure_report
    MAX_DAYS_PER_RUN=1 python -m jobs.daily_batch_failure_report
"""
import logging
import os
import sys
from datetime import date, timedelta

from pyspark.sql import functions as F

from common import config
from common.api_client import (
    SeoulApiError,
    SeoulApiTransientError,
    fetch_failure_reports_by_date,
    strip_pagination_meta,
)
from common.s3_utils import ensure_bucket, put_json
from common.spark_session import build_spark_session
from common.watermark import read_watermark, write_watermark
from schema.failure_report_schema import (
    SchemaValidationError,
    build_select_exprs,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 대여이력과 워터마크 키가 겹치면 안 되므로 데이터셋별로 분리
WATERMARK_KEY = "_meta/watermark/failure_report.json"


def _table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.failure_report"


def _process_one_day(spark, target_date: date) -> int:
    raw_rows = list(fetch_failure_reports_by_date(target_date))
    date_str = target_date.strftime("%Y-%m-%d")

    ensure_bucket(config.SETTINGS.raw_bucket)
    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/failure_report/api/reg_dt={date_str}/payload.json",
        {"reg_dt": date_str, "row_count": len(raw_rows), "rows": raw_rows},
    )

    if not raw_rows:
        logger.info("%s: 신규 데이터 없음", date_str)
        return 0

    rows = [strip_pagination_meta(r) for r in raw_rows]
    actual_columns = list(rows[0].keys())
    validate_and_report(actual_columns)

    raw_df = spark.createDataFrame(rows)
    select_exprs = build_select_exprs(actual_columns)
    mapped_df = raw_df.select(*select_exprs)

    bronze_df = (
        mapped_df.withColumn("reg_date_partition", F.lit(date_str))
        .withColumn("source_file", F.lit(f"api:{date_str}"))
        .withColumn("ingested_at", F.current_timestamp())
        .cache()
    )
    row_count = bronze_df.count()
    bronze_df.writeTo(_table_name()).overwritePartitions()
    bronze_df.unpersist()

    logger.info("%s: %d행 적재 완료", date_str, row_count)
    return row_count


def run() -> None:
    if config.SETTINGS.seoul_api_key == "sample":
        logger.warning(
            "SEOUL_API_KEY가 'sample'(데모 키)입니다. data.seoul.go.kr에서 발급받은 "
            "실제 인증키로 .env를 교체하지 않으면 API 호출이 계속 실패합니다."
        )

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-daily-batch-failure-report")

    last_processed = read_watermark(watermark_key=WATERMARK_KEY)
    start_date = last_processed + timedelta(days=1)
    end_date = date.today() - timedelta(days=1)

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
        logger.info("처리할 신규 날짜 없음 (워터마크=%s)", last_processed)
        return

    current = start_date
    while current <= end_date:
        try:
            _process_one_day(spark, current)
            write_watermark(current, watermark_key=WATERMARK_KEY)
        except (SchemaValidationError, SeoulApiError, SeoulApiTransientError) as e:
            logger.error("%s 처리 실패, 배치 중단: %s", current, e)
            sys.exit(1)
        current += timedelta(days=1)


if __name__ == "__main__":
    run()
