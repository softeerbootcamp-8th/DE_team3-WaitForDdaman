"""
Bronze 일 배치 잡 - 서울시 공공자전거 대여이력 (OA-15182)

전략: 증분 기준 = RENT_DT(대여일자)
- 워터마크(마지막으로 성공 처리된 날짜) 다음날부터 "어제"까지 날짜별로 순차 처리
  (오늘 데이터는 원천에서 아직 확정 전이므로 제외)
- 하루 처리가 "완전히" 성공했을 때만 그 날짜로 워터마크를 갱신한다
  -> 중간에 실패하면 워터마크가 마지막 성공일에 머물러 있어, 재실행 시 누락 없이 이어서 처리됨
- 재실행 시 동일 날짜 파티션을 덮어써서 멱등성 보장 (같은 날 두 번 돌려도 중복 적재 안 됨)

사용법:
    python -m jobs.daily_batch_rental_history
    MAX_DAYS_PER_RUN=1 python -m jobs.daily_batch_rental_history   # 로컬 테스트: 하루치만 처리
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
    fetch_rent_history_by_date,
    strip_pagination_meta,
)
from common.s3_utils import ensure_bucket, put_json
from common.spark_session import build_spark_session
from common.watermark import read_watermark, write_watermark
from schema.rental_history_schema import (
    SchemaValidationError,
    build_select_exprs,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.rental_history"


def _process_one_day(spark, target_date: date) -> int:
    # tbCycleRentData는 하루를 한 번에 못 주고 시간(0~23) 단위로 나눠서 응답하므로,
    # fetch_rent_history_by_date 내부에서 24번 호출해 하루치를 모아온다.
    raw_rows = list(fetch_rent_history_by_date(target_date))
    date_str = target_date.strftime("%Y-%m-%d")

    # API 원본 응답을 raw zone에 그대로 보존 (감사/재처리 대비)
    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/rental_history/api/rent_dt={date_str}/payload.json",
        {"rent_dt": date_str, "row_count": len(raw_rows), "rows": raw_rows},
    )

    if not raw_rows:
        logger.info("%s: 신규 데이터 없음", date_str)
        return 0

    # START_INDEX/END_INDEX/RNUM은 페이징 메타데이터일 뿐 실제 데이터 컬럼이 아니므로
    # 스키마 검증 전에 제거한다 (안 지우면 매 호출마다 "알 수 없는 컬럼" 경고가 계속 발생함)
    rows = [strip_pagination_meta(r) for r in raw_rows]

    actual_columns = list(rows[0].keys())
    validate_and_report(actual_columns)  # 필수 컬럼 없으면 SchemaValidationError -> 상위에서 배치 중단

    raw_df = spark.createDataFrame(rows)
    select_exprs = build_select_exprs(actual_columns)
    mapped_df = raw_df.select(*select_exprs)

    bronze_df = (
        mapped_df.withColumn("rent_date_partition", F.lit(date_str))
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
    # 백필을 안 거치고 daily_batch만 단독 실행하는 경우에도 안전하도록 버킷을 보장한다.
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-daily-batch-rental-history")

    last_processed = read_watermark()
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
            write_watermark(current)  # 이 날짜까지는 안전하게 커밋됨 - 성공한 날짜만 워터마크 갱신
        except (SchemaValidationError, SeoulApiError, SeoulApiTransientError) as e:
            # 안전하게 실패: 이후 날짜는 처리하지 않고 배치를 중단한다 (워터마크는 마지막 성공일 유지)
            logger.error("%s 처리 실패, 배치 중단: %s", current, e)
            sys.exit(1)
        current += timedelta(days=1)


if __name__ == "__main__":
    run()