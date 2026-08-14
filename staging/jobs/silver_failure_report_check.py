"""
Silver check - bronze.failure_report에 처리 대상 구간 데이터가 있는지 확인한다.

daily는 TARGET_DS(={{ ds }}) 하루치, backfill은 START_DATE~END_DATE 기간을 본다.
bronze.failure_report.reg_date_partition은 적재일이 아니라 reg_dttm 값에서 뽑아낸
날짜다(ingestion/jobs/backfill_failure_report.py:_derive_date_partition 참고) -
그래서 이 컬럼으로 필터링하면 "그 날짜에 실제로 신고된 데이터"를 정확히 골라낼 수 있다.

사용법:
    TARGET_DS=2026-08-13 python -m jobs.silver_failure_report_check
    START_DATE=2026-01-01 END_DATE=2026-06-30 python -m jobs.silver_failure_report_check
"""
import logging
import os
import sys
from datetime import date

from pyspark.sql import functions as F

from common import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BRONZE_TABLE = f"{config.SETTINGS.iceberg_catalog_name}.bronze.failure_report"


class ScopeError(Exception):
    """TARGET_DS도 START_DATE/END_DATE도 없을 때 - 잡을 즉시 실패시켜야 한다."""


def _resolve_date_range() -> tuple[date, date]:
    target_ds = os.getenv("TARGET_DS", "").strip()
    if target_ds:
        d = date.fromisoformat(target_ds)
        return d, d

    start_str = os.getenv("START_DATE", "").strip()
    end_str = os.getenv("END_DATE", "").strip()
    if start_str and end_str:
        start, end = date.fromisoformat(start_str), date.fromisoformat(end_str)
        if start > end:
            raise ScopeError(f"START_DATE({start})가 END_DATE({end})보다 뒤에 있음")
        return start, end

    raise ScopeError("TARGET_DS 또는 START_DATE/END_DATE 중 하나는 반드시 필요함")


def run() -> None:
    start, end = _resolve_date_range()
    spark = build_spark_session("silver-check-failure-report")

    count = (
        spark.table(BRONZE_TABLE)
        .where((F.col("reg_date_partition") >= str(start)) & (F.col("reg_date_partition") <= str(end)))
        .count()
    )
    logger.info("bronze.failure_report %s~%s: %d행", start, end, count)

    if count == 0:
        logger.error("해당 구간에 bronze 데이터가 없음 - bronze 적재 완료 여부 확인 필요")
        sys.exit(1)


if __name__ == "__main__":
    run()
