"""
Silver metrics - 이번 처리 구간(daily: TARGET_DS, backfill: START_DATE~END_DATE)의
silver.failure_report 최종 행수를 로그로 남긴다.

현재는 별도 메트릭 저장소가 없어 로그 출력만 한다 - 필요해지면
ingestion/common/s3_utils.py의 put_json 같은 걸로 확장하면 된다.

사용법:
    TARGET_DS=2026-08-13 python -m jobs.silver_failure_report_metrics
    START_DATE=2026-01-01 END_DATE=2026-06-30 python -m jobs.silver_failure_report_metrics
"""
import logging
import os
from datetime import date, timedelta

from pyspark.sql import functions as F

from common import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SILVER_TABLE = f"{config.SETTINGS.iceberg_catalog_name}.silver.failure_report"


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
    spark = build_spark_session("silver-metrics-failure-report")

    exclusive_end = end + timedelta(days=1)
    row_count = (
        spark.table(SILVER_TABLE)
        .where((F.col("reg_dttm") >= str(start)) & (F.col("reg_dttm") < str(exclusive_end)))
        .count()
    )
    logger.info("silver.failure_report %s~%s: %d행", start, end, row_count)


if __name__ == "__main__":
    run()
