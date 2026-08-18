"""
Iceberg mart 파티션과 postgres 서빙 테이블의 row count를 비교하는 공용 검증기.

build_mart_*/write_* 태스크와 별도 태스크로 분리하기 위한 모듈 - "Gold 읽기"/"RDS
write"/"검증"을 원자적인 별도 Airflow 태스크로 나누라는 요구사항(spec §6)에 따름.

사용법 (둘 다 필수):
    ICEBERG_TABLE=bike_catalog.gold.mart_bike_risk_daily POSTGRES_TABLE=bike_risk_daily \
        SNAPSHOT_DATE=2026-08-18 python -m jobs.verify_serving_sync
"""
import logging
import os
import sys
from datetime import date

from pyspark.sql import functions as F

from common.spark_session import build_spark_session
from serving_db import count_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class ServingSyncVerificationError(Exception):
    """Iceberg <-> Postgres row count 불일치."""


def verify_counts(spark, iceberg_table: str, postgres_table: str, snapshot_date: str) -> None:
    iceberg_count = spark.read.table(iceberg_table).filter(F.col("snapshot_date") == snapshot_date).count()
    pg_count = count_rows(postgres_table, snapshot_date)
    if iceberg_count != pg_count:
        raise ServingSyncVerificationError(
            f"{postgres_table} row count 불일치: iceberg={iceberg_count}, postgres={pg_count} "
            f"(snapshot_date={snapshot_date})"
        )
    logger.info("%s: 검증 통과 (iceberg=%d, postgres=%d)", postgres_table, iceberg_count, pg_count)


def run() -> None:
    iceberg_table = os.environ["ICEBERG_TABLE"]
    postgres_table = os.environ["POSTGRES_TABLE"]
    snapshot_date_str = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    spark = build_spark_session("serving-sync-verify")
    try:
        verify_counts(spark, iceberg_table, postgres_table, snapshot_date_str)
    except ServingSyncVerificationError as e:
        logger.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    run()
