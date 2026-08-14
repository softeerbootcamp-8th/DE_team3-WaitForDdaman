"""
Silver merge (daily 전용) - staging -> silver.failure_report MERGE INTO.

유일키(bike_no, reg_dttm, failure_type) 3개가 전부 일치하면 나머지 컬럼이 없는
스키마라 완전히 동일한 행이다. WHEN MATCHED에서 할 일이 없어 신규 행만 INSERT한다.
재실행해도 안전(멱등)하다.

사용법:
    python -m jobs.silver_failure_report_merge
"""
import logging

from common import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CATALOG = config.SETTINGS.iceberg_catalog_name
SILVER_TABLE = f"{CATALOG}.silver.failure_report"
SILVER_STAGING_TABLE = f"{CATALOG}.silver.failure_report_staging"


def _ensure_silver_table(spark) -> None:
    silver_db = SILVER_TABLE.rsplit(".", 1)[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            bike_no STRING,
            reg_dttm TIMESTAMP,
            failure_type STRING
        )
        USING iceberg
        PARTITIONED BY (days(reg_dttm))
        """
    )


def run() -> None:
    spark = build_spark_session("silver-merge-failure-report")
    _ensure_silver_table(spark)

    staging_count = spark.table(SILVER_STAGING_TABLE).count()

    spark.sql(
        f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING {SILVER_STAGING_TABLE} AS source
        ON  target.bike_no = source.bike_no
        AND target.reg_dttm = source.reg_dttm
        AND target.failure_type = source.failure_type
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    logger.info("MERGE 완료 (staging %d행 대상)", staging_count)


if __name__ == "__main__":
    run()
