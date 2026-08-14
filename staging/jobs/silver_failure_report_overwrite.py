"""
Silver overwrite (backfill 전용) - staging -> silver.failure_report INSERT OVERWRITE.

common/spark_session.py의 build_spark_session()이 partitionOverwriteMode=dynamic을
이미 설정해두므로 writeTo(...).overwritePartitions()는 staging에 존재하는
days(reg_dttm) 파티션만 덮어쓰고 나머지 기존 파티션은 그대로 둔다. bronze 잡들과
동일한 멱등성 패턴이라 재실행해도 안전하다.

사용법:
    python -m jobs.silver_failure_report_overwrite
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
    spark = build_spark_session("silver-overwrite-failure-report")
    _ensure_silver_table(spark)

    staging_df = spark.table(SILVER_STAGING_TABLE)
    row_count = staging_df.count()
    staging_df.writeTo(SILVER_TABLE).overwritePartitions()

    logger.info("INSERT OVERWRITE 완료: %d행", row_count)


if __name__ == "__main__":
    run()
