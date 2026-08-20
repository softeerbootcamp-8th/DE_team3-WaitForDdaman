"""
Silver overwrite - staging -> silver.failure_report INSERT OVERWRITE.

단일 DAG가 매번 브론즈 전체를 재처리하므로 staging도 매번 전체 데이터를 담고 있다.
common/spark_session.py의 build_spark_session()이 partitionOverwriteMode=dynamic을
이미 설정해두므로 writeTo(...).overwritePartitions()는 staging에 존재하는
days(reg_dttm) 파티션(=전체)만 덮어쓴다. bronze 잡들과 동일한 멱등성 패턴이라
재실행해도 안전하다.

사용법:
    python -m jobs.silver_failure_report_overwrite
"""
import logging

import config
from common.spark_session import build_spark_session
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import BRONZE_FAILURE_REPORT, SILVER_FAILURE_REPORT

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

    # overwrite가 끝난 뒤에만 갱신 - 이 시점의 Bronze 워터마크까지 Silver에
    # 반영됐다는 의미로 그대로 이어받는다
    bronze_watermark = read_watermark(watermark_key=BRONZE_FAILURE_REPORT)
    write_watermark(bronze_watermark, watermark_key=SILVER_FAILURE_REPORT)

    logger.info("INSERT OVERWRITE 완료: %d행 (Silver 워터마크=%s)", row_count, bronze_watermark)


if __name__ == "__main__":
    run()
