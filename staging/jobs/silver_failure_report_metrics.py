"""
Silver metrics - silver.failure_report 최종 행수를 로그로 남긴다.

현재는 별도 메트릭 저장소가 없어 로그 출력만 한다 - 필요해지면
ingestion/common/s3_utils.py의 put_json 같은 걸로 확장하면 된다.

사용법:
    python -m jobs.silver_failure_report_metrics
"""
import logging

import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SILVER_TABLE = f"{config.SETTINGS.iceberg_catalog_name}.silver.failure_report"


def run() -> None:
    spark = build_spark_session("silver-metrics-failure-report")

    row_count = spark.table(SILVER_TABLE).count()
    logger.info("silver.failure_report: %d행", row_count)


if __name__ == "__main__":
    run()
