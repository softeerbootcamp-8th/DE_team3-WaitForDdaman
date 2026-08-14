"""
Silver transform - bronze.failure_report 원본 전체를 확정 스키마로 변환해
staging 테이블(silver.failure_report_staging)에 적재한다.

- reg_dttm: STRING -> TIMESTAMP. 원본 포맷이 'yyyy-MM-dd HH:mm:ss'(19자, 2026-01~06
  API 수집분 실측 100%)와 'yyyyMMdd'(일부 backfill 파일, ingestion/jobs/
  backfill_failure_report.py:_derive_date_partition 주석 참고) 둘 다 나타날 수
  있어 순서대로 시도한다.
- failure_type: 뒤쪽 공백 제거(trim) - bronze CSV 파서가 ignoreTrailingWhiteSpace=false라
  '기타 ', '타이어 ' 같은 원본 공백이 그대로 살아있다.
- bike_no: 원본 그대로 (형식 100% 검증된 값이라 추가 정제 불필요).

집계/조인 없음 - 그레인은 bronze 1행 = silver 1행 그대로 유지한다(고장부위 신고
1건 = 1행). 단일 DAG가 매번 브론즈 전체를 재처리하는 구조라 이 잡도 항상 bronze
전체를 읽고, staging은 실행할 때마다 그 결과로 전체 덮어써진다(overwritePartitions,
아래 참고) - 이전 실행분이 남아 중복 누적되는 일은 없다.

사용법:
    python -m jobs.silver_failure_report_transform
"""
import logging

from pyspark.sql import functions as F

import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CATALOG = config.SETTINGS.iceberg_catalog_name
BRONZE_TABLE = f"{CATALOG}.bronze.failure_report"
SILVER_STAGING_TABLE = f"{CATALOG}.silver.failure_report_staging"


def _ensure_staging_table(spark) -> None:
    silver_db = SILVER_STAGING_TABLE.rsplit(".", 1)[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_STAGING_TABLE} (
            bike_no STRING,
            reg_dttm TIMESTAMP,
            failure_type STRING
        )
        USING iceberg
        """
    )


def run() -> None:
    spark = build_spark_session("silver-transform-failure-report")
    _ensure_staging_table(spark)

    bronze_df = spark.table(BRONZE_TABLE)

    # 원본 시각 표기가 파일마다 제각각(0패딩 유무, 구분자 -/./없음, 초/시각 유무 자체가
    # 다름)이라 모든 변형에 안전한 timestamp 패턴을 다 나열하는 대신 날짜 부분만 뽑아
    # 자정(00:00:00) TIMESTAMP로 통일한다. 다운스트림(risk_model)이 reg_dttm을
    # 날짜 단위로만 쓰고 있어 시각 정밀도가 필요 없고, bronze가 원본 문자열을 그대로
    # 보존하므로 나중에 시각까지 필요해지면 거기서 다시 뽑아 쓸 수 있다.
    date_only = F.regexp_replace(
        F.regexp_extract(F.col("reg_dttm"), r"(\d{4}[-.]\d{1,2}[-.]\d{1,2})", 1),
        r"\.",
        "-",
    )
    reg_dttm_ts = F.coalesce(
        F.to_date(date_only, "yyyy-M-d"),
        F.to_date(F.col("reg_dttm"), "yyyyMMdd"),
    ).cast("timestamp")

    silver_df = bronze_df.select(
        F.col("bike_no"),
        reg_dttm_ts.alias("reg_dttm"),
        F.trim(F.col("failure_type")).alias("failure_type"),
    )

    silver_df.writeTo(SILVER_STAGING_TABLE).overwritePartitions()

    row_count = spark.table(SILVER_STAGING_TABLE).count()
    logger.info("staging 적재 완료: %d행 (bronze 전체)", row_count)


if __name__ == "__main__":
    run()
