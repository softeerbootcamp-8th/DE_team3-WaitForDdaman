"""
Silver transform - bronze.failure_report 원본을 확정 스키마로 변환해
staging 테이블(silver.failure_report_staging)에 적재한다.

- reg_dttm: STRING -> TIMESTAMP. 원본 포맷이 'yyyy-MM-dd HH:mm:ss'(19자, 2026-01~06
  API 수집분 실측 100%)와 'yyyyMMdd'(일부 backfill 파일, ingestion/jobs/
  backfill_failure_report.py:_derive_date_partition 주석 참고) 둘 다 나타날 수
  있어 순서대로 시도한다.
- failure_type: 뒤쪽 공백 제거(trim) - bronze CSV 파서가 ignoreTrailingWhiteSpace=false라
  '기타 ', '타이어 ' 같은 원본 공백이 그대로 살아있다.
- bike_no: 원본 그대로 (형식 100% 검증된 값이라 추가 정제 불필요).

집계/조인 없음 - 그레인은 bronze 1행 = silver 1행 그대로 유지한다(고장부위 신고
1건 = 1행). staging은 이 잡을 실행할 때마다 처리 구간 결과로 전체 덮어써진다.

사용법:
    TARGET_DS=2026-08-13 python -m jobs.silver_failure_report_transform
    START_DATE=2026-01-01 END_DATE=2026-06-30 python -m jobs.silver_failure_report_transform
"""
import logging
import os
from datetime import date

from pyspark.sql import functions as F

import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CATALOG = config.SETTINGS.iceberg_catalog_name
BRONZE_TABLE = f"{CATALOG}.bronze.failure_report"
SILVER_STAGING_TABLE = f"{CATALOG}.silver.failure_report_staging"


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
    start, end = _resolve_date_range()
    spark = build_spark_session("silver-transform-failure-report")
    _ensure_staging_table(spark)

    bronze_df = spark.table(BRONZE_TABLE).where(
        (F.col("reg_date_partition") >= str(start)) & (F.col("reg_date_partition") <= str(end))
    )

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

    # staging은 파티션 없는 테이블이라 overwritePartitions()가 전체 덮어쓰기와
    # 동일하게 동작한다 (bronze 잡들과 같은 관용구) - 매 실행마다 이번 처리 구간
    # 결과로 통째로 교체된다.
    silver_df.writeTo(SILVER_STAGING_TABLE).overwritePartitions()

    row_count = spark.table(SILVER_STAGING_TABLE).count()
    logger.info("staging 적재 완료: %d행 (%s~%s)", row_count, start, end)


if __name__ == "__main__":
    run()
