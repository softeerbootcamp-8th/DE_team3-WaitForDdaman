"""
Iceberg mart 파티션과 postgres 서빙 테이블의 row count를 비교하는 공용 검증기.

build_mart_*/write_* 태스크와 별도 태스크로 분리하기 위한 모듈 - "Gold 읽기"/"RDS
write"/"검증"을 원자적인 별도 Airflow 태스크로 나누라는 요구사항(spec §6)에 따름.

### Spark 제거 (#172)
`spark.read.table(...).filter(...).count()`는 데이터 파일을 실제로 스캔한다.
`table.inspect.partitions()`는 매니페스트(파티션 메타데이터)만 읽어서 훨씬 가볍다
(ingestion/jobs/check_watermark_date.py와 동일 idiom) - mart 테이블은
PARTITIONED BY (snapshot_date)라 파티션당 record_count가 이미 그 파티션의 전체
행수와 같다.

사용법 (둘 다 필수):
    ICEBERG_TABLE=bike_catalog.gold.mart_bike_risk_daily POSTGRES_TABLE=bike_risk_daily \
        SNAPSHOT_DATE=2026-08-18 python -m jobs.verify_serving_sync
"""
import logging
import os
import sys
from datetime import date

import pandas as pd

import config
from common.iceberg_catalog import build_iceberg_catalog
from serving_db import count_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class ServingSyncVerificationError(Exception):
    """Iceberg <-> Postgres row count 불일치."""


def _strip_catalog_prefix(table_identifier: str, catalog_name: str) -> str:
    """ICEBERG_TABLE 환경변수는 Spark 시절 관례로 카탈로그 접두사가 붙은 전체
    식별자(`bike_catalog.gold.mart_bike_risk_daily`)를 쓴다. pyiceberg의
    catalog.load_table()은 카탈로그가 이미 정해진 객체라 접두사가 없어야 한다."""
    prefix = f"{catalog_name}."
    return table_identifier[len(prefix):] if table_identifier.startswith(prefix) else table_identifier


def _partition_row_count(parts_df: pd.DataFrame, snapshot_date: date) -> int:
    """table.inspect.partitions().to_pandas() 결과에서 특정 snapshot_date 파티션의
    record_count 합을 구한다 - 카탈로그 없이 동작하는 순수 로직이라 단위 테스트가
    가능하다. 매칭되는 파티션이 없으면(테이블/날짜 없음) 0을 반환한다."""
    if parts_df.empty:
        return 0
    matches = parts_df[parts_df["partition"].apply(lambda p: p["snapshot_date"] == snapshot_date)]
    if matches.empty:
        return 0
    return int(matches["record_count"].sum())


def verify_counts(catalog, iceberg_table: str, postgres_table: str, snapshot_date_str: str) -> None:
    table_identifier = _strip_catalog_prefix(iceberg_table, config.SETTINGS.iceberg_catalog_name)
    table = catalog.load_table(table_identifier)
    parts_df = table.inspect.partitions().to_pandas()
    iceberg_count = _partition_row_count(parts_df, date.fromisoformat(snapshot_date_str))

    pg_count = count_rows(postgres_table, snapshot_date_str)
    if iceberg_count != pg_count:
        raise ServingSyncVerificationError(
            f"{postgres_table} row count 불일치: iceberg={iceberg_count}, postgres={pg_count} "
            f"(snapshot_date={snapshot_date_str})"
        )
    logger.info("%s: 검증 통과 (iceberg=%d, postgres=%d)", postgres_table, iceberg_count, pg_count)


def run() -> None:
    iceberg_table = os.environ["ICEBERG_TABLE"]
    postgres_table = os.environ["POSTGRES_TABLE"]
    snapshot_date_str = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    catalog = build_iceberg_catalog()
    try:
        verify_counts(catalog, iceberg_table, postgres_table, snapshot_date_str)
    except ServingSyncVerificationError as e:
        logger.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    run()
