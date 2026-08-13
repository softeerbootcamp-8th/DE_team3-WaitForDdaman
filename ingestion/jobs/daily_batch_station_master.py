"""
Bronze 일 배치 잡 - 서울시 공공자전거 대여소 정보 (OA-13252)

앞선 두 데이터셋과 다른 점: tbCycleStationInfo API는 날짜 파라미터를 받지 않고
호출 시점의 "전체 대여소 스냅샷"을 반환한다. 그래서:
    - 워터마크가 필요 없다 (증분 기준이 될 컬럼 자체가 없음)
    - 매일 실행하면 그 날짜의 스냅샷이 하나씩 쌓인다 → snapshot_date 파티션
    - 재실행하면 같은 날짜 파티션을 덮어쓴다 (멱등성)

이렇게 매일 스냅샷을 축적해두는 게 Silver의 SCD Type 2(유효기간 부여)의 입력이 된다.
원천이 반기 단위로만 파일을 공개하는 것과 달리, API를 매일 찍어두면 신설/폐쇄를
하루 단위로 포착할 수 있다.

사용법:
    python -m jobs.daily_batch_station_master
    SNAPSHOT_DATE=2026-08-11 python -m jobs.daily_batch_station_master   # 특정 날짜로 적재
"""
import logging
import os
import sys
from datetime import date

from pyspark.sql import functions as F

from common import config
from common.api_client import (
    SeoulApiError,
    SeoulApiTransientError,
    fetch_station_info,
    strip_pagination_meta,
)
from common.s3_utils import ensure_bucket, put_json
from common.spark_session import build_spark_session
from schema.station_master_schema import (
    SchemaValidationError,
    build_select_exprs,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.station_master"


def _ensure_bronze_table(spark) -> None:
    """백필 잡과 동일한 테이블을 공유한다 (백필=파일 소스, 일배치=API 소스)."""
    from jobs.backfill_station_master import _ensure_bronze_table as ensure_table

    ensure_table(spark)


def _process_snapshot(spark, snapshot_date: str) -> int:
    raw_rows = list(fetch_station_info())

    ensure_bucket(config.SETTINGS.raw_bucket)
    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/station_master/api/snapshot_date={snapshot_date}/payload.json",
        {"snapshot_date": snapshot_date, "row_count": len(raw_rows), "rows": raw_rows},
    )

    if not raw_rows:
        logger.warning("%s: API 응답이 비어있음 (0건) - 적재 생략", snapshot_date)
        return 0

    # START_INDEX/END_INDEX/RNUM은 페이징 메타데이터라 실제 데이터 컬럼이 아니므로 제거
    rows = [strip_pagination_meta(r) for r in raw_rows]
    actual_columns = list(rows[0].keys())
    logger.info("API 응답 필드: %s", actual_columns)
    validate_and_report(actual_columns)

    raw_df = spark.createDataFrame(rows)
    mapped_df = raw_df.select(*build_select_exprs(actual_columns))

    bronze_df = (
        mapped_df.withColumn("snapshot_date", F.lit(snapshot_date))
        .withColumn("source_file", F.lit(f"api:{snapshot_date}"))
        .withColumn("ingested_at", F.current_timestamp())
        .cache()
    )
    row_count = bronze_df.count()
    bronze_df.writeTo(_table_name()).overwritePartitions()
    bronze_df.unpersist()

    logger.info("%s: %d행 적재 완료", snapshot_date, row_count)
    return row_count


def run() -> None:
    if config.SETTINGS.seoul_api_key == "sample":
        logger.warning(
            "SEOUL_API_KEY가 'sample'(데모 키)입니다. data.seoul.go.kr에서 발급받은 "
            "실제 인증키로 .env를 교체하지 않으면 API 호출이 계속 실패합니다."
        )

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-daily-batch-station-master")
    _ensure_bronze_table(spark)

    # 이 API는 "지금 시점의 전체 스냅샷"만 주므로 과거 날짜를 소급 조회할 수 없다.
    # 따라서 기본값은 오늘이며, 재처리 목적으로만 SNAPSHOT_DATE를 명시한다.
    snapshot_date = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    try:
        _process_snapshot(spark, snapshot_date)
    except (SchemaValidationError, SeoulApiError, SeoulApiTransientError) as e:
        logger.error("%s 스냅샷 처리 실패: %s", snapshot_date, e)
        sys.exit(1)


if __name__ == "__main__":
    run()
