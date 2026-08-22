"""
Bronze 일 배치 - 실시간 대여정보 (OA-15493 / bikeList)

이 원천을 수집하는 목적은 "지금 실제로 운영 중인 대여소가 어디인지" 판별하는 것이다
(2026-08-16 사용자 확인). 거치대 수/주차된 자전거 수/거치율 자체를 활용할 계획은 없다.

이 원천은 파일 백필이 없다. API 일일 전체 스냅샷 하나로만 적재한다.
station_master(tbCycleStationInfo)와 같은 "날짜 파라미터 없는 전체 스냅샷" 구조라
워터마크가 필요 없고, 파티션 키도 이벤트 발생일이 아니라 snapshot_date다.

⚠️ 과거를 소급 조회할 수 없다. 오늘 스냅샷을 놓치면 오늘 어떤 대여소가 실제로
   운영 중이었는지 영구히 알 수 없게 된다. 실패 시 반드시 사람이 인지해야 한다.

사용법:
    python -m jobs.daily_batch_station_active
    SNAPSHOT_DATE=2026-08-14 python -m jobs.daily_batch_station_active   # 재처리 전용
"""
import logging
import os
import sys
from datetime import date

from pyspark.sql import functions as F

import config
from common.api_client import (
    SeoulApiError,
    SeoulApiTransientError,
    fetch_station_active,
    strip_pagination_meta,
)
from common.s3_utils import ensure_bucket, get_json, put_json
from common.spark_session import build_spark_session
from schema.station_active_schema import (
    SchemaValidationError,
    build_select_exprs,
    collect_response_fields,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.station_active"


def _ensure_bronze_table(spark) -> None:
    """
    Bronze 원칙대로 원본 필드를 전부 STRING으로 보존한다.
    타입 캐스팅은 Silver 책임 - 여기서 캐스팅하면 실패한 값이 조용히 null이 되어
    원천에 무엇이 왔는지 알 수 없게 된다.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name()} (
            station_id           STRING COMMENT '대여소 ID (ST-4), 골드 조인 키',
            station_name         STRING COMMENT '대여소명',
            rack_tot_cnt         STRING COMMENT '거치대 수',
            parking_bike_tot_cnt STRING COMMENT '주차된 자전거 수',
            shared               STRING COMMENT '거치율 (100 초과 가능)',
            latitude             STRING COMMENT '위도',
            longitude            STRING COMMENT '경도',
            snapshot_date        STRING COMMENT '스냅샷 기준일 (YYYY-MM-DD), 파티션 키',
            source_file          STRING COMMENT '출처 (api:YYYY-MM-DD)',
            ingested_at          TIMESTAMP COMMENT '적재 시각'
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )
    # Iceberg가 직접 분산/정렬하게 해서 FanoutWriter의 높은 메모리 사용
    # (파티션별 파일 동시 오픈)을 피한다.
    spark.sql(
        f"ALTER TABLE {_table_name()} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )


def _process_snapshot(spark, snapshot_date: str) -> int:
    settings = config.SETTINGS
    s3_key = f"raw/station_active/api/snapshot_date={snapshot_date}/payload.json"

    if settings.raw_source == "s3":
        logger.info("%s: S3 raw payload 읽기 시도 (s3://%s/%s)", snapshot_date, settings.raw_bucket, s3_key)
        payload = get_json(settings.raw_bucket, s3_key)
        if payload is None:
            raise FileNotFoundError(
                f"S3 raw payload가 존재하지 않습니다: s3://{settings.raw_bucket}/{s3_key}. "
                f"fetch_station_active_raw Lambda가 실패했거나 아직 실행되지 않았습니다."
            )
        raw_rows = payload.get("rows", [])
    else:
        logger.info("%s: 공공 API 직접 호출 (RAW_SOURCE=api)", snapshot_date)
        raw_rows = list(fetch_station_active())

        # 변환 전 원본 응답을 그대로 보존한다 (lineage).
        ensure_bucket(settings.raw_bucket)
        put_json(
            settings.raw_bucket,
            s3_key,
            {"snapshot_date": snapshot_date, "row_count": len(raw_rows), "rows": raw_rows},
        )

    if not raw_rows:
        logger.warning("%s: raw 데이터가 비어있음 (0건) - 적재 생략", snapshot_date)
        return 0

    rows = [strip_pagination_meta(r) for r in raw_rows]
    actual_columns = collect_response_fields(rows)
    logger.info("필드 목록: %s", actual_columns)
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
    settings = config.SETTINGS
    if settings.raw_source == "api" and settings.seoul_api_key in ("", "sample"):
        logger.error(
            "RAW_SOURCE=api 모드이지만 SEOUL_API_KEY가 비어있거나 'sample'(데모 키)입니다. "
            "data.seoul.go.kr에서 발급받은 실제 인증키로 ingestion/.env를 채우세요."
        )
        sys.exit(1)

    ensure_bucket(settings.raw_bucket)
    ensure_bucket(settings.warehouse_bucket)

    spark = build_spark_session("bronze-daily-batch-station-active")
    _ensure_bronze_table(spark)

    # 이 API는 "지금 시점의 전체 스냅샷"만 주므로 과거 날짜를 소급 조회할 수 없다.
    # 기본값은 오늘이며, SNAPSHOT_DATE는 재처리 목적으로만 쓴다.
    snapshot_date = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    try:
        _process_snapshot(spark, snapshot_date)
    except (SchemaValidationError, SeoulApiError, SeoulApiTransientError, FileNotFoundError) as e:
        logger.error("%s 스냅샷 처리 실패: %s", snapshot_date, e)
        sys.exit(1)


if __name__ == "__main__":
    run()
