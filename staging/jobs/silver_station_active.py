"""
Silver - 실시간 대여정보 필터 테이블

이 원천(bikeList)을 수집하는 목적은 재고 수치(거치대 수/주차된 자전거 수/거치율)가
아니라 "오늘 실제로 운영 중인 대여소가 어디인지" 판별하는 것이다(bronze의
schema/station_active_schema.py 참고). 대여소명·위경도·자치구 등 서술 속성은
silver.station_master가 이미 갖고 있으므로, 여기서는 그 날 API 응답에 실제로
존재했던 station_id 집합만 남긴다.

운영/미운영 자체의 최종 판정(Gold의 build_station_active)은 이 테이블을 넘겨받는
담당 4가 한다. 담당 2는 그 판정 로직을 구현하지 않는다.

사용법:
    python -m jobs.silver_station_active
    SNAPSHOT_DATE=2026-08-14 python -m jobs.silver_station_active   # 특정 날짜 재처리
"""
import logging
import os
import sys
from datetime import date

from pyspark.sql import functions as F

import config
from common.spark_session import build_spark_session
from common.watermark import write_watermark
from config.watermark_keys import SILVER_STATION_ACTIVE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SILVER_COLUMNS = ["snapshot_date", "station_id"]


def normalize(bronze_df):
    """
    브론즈 DataFrame을 실버 2컬럼(snapshot_date, station_id)으로 정제한다.
    읽기/쓰기를 하지 않는다.

    station_id가 null인 행은 드롭한다. 같은 스냅샷 내 station_id 중복은
    dropDuplicates로 하나만 남긴다.
    """
    df = bronze_df.select(
        F.col("snapshot_date").cast("date").alias("snapshot_date"),
        F.col("station_id"),
    )

    not_null = df.filter(F.col("station_id").isNotNull())
    deduped = not_null.dropDuplicates(["station_id"])

    return deduped.select(*SILVER_COLUMNS)


def _log_quality(bronze_df, silver_df) -> None:
    """드롭/중복 건수를 남긴다. 조용히 넘기면 원천 이상을 놓친다."""
    total = bronze_df.count()
    null_count = bronze_df.filter(F.col("station_id").isNull()).count()
    kept = silver_df.count()
    dup_count = total - null_count - kept

    logger.info("브론즈 %d행 -> 실버 %d행", total, kept)
    if null_count:
        logger.warning("station_id null: %d건 (드롭)", null_count)
    if dup_count > 0:
        logger.warning("station_id 중복: %d건 (dropDuplicates로 제거)", dup_count)


def _table_name(layer: str) -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.{layer}.station_active"


def _ensure_silver_table(spark) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name("silver")} (
            snapshot_date DATE   COMMENT '스냅샷 기준일, 파티션 키',
            station_id    STRING COMMENT '대여소 ID (ST-4), station_master 조인 키'
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )
    spark.sql(
        f"ALTER TABLE {_table_name('silver')} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )


def _read_bronze(spark, snapshot_date: str | None):
    """station_active는 파일 백필이 없으므로 station_master와 달리 source_file 필터가 필요 없다."""
    df = spark.table(_table_name("bronze"))

    if snapshot_date:
        return df.filter(F.col("snapshot_date") == snapshot_date), snapshot_date

    latest = df.agg(F.max("snapshot_date")).collect()[0][0]
    if latest is None:
        raise ValueError("브론즈에 station_active 스냅샷이 없습니다. 일 배치를 먼저 실행하세요.")
    return df.filter(F.col("snapshot_date") == latest), latest


def run() -> None:
    spark = build_spark_session("silver-station-active")
    _ensure_silver_table(spark)

    bronze_df, snapshot_date = _read_bronze(spark, os.getenv("SNAPSHOT_DATE"))
    logger.info("브론즈 스냅샷 %s 처리 시작", snapshot_date)

    bronze_df = bronze_df.cache()
    silver_df = normalize(bronze_df).cache()
    _log_quality(bronze_df, silver_df)

    if silver_df.count() == 0:
        logger.error("%s: 정제 후 남은 행이 0건 - silver 적재 중단", snapshot_date)
        bronze_df.unpersist()
        silver_df.unpersist()
        sys.exit(1)

    silver_df.writeTo(_table_name("silver")).overwritePartitions()
    bronze_df.unpersist()
    silver_df.unpersist()

    # dag_gold_dim_fact가 이 워터마크로 "오늘 치 실버가 준비됐는지" 확인한다
    # (Asset 트리거 DagRun은 logical_date가 없어 ExternalTaskSensor를 못 씀).
    processed_date = snapshot_date if isinstance(snapshot_date, date) else date.fromisoformat(snapshot_date)
    write_watermark(processed_date, watermark_key=SILVER_STATION_ACTIVE)

    logger.info("%s: 실버 적재 완료", snapshot_date)


if __name__ == "__main__":
    run()
