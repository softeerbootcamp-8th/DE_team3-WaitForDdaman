"""
Gold(TEMP) - 활성 대여소 목록 (silver.station_master + silver.station_active -> gold.station_active)

### 조인의 의미
silver.station_master는 "등록된 모든 대여소"의 최신 스냅샷이고, silver.station_active는
"현재 실시간으로 상태가 보고되는(=운영 중인) 대여소"다(지금은 더미로 station_master를
그대로 복사해서 채워져 있음 - staging/jobs/silver_station_active_seed.py 참고).
두 테이블을 station_id로 INNER JOIN해서 "실제로 운영 중인 대여소"만 남기고,
설명 컬럼(station_name/region/district/hold_num/위경도)은 station_master(마스터
데이터)에서 가져온다.

### TEMP 성격 - 매번 전체 덮어쓰기
gold.bike_location과 동일한 이유로 파티션 없이 매번 테이블 전체를 덮어쓴다.

사용법:
    python -m jobs.build_station_active
"""
import logging
import os
from datetime import date

from pyspark.sql import functions as F

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _station_master_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.station_master"


def _station_active_silver_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.station_active"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.station_active"


def _ensure_gold_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            station_id    STRING,
            station_name  STRING,
            region        STRING,
            district      STRING,
            hold_num      INT,
            latitude      DOUBLE,
            longitude     DOUBLE,
            snapshot_date DATE
        )
        USING iceberg
        """
    )


def _latest_snapshot(spark, table: str):
    latest = spark.read.table(table).agg(F.max("snapshot_date")).collect()[0][0]
    return spark.read.table(table).filter(F.col("snapshot_date") == latest)


def build_station_active(spark, snapshot_date: str):
    master_df = _latest_snapshot(spark, _station_master_table())
    # 대여소 수가 적어(수백~수천 건) broadcast join이 안전하고 빠르다
    active_ids_df = F.broadcast(
        _latest_snapshot(spark, _station_active_silver_table()).select("station_id").distinct()
    )

    return (
        master_df.join(active_ids_df, on="station_id", how="inner")
        .select(
            "station_id",
            "station_name",
            "region",
            "district",
            "hold_num",
            "latitude",
            "longitude",
        )
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
    )


def run() -> None:
    snapshot_date = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("gold-build-station-active")
    _ensure_gold_table(spark)

    out_df = build_station_active(spark, snapshot_date).cache()
    row_count = out_df.count()
    out_df.writeTo(_gold_table()).overwritePartitions()
    out_df.unpersist()

    logger.info("%s: gold.station_active %d행 갱신 완료", snapshot_date, row_count)


if __name__ == "__main__":
    run()
