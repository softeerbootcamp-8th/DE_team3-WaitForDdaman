"""
Gold(TEMP) - 자전거별 현재 위치 (silver.rental_history -> gold.bike_location)

### "현재 위치"의 정의
자전거별로 rent_dt가 가장 최근인 대여이력 1건만 본다.
    - 그 건이 이미 반납됐다(return_dt IS NOT NULL) -> 반납한 대여소(return_station_id)에 있음
    - 그 건이 아직 반납 안 됐다(return_dt IS NULL) -> 지금 운행 중이라 대여소에 없음 (last_station_id=NULL)

### last_event_at을 함께 저장하는 이유
gold.fact_station_inventory가 이 위치 정보와 silver.bike_man_action(수거/배치)
이벤트 중 어느 쪽이 더 최신인지 비교해서 최종 위치를 정해야 한다. 그 비교 기준
시각(반납 시각 또는 아직 반납 전이면 대여 시각)을 여기서 같이 내려준다.

### TEMP 성격 - 매번 전체 덮어쓰기
"현재 상태"만 의미가 있고 이력을 쌓지 않는다. 파티션 없이 매 실행마다 테이블
전체를 덮어쓴다(overwritePartitions는 파티션이 없는 테이블에서는 전체 교체와
동일하게 동작함).

사용법:
    python -m jobs.build_bike_location
"""
import logging
import os
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _silver_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.rental_history"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_location"


def _ensure_gold_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            bike_id        STRING,
            last_station_id STRING,
            last_event_at  TIMESTAMP,
            snapshot_date  DATE
        )
        USING iceberg
        """
    )


def build_bike_location(spark, snapshot_date: str):
    latest_rental = Window.partitionBy("bike_id").orderBy(F.col("rent_dt").desc())

    latest_df = (
        spark.read.table(_silver_table())
        .withColumn("_rn", F.row_number().over(latest_rental))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return latest_df.select(
        "bike_id",
        # 아직 반납 전(return_dt NULL)이면 운행 중이라 대여소에 없음
        F.when(F.col("return_dt").isNotNull(), F.col("return_station_id")).alias("last_station_id"),
        F.coalesce(F.col("return_dt"), F.col("rent_dt")).alias("last_event_at"),
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
    )


def run() -> None:
    snapshot_date = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("gold-build-bike-location")
    _ensure_gold_table(spark)

    out_df = build_bike_location(spark, snapshot_date).cache()
    row_count = out_df.count()
    out_df.writeTo(_gold_table()).overwritePartitions()
    out_df.unpersist()

    logger.info("%s: gold.bike_location %d행 갱신 완료", snapshot_date, row_count)


if __name__ == "__main__":
    run()
