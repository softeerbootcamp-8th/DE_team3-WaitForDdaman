"""
Gold - 대여소별 자전거 재고 (gold.bike_location + gold.station_active +
silver.bike_man_action -> gold.fact_station_inventory)

### 최종 위치 결정 규칙
자전거의 "진짜 현재 위치"는 대여이력 기준 위치(gold.bike_location)와 수거/배치
이벤트(silver.bike_man_action) 중 시각이 더 최신인 쪽을 따른다.

    1. bike_man_action에 해당 자전거 이벤트가 아예 없거나, 있어도
       bike_location.last_event_at보다 오래됐다 -> bike_location 그대로 사용
    2. 가장 최신 이벤트가 COLLECT(수거)다 -> 필드에서 제거된 상태이므로
       재고 집계에서 제외 (어느 대여소에도 속하지 않음)
    3. 가장 최신 이벤트가 DEPLOY(배치)이고 station_id가 있다 -> 그 station_id로
       위치를 덮어씀 (배치 이벤트가 대여이력보다 최신 정보이므로)
    4. DEPLOY인데 station_id가 없다(이례적 케이스) -> bike_location 값으로 폴백

### bike_cnt / target_bike_cnt
gold.station_active(운영 중인 대여소만)를 기준으로 대여소별 자전거 수를 센다.
자전거가 하나도 없는 대여소도 0으로 나와야 하므로 station_active를 기준(left side)
으로 두고 자전거 수를 왼쪽 조인한다. target_bike_cnt는 거치대 수(hold_num)를
목표치로 사용한다.

### 전체 덮어쓰기 (TEMP류 입력에 의존하는 최신 상태)
사용법:
    python -m jobs.build_fact_station_inventory
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


def _bike_location_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_location"


def _station_active_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.station_active"


def _bike_man_action_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.bike_man_action"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_station_inventory"


def _ensure_gold_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            station_id      STRING,
            bike_cnt        INT,
            hold_num        INT,
            target_bike_cnt INT,
            snapshot_date   DATE
        )
        USING iceberg
        """
    )


def _latest_bike_man_action_per_bike(spark):
    """자전거별 가장 최신 COLLECT/DEPLOY 이벤트 1건만 남긴다."""
    window = Window.partitionBy("bike_id").orderBy(F.col("occurred_at").desc())
    return (
        spark.read.table(_bike_man_action_table())
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "bike_id",
            F.col("event_type").alias("action_event_type"),
            F.col("station_id").alias("action_station_id"),
            F.col("occurred_at").alias("action_at"),
        )
    )


def _resolve_bike_station(spark):
    """자전거별 최종 위치(effective_station_id, excluded)를 계산한다."""
    bike_location = spark.read.table(_bike_location_table())
    latest_action = _latest_bike_man_action_per_bike(spark)

    joined = bike_location.join(latest_action, on="bike_id", how="left")

    action_is_newer = F.col("action_at").isNotNull() & (
        F.col("action_at") > F.col("last_event_at")
    )

    return joined.select(
        "bike_id",
        F.when(
            action_is_newer & (F.col("action_event_type") == "COLLECT"),
            F.lit(None).cast("string"),
        )
        .when(
            action_is_newer & (F.col("action_event_type") == "DEPLOY"),
            F.coalesce(F.col("action_station_id"), F.col("last_station_id")),
        )
        .otherwise(F.col("last_station_id"))
        .alias("effective_station_id"),
        # 최신 이벤트가 COLLECT면 필드에서 제거된 상태 -> 재고 집계 제외
        (action_is_newer & (F.col("action_event_type") == "COLLECT")).alias("excluded"),
    )


def build_fact_station_inventory(spark, snapshot_date: str):
    resolved = _resolve_bike_station(spark)
    active_bikes = resolved.filter(~F.col("excluded") & F.col("effective_station_id").isNotNull())

    bike_counts = active_bikes.groupBy(
        F.col("effective_station_id").alias("station_id")
    ).agg(F.count("*").alias("bike_cnt"))

    station_active = spark.read.table(_station_active_table())

    return (
        station_active.select("station_id", "hold_num")
        .join(bike_counts, on="station_id", how="left")
        .select(
            "station_id",
            F.coalesce(F.col("bike_cnt"), F.lit(0)).cast("int").alias("bike_cnt"),
            "hold_num",
            F.col("hold_num").alias("target_bike_cnt"),
            F.lit(snapshot_date).cast("date").alias("snapshot_date"),
        )
    )


def run() -> None:
    snapshot_date = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("gold-build-fact-station-inventory")
    _ensure_gold_table(spark)

    out_df = build_fact_station_inventory(spark, snapshot_date).cache()
    row_count = out_df.count()
    out_df.writeTo(_gold_table()).overwritePartitions()
    out_df.unpersist()

    logger.info("%s: gold.fact_station_inventory %d행 갱신 완료", snapshot_date, row_count)


if __name__ == "__main__":
    run()
