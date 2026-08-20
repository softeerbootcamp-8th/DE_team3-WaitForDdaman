"""
Gold - 대여소별 서빙 마트 (station_active + fact_station_inventory + 위험도 집계
-> gold.mart_station_daily, {{ ds }} 파티션 단위 OVERWRITE) -> postgres.station_daily로
넘어갈 최종 모양.

### 컬럼명은 상류 소스 그대로 유지
latitude/longitude(station_active) / bike_cnt(fact_station_inventory) / risk_cnt
(station_risk_shared)를 마트 출력에서 다른 이름으로 바꾸지 않는다 - gold mart와
postgres 서빙 테이블의 컬럼명을 동일하게 맞춰서 두 스키마를 나란히 봤을 때 헷갈리지
않게 하기 위함. API/프론트가 SVG 픽셀 좌표용 x/y를 쓰던 것은 이 컬럼명과 무관한
API 응답 계층의 별도 매핑으로 처리한다(app 연동 작업에서 다룸).

### urgency 기준
DetailPanel.tsx의 "정상자전거 비율 {healthyRatio}% -> {stationUrgency} (70% 기준)"
문구 그대로: healthy_ratio(Normal 비율) 70% 이상이면 "여유있음", 미만이면 "부족함".

사용법:
    python -m jobs.build_mart_station_daily
"""
import logging
import os
from datetime import date

from pyspark.sql import functions as F

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session
from station_risk_shared import read_station_risk_agg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HEALTHY_RATIO_THRESHOLD = 70.0

MART_COLUMNS = [
    "snapshot_date", "station_id", "station_name", "region", "district",
    "latitude", "longitude", "hold_num", "bike_cnt", "risk_cnt", "healthy_ratio", "urgency",
]


def _station_active_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.station_active"


def _fact_station_inventory_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_station_inventory"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.mart_station_daily"


def _ensure_mart_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            snapshot_date DATE,
            station_id STRING,
            station_name STRING,
            region STRING,
            district STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            hold_num INT,
            bike_cnt INT,
            risk_cnt INT,
            healthy_ratio DOUBLE,
            urgency STRING
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def build_mart_station_daily(station_active_df, inventory_df, station_risk_df, snapshot_date):
    # inventory_df.bike_cnt(gold.fact_station_inventory) / station_risk_df.risk_cnt
    # (station_risk_shared) 이름 그대로 조인하고 최종 select까지 그대로 유지한다 -
    # gold mart와 postgres 서빙 테이블의 컬럼명을 동일하게 맞춘다.
    joined = (
        station_active_df.select(
            "station_id", "station_name", "region", "district", "latitude", "longitude", "hold_num"
        )
        .join(inventory_df.select("station_id", "bike_cnt"), on="station_id", how="left")
        .join(station_risk_df, on="station_id", how="left")
    )

    joined = (
        joined.withColumn("bike_cnt", F.coalesce(F.col("bike_cnt"), F.lit(0)))
        .withColumn("risk_cnt", F.coalesce(F.col("risk_cnt"), F.lit(0)))
        .withColumn("healthy_ratio", F.coalesce(F.col("healthy_ratio"), F.lit(100.0)))
        .withColumn(
            "urgency",
            F.when(F.col("healthy_ratio") >= HEALTHY_RATIO_THRESHOLD, F.lit("여유있음")).otherwise(F.lit("부족함")),
        )
    )

    return joined.withColumn("snapshot_date", F.lit(str(snapshot_date)).cast("date")).select(*MART_COLUMNS)


def _read_and_build(spark, snapshot_date: date):
    date_str = snapshot_date.strftime("%Y-%m-%d")

    station_active_df = spark.read.table(_station_active_table())
    inventory_df = spark.read.table(_fact_station_inventory_table())
    station_risk_df = read_station_risk_agg(spark, date_str)

    return build_mart_station_daily(station_active_df, inventory_df, station_risk_df, snapshot_date)


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE")
    target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("gold-build-mart-station-daily")
    _ensure_mart_table(spark)

    out_df = _read_and_build(spark, target_date).cache()
    row_count = out_df.count()
    out_df.writeTo(_gold_table()).overwritePartitions()
    out_df.unpersist()

    logger.info("%s: gold.mart_station_daily %d행 갱신 완료", target_date, row_count)


if __name__ == "__main__":
    run()
