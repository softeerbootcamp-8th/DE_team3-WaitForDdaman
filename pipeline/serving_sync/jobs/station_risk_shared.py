"""
mart_bike_risk_daily / mart_station_daily가 공통으로 쓰는 대여소별 위험도 집계.

I/O(Iceberg 읽기)와 순수 로직을 분리한다 - 이 저장소의 기존 Gold job들(build_station_active
등)은 스스로 spark.read.table()을 호출해서 단위 테스트 지점이 없는데, 이 잡부터는 pure
function으로 나눠서 station_risk_agg만 단독으로 pytest 검증한다.
"""
from pyspark.sql import functions as F

import config


def fact_bike_risk_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_bike_risk"


def bike_location_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_location"


def station_risk_agg(risk_df, location_df):
    """
    risk_df: (bike_id, risk_grade) - gold.fact_bike_risk의 특정 snapshot_date 파티션
    location_df: (bike_id, last_station_id) - gold.bike_location 전체(TEMP, 파티션 없음)

    반환: (station_id, risk_cnt, healthy_ratio) - risk_cnt는 Warning/Critical 수,
    healthy_ratio는 Normal 비율(%, 0~100). risk-scored bike가 하나도 없는 대여소는
    이 함수의 결과에 아예 나타나지 않는다(호출부에서 기본값 100.0으로 채움).
    """
    joined = (
        risk_df.select("bike_id", "risk_grade")
        .join(location_df.select("bike_id", "last_station_id"), on="bike_id", how="inner")
        .filter(F.col("last_station_id").isNotNull())
    )
    agg = joined.groupBy(F.col("last_station_id").alias("station_id")).agg(
        F.count("*").alias("risk_scored_cnt"),
        F.sum(F.when(F.col("risk_grade") != "Normal", 1).otherwise(0)).alias("risk_cnt"),
    )
    return agg.withColumn(
        "healthy_ratio",
        F.round(F.lit(100.0) * (F.col("risk_scored_cnt") - F.col("risk_cnt")) / F.col("risk_scored_cnt"), 1),
    ).select("station_id", "risk_cnt", "healthy_ratio")


def read_station_risk_agg(spark, snapshot_date: str):
    risk_df = spark.read.table(fact_bike_risk_table()).filter(F.col("snapshot_date") == snapshot_date)
    location_df = spark.read.table(bike_location_table())
    return station_risk_agg(risk_df, location_df)
