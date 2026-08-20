"""
gold_dim_fact 결과 확인용 스크립트 (dim_bike/bike_location/station_active/fact_station_inventory)

사용법 (컨테이너 안에서, docker-compose.local.yml이 ./scripts를 /opt/airflow/scripts로 마운트함):
    docker exec airflow-scheduler bash -c '
    cd /opt/airflow/ingestion &&
    set -a && source .env && set +a &&
    PYTHONPATH=/opt/airflow/ingestion:$PYTHONPATH python /opt/airflow/scripts/check_gold_dim_fact.py
    '
"""
from pyspark.sql import functions as F

from common.spark_session import build_spark_session

spark = build_spark_session("check-gold-dim-fact")

print("=== gold.dim_bike ===")
dim_bike = spark.read.table("bike_catalog.gold.dim_bike")
print("row count:", dim_bike.count())
dim_bike.orderBy(F.desc("snapshot_date")).show(10, truncate=False)

print("\n=== gold.bike_location (TEMP - 항상 최신 상태) ===")
bike_location = spark.read.table("bike_catalog.gold.bike_location")
print("row count:", bike_location.count())
bike_location.groupBy(bike_location.last_station_id.isNull().alias("in_transit_null")).count().show()
bike_location.show(10, truncate=False)

print("\n=== gold.station_active (TEMP - 항상 최신 상태) ===")
station_active = spark.read.table("bike_catalog.gold.station_active")
print("row count:", station_active.count())
station_active.show(10, truncate=False)

print("\n=== gold.fact_station_inventory ===")
fact_station_inventory = spark.read.table("bike_catalog.gold.fact_station_inventory")
print("row count:", fact_station_inventory.count())
fact_station_inventory.select(
    F.sum("bike_cnt").alias("total_bikes_in_inventory"),
    F.sum("target_bike_cnt").alias("total_target"),
    F.countDistinct("station_id").alias("station_count"),
).show()
fact_station_inventory.orderBy(F.desc("bike_cnt")).show(10, truncate=False)
