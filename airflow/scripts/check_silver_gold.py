"""
Silver/Gold 테이블 상태 확인용 스크립트.

사용법 (컨테이너 안에서, docker-compose.local.yml이 ./scripts를 /opt/airflow/scripts로 마운트함):
    docker exec airflow-scheduler bash -c '
    cd /opt/airflow/ingestion &&
    set -a && source .env && set +a &&
    PYTHONPATH=/opt/airflow/ingestion:$PYTHONPATH python /opt/airflow/scripts/check_silver_gold.py
    '
"""
from pyspark.sql import functions as F

from common.spark_session import build_spark_session
from common.watermark import read_watermark
from config.watermark_keys import BRONZE_RENTAL_HISTORY, GOLD_DIM_BIKE, SILVER_RENTAL_HISTORY

spark = build_spark_session("check-silver-gold")

print("=== 워터마크 ===")
print("bronze_watermark:", read_watermark(watermark_key=BRONZE_RENTAL_HISTORY))
print("silver_watermark:", read_watermark(watermark_key=SILVER_RENTAL_HISTORY))
print("gold_watermark:  ", read_watermark(watermark_key=GOLD_DIM_BIKE))

print("\n=== silver.rental_history ===")
silver_df = spark.read.table("bike_catalog.silver.rental_history")
print("row count:", silver_df.count())
silver_df.select(
    F.min("rent_date_partition").alias("min_date"),
    F.max("rent_date_partition").alias("max_date"),
).show()
silver_df.show(5, truncate=False)

print("\n=== gold.dim_bike ===")
gold_df = spark.read.table("bike_catalog.gold.dim_bike")
print("row count:", gold_df.count())
gold_df.select(
    F.min("snapshot_date").alias("min_date"),
    F.max("snapshot_date").alias("max_date"),
    F.countDistinct("start_year").alias("distinct_years"),
).show()
gold_df.orderBy("snapshot_date").show(10, truncate=False)
