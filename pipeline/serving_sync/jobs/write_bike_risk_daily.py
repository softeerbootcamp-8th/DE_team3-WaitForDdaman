"""
gold.mart_bike_risk_daily의 {{ ds }} 파티션을 읽어 postgres.bike_risk_daily로
UPSERT한다. 하루 파티션은 로컬 규모에서 작으므로 Spark JDBC writer(UPSERT 미지원) 대신
collect() 후 psycopg2 batch UPSERT로 구현한다 (spec §3, 사용자 확정).

사용법:
    SNAPSHOT_DATE=2026-08-18 python -m jobs.write_bike_risk_daily
"""
import logging
import os
from datetime import date

from pyspark.sql import functions as F

import config
from common.spark_session import build_spark_session
from serving_db import ensure_serving_tables, upsert_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TABLE = "bike_risk_daily"
CONFLICT_KEYS = ["bike_id", "snapshot_date"]
COLUMNS = [
    "snapshot_date", "bike_id", "station_id", "station_name", "region", "district",
    "healthy_ratio", "risk_grade", "risk_score", "dist_km", "start_year", "aging",
    "fail_history", "action",
]


def _mart_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.mart_bike_risk_daily"


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    ensure_serving_tables()

    spark = build_spark_session("serving-sync-write-bike-risk-daily")
    df = spark.read.table(_mart_table()).filter(F.col("snapshot_date") == snapshot_date_str).select(*COLUMNS)

    rows = [tuple(r[c] for c in COLUMNS) for r in df.collect()]
    written = upsert_rows(TABLE, COLUMNS, CONFLICT_KEYS, rows)
    logger.info("%s: postgres.%s %d행 UPSERT 완료", snapshot_date_str, TABLE, written)


if __name__ == "__main__":
    run()
