"""
gold.mart_station_daily의 {{ ds }} 파티션을 읽어 postgres.station_daily로 UPSERT한다.
write_bike_risk_daily.py와 동일한 이유로 collect() + psycopg2 batch UPSERT.

사용법:
    SNAPSHOT_DATE=2026-08-18 python -m jobs.write_station_daily
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

TABLE = "station_daily"
CONFLICT_KEYS = ["station_id", "snapshot_date"]
COLUMNS = [
    "snapshot_date", "station_id", "station_name", "region", "district",
    "x", "y", "hold_num", "bike_count", "risk_count", "healthy_ratio", "urgency",
]


def _mart_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.mart_station_daily"


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    ensure_serving_tables()

    spark = build_spark_session("serving-sync-write-station-daily")
    df = spark.read.table(_mart_table()).filter(F.col("snapshot_date") == snapshot_date_str).select(*COLUMNS)

    rows = [tuple(r[c] for c in COLUMNS) for r in df.collect()]
    written = upsert_rows(TABLE, COLUMNS, CONFLICT_KEYS, rows)
    logger.info("%s: postgres.%s %d행 UPSERT 완료", snapshot_date_str, TABLE, written)


if __name__ == "__main__":
    run()
