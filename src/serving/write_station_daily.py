"""
gold.mart_station_daily의 {{ ds }} 파티션을 읽어 postgres.station_daily로 파티션
교체(delete+insert)한다. write_bike_risk_daily.py와 동일한 이유(mart 축소 시
UPSERT로는 사라진 행이 postgres에 남아 row count 비교가 어긋남)로 파티션 교체를 쓴다.

### Spark 제거 (#172)
write_bike_risk_daily.py와 동일 - pyiceberg 스캔 + psycopg2로 옮긴다. 로컬 실행과
Lambda 핸들러가 이 run()을 공유한다.

사용법:
    SNAPSHOT_DATE=2026-08-18 python -m jobs.write_station_daily
"""
import logging
import os
from datetime import date

from pyiceberg.expressions import EqualTo

from common.iceberg_catalog import build_iceberg_catalog
from serving.serving_db import ensure_serving_tables, replace_partition
from serving.serving_db import rows_for_insert as _rows_for_insert

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MART_TABLE = "gold.mart_station_daily"
TABLE = "station_daily"
COLUMNS = [
    "snapshot_date", "station_id", "station_name", "region", "district",
    "latitude", "longitude", "hold_num", "bike_cnt", "risk_cnt", "healthy_ratio", "urgency",
]


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    ensure_serving_tables()

    catalog = build_iceberg_catalog()
    arrow_table = catalog.load_table(MART_TABLE).scan(
        row_filter=EqualTo("snapshot_date", snapshot_date_str),
        selected_fields=tuple(COLUMNS),
    ).to_arrow()

    rows = _rows_for_insert(arrow_table, COLUMNS)
    written = replace_partition(TABLE, COLUMNS, snapshot_date_str, rows)
    logger.info("%s: postgres.%s %d행 파티션 교체 완료", snapshot_date_str, TABLE, written)


if __name__ == "__main__":
    run()
