"""
gold.mart_bike_risk_daily의 {{ ds }} 파티션을 읽어 postgres.bike_risk_daily로 파티션
교체(delete+insert)한다. 하루 파티션은 로컬 규모에서 작으므로 대량 쓰기 대신
읽어서 psycopg2로 처리한다 (spec §3, 사용자 확정).

UPSERT(ON CONFLICT DO UPDATE)가 아니라 파티션 삭제 후 재삽입인 이유: mart가 이전
실행보다 줄어드는 경우(예: 대여소 비활성화로 해당 자전거가 빠짐)에도 UPSERT는 사라진
행을 postgres에서 지우지 않아 verify_serving_sync의 row count 비교가 영원히 어긋난다.
Iceberg 쪽 build_mart_*가 overwrite_partition을 쓰는 것과 동일한 의미로 맞춘다.

### Spark 제거 (#172)
Lambda 안에서 JVM(Spark)을 못 띄우므로 pyiceberg 스캔 + psycopg2로 옮긴다. 이 잡의
run()은 로컬 실행(`python -m jobs.write_bike_risk_daily`)과 Lambda 핸들러(얇은 래퍼가
이 run()을 그대로 호출) 양쪽에서 공유되는 진입점이다.

사용법:
    SNAPSHOT_DATE=2026-08-18 python -m jobs.write_bike_risk_daily
"""
import logging
import os
from datetime import date

from pyiceberg.expressions import EqualTo

from common.iceberg_catalog import build_iceberg_catalog
from serving_db import ensure_serving_tables, replace_partition
from serving_db import rows_for_insert as _rows_for_insert

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MART_TABLE = "gold.mart_bike_risk_daily"
TABLE = "bike_risk_daily"
COLUMNS = [
    "snapshot_date", "bike_id", "station_id", "station_name", "region", "district",
    "healthy_ratio", "risk_grade", "risk_score", "dist_km", "start_year", "aging",
    "fail_history",
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
