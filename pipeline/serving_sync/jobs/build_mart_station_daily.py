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

### Spark 제거 (#172)
읽기/쓰기는 pyiceberg, 조인은 DuckDB SQL로 옮긴다.

사용법:
    python -m jobs.build_mart_station_daily
"""
import logging
import os
from datetime import date

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, DoubleType, IntegerType, NestedField, StringType

import config
from common.duckdb_io import connect, query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket
from station_risk_shared import read_station_risk_agg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HEALTHY_RATIO_THRESHOLD = 70.0

STATION_ACTIVE_TABLE = "gold.station_active"
FACT_STATION_INVENTORY_TABLE = "gold.fact_station_inventory"
GOLD_TABLE = "gold.mart_station_daily"
PARTITION_COLUMN = "snapshot_date"

MART_COLUMNS = [
    "snapshot_date", "station_id", "station_name", "region", "district",
    "latitude", "longitude", "hold_num", "bike_cnt", "risk_cnt", "healthy_ratio", "urgency",
]

GOLD_SCHEMA = Schema(
    NestedField(1, "snapshot_date", DateType(), required=False),
    NestedField(2, "station_id", StringType(), required=False),
    NestedField(3, "station_name", StringType(), required=False),
    NestedField(4, "region", StringType(), required=False),
    NestedField(5, "district", StringType(), required=False),
    NestedField(6, "latitude", DoubleType(), required=False),
    NestedField(7, "longitude", DoubleType(), required=False),
    NestedField(8, "hold_num", IntegerType(), required=False),
    NestedField(9, "bike_cnt", IntegerType(), required=False),
    NestedField(10, "risk_cnt", IntegerType(), required=False),
    NestedField(11, "healthy_ratio", DoubleType(), required=False),
    NestedField(12, "urgency", StringType(), required=False),
)
GOLD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)

# inventory.bike_cnt / station_risk.risk_cnt 이름 그대로 조인하고 최종 select까지
# 그대로 유지한다 - gold mart와 postgres 서빙 테이블의 컬럼명을 동일하게 맞춘다.
_MART_SQL = """
    SELECT
        sa.station_id,
        sa.station_name,
        sa.region,
        sa.district,
        sa.latitude,
        sa.longitude,
        sa.hold_num,
        CAST(COALESCE(inv.bike_cnt, 0) AS INT) AS bike_cnt,
        CAST(COALESCE(sr.risk_cnt, 0) AS INT) AS risk_cnt,
        COALESCE(sr.healthy_ratio, 100.0) AS healthy_ratio,
        CASE WHEN COALESCE(sr.healthy_ratio, 100.0) >= ? THEN '여유있음' ELSE '부족함' END AS urgency,
        CAST(? AS DATE) AS snapshot_date
    FROM station_active sa
    LEFT JOIN inventory inv ON sa.station_id = inv.station_id
    LEFT JOIN station_risk sr ON sa.station_id = sr.station_id
"""


def build_mart_station_daily(
    station_active_table: pa.Table,
    inventory_table: pa.Table,
    station_risk_table: pa.Table,
    snapshot_date: str,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """카탈로그 없이 세 PyArrow Table만으로 동작하는 순수 로직이라 단위 테스트가 가능하다."""
    conn = con or connect()
    conn.register("station_active", station_active_table)
    conn.register("inventory", inventory_table)
    conn.register("station_risk", station_risk_table)
    return query_arrow(conn, _MART_SQL, [HEALTHY_RATIO_THRESHOLD, snapshot_date]).select(MART_COLUMNS)


def _ensure_gold_table(catalog):
    catalog.create_namespace_if_not_exists("gold")
    try:
        return catalog.load_table(GOLD_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", GOLD_TABLE)
        return catalog.create_table(GOLD_TABLE, schema=GOLD_SCHEMA, partition_spec=GOLD_PARTITION_SPEC)


def _read_and_build(catalog, snapshot_date: date) -> pa.Table:
    date_str = snapshot_date.strftime("%Y-%m-%d")

    station_active_table = catalog.load_table(STATION_ACTIVE_TABLE).scan().to_arrow()
    inventory_table = catalog.load_table(FACT_STATION_INVENTORY_TABLE).scan().to_arrow()
    station_risk_table = read_station_risk_agg(catalog, date_str)

    return build_mart_station_daily(station_active_table, inventory_table, station_risk_table, date_str)


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE")
    target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    gold_table = _ensure_gold_table(catalog)

    out_table = _read_and_build(catalog, target_date)
    row_count = len(out_table)
    overwrite_partition(gold_table, out_table, PARTITION_COLUMN, target_date.strftime("%Y-%m-%d"))

    logger.info("%s: gold.mart_station_daily %d행 갱신 완료", target_date, row_count)


if __name__ == "__main__":
    run()
