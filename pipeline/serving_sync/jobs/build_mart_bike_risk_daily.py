"""
Gold - 자전거별 서빙 마트 (여러 gold 테이블 join -> gold.mart_bike_risk_daily,
{{ ds }} 파티션 단위 OVERWRITE) -> postgres.bike_risk_daily로 넘어갈 최종 모양.

gold.fact_bike_decision은 오늘자 위험 자전거 중 의사결정 대상 bike_id를 확정하는
상위 산출물이다. mart_bike_risk_daily는 서빙 화면에 필요한 위험도/위치/이력 컬럼만
노출하며, 의사결정 action은 이 mart와 postgres 서빙 테이블에 싣지 않는다.

### healthy_ratio 기본값
risk-scored bike가 하나도 없는 대여소(station_risk_shared.station_risk_agg 결과에
안 나타남)는 100.0(완전 정상)으로 취급한다 - "위험 신호 없음 = 정상"이라는 보수적 기본값.

### Spark 제거 (#172)
읽기/쓰기는 pyiceberg, 조인/집계는 DuckDB SQL로 옮긴다. _fail_history_agg의
"자전거별 최근 N건"은 Spark에서 row_number() + collect_list + sort_array 3단계였는데,
DuckDB는 QUALIFY row_number() + array_agg(... ORDER BY ...) 2단계로 표현된다
(array_agg가 정렬을 함께 받음).

사용법:
    python -m jobs.build_mart_bike_risk_daily
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
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    ListType,
    NestedField,
    StringType,
)

import config
from common.duckdb_io import query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket
from station_risk_shared import read_station_risk_agg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

FACT_BIKE_RISK_TABLE = "gold.fact_bike_risk"
FACT_BIKE_DECISION_TABLE = "gold.fact_bike_decision"
BIKE_LOCATION_TABLE = "gold.bike_location"
STATION_ACTIVE_TABLE = "gold.station_active"
DIM_BIKE_TABLE = "gold.dim_bike"
BIKE_FEATURES_TABLE = "gold.bike_features_daily"
FAILURE_REPORT_TABLE = "silver.failure_report"
GOLD_TABLE = "gold.mart_bike_risk_daily"
PARTITION_COLUMN = "snapshot_date"

MART_COLUMNS = [
    "snapshot_date", "bike_id", "station_id", "station_name", "region", "district",
    "healthy_ratio", "risk_score", "risk_grade", "dist_km", "start_year", "aging",
    "fail_history",
]

GOLD_SCHEMA = Schema(
    NestedField(1, "snapshot_date", DateType(), required=False),
    NestedField(2, "bike_id", StringType(), required=False),
    NestedField(3, "station_id", StringType(), required=False),
    NestedField(4, "station_name", StringType(), required=False),
    NestedField(5, "region", StringType(), required=False),
    NestedField(6, "district", StringType(), required=False),
    NestedField(7, "healthy_ratio", DoubleType(), required=False),
    NestedField(8, "risk_score", DoubleType(), required=False),
    NestedField(9, "risk_grade", StringType(), required=False),
    NestedField(10, "dist_km", DoubleType(), required=False),
    NestedField(11, "start_year", IntegerType(), required=False),
    NestedField(12, "aging", IntegerType(), required=False),
    NestedField(13, "fail_history", ListType(element_id=100, element=StringType(), element_required=False), required=False),
)
GOLD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)

# 자전거별 최근 고장신고 limit건을 "YYYY-MM-DD 유형" 문자열 배열로(최신순).
_FAIL_HISTORY_SQL = """
    WITH filtered AS (
        SELECT
            bike_no AS bike_id,
            reg_dttm,
            strftime(reg_dttm, '%Y-%m-%d') || ' ' || failure_type AS entry
        FROM failure
        WHERE reg_dttm < CAST(? AS TIMESTAMPTZ)
    ),
    ranked AS (
        SELECT bike_id, reg_dttm, entry
        FROM filtered
        QUALIFY row_number() OVER (PARTITION BY bike_id ORDER BY reg_dttm DESC) <= ?
    )
    SELECT bike_id, array_agg(entry ORDER BY reg_dttm DESC) AS fail_history
    FROM ranked
    GROUP BY bike_id
"""

# base: risk와 decision(의사결정 대상)을 INNER JOIN해서 오늘 의사결정 대상 자전거만
# 남기고, bike_location에서 현재 station_id를 붙인다.
_MART_SQL = """
    WITH decision_dedup AS (
        SELECT DISTINCT bike_id FROM decision
    ),
    base AS (
        SELECT r.bike_id, r.risk_score, r.risk_grade, l.last_station_id AS station_id
        FROM risk r
        INNER JOIN decision_dedup d ON r.bike_id = d.bike_id
        LEFT JOIN location l ON r.bike_id = l.bike_id
    )
    SELECT
        b.bike_id,
        b.station_id,
        sa.station_name,
        sa.region,
        sa.district,
        COALESCE(sr.healthy_ratio, 100.0) AS healthy_ratio,
        b.risk_score,
        b.risk_grade,
        f.dist_km,
        db.start_year,
        CAST(date_part('year', CAST(? AS DATE)) - db.start_year AS INT) AS aging,
        COALESCE(fh.fail_history, []) AS fail_history,
        CAST(? AS DATE) AS snapshot_date
    FROM base b
    LEFT JOIN station_active sa ON b.station_id = sa.station_id
    LEFT JOIN dim_bike db ON b.bike_id = db.bike_id
    LEFT JOIN features f ON b.bike_id = f.bike_id
    LEFT JOIN station_risk sr ON b.station_id = sr.station_id
    LEFT JOIN fail_history fh ON b.bike_id = fh.bike_id
"""


def _fail_history_agg(
    failure_table: pa.Table,
    as_of_date: str,
    limit: int = 5,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    conn = con or duckdb.connect(":memory:")
    conn.execute("SET TimeZone='UTC'")
    conn.register("failure", failure_table)
    return query_arrow(conn, _FAIL_HISTORY_SQL, [as_of_date, limit])


def build_mart_bike_risk_daily(
    risk_table: pa.Table,
    decision_table: pa.Table,
    location_table: pa.Table,
    station_active_table: pa.Table,
    dim_bike_table: pa.Table,
    features_table: pa.Table,
    station_risk_table: pa.Table,
    fail_history_table: pa.Table,
    snapshot_date: str,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """카탈로그 없이 PyArrow Table들만으로 동작하는 순수 로직이라 단위 테스트가 가능하다.
    fail_history_table은 이미 _fail_history_agg()를 거친(bike_id, fail_history) 결과다."""
    conn = con or duckdb.connect(":memory:")
    conn.register("risk", risk_table)
    conn.register("decision", decision_table)
    conn.register("location", location_table)
    conn.register("station_active", station_active_table)
    conn.register("dim_bike", dim_bike_table)
    conn.register("features", features_table)
    conn.register("station_risk", station_risk_table)
    conn.register("fail_history", fail_history_table)
    result = query_arrow(conn, _MART_SQL, [snapshot_date, snapshot_date])
    return result.select(MART_COLUMNS)


def _ensure_gold_table(catalog):
    catalog.create_namespace_if_not_exists("gold")
    try:
        table = catalog.load_table(GOLD_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", GOLD_TABLE)
        return catalog.create_table(GOLD_TABLE, schema=GOLD_SCHEMA, partition_spec=GOLD_PARTITION_SPEC)

    # 레거시 action 컬럼 제거(#93 이전 데이터 잔존분). 없으면 아무 것도 안 함.
    if "action" in {f.name for f in table.schema().fields}:
        with table.update_schema() as update:
            update.delete_column("action")
        table = catalog.load_table(GOLD_TABLE)
    return table


def _read_and_build(catalog, snapshot_date: date) -> pa.Table | None:
    from pyiceberg.expressions import EqualTo

    date_str = snapshot_date.strftime("%Y-%m-%d")

    risk_table = catalog.load_table(FACT_BIKE_RISK_TABLE).scan(
        row_filter=EqualTo("snapshot_date", date_str)
    ).to_arrow()
    if len(risk_table) == 0:
        return None

    decision_table = catalog.load_table(FACT_BIKE_DECISION_TABLE).scan(
        row_filter=EqualTo("snapshot_date", date_str)
    ).to_arrow()
    location_table = catalog.load_table(BIKE_LOCATION_TABLE).scan().to_arrow()
    station_active_table = catalog.load_table(STATION_ACTIVE_TABLE).scan().to_arrow()

    con = duckdb.connect(":memory:")
    con.register(
        "dim_bike_raw",
        catalog.load_table(DIM_BIKE_TABLE).scan(selected_fields=("bike_id", "start_year")).to_arrow(),
    )
    dim_bike_table = query_arrow(
        con, "SELECT DISTINCT ON (bike_id) bike_id, start_year FROM dim_bike_raw ORDER BY bike_id"
    )

    features_table = catalog.load_table(BIKE_FEATURES_TABLE).scan(
        row_filter=EqualTo("snapshot_date", date_str)
    ).to_arrow()
    station_risk_table = read_station_risk_agg(catalog, date_str)

    raw_failure_table = catalog.load_table(FAILURE_REPORT_TABLE).scan(
        selected_fields=("bike_no", "reg_dttm", "failure_type")
    ).to_arrow()
    fail_history_table = _fail_history_agg(raw_failure_table, date_str)

    return build_mart_bike_risk_daily(
        risk_table, decision_table, location_table, station_active_table, dim_bike_table,
        features_table, station_risk_table, fail_history_table, date_str,
    )


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE")
    target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    gold_table = _ensure_gold_table(catalog)

    out_table = _read_and_build(catalog, target_date)
    if out_table is None:
        logger.info("%s: fact_bike_risk에 처리할 데이터 없음", target_date)
        return

    row_count = len(out_table)
    overwrite_partition(gold_table, out_table, PARTITION_COLUMN, target_date.strftime("%Y-%m-%d"))

    logger.info("%s: gold.mart_bike_risk_daily %d행 갱신 완료", target_date, row_count)


if __name__ == "__main__":
    run()
