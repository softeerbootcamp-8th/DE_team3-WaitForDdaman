"""
mart_bike_risk_daily / mart_station_daily가 공통으로 쓰는 대여소별 위험도 집계.

I/O(Iceberg 읽기)와 순수 로직을 분리한다 - station_risk_agg만 단독으로 pytest 검증한다.

### Spark 제거 (#172)
읽기는 pyiceberg, 집계는 DuckDB SQL로 옮긴다.
"""
import duckdb
import pyarrow as pa
from pyiceberg.expressions import EqualTo

from common.duckdb_io import query_arrow

FACT_BIKE_RISK_TABLE = "gold.fact_bike_risk"
BIKE_LOCATION_TABLE = "gold.bike_location"

# risk-scored bike가 하나도 없는 대여소는 결과에 아예 나타나지 않는다
# (호출부에서 기본값 100.0으로 채움).
_STATION_RISK_AGG_SQL = """
    WITH joined AS (
        SELECT r.bike_id, r.risk_grade, l.last_station_id AS station_id
        FROM risk r
        INNER JOIN location l ON r.bike_id = l.bike_id
        WHERE l.last_station_id IS NOT NULL
    ),
    agg AS (
        SELECT
            station_id,
            COUNT(*) AS risk_scored_cnt,
            SUM(CASE WHEN risk_grade != 'Normal' THEN 1 ELSE 0 END) AS risk_cnt
        FROM joined
        GROUP BY station_id
    )
    SELECT
        station_id,
        CAST(risk_cnt AS INT) AS risk_cnt,
        ROUND(100.0 * (risk_scored_cnt - risk_cnt) / risk_scored_cnt, 1) AS healthy_ratio
    FROM agg
"""


def station_risk_agg(
    risk_table: pa.Table,
    location_table: pa.Table,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """
    risk_table: (bike_id, risk_grade) - gold.fact_bike_risk의 특정 snapshot_date 파티션
    location_table: (bike_id, last_station_id) - gold.bike_location 전체(TEMP, 파티션 없음)

    반환: (station_id, risk_cnt, healthy_ratio) - risk_cnt는 Warning/Critical 수,
    healthy_ratio는 Normal 비율(%, 0~100). risk-scored bike가 하나도 없는 대여소는
    이 함수의 결과에 아예 나타나지 않는다(호출부에서 기본값 100.0으로 채움).
    카탈로그 없이 두 PyArrow Table만으로 동작하는 순수 로직이라 단위 테스트가 가능하다.
    """
    conn = con or duckdb.connect(":memory:")
    conn.register("risk", risk_table)
    conn.register("location", location_table)
    return query_arrow(conn, _STATION_RISK_AGG_SQL)


def read_station_risk_agg(catalog, snapshot_date: str) -> pa.Table:
    risk_table = catalog.load_table(FACT_BIKE_RISK_TABLE).scan(
        row_filter=EqualTo("snapshot_date", snapshot_date),
        selected_fields=("bike_id", "risk_grade"),
    ).to_arrow()
    location_table = catalog.load_table(BIKE_LOCATION_TABLE).scan(
        selected_fields=("bike_id", "last_station_id")
    ).to_arrow()
    return station_risk_agg(risk_table, location_table)
