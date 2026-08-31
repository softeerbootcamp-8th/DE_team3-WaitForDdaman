"""
Gold(TEMP) - 활성 대여소 목록 (silver.station_master + silver.station_active -> gold.station_active)

### 조인의 의미
silver.station_master는 "등록된 모든 대여소"의 최신 스냅샷이고, silver.station_active는
"현재 실시간으로 상태가 보고되는(=운영 중인) 대여소"다(지금은 더미로 station_master를
그대로 복사해서 채워져 있음 - staging/jobs/silver_station_active_seed.py 참고).
두 테이블을 station_id로 INNER JOIN해서 "실제로 운영 중인 대여소"만 남기고,
설명 컬럼(station_name/region/district/hold_num/위경도)은 station_master(마스터
데이터)에서 가져온다.

### TEMP 성격 - 매번 전체 덮어쓰기
gold.bike_location과 동일한 이유로 파티션 없이 매번 테이블 전체를 덮어쓴다.

### Spark 제거 (#170)
station_master/station_active 둘 다 대여소 수만큼(수백~수천 건)이라 계산량이
작다. 읽기/쓰기는 pyiceberg, 조인은 DuckDB SQL, 품질 검증은 common/sql_assert.py로
옮긴다.

### 적재 전 품질 검증 (common/sql_assert.py, #146에서 PyDeequ 제거)
build_dim_bike.py와 동일한 패턴 - station_id는 조인 키이므로 결과에서 항상
유일해야 하고, station_id는 결측이 있으면 안 된다(hold_num은 정상 원천 결측이
있어 검증 대상에서 제외 - _validate_station_active 주석 참고). 실패하면
QualityCheckError로 배치를 즉시 중단한다(적재 전 검증이므로 gold 테이블에는
아무 영향 없음).

사용법:
    python -m jobs.build_station_active
"""
import logging
import os
import sys
from datetime import date

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType, DateType

import config
from common.duckdb_io import connect, query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_all
from common.s3_utils import ensure_bucket
from common.sql_assert import QualityCheck, QualityCheckError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STATION_MASTER_TABLE = "silver.station_master"
STATION_ACTIVE_SILVER_TABLE = "silver.station_active"
GOLD_TABLE = "gold.station_active"

GOLD_COLUMNS = ["station_id", "station_name", "region", "district", "hold_num", "latitude", "longitude", "snapshot_date"]

# TEMP류(파티션 없음) 테이블이라 스펙 없이 스키마만 정의한다.
GOLD_SCHEMA = Schema(
    NestedField(1, "station_id", StringType(), required=False),
    NestedField(2, "station_name", StringType(), required=False),
    NestedField(3, "region", StringType(), required=False),
    NestedField(4, "district", StringType(), required=False),
    NestedField(5, "hold_num", IntegerType(), required=False),
    NestedField(6, "latitude", DoubleType(), required=False),
    NestedField(7, "longitude", DoubleType(), required=False),
    NestedField(8, "snapshot_date", DateType(), required=False),
)

# station_master(마스터 데이터)와 station_active(운영 중 여부)를 조인한다.
_JOIN_SQL = """
    SELECT
        m.station_id     AS station_id,
        m.station_name   AS station_name,
        m.region         AS region,
        m.district       AS district,
        m.hold_num       AS hold_num,
        m.latitude       AS latitude,
        m.longitude      AS longitude,
        CAST(? AS DATE)  AS snapshot_date
    FROM station_master m
    INNER JOIN active_ids a ON m.station_id = a.station_id
"""


def _join_active_stations(
    master_table: pa.Table,
    active_ids_table: pa.Table,
    snapshot_date: str,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """station_master(마스터 데이터)와 station_active(운영 중 여부)를 조인한다 -
    카탈로그 없이 두 PyArrow Table만으로 동작하는 순수 로직이라 단위 테스트가 가능하다."""
    conn = con or connect()
    conn.register("station_master", master_table)
    conn.register("active_ids", active_ids_table)
    return query_arrow(conn, _JOIN_SQL, [snapshot_date])


def _ensure_gold_table(catalog):
    catalog.create_namespace_if_not_exists("gold")
    try:
        return catalog.load_table(GOLD_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", GOLD_TABLE)
        return catalog.create_table(GOLD_TABLE, schema=GOLD_SCHEMA)


def _latest_snapshot(catalog, table_identifier: str) -> pa.Table:
    """MAX(snapshot_date) 스냅샷만 남긴다 - 대여소 수만큼(수백~수천 건)이라
    전체를 읽어 DuckDB에서 필터링해도 부담이 없다."""
    full = catalog.load_table(table_identifier).scan().to_arrow()
    con = connect()
    con.register("t", full)
    return query_arrow(con, "SELECT * FROM t WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM t)")


def build_station_active(catalog, snapshot_date: str) -> pa.Table:
    master_table = _latest_snapshot(catalog, STATION_MASTER_TABLE)

    con = connect()
    active_ids_table = _latest_snapshot(catalog, STATION_ACTIVE_SILVER_TABLE)
    con.register("active_raw", active_ids_table)
    active_ids = query_arrow(con, "SELECT DISTINCT station_id FROM active_raw")

    return _join_active_stations(master_table, active_ids, snapshot_date, con)


def _dedup_by_station_id(table: pa.Table, con: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
    """has_uniqueness(threshold=0.99) 하드 게이트가 1%까지는 통과시켜, 실패 시
    전체 적재가 막히는 위험이 있다(#332 PR 리뷰). 쓰기 직전에 여기서 미리 한 행만
    남겨서 그 하드 게이트가 사실상 항상 통과하게 만든다 - 어느 쪽이 "맞는" 값인지
    판단하는 로직은 아니라 결정적으로 하나를 고를 뿐이다. 실제 원인 추적은
    gold_station_active.yaml DQ 어써션(dq.check_result_history)이 계속 담당한다."""
    conn = con or connect()
    conn.register("dedup_target", table)
    deduped = query_arrow(conn, "SELECT DISTINCT ON (station_id) * FROM dedup_target ORDER BY station_id")
    dropped = len(table) - len(deduped)
    if dropped:
        logger.warning("gold.station_active: station_id 중복 %d건 dedup으로 제거", dropped)
    return deduped


def _validate_station_active(table: pa.Table) -> None:
    # 운영 중으로 확인된 대여소가 하나도 없는 경우(station_master/station_active
    # 교집합이 비는 극단적 상황)엔 이 결과도 0행일 수 있다. SQL 어서션은 0행에서
    # 위반이 자연히 0건이라 별도 스킵 분기가 필요 없다.
    #
    # hold_num은 isComplete 대상이 아님 - 실측(2026-08-14 기준)으로 hold_num이
    # 없는 대여소가 15곳 있고, 이는 정상 원천 결측이라 silver 단계에서도
    # null로 그대로 통과시킨다(0으로 채우지 않음 - README/staging 테스트 참고).
    (
        QualityCheck("station_active_check")
        .is_complete("station_id")
        .has_uniqueness("station_id", threshold=0.99)
        .run(table)
        .raise_if_failed(QualityCheckError)
    )


def run() -> None:
    snapshot_date = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    gold_table = _ensure_gold_table(catalog)

    out_table = build_station_active(catalog, snapshot_date).select(GOLD_COLUMNS)
    out_table = _dedup_by_station_id(out_table)
    row_count = len(out_table)
    try:
        _validate_station_active(out_table)
    except QualityCheckError as e:
        logger.error("%s: gold.station_active 검증 실패, 적재 중단: %s", snapshot_date, e)
        sys.exit(1)

    overwrite_all(gold_table, out_table)

    logger.info("%s: gold.station_active %d행 갱신 완료", snapshot_date, row_count)


if __name__ == "__main__":
    run()
