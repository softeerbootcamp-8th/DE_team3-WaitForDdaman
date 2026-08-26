"""
gold_risk_decision 원안의 "7. build_fact_bike_decision" 단계 구현 -
gold.fact_bike_risk + gold.fact_station_inventory -> 대여중단 여부 결정.

action은 대여중단/보류 2가지뿐이다. capacity(정비 인력 기준)로 "대여중단" 중 오늘 몇 대를
실제로 수거할지 정하는 건 이 job 스코프 밖(mart 단계)이라 여기서는 만들지 않는다.

dim_bike처럼 날짜 범위를 누적 처리하는 잡이 아니라 하루치(target_date)를 통째로
재계산해서 OVERWRITE하는 구조라 워터마크가 없다 - 파티션을 다시 덮어써도 같은 입력이면
같은 결과가 나오므로 재실행이 그냥 멱등하게 처리된다.

### Spark 제거 (#171)
읽기/쓰기는 pyiceberg, 재고 조인 + 랭킹 윈도우는 pyarrow에 윈도우 함수가 없어 DuckDB
SQL(row_number() OVER)로 옮긴다. 병합 로직은 순수 DuckDB SQL이라 단위 테스트가 가능하다.

사용법:
    python -m jobs.build_fact_bike_decision
    SNAPSHOT_DATE=2026-08-17 python -m jobs.build_fact_bike_decision
"""
import logging
import os
import sys
from datetime import date

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, NestedField, StringType

import config
from common.duckdb_io import connect, query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket
from common.sql_assert import QualityCheck, QualityCheckError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SUSPEND = "대여중단"
HOLD = "보류"

FACT_BIKE_RISK_TABLE = "gold.fact_bike_risk"
BIKE_LOCATION_TABLE = "gold.bike_location"
# NOTE: 대여소 재고 입력을 배치 추정(fact_station_inventory) vs 실시간(station_inventory_snapshot)
# 중 뭘 쓸지 아직 팀 확정 전. 지금은 fact_station_inventory(bike_cnt/hold_num/target_bike_cnt)
# 기준으로 짜뒀고, 확정되면 이 테이블명과 join 컬럼명만 바꾸면 된다.
STATION_INVENTORY_TABLE = "gold.fact_station_inventory"
GOLD_TABLE = "gold.fact_bike_decision"
PARTITION_COLUMN = "snapshot_date"

GOLD_SCHEMA = Schema(
    NestedField(1, "snapshot_date", DateType(), required=False),
    NestedField(2, "bike_id", StringType(), required=False),
    NestedField(3, "action", StringType(), required=False),
)
GOLD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)


def _ensure_fact_bike_decision_table(catalog):
    catalog.create_namespace_if_not_exists("gold")
    try:
        return catalog.load_table(GOLD_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", GOLD_TABLE)
        return catalog.create_table(GOLD_TABLE, schema=GOLD_SCHEMA, partition_spec=GOLD_PARTITION_SPEC)


def _validate_fact_bike_decision(table: pa.Table, risk_table: pa.Table) -> None:
    """오늘자 파티션만 검증한다 (OVERWRITE 구조)."""
    (
        QualityCheck("fact_bike_decision_check")
        .is_complete("bike_id")
        .is_complete("action")
        .is_contained_in("action", [SUSPEND, HOLD])
        .run(table)
        .raise_if_failed(QualityCheckError)
    )

    # 참조 무결성: 오늘자 fact_bike_decision의 모든 bike_id는 오늘자 fact_bike_risk에도 있어야 한다.
    conn = connect()
    conn.register("decision", table)
    conn.register("risk", risk_table)
    orphan_count = query_arrow(
        conn,
        "SELECT COUNT(*) AS cnt FROM decision d LEFT JOIN risk r ON d.bike_id = r.bike_id WHERE r.bike_id IS NULL",
    )["cnt"][0].as_py()
    if orphan_count > 0:
        raise QualityCheckError(f"fact_bike_risk에 없는 bike_id {orphan_count}건 발견")


# 대여소별 suspendable_bike_cnt(여유분), critical_cnt(즉시 대여중단 확정 수)
# 설계 문서엔 "Critical부터 순서대로 잔여량 차감" 식 반복 처리로 설명돼있지만,
# Critical은 예산 확인 없이 무조건 대여중단(=항상 critical_cnt만큼만 소진)이고
# Warning은 고정 랭킹 컷이라, 반복문 없이 이 닫힌 식(suspendable - critical) +
# 랭킹 비교 한 번으로 동일한 결과가 나온다 (문서 예시 두 개로 직접 검증함).
_DECIDE_ACTIONS_SQL = """
    WITH bikes AS (
        -- 재고 정보 없는 대여소(운영 중단 등)면 warning_available_cnt가 null로
        -- 남아 그 대여소의 Warning 자전거는 비교 조건이 거짓이 되어 자동으로
        -- 보류 처리된다 (에러로 죽지 않고 안전한 쪽으로 fallback).
        SELECT
            r.bike_id, r.risk_score, r.risk_grade, l.last_station_id AS station_id,
            i.bike_cnt, i.target_bike_cnt
        FROM risk r
        JOIN location l ON r.bike_id = l.bike_id
        LEFT JOIN inventory i ON l.last_station_id = i.station_id
    ),
    station_cap AS (
        SELECT
            station_id,
            GREATEST(0, GREATEST(0, bike_cnt - target_bike_cnt)
                - SUM(CASE WHEN risk_grade = 'Critical' THEN 1 ELSE 0 END)) AS warning_available_cnt
        FROM bikes
        GROUP BY station_id, bike_cnt, target_bike_cnt
    ),
    ranked AS (
        SELECT
            b.bike_id, b.risk_grade,
            row_number() OVER (PARTITION BY b.station_id ORDER BY b.risk_score DESC) AS warning_rank,
            sc.warning_available_cnt
        FROM bikes b
        LEFT JOIN station_cap sc ON b.station_id = sc.station_id
    )
    SELECT
        CAST(? AS DATE) AS snapshot_date,
        bike_id,
        CASE
            WHEN risk_grade = 'Critical' THEN ?
            WHEN risk_grade = 'Warning' AND warning_rank <= COALESCE(warning_available_cnt, 0) THEN ?
            ELSE ?
        END AS action
    FROM ranked
"""


def _decide_actions(
    risk_table: pa.Table, location_table: pa.Table, inventory_table: pa.Table, target_date: date
) -> pa.Table:
    conn = connect()
    conn.register("risk", risk_table)
    conn.register("location", location_table)
    conn.register("inventory", inventory_table)
    return query_arrow(
        conn, _DECIDE_ACTIONS_SQL, [target_date.strftime("%Y-%m-%d"), SUSPEND, SUSPEND, HOLD]
    )


def _process_date(catalog, gold_table, target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")

    risk_table = catalog.load_table(FACT_BIKE_RISK_TABLE).scan(
        row_filter=EqualTo("snapshot_date", date_str)
    ).to_arrow()
    row_count = len(risk_table)
    if row_count == 0:
        logger.info("%s: fact_bike_risk에 처리할 데이터 없음", date_str)
        return 0

    location_table = catalog.load_table(BIKE_LOCATION_TABLE).scan(
        selected_fields=("bike_id", "last_station_id")
    ).to_arrow()
    inventory_table = catalog.load_table(STATION_INVENTORY_TABLE).scan(
        selected_fields=("station_id", "bike_cnt", "target_bike_cnt")
    ).to_arrow()

    out_table = _decide_actions(risk_table, location_table, inventory_table, target_date)
    overwrite_partition(gold_table, out_table, PARTITION_COLUMN, date_str)

    written = catalog.load_table(GOLD_TABLE).scan(row_filter=EqualTo("snapshot_date", date_str)).to_arrow()
    _validate_fact_bike_decision(written, risk_table)  # 실패 시 QualityCheckError -> 배치 중단

    suspend_count = written["action"].to_pylist().count(SUSPEND)
    logger.info("%s: 자전거 %d대 중 %d대 대여중단 결정", date_str, row_count, suspend_count)
    return row_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    gold_table = _ensure_fact_bike_decision_table(catalog)

    snapshot_date_str = os.getenv("SNAPSHOT_DATE")
    target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

    try:
        _process_date(catalog, gold_table, target_date)
    except QualityCheckError as e:
        logger.error("%s 처리 실패, 배치 중단: %s", target_date, e)
        sys.exit(1)


if __name__ == "__main__":
    run()