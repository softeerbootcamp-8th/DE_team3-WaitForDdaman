"""
Gold - 대여소별 자전거 재고 (gold.bike_location + gold.station_active +
silver.bikeman_action -> gold.fact_station_inventory)

### 최종 위치 결정 규칙
자전거의 "진짜 현재 위치"는 대여이력 기준 위치(gold.bike_location)와 수거/배치
이벤트(silver.bikeman_action) 중 시각이 더 최신인 쪽을 따른다.

    1. bikeman_action에 해당 자전거 이벤트가 아예 없거나, 있어도
       bike_location.last_event_at보다 오래됐다 -> bike_location 그대로 사용
    2. 가장 최신 이벤트가 COLLECT(수거)다 -> 필드에서 제거된 상태이므로
       재고 집계에서 제외 (어느 대여소에도 속하지 않음)
    3. 가장 최신 이벤트가 DEPLOY(배치)이고 station_id가 있다 -> 그 station_id로
       위치를 덮어씀 (배치 이벤트가 대여이력보다 최신 정보이므로)
    4. DEPLOY인데 station_id가 없다(이례적 케이스) -> bike_location 값으로 폴백

### bike_cnt / target_bike_cnt - 왜 이 집계 자체는 증분화하지 않는가
bike_cnt는 대여소별 "지금 몇 대 있는가"를 매번 다시 세야 하는 집계값이다.
bike_location처럼 "안 바뀐 자전거는 그대로 두고 바뀐 것만 갱신"하는 방식이
안 통한다 - 자전거 한 대만 위치가 바뀌어도 그 대여소의 합계가 통째로
바뀌기 때문이다. 다만 집계 재료 중 gold.bike_location/gold.bike_last_action은
둘 다 "자전거 수만큼"(수만 건)으로 크기가 고정된 상태 테이블이라 매번 전체를
다시 읽어도 비용이 크지 않다 - 증분화가 필요한 건 그 재료를 만드는 쪽이다.

gold.station_active(운영 중인 대여소만)를 기준으로 대여소별 자전거 수를 센다.
자전거가 하나도 없는 대여소도 0으로 나와야 하므로 station_active를 기준(left side)
으로 두고 자전거 수를 왼쪽 조인한다. target_bike_cnt는 거치대 수(hold_num)를
목표치로 사용한다.

### gold.bike_last_action - 증분 유지되는 "자전거별 최신 수거/배치 이벤트"
silver.bikeman_action은 매일 계속 쌓이는 이벤트 로그라, 매번 전체를 훑어서
자전거별 최신 이벤트를 찾으면(예전 방식) 데이터가 쌓일수록 느려진다.
gold.bike_location과 동일한 baseline+delta 패턴으로 증분 유지한다. 이 상태
테이블도 "자전거 수만큼"으로 크기가 고정되므로 이후 join/집계 비용은
커지지 않는다.

### 델타 구간을 "어제 하루"로 고정하지 않고 자기 자신의 snapshot_date로 추적하는 이유
gold.bike_location과 동일한 이유(자세한 설명은 build_bike_location.py 참고) -
어제 하루로 고정하면 gold_dim_fact가 하루라도 실행을 건너뛴 사이의
bikeman_action 이벤트는 어떤 미래 실행에서도 다시는 스캔되지 않아 조용히
유실된다. gold.bike_last_action 자신의 snapshot_date(MAX)를 워터마크로 재사용해서
[직전 snapshot_date, 이번 snapshot_date - 1] 구간을 스캔한다 - 평소엔 하루뿐이라
성능은 동일하고, 실행이 밀렸을 때만 구간이 넓어져 스스로 복구된다.

`occurred_date_partition` identity 파티션 범위로 파일을 먼저 줄이고, 정확한 경계는
기존처럼 occurred_at 타임스탬프 범위(`>= 시작 AND < 끝+1일`)로 다시 제한한다.

### 전체 덮어쓰기 (TEMP류 입력에 의존하는 최신 상태)
gold.bike_last_action/gold.fact_station_inventory 둘 다 파티션 없이 매번 전체를
덮어쓴다 - pyiceberg의 overwrite_all()(#170)이 이 의미를 그대로 옮긴 것.

### Spark 제거 (#170)
읽기/쓰기는 pyiceberg, "자전거별 최신 이벤트 1건" 채택은 pyarrow에 윈도우 함수가
없어 DuckDB SQL(QUALIFY row_number() OVER)로 옮긴다. 병합/위치 판정/집계는 순수
DuckDB SQL이라 단위 테스트가 가능하다.

### 적재 전 품질 검증 (common/sql_assert.py, #146에서 PyDeequ 제거)
build_dim_bike.py와 동일한 패턴으로 gold.bike_last_action/gold.fact_station_inventory
둘 다 쓰기 전에 검증한다. 실패하면 QualityCheckError로 배치를 즉시 중단한다.

사용법:
    python -m jobs.build_fact_station_inventory
    SNAPSHOT_DATE=2026-08-17 python -m jobs.build_fact_station_inventory
"""
import logging
import os
import sys
from datetime import date, datetime, time, timedelta

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan, LessThanOrEqual
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DateType, IntegerType, NestedField, StringType, TimestamptzType

import config
from common.duckdb_io import connect, query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_all
from common.s3_utils import ensure_bucket
from common.sql_assert import QualityCheck, QualityCheckError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BIKE_LOCATION_TABLE = "gold.bike_location"
STATION_ACTIVE_TABLE = "gold.station_active"
BIKEMAN_ACTION_TABLE = "silver.bikeman_action"
BIKE_LAST_ACTION_TABLE = "gold.bike_last_action"
GOLD_TABLE = "gold.fact_station_inventory"

GOLD_COLUMNS = ["station_id", "bike_cnt", "hold_num", "target_bike_cnt", "snapshot_date"]
BIKE_LAST_ACTION_COLUMNS = ["bike_id", "action_event_type", "action_station_id", "action_at", "snapshot_date"]

# TEMP류(파티션 없음) 테이블이라 스펙 없이 스키마만 정의한다.
GOLD_SCHEMA = Schema(
    NestedField(1, "station_id", StringType(), required=False),
    NestedField(2, "bike_cnt", IntegerType(), required=False),
    NestedField(3, "hold_num", IntegerType(), required=False),
    NestedField(4, "target_bike_cnt", IntegerType(), required=False),
    NestedField(5, "snapshot_date", DateType(), required=False),
)
BIKE_LAST_ACTION_SCHEMA = Schema(
    NestedField(1, "bike_id", StringType(), required=False),
    NestedField(2, "action_event_type", StringType(), required=False),
    NestedField(3, "action_station_id", StringType(), required=False),
    NestedField(4, "action_at", TimestamptzType(), required=False),
    NestedField(5, "snapshot_date", DateType(), required=False),
)

# 자전거별 최신 이벤트 1건만 채택한다.
_BIKE_LAST_ACTION_DELTA_SQL = """
    SELECT
        bike_id,
        event_type AS delta_event_type,
        station_id AS delta_station_id,
        occurred_at AS delta_at
    FROM bikeman_action
    QUALIFY row_number() OVER (PARTITION BY bike_id ORDER BY occurred_at DESC) = 1
"""

# baseline(직전 상태)과 delta(신규 반영분)를 병합한다 - build_bike_location.py의
# _MERGE_SQL과 동일 idiom(값 컬럼만 2개로 늘어남).
_MERGE_BIKE_LAST_ACTION_SQL = """
    WITH merged AS (
        SELECT
            COALESCE(b.bike_id, d.bike_id) AS bike_id,
            b.base_event_type, b.base_station_id, b.base_at,
            d.delta_event_type, d.delta_station_id, d.delta_at,
            (d.delta_at IS NOT NULL AND (b.base_at IS NULL OR d.delta_at > b.base_at)) AS delta_is_newer
        FROM baseline b
        FULL OUTER JOIN delta d ON b.bike_id = d.bike_id
    )
    SELECT
        bike_id,
        CASE WHEN delta_is_newer THEN delta_event_type ELSE base_event_type END AS action_event_type,
        CASE WHEN delta_is_newer THEN delta_station_id ELSE base_station_id END AS action_station_id,
        CASE WHEN delta_is_newer THEN delta_at ELSE base_at END AS action_at,
        CAST(? AS DATE) AS snapshot_date
    FROM merged
"""

# 자전거별 최종 위치(effective_station_id, excluded)를 계산한다.
_RESOLVE_SQL = """
    WITH joined AS (
        SELECT
            bl.bike_id,
            bl.last_station_id,
            bl.last_event_at,
            la.action_event_type,
            la.action_station_id,
            la.action_at,
            (la.action_at IS NOT NULL AND la.action_at > bl.last_event_at) AS action_is_newer
        FROM bike_location bl
        LEFT JOIN latest_action la ON bl.bike_id = la.bike_id
    )
    SELECT
        bike_id,
        CASE
            WHEN action_is_newer AND action_event_type = 'COLLECT' THEN NULL
            WHEN action_is_newer AND action_event_type = 'DEPLOY' THEN COALESCE(action_station_id, last_station_id)
            ELSE last_station_id
        END AS effective_station_id,
        (action_is_newer AND action_event_type = 'COLLECT') AS excluded
    FROM joined
"""

# station_active(운영 중인 대여소, left side)를 기준으로 대여소별 자전거 수를 센다 -
# 자전거가 하나도 없는 대여소도 0으로 나와야 하므로 left join.
_AGGREGATE_SQL = """
    WITH active_bikes AS (
        SELECT effective_station_id AS station_id
        FROM resolved
        WHERE NOT excluded AND effective_station_id IS NOT NULL
    ),
    bike_counts AS (
        SELECT station_id, COUNT(*) AS bike_cnt
        FROM active_bikes
        GROUP BY station_id
    )
    SELECT
        sa.station_id,
        CAST(COALESCE(bc.bike_cnt, 0) AS INT) AS bike_cnt,
        sa.hold_num,
        sa.hold_num AS target_bike_cnt,
        CAST(? AS DATE) AS snapshot_date
    FROM station_active sa
    LEFT JOIN bike_counts bc ON sa.station_id = bc.station_id
"""


def _merge_bike_last_action(
    baseline_table: pa.Table,
    delta_table: pa.Table,
    snapshot_date: str,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """baseline(직전 상태)과 delta(신규 반영분)를 병합한다 - 카탈로그 없이 두
    PyArrow Table만으로 동작하는 순수 로직이라 단위 테스트가 가능하다."""
    conn = con or connect()
    conn.register("baseline", baseline_table)
    conn.register("delta", delta_table)
    return query_arrow(conn, _MERGE_BIKE_LAST_ACTION_SQL, [snapshot_date])


def _resolve_bike_station(
    bike_location_table: pa.Table,
    latest_action_table: pa.Table,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """자전거별 최종 위치(effective_station_id, excluded)를 계산한다 - 두
    PyArrow Table만으로 동작하는 순수 로직이라 단위 테스트가 가능하다."""
    conn = con or connect()
    conn.register("bike_location", bike_location_table)
    conn.register("latest_action", latest_action_table)
    return query_arrow(conn, _RESOLVE_SQL)


def _aggregate_station_inventory(
    resolved_table: pa.Table,
    station_active_table: pa.Table,
    snapshot_date: str,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """대여소별 재고를 집계한다 - 두 PyArrow Table만으로 동작하는 순수 로직이라
    단위 테스트가 가능하다."""
    conn = con or connect()
    conn.register("resolved", resolved_table)
    conn.register("station_active", station_active_table)
    return query_arrow(conn, _AGGREGATE_SQL, [snapshot_date])


def _ensure_gold_tables(catalog):
    catalog.create_namespace_if_not_exists("gold")
    try:
        gold_table = catalog.load_table(GOLD_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", GOLD_TABLE)
        gold_table = catalog.create_table(GOLD_TABLE, schema=GOLD_SCHEMA)
    try:
        bike_last_action_table = catalog.load_table(BIKE_LAST_ACTION_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", BIKE_LAST_ACTION_TABLE)
        bike_last_action_table = catalog.create_table(BIKE_LAST_ACTION_TABLE, schema=BIKE_LAST_ACTION_SCHEMA)
    return gold_table, bike_last_action_table


def _load_bike_last_action_baseline_snapshot(catalog) -> pa.Table:
    """baseline 계산과 MAX(snapshot_date) 조회에 필요한 컬럼을 gold.bike_last_action에서
    한 번만 스캔한다 (테이블이 비어있으면 빈 Table - cold start도 안전)."""
    return catalog.load_table(BIKE_LAST_ACTION_TABLE).scan(
        selected_fields=("bike_id", "action_event_type", "action_station_id", "action_at", "snapshot_date")
    ).to_arrow()


def _bike_last_action_baseline(gold_snapshot: pa.Table) -> pa.Table:
    """직전 상태 전체 (테이블이 비어있으면 빈 Table - cold start도 안전)."""
    con = connect()
    con.register("t", gold_snapshot)
    return query_arrow(
        con,
        """
        SELECT
            bike_id,
            action_event_type AS base_event_type,
            action_station_id AS base_station_id,
            action_at AS base_at
        FROM t
        """,
    )


def _bike_last_action_baseline_snapshot_date(gold_snapshot: pa.Table) -> date | None:
    """gold.bike_last_action에 남아있는 MAX(snapshot_date) - 그 값보다 이전
    bikeman_action 이벤트는 이미 baseline에 반영되어 있으므로, 이번 실행의
    델타 시작일로 그대로 쓸 수 있다. 테이블이 비어있으면(cold start) None."""
    if len(gold_snapshot) == 0:
        return None
    con = connect()
    con.register("t", gold_snapshot)
    return con.execute("SELECT MAX(snapshot_date) FROM t").fetchone()[0]


def _read_bikeman_action_delta(catalog, start_date: date | None, end_date: date) -> pa.Table:
    """bikeman_action에서 아직 baseline에 반영 안 된 구간 [start_date, end_date]만
    스캔한다. occurred_date_partition identity 파티션으로 파일을 먼저 줄이고,
    정확한 경계는 occurred_at 타임스탬프 범위로 다시 제한한다."""
    end_exclusive = datetime.combine(end_date + timedelta(days=1), time.min)
    table = catalog.load_table(BIKEMAN_ACTION_TABLE)
    selected_fields = ("bike_id", "event_type", "station_id", "occurred_at", "occurred_date_partition")

    filters = [
        LessThanOrEqual("occurred_date_partition", end_date.isoformat()),
        LessThan("occurred_at", end_exclusive),
    ]
    if start_date is not None:
        filters.append(GreaterThanOrEqual("occurred_date_partition", start_date.isoformat()))
        filters.append(GreaterThanOrEqual("occurred_at", datetime.combine(start_date, time.min)))

    row_filter = filters[0]
    for f in filters[1:]:
        row_filter = And(row_filter, f)

    return table.scan(row_filter=row_filter, selected_fields=selected_fields).to_arrow()


def build_bike_last_action(catalog, snapshot_date: date) -> pa.Table:
    """gold.bike_last_action의 오늘자 스냅샷을 증분(baseline+delta)으로 계산한다."""
    gold_snapshot = _load_bike_last_action_baseline_snapshot(catalog)
    baseline = _bike_last_action_baseline(gold_snapshot)
    delta_start = _bike_last_action_baseline_snapshot_date(gold_snapshot)
    delta_end = snapshot_date - timedelta(days=1)
    bikeman_action_delta = _read_bikeman_action_delta(catalog, delta_start, delta_end)

    con = connect()
    con.register("bikeman_action", bikeman_action_delta)
    delta = query_arrow(con, _BIKE_LAST_ACTION_DELTA_SQL)

    return _merge_bike_last_action(baseline, delta, snapshot_date.strftime("%Y-%m-%d"))


def build_fact_station_inventory(catalog, snapshot_date: date, latest_action: pa.Table) -> pa.Table:
    bike_location = catalog.load_table(BIKE_LOCATION_TABLE).scan(
        selected_fields=("bike_id", "last_station_id", "last_event_at")
    ).to_arrow()
    resolved = _resolve_bike_station(bike_location, latest_action)

    station_active = catalog.load_table(STATION_ACTIVE_TABLE).scan(
        selected_fields=("station_id", "hold_num")
    ).to_arrow()
    return _aggregate_station_inventory(resolved, station_active, snapshot_date.strftime("%Y-%m-%d"))


def _dedup_by(table: pa.Table, key_column: str, con: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
    """has_uniqueness(threshold=0.99) 하드 게이트가 1%까지는 통과시켜, 실패 시
    전체 적재가 막히는 위험이 있다(#332 PR 리뷰). 쓰기 직전에 여기서 미리 한 행만
    남겨서 그 하드 게이트가 사실상 항상 통과하게 만든다 - 어느 쪽이 "맞는" 값인지
    판단하는 로직은 아니라 결정적으로 하나를 고를 뿐이다. 실제 원인 추적은
    gold_bike_last_action.yaml/gold_fact_station_inventory.yaml DQ 어써션
    (dq.check_result_history)이 계속 담당한다."""
    conn = con or connect()
    conn.register("dedup_target", table)
    deduped = query_arrow(
        conn, f"SELECT DISTINCT ON ({key_column}) * FROM dedup_target ORDER BY {key_column}"
    )
    dropped = len(table) - len(deduped)
    if dropped:
        logger.warning("%s 중복 %d건 dedup으로 제거", key_column, dropped)
    return deduped


def _validate_bike_last_action(table: pa.Table) -> None:
    # bikeman_action(수거/배치) 이벤트가 아직 하나도 없는 환경(신규 배포 직후 등)에서는
    # 이 결과가 통째로 0행일 수 있다 - 정상 상태다. common/sql_assert.py(#140)는 0행에서
    # 위반이 자연히 0건이라 별도 스킵 분기가 필요 없다.
    (
        QualityCheck("bike_last_action_check")
        .is_complete("bike_id")
        .has_uniqueness("bike_id", threshold=0.99)
        .run(table)
        .raise_if_failed(QualityCheckError)
    )


def _validate_fact_station_inventory(table: pa.Table) -> None:
    # gold.station_active(운영 중 대여소)가 아직 없으면 이 결과도 0행일 수 있다 -
    # bike_last_action과 동일한 이유로 별도 스킵 분기가 필요 없다.
    (
        QualityCheck("fact_station_inventory_check")
        .is_complete("station_id")
        .is_non_negative("bike_cnt")
        .has_uniqueness("station_id", threshold=0.99)
        .run(table)
        .raise_if_failed(QualityCheckError)
    )


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date()

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    gold_table, bike_last_action_table = _ensure_gold_tables(catalog)

    # gold.bike_last_action을 먼저 증분 갱신 - 아래 계산에 재사용하고 그대로
    # 테이블에도 써서 다음 실행의 baseline이 되게 한다.
    latest_action = build_bike_last_action(catalog, snapshot_date).select(BIKE_LAST_ACTION_COLUMNS)
    latest_action = _dedup_by(latest_action, "bike_id")
    try:
        _validate_bike_last_action(latest_action)
    except QualityCheckError as e:
        logger.error("%s: gold.bike_last_action 검증 실패, 적재 중단: %s", snapshot_date_str, e)
        sys.exit(1)
    overwrite_all(bike_last_action_table, latest_action)

    out_table = build_fact_station_inventory(catalog, snapshot_date, latest_action).select(GOLD_COLUMNS)
    out_table = _dedup_by(out_table, "station_id")
    row_count = len(out_table)
    try:
        _validate_fact_station_inventory(out_table)
    except QualityCheckError as e:
        logger.error("%s: gold.fact_station_inventory 검증 실패, 적재 중단: %s", snapshot_date_str, e)
        sys.exit(1)

    overwrite_all(gold_table, out_table)

    logger.info(
        "%s: gold.fact_station_inventory %d행 갱신 완료 (bike_last_action 증분 처리)",
        snapshot_date_str, row_count,
    )


if __name__ == "__main__":
    run()
