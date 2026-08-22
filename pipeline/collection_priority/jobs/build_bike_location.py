"""
Gold(TEMP) - 자전거별 현재 위치 (silver.rental_history -> gold.bike_location)

### 증분 처리
매번 silver.rental_history 전체를 스캔+정렬하면 데이터가 쌓일수록 배치가
점점 느려진다. 대신 기존 gold.bike_location(직전 상태)을 baseline으로 읽고,
아직 반영 안 된 파티션만 스캔해서 델타를 구한 뒤 병합한다.

### 델타 구간을 "어제 하루"로 고정하지 않고 자기 자신의 snapshot_date로 추적하는 이유
처음엔 델타 구간을 무조건 `snapshot_date - 1`(어제) 하루로 고정했었다. 이러면
gold_dim_fact가 하루라도 실행을 건너뛰면(예: catchup=False 상태에서 스케줄러
장애) 그 사이에 낀 rent_date_partition은 그 뒤로 "어제"가 아니게 되어 어떤 미래
실행에서도 다시는 스캔되지 않는다 - 해당 파티션에만 활동이 있던 자전거의 위치가
영영 갱신되지 않는 조용한 데이터 유실이 발생한다.

이를 막기 위해 gold.bike_location 자신의 snapshot_date 컬럼을 워터마크로 재사용한다.
이 테이블은 매 실행마다 전체가 동일한 snapshot_date로 덮어써지므로(TEMP 성격),
MAX(snapshot_date)가 곧 "그 값 - 1까지의 rent_date_partition은 이미 반영됨"을
뜻한다. 그래서 이번 실행의 델타 시작일 = 직전 MAX(snapshot_date)(비어있으면
None=하한 없음, 즉 전체 스캔), 끝일 = 이번 snapshot_date - 1이다. 평소(매일
정상 실행)엔 이 구간이 정확히 하루뿐이라 기존과 성능이 동일하고, 실행이
밀렸을 때만 자동으로 구간이 넓어져 누락을 스스로 복구한다(self-healing).
별도 워터마크 파일이 필요 없다.

오늘(아직 반영 안 된 파티션 기준 신규) 활동이 없는 자전거는 baseline 그대로 유지
(carry-forward)하고, 활동이 있는 자전거만 위치를 갱신한다. baseline/delta
둘 다 있는 경우 last_event_at(반납 시각)이 더 최신인 쪽을 채택해서, 같은 날
재실행되거나 날짜 순서가 어긋나는 백필 시나리오에서도 과거 데이터로 최신
상태를 덮어쓰지 않는다(idempotent).

### "현재 위치"의 정의
자전거별로 가장 최근 반납 건의 반납 대여소(return_station_id)를 현재 위치로
본다. silver.rental_history는 이미 반납 완료된 건만 들어오므로(return_dt가
비어있는 진행 중 대여 레코드는 존재하지 않음) "아직 반납 안 됨" 분기는 없다.

last_station_id가 null인 경우는 데이터 결측이 아니라, 대여소가 아닌 엉뚱한
곳에 반납된 경우다(예: 노상 방치). 그런 자전거는 어느 대여소에도 속하지
않는 게 맞으므로, gold.fact_station_inventory에서 재고 집계 대상에서
자연스럽게 빠지는 게 의도된 동작이다.

### last_event_at을 함께 저장하는 이유
gold.fact_station_inventory가 이 위치 정보와 silver.bikeman_action(수거/배치)
이벤트 중 어느 쪽이 더 최신인지 비교해서 최종 위치를 정해야 한다. 그 비교
기준 시각(반납 시각)을 여기서 같이 내려준다. 병합 시 baseline/delta 중
최신 쪽을 고르는 기준으로도 재사용된다.

### TEMP 성격 - 매번 전체 덮어쓰기
"현재 상태"만 의미가 있고 이력을 쌓지 않는다. 파티션 없이 매 실행마다 테이블
전체를 덮어쓴다 - pyiceberg의 overwrite_all()(#170)이 이 의미를 그대로 옮긴 것.

### Spark 제거 (#170)
읽기/쓰기는 pyiceberg, delta의 "자전거별 최신 반납 1건"은 pyarrow에 윈도우
함수가 없어 DuckDB SQL(QUALIFY row_number() OVER)로 옮긴다. 병합(baseline+delta)은
순수 DuckDB SQL이라 단위 테스트가 가능하다.

### 적재 전 품질 검증 (common/sql_assert.py, #146에서 PyDeequ 제거)
build_dim_bike.py와 동일한 패턴 - bike_id는 병합 키이므로 병합 결과에서 항상
유일해야 하고(hasUniqueness), bike_id/snapshot_date는 결측이 있으면 안 된다.
실패하면 QualityCheckError로 배치를 즉시 중단한다(적재 전 검증이므로 gold
테이블에는 아무 영향 없음 - overwrite_all보다 먼저 검증함).

사용법:
    python -m jobs.build_bike_location
    SNAPSHOT_DATE=2026-08-17 python -m jobs.build_bike_location
"""
import logging
import os
import sys
from datetime import date, datetime, timedelta

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.schema import Schema
from pyiceberg.types import DateType, NestedField, StringType, TimestamptzType

import config
from common.duckdb_io import query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_all
from common.s3_utils import ensure_bucket
from common.sql_assert import QualityCheck, QualityCheckError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SILVER_TABLE = "silver.rental_history"
GOLD_TABLE = "gold.bike_location"

GOLD_COLUMNS = ["bike_id", "last_station_id", "last_event_at", "snapshot_date"]

# TEMP류(파티션 없음) 테이블이라 스펙 없이 스키마만 정의한다.
GOLD_SCHEMA = Schema(
    NestedField(1, "bike_id", StringType(), required=False),
    NestedField(2, "last_station_id", StringType(), required=False),
    NestedField(3, "last_event_at", TimestamptzType(), required=False),
    NestedField(4, "snapshot_date", DateType(), required=False),
)

# 자전거별 가장 최근 반납 1건만 채택한다.
_DELTA_SQL = """
    SELECT
        bike_id,
        return_station_id AS delta_station_id,
        return_dt         AS delta_event_at
    FROM silver_rental_history
    QUALIFY row_number() OVER (PARTITION BY bike_id ORDER BY return_dt DESC) = 1
"""

# baseline(직전 상태)과 delta(신규 반영분)를 병합한다. baseline에 없던(신규)
# 자전거는 base_event_at이 null이라 무조건 delta 채택. 둘 다 있으면 반납
# 시각이 더 최신인 쪽 채택 - 재실행/백필에도 안전(idempotent).
_MERGE_SQL = """
    WITH merged AS (
        SELECT
            COALESCE(b.bike_id, d.bike_id) AS bike_id,
            b.base_station_id,
            b.base_event_at,
            d.delta_station_id,
            d.delta_event_at,
            (d.delta_event_at IS NOT NULL
                AND (b.base_event_at IS NULL OR d.delta_event_at > b.base_event_at)
            ) AS delta_is_newer
        FROM baseline b
        FULL OUTER JOIN delta d ON b.bike_id = d.bike_id
    )
    SELECT
        bike_id,
        CASE WHEN delta_is_newer THEN delta_station_id ELSE base_station_id END AS last_station_id,
        CASE WHEN delta_is_newer THEN delta_event_at ELSE base_event_at END AS last_event_at,
        CAST(? AS DATE) AS snapshot_date
    FROM merged
"""


def _delta(silver_table: pa.Table, con: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
    """silver.rental_history(범위 스캔 결과)에서 자전거별 가장 최근 반납 1건만
    채택한다 - QUALIFY(윈도우)가 있어 pyarrow만으로는 옮길 수 없다."""
    conn = con or duckdb.connect(":memory:")
    conn.register("silver_rental_history", silver_table)
    return query_arrow(conn, _DELTA_SQL)


def _merge_baseline_delta(
    baseline_table: pa.Table,
    delta_table: pa.Table,
    snapshot_date: str,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """baseline(직전 상태)과 delta(신규 반영분)를 병합한다 - 카탈로그 없이 두
    PyArrow Table만으로 동작하는 순수 로직이라 단위 테스트가 가능하다."""
    conn = con or duckdb.connect(":memory:")
    conn.register("baseline", baseline_table)
    conn.register("delta", delta_table)
    return query_arrow(conn, _MERGE_SQL, [snapshot_date])


def _ensure_gold_table(catalog):
    catalog.create_namespace_if_not_exists("gold")
    try:
        return catalog.load_table(GOLD_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", GOLD_TABLE)
        return catalog.create_table(GOLD_TABLE, schema=GOLD_SCHEMA)


def _baseline(catalog) -> pa.Table:
    """직전 상태 전체 (테이블이 비어있으면 빈 Table - cold start도 안전)."""
    full = catalog.load_table(GOLD_TABLE).scan(
        selected_fields=("bike_id", "last_station_id", "last_event_at")
    ).to_arrow()
    con = duckdb.connect(":memory:")
    con.register("t", full)
    return query_arrow(
        con,
        "SELECT bike_id, last_station_id AS base_station_id, last_event_at AS base_event_at FROM t",
    )


def _baseline_snapshot_date(catalog) -> date | None:
    """gold.bike_location에 남아있는 MAX(snapshot_date) - 그 값보다 이전
    rent_date_partition은 이미 baseline에 반영되어 있다는 뜻이므로, 이번 실행의
    델타 시작일로 그대로 쓸 수 있다. 테이블이 비어있으면(cold start) None."""
    full = catalog.load_table(GOLD_TABLE).scan(selected_fields=("snapshot_date",)).to_arrow()
    if len(full) == 0:
        return None
    con = duckdb.connect(":memory:")
    con.register("t", full)
    row = con.execute("SELECT MAX(snapshot_date) FROM t").fetchone()
    return row[0]


def _read_silver_delta(catalog, start_date: date | None, end_date: date) -> pa.Table:
    """silver.rental_history에서 아직 baseline에 반영 안 된 구간 [start_date, end_date]만
    스캔한다. start_date가 None이면(cold start) 하한 없이 end_date까지 전체를 스캔한다."""
    end_str = end_date.strftime("%Y-%m-%d")
    table = catalog.load_table(SILVER_TABLE)
    selected_fields = ("bike_id", "return_station_id", "return_dt", "rent_date_partition")
    if start_date is not None:
        start_str = start_date.strftime("%Y-%m-%d")
        row_filter = And(
            GreaterThanOrEqual("rent_date_partition", start_str),
            LessThanOrEqual("rent_date_partition", end_str),
        )
    else:
        row_filter = LessThanOrEqual("rent_date_partition", end_str)
    return table.scan(row_filter=row_filter, selected_fields=selected_fields).to_arrow()


def build_bike_location(catalog, snapshot_date: date) -> pa.Table:
    baseline = _baseline(catalog)
    delta_start = _baseline_snapshot_date(catalog)
    delta_end = snapshot_date - timedelta(days=1)
    silver_delta = _read_silver_delta(catalog, delta_start, delta_end)
    delta = _delta(silver_delta)

    return _merge_baseline_delta(baseline, delta, snapshot_date.strftime("%Y-%m-%d"))


def _validate_bike_location(table: pa.Table) -> None:
    # 아직 반납 완료된 대여이력이 하나도 없는 환경(서비스 최초 구동 직후 등)에서는
    # 이 결과도 0행일 수 있다. common/sql_assert.py(#140)는 0행에서 위반이 자연히
    # 0건이라(과거 PyDeequ와 달리 빈 데이터셋을 실패로 취급하지 않음) 별도 스킵
    # 분기가 필요 없다.
    (
        QualityCheck("bike_location_check")
        .is_complete("bike_id")
        .is_complete("snapshot_date")
        .has_uniqueness("bike_id", threshold=0.99)
        .run(table)
        .raise_if_failed(QualityCheckError)
    )


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date()

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    gold_table = _ensure_gold_table(catalog)

    out_table = build_bike_location(catalog, snapshot_date).select(GOLD_COLUMNS)
    row_count = len(out_table)
    try:
        _validate_bike_location(out_table)  # 실패 시 QualityCheckError -> 적재 없이 배치 중단
    except QualityCheckError as e:
        logger.error("%s: gold.bike_location 검증 실패, 적재 중단: %s", snapshot_date_str, e)
        sys.exit(1)

    overwrite_all(gold_table, out_table)

    logger.info("%s: gold.bike_location %d행 갱신 완료 (증분 처리)", snapshot_date_str, row_count)


if __name__ == "__main__":
    run()
