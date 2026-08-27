"""
gold_risk_decision 원안의 "4-a/4-b 필터 + 5. run_risk_scoring_model + 6. build_fact_bike_risk"
세 단계를 한 job으로 구현 - gold.bike_features_daily -> 자전거별 risk_score/risk_grade
산출. 모델 로드/추론 자체는 run_risk_scoring_model.py에 있고 여기서는 그걸 불러 쓴다.

수거(정비중이라 미배치) 자전거만 제외하고, 대여중단 상태였던 자전거도 매일 다시
추론 대상에 포함시킨다 - 대여소 재고 상황이 바뀌면 어제 대여중단이었던 자전거가
오늘은 보류로 풀릴 수도 있어서, "한 번 대여중단되면 고정" 구조로 만들면 안 된다.
이 필터는 bikeman_action 최신 이벤트만 보므로 최초 실행일(cold start)에도 이력이
없으면 자연히 아무도 제외되지 않아 별도 분기가 필요 없다.

dim_bike처럼 날짜 범위를 누적 처리하지 않고 하루치를 통째로 재계산해 OVERWRITE하므로
워터마크가 없다.

### Spark 제거 (#171)
읽기/쓰기는 pyiceberg, 최신 이벤트 판정은 pyarrow에 윈도우 함수가 없어 DuckDB
SQL(QUALIFY row_number() OVER)로 옮긴다. 모델 로드/추론(run_risk_scoring_model.score())은
이미 pandas 기반이라 그대로 - 입력을 읽는 부분만 DuckDB/pyiceberg로 바뀐다.

사용법:
    python -m jobs.build_fact_bike_risk
    SNAPSHOT_DATE=2026-08-17 python -m jobs.build_fact_bike_risk
"""
import logging
import os
import sys
from datetime import date

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, LessThanOrEqual
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, DoubleType, NestedField, StringType

import config
from common.duckdb_io import connect, query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket
from common.sql_assert import QualityCheck, QualityCheckError
from jobs.run_risk_scoring_model import load_model, score

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BIKE_FEATURES_TABLE = "gold.bike_features_daily"
BIKEMAN_ACTION_TABLE = "silver.bikeman_action"
GOLD_TABLE = "gold.fact_bike_risk"
PARTITION_COLUMN = "snapshot_date"

GOLD_SCHEMA = Schema(
    NestedField(1, "snapshot_date", DateType(), required=False),
    NestedField(2, "bike_id", StringType(), required=False),
    NestedField(3, "risk_score", DoubleType(), required=False),
    NestedField(4, "risk_grade", StringType(), required=False),
    NestedField(5, "model_version", StringType(), required=False),
)
GOLD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)


def _ensure_fact_bike_risk_table(catalog):
    catalog.create_namespace_if_not_exists("gold")
    try:
        return catalog.load_table(GOLD_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", GOLD_TABLE)
        return catalog.create_table(GOLD_TABLE, schema=GOLD_SCHEMA, partition_spec=GOLD_PARTITION_SPEC)


def _dedup_by_bike_id(table: pa.Table, con: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
    """has_uniqueness(threshold=0.99) 하드 게이트가 1%까지는 통과시켜, 실패 시
    전체 적재가 막히는 위험이 있다(#332 PR 리뷰). 이 잡은 검증이 커밋 뒤에 도는
    구조라(overwrite_partition 먼저, validate는 그 뒤 재조회) 실패해도 롤백이
    안 된다 - 그래서 커밋 전인 여기서 미리 한 행만 남겨서 애초에 그 상황 자체가
    안 생기게 한다. 어느 쪽이 "맞는" 값인지 판단하는 로직은 아니라 결정적으로
    하나를 고를 뿐이다. 실제 원인 추적은 gold_fact_bike_risk.yaml DQ 어써션
    (dq.check_result_history)이 계속 담당한다."""
    conn = con or connect()
    conn.register("dedup_target", table)
    deduped = query_arrow(conn, "SELECT DISTINCT ON (bike_id) * FROM dedup_target ORDER BY bike_id")
    dropped = len(table) - len(deduped)
    if dropped:
        logger.warning("gold.fact_bike_risk: bike_id 중복 %d건 dedup으로 제거", dropped)
    return deduped


def _validate_fact_bike_risk(table: pa.Table) -> None:
    """오늘자 파티션만 검증한다 (OVERWRITE 구조라 dim_bike처럼 테이블 전체를 볼 필요 없음)."""
    (
        QualityCheck("fact_bike_risk_check")
        .is_complete("bike_id")
        .is_complete("risk_score")
        .is_complete("risk_grade")
        .is_contained_in("risk_grade", ["Normal", "Warning", "Critical"])
        .satisfies("risk_score >= 0 AND risk_score <= 100", "risk_score_range")
        .has_uniqueness("bike_id", threshold=0.99)
        .run(table)
        .raise_if_failed(QualityCheckError)
    )

    # Critical 컷오프를 상위 1%로 잡았으니, 실제 비율이 크게 벗어나면 모델/입력 데이터
    # 이상 신호다 (예: feature 스케일이 이상해서 다들 같은 leaf로 몰리는 경우).
    # 배치를 죽일 정도로 확신할 순 없어서 경고만 남긴다.
    total = len(table)
    critical_count = table["risk_grade"].to_pylist().count("Critical")
    critical_ratio = critical_count / total if total else 0
    if not (0.0 <= critical_ratio <= 0.05):
        logger.warning("Critical 비율이 예상 범위(0~5%%) 밖: %.2f%%", critical_ratio * 100)


_LATEST_COLLECTED_SQL = """
    SELECT bike_id
    FROM actions
    QUALIFY row_number() OVER (PARTITION BY bike_id ORDER BY occurred_at DESC) = 1
        AND event_type = 'COLLECT'
"""


def _latest_collected_bike_ids(actions_table: pa.Table) -> pa.Table:
    """bike_id별 가장 최근 이벤트가 'COLLECT'인 자전거만 남긴다 - 수거 뒤에 배치
    이벤트가 이미 찍혔으면(최신이 DEPLOY 등) 대상에서 빠진다."""
    conn = connect()
    conn.register("actions", actions_table)
    return query_arrow(conn, _LATEST_COLLECTED_SQL)


_EXCLUDE_COLLECTED_SQL = """
    SELECT f.*
    FROM features f
    LEFT JOIN collected c ON f.bike_id = c.bike_id
    WHERE c.bike_id IS NULL
"""


def _exclude_collected_bikes(features_table: pa.Table, collected_table: pa.Table) -> pa.Table:
    conn = connect()
    conn.register("features", features_table)
    conn.register("collected", collected_table)
    return query_arrow(conn, _EXCLUDE_COLLECTED_SQL)


def _currently_collected_bike_ids(catalog, as_of: date) -> pa.Table:
    """4-a/4-b (skip_filter_first_run / apply_lagged_filter) 구현.

    bikeman_action에서 bike_id별 최신 이벤트가 'COLLECT'인 자전거 = 아직 미배치(정비중).
    대여중단 상태는 여기서 제외되지 않는다 - 수거 이벤트가 없거나, 수거 뒤에 배치
    이벤트가 이미 찍혔으면 오늘도 추론 대상에 포함된다.

    cold start(최초 실행일)에도 이 함수 하나로 충분하다 - bikeman_action 이력이
    아직 없으면 자연히 아무도 안 걸러지므로, 원안의 4-a(필터 스킵)와 4-b(필터 적용)가
    실질적으로 같은 코드가 된다. 그래서 DAG에는 두 이름이 남아있지만 여기 함수는 하나뿐이다.
    """
    try:
        table = catalog.load_table(BIKEMAN_ACTION_TABLE)
    except NoSuchTableError:
        return pa.table({"bike_id": pa.array([], type=pa.string())})

    cutoff = f"{as_of.isoformat()}T00:00:00+00:00"
    row_filter = And(
        LessThanOrEqual("occurred_date_partition", as_of.isoformat()),
        LessThanOrEqual("occurred_at", cutoff),
    )
    actions = table.scan(row_filter=row_filter, selected_fields=("bike_id", "event_type", "occurred_at")).to_arrow()
    return _latest_collected_bike_ids(actions)


def _score_features(features_table: pa.Table, target_date: date) -> pa.Table:
    art = load_model()
    feat_pd = features_table.to_pandas().set_index("bike_id")
    scored_pd = score(feat_pd, art).reset_index()

    conn = connect()
    conn.register("scored", scored_pd)
    return query_arrow(
        conn,
        """
        SELECT
            CAST(? AS DATE) AS snapshot_date,
            bike_id,
            CAST(risk_score AS DOUBLE) AS risk_score,
            CAST(risk_grade AS VARCHAR) AS risk_grade,
            CAST(model_version AS VARCHAR) AS model_version
        FROM scored
        """,
        [target_date.strftime("%Y-%m-%d")],
    )


def _process_date(catalog, gold_table, target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")

    features_table = catalog.load_table(BIKE_FEATURES_TABLE).scan(
        row_filter=EqualTo("snapshot_date", date_str)
    ).to_arrow()
    row_count = len(features_table)
    if row_count == 0:
        logger.info("%s: bike_features_daily에 처리할 데이터 없음", date_str)
        return 0

    # 4-a/4-b: 정비중(수거→미배치) 자전거 제외
    collected = _currently_collected_bike_ids(catalog, target_date)
    eligible_table = _exclude_collected_bikes(features_table, collected)
    eligible_count = len(eligible_table)
    if eligible_count == 0:
        logger.info("%s: 정비중 제외 후 추론 대상 없음", date_str)
        return 0

    # 5: run_risk_scoring_model, 6: build_fact_bike_risk
    out_table = _score_features(eligible_table, target_date)
    out_table = _dedup_by_bike_id(out_table)
    overwrite_partition(gold_table, out_table, PARTITION_COLUMN, date_str)

    written = catalog.load_table(GOLD_TABLE).scan(row_filter=EqualTo("snapshot_date", date_str)).to_arrow()
    _validate_fact_bike_risk(written)  # 실패 시 QualityCheckError -> 배치 중단

    logger.info(
        "%s: 자전거 %d대 risk_score 산출 (정비중 %d대 제외)",
        date_str, eligible_count, row_count - eligible_count,
    )
    return eligible_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    gold_table = _ensure_fact_bike_risk_table(catalog)

    snapshot_date_str = os.getenv("SNAPSHOT_DATE")
    target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

    try:
        _process_date(catalog, gold_table, target_date)
    except QualityCheckError as e:
        logger.error("%s 처리 실패, 배치 중단: %s", target_date, e)
        sys.exit(1)


if __name__ == "__main__":
    run()