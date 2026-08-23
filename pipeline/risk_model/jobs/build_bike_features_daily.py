"""
gold_risk_decision의 추론 입력을 만드는 feature 엔지니어링 job -
silver.rental_history + silver.failure_report -> gold.bike_features_daily (추론용)

피처 로직은 pipeline/train_risk_model/features.py의 build_samples()/read_rental()/
apply_trip_filters()를 그대로 재사용한다. train_risk_model README의 설계 원칙
("피처 로직은 features.py 하나를 학습·추론이 공유 - 갈라지면 train-serving skew")에
맞춰, features.py 자체는 수정하지 않는다.

기준일 이전 14일 rolling window라 dim_bike처럼 누적 처리하지 않는다 - 워터마크 없이
SNAPSHOT_DATE(기본값 오늘) 하루치를 매번 통째로 재계산해 OVERWRITE.

### Spark 제거 (#171)
features.py(#149)가 이미 SqlEngine을 통해 Spark/DuckDB 방언을 감춰주므로, 피처
계산 자체는 SqlEngine.for_duckdb로 감싸기만 하면 그대로 재사용된다. 이 파일에서
직접 짜여있던 나머지(대여중단 필터링, 읽기/쓰기)만 pyiceberg+DuckDB로 옮긴다.

### 대여중단(SUSPEND) 당일 대여 기록 제외 (#85)
gold.fact_bike_decision에서 action == '대여중단'으로 결정된 자전거인데 같은 날
rental_history에 대여 기록이 남아있으면 모순된(이상치) 데이터다 - 오늘 피처(trips,
dist_km 등) 계산에 그대로 섞여 들어가면 안 된다. build_samples()가 이미 지원하는
`rent` 오버라이드 파라미터("필터 이중 적용 방지" 주석 참고)를 이용해, features.py는
그대로 두고 이 파일에서만 사전 필터링한 rent를 넘긴다 - 학습 파이프라인엔 영향 없음.

자전거 자체를 영구 제외하지 않는다(추론 대상에서 계속 빠지면 재평가 기회가 없어짐,
build_fact_bike_risk.py의 "한 번 대여중단되면 고정 구조로 만들면 안 된다" 원칙과 동일)
- 모순이 발생한 그 날짜의 rental 레코드만 걸러내고, 다른 날짜 이력은 정상 반영한다.

사용법:
    python -m jobs.build_bike_features_daily
    SNAPSHOT_DATE=2026-08-17 python -m jobs.build_bike_features_daily
"""
import logging
import os
import sys
from datetime import date, timedelta

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, DoubleType, IntegerType, NestedField, StringType

import config
from common.duckdb_io import query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket
from common.sql_assert import QualityCheck, QualityCheckError
from pipeline.train_risk_model.features import apply_trip_filters, build_samples, read_rental
from pipeline.train_risk_model.settings import load_config
from pipeline.train_risk_model.sql_engine import SqlEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SUSPEND = "대여중단"

GOLD_TABLE = "gold.bike_features_daily"
FACT_BIKE_DECISION_TABLE = "gold.fact_bike_decision"
PARTITION_COLUMN = "snapshot_date"

GOLD_SCHEMA = Schema(
    NestedField(1, "snapshot_date", DateType(), required=False),
    NestedField(2, "bike_id", StringType(), required=False),
    NestedField(3, "trips", IntegerType(), required=False),
    NestedField(4, "dist_km", DoubleType(), required=False),
    NestedField(5, "instant_ret", IntegerType(), required=False),
    NestedField(6, "fail_150d", IntegerType(), required=False),
    NestedField(7, "days_since_fail", IntegerType(), required=False),
    NestedField(8, "days_since_last_rent", IntegerType(), required=False),
    NestedField(9, "trend_ratio", DoubleType(), required=False),
)
GOLD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)


def _ensure_bike_features_daily_table(catalog):
    catalog.create_namespace_if_not_exists("gold")
    try:
        return catalog.load_table(GOLD_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", GOLD_TABLE)
        return catalog.create_table(GOLD_TABLE, schema=GOLD_SCHEMA, partition_spec=GOLD_PARTITION_SPEC)


def _validate_bike_features_daily(table: pa.Table) -> None:
    (
        QualityCheck("bike_features_daily_check")
        .is_complete("bike_id")
        .is_complete("trips")
        .is_complete("trend_ratio")
        .has_uniqueness("bike_id", threshold=0.99)
        .run(table)
        .raise_if_failed(QualityCheckError)
    )


def _suspended_bike_days(catalog, target_date: date, window_days: int) -> pa.Table:
    """[target_date - window_days, target_date) 구간에서 action == SUSPEND인 (bike_id, snapshot_date).

    fact_bike_decision은 이 DAG 뒤 단계(build_fact_bike_decision)가 만들기 때문에,
    최초 실행일엔 테이블 자체가 아직 없다 - 그 경우 자연히 제외 대상 0건으로 처리한다
    (build_fact_bike_risk.py의 _currently_collected_bike_ids와 동일한 콜드스타트 패턴).
    """
    try:
        table = catalog.load_table(FACT_BIKE_DECISION_TABLE)
    except NoSuchTableError:
        return pa.table({
            "bike_id": pa.array([], type=pa.string()),
            "snapshot_date": pa.array([], type=pa.date32()),
        })

    start_str = (target_date - timedelta(days=window_days)).strftime("%Y-%m-%d")
    end_str = target_date.strftime("%Y-%m-%d")
    row_filter = And(
        And(
            GreaterThanOrEqual("snapshot_date", start_str),
            LessThan("snapshot_date", end_str),
        ),
        EqualTo("action", SUSPEND),
    )
    return table.scan(row_filter=row_filter, selected_fields=("bike_id", "snapshot_date")).to_arrow()


_EXCLUDE_SUSPENDED_SQL = """
    SELECT r.*
    FROM rent r
    LEFT JOIN suspended s
        ON r.bike_id = s.bike_id AND CAST(r.rent_at AS DATE) = s.snapshot_date
    WHERE s.bike_id IS NULL
"""


def _exclude_suspended_rental_days(rent_table: pa.Table, suspended_table: pa.Table) -> pa.Table:
    """SUSPEND 결정이 난 바로 그 날짜에 대여된 레코드(모순 데이터)를 제거한다.

    rent_table은 다른 날짜 이력을 그대로 유지한다 - 이 자전거 자체를 오늘 추론 대상에서
    빼는 게 아니라, 모순이 발생한 그 하루치 대여 레코드만 걸러내는 것이다.
    """
    conn = duckdb.connect(":memory:")
    conn.register("rent", rent_table)
    conn.register("suspended", suspended_table)
    return query_arrow(conn, _EXCLUDE_SUSPENDED_SQL)


_CAST_OUTPUT_SQL = """
    SELECT
        snapshot_date,
        bike_id,
        CAST(trips AS INTEGER) AS trips,
        dist_km,
        CAST(instant_ret AS INTEGER) AS instant_ret,
        CAST(fail_150d AS INTEGER) AS fail_150d,
        CAST(days_since_fail AS INTEGER) AS days_since_fail,
        CAST(days_since_last_rent AS INTEGER) AS days_since_last_rent,
        trend_ratio
    FROM samples
"""


def _build_features(catalog, con, cfg, target_date: date) -> pa.Table:
    engine = SqlEngine.for_duckdb(con)
    window_days = int(cfg.get_path("run.window_days", 14))

    rent = apply_trip_filters(engine, read_rental(engine, cfg), cfg)
    suspended = _suspended_bike_days(catalog, target_date, window_days)
    filtered_rent = _exclude_suspended_rental_days(rent, suspended)

    excluded_count = len(rent) - len(filtered_rent)
    logger.info(
        "%s: SUSPEND 당일 대여 모순 레코드 %d건 제외 (SUSPEND bike-day %d건 대상)",
        target_date, excluded_count, len(suspended),
    )

    df = build_samples(engine, cfg, [target_date], anchor_type="serve", rent=filtered_rent, with_labels=False)
    # DuckDB의 COUNT/SUM류 집계는 BIGINT(64비트)를 내는데, gold.bike_features_daily
    # 스키마(기존 Spark DDL과 동일하게 유지)는 INT(32비트)라 캐스트 없인 pyiceberg
    # 쓰기가 스키마 불일치로 거부된다 - 실측(parity 검증)으로 확인함.
    con.register("samples", df)
    return query_arrow(con, _CAST_OUTPUT_SQL)


def _process_date(catalog, gold_table, con, cfg, target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")

    feat_table = _build_features(catalog, con, cfg, target_date)
    row_count = len(feat_table)
    if row_count == 0:
        logger.info("%s: 최근 %d일 내 대여 이력 없음", date_str, int(cfg.get_path("run.window_days", 14)))
        return 0

    overwrite_partition(gold_table, feat_table, PARTITION_COLUMN, date_str)

    written = catalog.load_table(GOLD_TABLE).scan(row_filter=EqualTo("snapshot_date", date_str)).to_arrow()
    _validate_bike_features_daily(written)  # 실패 시 QualityCheckError -> 배치 중단

    logger.info("%s: 자전거 %d대 feature 산출", date_str, row_count)
    return row_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    cfg = load_config()
    catalog = build_iceberg_catalog()
    gold_table = _ensure_bike_features_daily_table(catalog)

    snapshot_date_str = os.getenv("SNAPSHOT_DATE")
    target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

    con = duckdb.connect(":memory:")
    try:
        _process_date(catalog, gold_table, con, cfg, target_date)
    except QualityCheckError as e:
        logger.error("%s 처리 실패, 배치 중단: %s", target_date, e)
        sys.exit(1)


if __name__ == "__main__":
    run()