"""Silver 따맨 행동 이력 변환 (DuckDB + PyArrow + PyIceberg).

Bronze와 동일한 ``occurred_date_partition`` 문자열 identity 파티션을 사용한다.
기존 ``days(occurred_at)`` 테이블은 임시 테이블에 전체 이력을 재구축한 뒤 백업을
남기고 교체한다. 3일 lookback, watermark, quarantine 및 DQ 기록은 유지한다.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField, StringType, TimestamptzType

import config
from common.cutoff_utils import parse_collection_cutoff
from common.duckdb_io import connect, query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import append, overwrite_partition
from common.s3_utils import ensure_bucket
from common.sql_assert import QualityCheck, QualityCheckResult
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import SILVER_BIKEMAN_ACTION
from schemas.bikeman_action_schema import ALLOWED_EVENT_TYPES, SERVICE_START_DATE, classify_rows, dedup_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

WATERMARK_KEY = SILVER_BIKEMAN_ACTION
LOOKBACK_DAYS = 3
BRONZE_TABLE = "bronze.bikeman_event"
SILVER_TABLE = "silver.bikeman_action"
QUARANTINE_TABLE = "silver.bikeman_action_quarantine"
DQ_RESULT_TABLE = "silver.dq_check_result"
REBUILD_TABLE = "silver.bikeman_action_identity_rebuild"
BACKUP_TABLE = "silver.bikeman_action_hidden_partition_backup"
PARTITION_COLUMN = "occurred_date_partition"
BRONZE_FIELDS = ("bike_id", "event_type", "occurred_at", "station_id", "received_at")
SILVER_COLUMNS = [
    "bike_id", "event_type", "occurred_at", "station_id", "ingested_at", PARTITION_COLUMN,
]

SILVER_SCHEMA = Schema(
    NestedField(1, "bike_id", StringType(), required=False),
    NestedField(2, "event_type", StringType(), required=False),
    NestedField(3, "occurred_at", TimestamptzType(), required=False),
    NestedField(4, "station_id", StringType(), required=False),
    NestedField(5, "ingested_at", TimestamptzType(), required=False),
    NestedField(6, PARTITION_COLUMN, StringType(), required=False),
)
SILVER_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=6, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)
SILVER_PROPERTIES = {"write.distribution-mode": "hash"}

QUARANTINE_SCHEMA = Schema(
    NestedField(1, "bike_id", StringType(), required=False),
    NestedField(2, "event_type", StringType(), required=False),
    NestedField(3, "occurred_at", TimestamptzType(), required=False),
    NestedField(4, "received_at", TimestamptzType(), required=False),
    NestedField(5, "quarantine_reason", StringType(), required=False),
    NestedField(6, "quarantined_at", TimestamptzType(), required=False),
)
DQ_RESULT_SCHEMA = Schema(
    NestedField(1, "dataset", StringType(), required=False),
    NestedField(2, "occurred_date", StringType(), required=False),
    NestedField(3, "check_name", StringType(), required=False),
    NestedField(4, "check_level", StringType(), required=False),
    NestedField(5, "check_status", StringType(), required=False),
    NestedField(6, "constraint_desc", StringType(), required=False),
    NestedField(7, "constraint_status", StringType(), required=False),
    NestedField(8, "constraint_message", StringType(), required=False),
    NestedField(9, "run_at", TimestamptzType(), required=False),
)

SILVER_ARROW_SCHEMA = pa.schema([
    pa.field("bike_id", pa.string()),
    pa.field("event_type", pa.string()),
    pa.field("occurred_at", pa.timestamp("us", tz="UTC")),
    pa.field("station_id", pa.string()),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    pa.field(PARTITION_COLUMN, pa.string()),
])
QUARANTINE_ARROW_SCHEMA = pa.schema([
    pa.field("bike_id", pa.string()),
    pa.field("event_type", pa.string()),
    pa.field("occurred_at", pa.timestamp("us", tz="UTC")),
    pa.field("received_at", pa.timestamp("us", tz="UTC")),
    pa.field("quarantine_reason", pa.string()),
    pa.field("quarantined_at", pa.timestamp("us", tz="UTC")),
])
DQ_ARROW_SCHEMA = pa.schema([
    pa.field("dataset", pa.string()),
    pa.field("occurred_date", pa.string()),
    pa.field("check_name", pa.string()),
    pa.field("check_level", pa.string()),
    pa.field("check_status", pa.string()),
    pa.field("constraint_desc", pa.string()),
    pa.field("constraint_status", pa.string()),
    pa.field("constraint_message", pa.string()),
    pa.field("run_at", pa.timestamp("us", tz="UTC")),
])

_CAST_SQL = """
SELECT
    CAST(bike_id AS VARCHAR) AS bike_id,
    CAST(event_type AS VARCHAR) AS event_type,
    TRY_CAST(occurred_at AS TIMESTAMPTZ) AS occurred_at,
    CAST(station_id AS VARCHAR) AS station_id,
    TRY_CAST(received_at AS TIMESTAMPTZ) AS received_at
FROM bronze_bikeman_action
"""


def _connect() -> duckdb.DuckDBPyConnection:
    con = connect()
    con.execute("SET TimeZone='UTC'")
    return con


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def transform(
    bronze_table: pa.Table,
    target_date: date,
    run_at: datetime | None = None,
) -> tuple[pa.Table, pa.Table]:
    """Bronze 한 날짜를 Silver와 quarantine Arrow 테이블로 변환한다."""
    now = _as_utc(run_at or datetime.now(timezone.utc))
    con = _connect()
    con.register("bronze_bikeman_action", bronze_table)
    typed_rows = query_arrow(con, _CAST_SQL).to_pylist()
    for row in typed_rows:
        row["occurred_at"] = _as_utc(row.get("occurred_at"))
        row["received_at"] = _as_utc(row.get("received_at"))

    valid_rows, quarantine_rows = classify_rows(typed_rows, now)
    valid_rows = dedup_rows(valid_rows)
    date_str = target_date.isoformat()
    silver_rows = [{
        "bike_id": row.get("bike_id"),
        "event_type": row.get("event_type"),
        "occurred_at": row.get("occurred_at"),
        "station_id": row.get("station_id"),
        "ingested_at": now,
        PARTITION_COLUMN: date_str,
    } for row in valid_rows]
    quarantined_rows = [{
        "bike_id": row.get("bike_id"),
        "event_type": row.get("event_type"),
        "occurred_at": row.get("occurred_at"),
        "received_at": row.get("received_at"),
        "quarantine_reason": row.get("quarantine_reason"),
        "quarantined_at": now,
    } for row in quarantine_rows]
    return (
        pa.Table.from_pylist(silver_rows, schema=SILVER_ARROW_SCHEMA),
        pa.Table.from_pylist(quarantined_rows, schema=QUARANTINE_ARROW_SCHEMA),
    )


def validate(silver_table: pa.Table) -> QualityCheckResult:
    """기존 PyDeequ의 완전성 3개와 event_type enum 제약을 그대로 수행한다."""
    return (
        QualityCheck("bikeman_action_silver_checks")
        .is_complete("bike_id")
        .is_complete("event_type")
        .is_complete("occurred_at")
        .is_contained_in("event_type", sorted(ALLOWED_EVENT_TYPES))
        .run(silver_table)
    )


def _ensure_auxiliary_tables(catalog) -> None:
    catalog.create_namespace_if_not_exists("silver")
    try:
        catalog.load_table(QUARANTINE_TABLE)
    except NoSuchTableError:
        catalog.create_table(QUARANTINE_TABLE, schema=QUARANTINE_SCHEMA)
    try:
        catalog.load_table(DQ_RESULT_TABLE)
    except NoSuchTableError:
        catalog.create_table(DQ_RESULT_TABLE, schema=DQ_RESULT_SCHEMA)


def _create_silver_table(catalog, identifier: str):
    return catalog.create_table(
        identifier,
        schema=SILVER_SCHEMA,
        partition_spec=SILVER_PARTITION_SPEC,
        properties=SILVER_PROPERTIES,
    )


def _uses_identity_partition(table) -> bool:
    fields = table.spec().fields
    return (
        len(fields) == 1
        and fields[0].name == PARTITION_COLUMN
        and fields[0].transform.__class__.__name__ == "IdentityTransform"
        and table.schema().find_field(fields[0].source_id).name == PARTITION_COLUMN
    )


def _read_bronze_day(catalog, target_date: date) -> pa.Table:
    return catalog.load_table(BRONZE_TABLE).scan(
        row_filter=EqualTo(PARTITION_COLUMN, target_date.isoformat()),
        selected_fields=BRONZE_FIELDS,
    ).to_arrow()


def _dq_arrow(result: QualityCheckResult, date_str: str, run_at: datetime) -> pa.Table:
    rows = [{
        "dataset": "bikeman_action",
        "occurred_date": date_str,
        "check_name": result.check_name,
        "check_level": "Error",
        "check_status": result.status,
        "constraint_desc": constraint.description,
        "constraint_status": constraint.status,
        "constraint_message": constraint.message,
        "run_at": run_at,
    } for constraint in result.results]
    return pa.Table.from_pylist(rows, schema=DQ_ARROW_SCHEMA)


def _write_dq_result(catalog, result: QualityCheckResult, date_str: str, run_at: datetime) -> None:
    try:
        append(DQ_RESULT_TABLE, _dq_arrow(result, date_str, run_at), catalog=catalog)
    except Exception:
        logger.exception(
            "dq_check_result 적재 실패 (bikeman_action, %s) - 검증 결과 자체는 %s",
            date_str,
            "PASS" if result.is_success else "FAIL",
        )


def _process_one_day(
    catalog,
    silver_table,
    target_date: date,
    *,
    write_observability: bool = True,
) -> tuple[int, bool]:
    date_str = target_date.isoformat()
    bronze_arrow = _read_bronze_day(catalog, target_date)
    if len(bronze_arrow) == 0:
        logger.info("%s: Bronze에 신규 데이터 없음", date_str)
        return 0, True

    run_at = datetime.now(timezone.utc)
    silver_arrow, quarantine_arrow = transform(bronze_arrow, target_date, run_at)
    if write_observability and len(quarantine_arrow):
        append(QUARANTINE_TABLE, quarantine_arrow, catalog=catalog)
        logger.warning("Silver quarantine 적재: %d건", len(quarantine_arrow))

    if len(silver_arrow) == 0:
        logger.warning("%s: 전체가 quarantine 처리됨 (%d건)", date_str, len(quarantine_arrow))
        return 0, True

    result = validate(silver_arrow)
    if write_observability:
        _write_dq_result(catalog, result, date_str, run_at)
    if not result.is_success:
        logger.error("%s: 품질 검증 실패로 Silver 적재 중단", date_str)
        return 0, False

    overwrite_partition(
        silver_table, silver_arrow, PARTITION_COLUMN, date_str, catalog=catalog,
    )
    logger.info(
        "%s: Silver %d행 적재 완료 (quarantine %d건)",
        date_str, len(silver_arrow), len(quarantine_arrow),
    )
    return len(silver_arrow), True


def _table_exists(catalog, identifier: str) -> bool:
    try:
        catalog.load_table(identifier)
        return True
    except NoSuchTableError:
        return False


def _rebuild_identity_table(catalog, end_date: date):
    """임시 테이블 전체 적재가 끝난 뒤에만 기존 hidden-partition 테이블을 교체한다."""
    if _table_exists(catalog, BACKUP_TABLE):
        raise RuntimeError(
            f"이전 백업 테이블 {BACKUP_TABLE}이 남아 있어 자동 재구축을 중단한다. "
            "백업 확인 후 이름 변경 또는 삭제가 필요하다."
        )
    if _table_exists(catalog, REBUILD_TABLE):
        logger.warning("중단된 이전 재구축 임시 테이블 %s를 다시 만든다", REBUILD_TABLE)
        catalog.drop_table(REBUILD_TABLE)

    rebuild_table = _create_silver_table(catalog, REBUILD_TABLE)
    current = SERVICE_START_DATE
    try:
        while current <= end_date:
            _, passed = _process_one_day(
                catalog, rebuild_table, current, write_observability=False,
            )
            if not passed:
                raise RuntimeError(f"{current} 품질 검증 실패로 identity 파티션 재구축 중단")
            current += timedelta(days=1)
    except Exception:
        logger.exception("%s 전체 재구축 실패 - 기존 %s는 변경하지 않음", REBUILD_TABLE, SILVER_TABLE)
        raise

    catalog.rename_table(SILVER_TABLE, BACKUP_TABLE)
    try:
        catalog.rename_table(REBUILD_TABLE, SILVER_TABLE)
    except Exception:
        logger.exception("임시 테이블 교체 실패 - 기존 테이블 이름 복구 시도")
        catalog.rename_table(BACKUP_TABLE, SILVER_TABLE)
        raise

    logger.warning(
        "%s를 identity 파티션으로 재구축 완료. 기존 테이블은 %s에 보존됨",
        SILVER_TABLE, BACKUP_TABLE,
    )
    return catalog.load_table(SILVER_TABLE)


def _ensure_silver_table(catalog, rebuild_end_date: date):
    catalog.create_namespace_if_not_exists("silver")
    try:
        table = catalog.load_table(SILVER_TABLE)
    except NoSuchTableError:
        backup_exists = _table_exists(catalog, BACKUP_TABLE)
        rebuild_exists = _table_exists(catalog, REBUILD_TABLE)
        if backup_exists and rebuild_exists:
            # 기존 -> backup rename 직후 프로세스가 강제 종료된 경우다. 임시 테이블은
            # 이미 전체 적재가 끝난 상태이므로 두 번째 rename부터 이어서 완료한다.
            logger.warning("중단된 identity 파티션 교체를 %s -> %s부터 재개", REBUILD_TABLE, SILVER_TABLE)
            catalog.rename_table(REBUILD_TABLE, SILVER_TABLE)
            return catalog.load_table(SILVER_TABLE)
        if backup_exists:
            # 첫 rename 뒤 임시 테이블이 사라진 비정상 상태에서는 빈 본 테이블을 만들지
            # 않고 기존 hidden-partition 테이블부터 원래 이름으로 복구한다.
            logger.warning("중단된 identity 파티션 교체에서 기존 테이블 이름을 복구")
            catalog.rename_table(BACKUP_TABLE, SILVER_TABLE)
            table = catalog.load_table(SILVER_TABLE)
        else:
            return _create_silver_table(catalog, SILVER_TABLE)
    if _uses_identity_partition(table):
        return table
    return _rebuild_identity_table(catalog, rebuild_end_date)


def _processing_window(last_processed: date, end_date: date) -> tuple[date, date] | None:
    """(실제 재처리 시작일, 신규 워터마크 시작일)을 반환한다.

    Bronze Asset은 같은 날짜의 늦게 도착한 이벤트 때문에 다시 발행될 수 있다. Silver
    워터마크가 이미 end_date여도 최근 LOOKBACK_DAYS는 다시 읽어야 그 정정분이 전파된다.
    워터마크가 end_date보다 미래인 비정상 상태에서만 처리하지 않는다.
    """
    if last_processed > end_date:
        return None
    watermark_start = last_processed + timedelta(days=1)
    reprocess_start = max(
        watermark_start - timedelta(days=LOOKBACK_DAYS),
        SERVICE_START_DATE,
    )
    return reprocess_start, watermark_start


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)
    catalog = build_iceberg_catalog()
    _ensure_auxiliary_tables(catalog)

    cutoff = parse_collection_cutoff(os.getenv("COLLECTION_CUTOFF_AT"))
    as_of_date = cutoff.date()

    last_processed = read_watermark(watermark_key=WATERMARK_KEY)
    start_date = last_processed + timedelta(days=1)
    end_date = as_of_date - timedelta(days=1)
    max_days = os.getenv("MAX_DAYS_PER_RUN")
    if max_days:
        end_date = min(end_date, start_date + timedelta(days=int(max_days) - 1))

    # 신규 날짜가 없어도 최초 #143 배포에서는 기존 hidden partition을 전환한다.
    rebuild_end_date = max(last_processed, SERVICE_START_DATE - timedelta(days=1))
    silver_table = _ensure_silver_table(catalog, rebuild_end_date)
    processing_window = _processing_window(last_processed, end_date)
    if processing_window is None:
        logger.info("처리할 날짜 없음 (워터마크=%s, 처리 상한=%s)", last_processed, end_date)
        return

    reprocess_start, start_date = processing_window
    current = reprocess_start
    while current <= end_date:
        try:
            _, passed = _process_one_day(catalog, silver_table, current)
            if not passed:
                logger.error("%s 처리 실패(DQ), 배치 중단: 워터마크 유지", current)
                sys.exit(1)
            if current >= start_date:
                write_watermark(current, watermark_key=WATERMARK_KEY)
        except Exception as exc:
            logger.error("%s 처리 중 예외, 배치 중단: %s", current, exc)
            sys.exit(1)
        current += timedelta(days=1)


if __name__ == "__main__":
    run()
