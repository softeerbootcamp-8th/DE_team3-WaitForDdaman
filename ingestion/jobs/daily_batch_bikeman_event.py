"""
Bronze 일 배치 잡 - 따맨/bikeman 수거/배치 이벤트

전략: 증분 기준 = occurred_at(발생일), 다른 3개 원천의 daily_batch_*.py와 동일한
패턴(날짜 단위 워터마크, 날짜별 순차 처리, overwrite_partition으로 멱등성 보장)을 따른다.
다만 조회 소스가 공공 API가 아니라 우리 자신의 Postgres(bikeman 스키마)라는 점만 다르다.

### 3일 lookback 재처리 (다른 3개 원천과의 유일한 구조적 차이)
bikeman은 "오프라인 작업 후 몰아서 제출"이 정상 케이스다(source_data 문서 참고) - 즉
occurred_at(발생)과 received_at(서버 수신) 사이에 수 시간~며칠 지연이 생길 수 있다.
그래서 "어제"만 처리하면, 이미 확정해서 넘어간 날짜에 늦게 도착한 이벤트를 영구히
놓친다. 이를 막기 위해 매 실행마다 처리 시작점을 LOOKBACK_DAYS만큼 앞당겨서
재계산한다. occurred_at 기준으로 그날 전체를 다시 조회해서 해당 파티션을 통째로
덮어쓰므로(overwrite_partition), 같은 날짜를 여러 번 재처리해도 안전하다(멱등).

Spark를 완전히 제거하고 PyArrow + PyIceberg(SqlCatalog)로 경량화/고속화 (Issue #142).

사용법:
    python -m jobs.daily_batch_bikeman_event
    MAX_DAYS_PER_RUN=1 python -m jobs.daily_batch_bikeman_event
"""
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

import pyarrow as pa

import config
from common.cutoff_utils import parse_collection_cutoff
from common.db_client import BikemanDbError, fetch_events_by_date
from common.iceberg_io import append, overwrite_partition
from common.s3_utils import ensure_bucket, put_json
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import BIKEMAN_EVENT
from schema.bikeman_event_schema import (
    SchemaValidationError,
    validate_and_report,
    validate_event_types,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

WATERMARK_KEY = BIKEMAN_EVENT
LOOKBACK_DAYS = 3
SERVICE_START_DATE = date(2026, 6, 30)


def _json_safe(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _json_safe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{k: _json_safe(v) for k, v in r.items()} for r in rows]


def _to_utc_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _table_name() -> str:
    return "bronze.bikeman_event"


def _quarantine_table_name() -> str:
    return "bronze.bikeman_event_quarantine"


def _build_arrow_table(rows: List[Dict[str, Any]], date_str: str) -> pa.Table:
    ingested_at = datetime.now(timezone.utc)
    source_file_val = f"postgres:bikeman.fact_worker_event:{date_str}"
    normalized = [{
        "event_id": str(r.get("event_id") or "") or None,
        "event_type": str(r.get("event_type") or "") or None,
        "bike_id": str(r.get("bike_id") or "") or None,
        "station_id": str(r.get("station_id") or "") or None,
        "worker_id": str(r.get("worker_id") or "") or None,
        "occurred_at": _to_utc_datetime(r.get("occurred_at")),
        "received_at": _to_utc_datetime(r.get("received_at")),
        "occurred_date_partition": date_str,
        "source_file": source_file_val,
        "ingested_at": ingested_at,
    } for r in rows]
    schema = pa.schema([
        pa.field("event_id", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("bike_id", pa.string()),
        pa.field("station_id", pa.string()),
        pa.field("worker_id", pa.string()),
        pa.field("occurred_at", pa.timestamp("us", tz="UTC")),
        pa.field("received_at", pa.timestamp("us", tz="UTC")),
        pa.field("occurred_date_partition", pa.string()),
        pa.field("source_file", pa.string()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    ])
    return pa.Table.from_pylist(normalized, schema=schema)


def _process_one_day(target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")
    raw_rows = fetch_events_by_date(target_date)

    ensure_bucket(config.SETTINGS.raw_bucket)
    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/bikeman_event/postgres/occurred_date={date_str}/payload.json",
        {"occurred_date": date_str, "row_count": len(raw_rows), "rows": _json_safe_rows(raw_rows)},
    )

    if not raw_rows:
        logger.info("%s: 신규 데이터 없음", date_str)
        return 0

    actual_columns = list(raw_rows[0].keys())
    validate_and_report(actual_columns)

    valid_rows, invalid_rows = validate_event_types(raw_rows)

    # 격리 테이블 적재 (잘못된 event_type)
    if invalid_rows:
        logger.warning("%s: 허용되지 않은 event_type %d건 발견 -> quarantine 격리", date_str, len(invalid_rows))
        quarantine_table = _build_arrow_table(invalid_rows, date_str)
        append(_quarantine_table_name(), quarantine_table)

    if not valid_rows:
        logger.info("%s: 유효한 이벤트 없음 (전량 격리)", date_str)
        return 0

    arrow_table = _build_arrow_table(valid_rows, date_str)
    row_count = len(arrow_table)

    overwrite_partition(_table_name(), arrow_table, "occurred_date_partition", date_str)
    logger.info("%s: %d행 PyIceberg 적재 완료", date_str, row_count)
    return row_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    cutoff = parse_collection_cutoff(os.getenv("COLLECTION_CUTOFF_AT"))
    as_of_date = cutoff.date()

    last_processed = read_watermark(watermark_key=WATERMARK_KEY)
    start_date = max(SERVICE_START_DATE, last_processed - timedelta(days=LOOKBACK_DAYS - 1))
    end_date = as_of_date - timedelta(days=1)

    max_days = os.getenv("MAX_DAYS_PER_RUN")
    if max_days:
        capped_end = start_date + timedelta(days=int(max_days) - 1)
        if capped_end < end_date:
            logger.info(
                "MAX_DAYS_PER_RUN=%s 적용 - 이번 실행은 %s ~ %s까지만 처리 (원래 끝: %s)",
                max_days, start_date, capped_end, end_date,
            )
            end_date = capped_end

    if start_date > end_date:
        logger.info("처리할 신규 날짜 없음 (워터마크=%s)", last_processed)
        return

    logger.info(
        "bikeman_event 3일 lookback 처리 시작: %s ~ %s (워터마크=%s, lookback=%d일)",
        start_date, end_date, last_processed, LOOKBACK_DAYS,
    )

    current = start_date
    while current <= end_date:
        try:
            _process_one_day(current)
        except (SchemaValidationError, BikemanDbError) as e:
            logger.error("%s 처리 실패, 배치 중단: %s", current, e)
            sys.exit(1)
        current += timedelta(days=1)

    write_watermark(end_date, watermark_key=WATERMARK_KEY)
    logger.info("bikeman_event 워터마크 갱신 완료: %s", end_date)


if __name__ == "__main__":
    run()
