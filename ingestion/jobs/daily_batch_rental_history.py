"""
Bronze 일 배치 잡 - 서울시 공공자전거 대여이력 (OA-15182)

전략: 증분 기준 = RENT_DT(대여일자)
- 워터마크(마지막으로 성공 처리된 날짜) 다음날부터 "어제"까지 날짜별로 순차 처리
  (오늘 데이터는 원천에서 아직 확정 전이므로 제외)
- 하루 처리가 "완전히" 성공했을 때만 그 날짜로 워터마크를 갱신한다
  -> 중간에 실패하면 워터마크가 마지막 성공일에 머물러 있어, 재실행 시 누락 없이 이어서 처리됨
- 재실행 시 동일 날짜 파티션을 덮어써서 멱등성 보장 (같은 날 두 번 돌려도 중복 적재 안 됨)
- tbCycleRentData 24시간 API 호출을 ThreadPoolExecutor로 병렬화하여 수행 시간 최적화 (Issue #142)
- Spark를 완전히 제거하고 PyArrow + PyIceberg(SqlCatalog)로 경량화/고속화 (Issue #142)

사용법:
    python -m jobs.daily_batch_rental_history
    MAX_DAYS_PER_RUN=1 python -m jobs.daily_batch_rental_history   # 로컬 테스트: 하루치만 처리
"""
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

import pyarrow as pa

import config
from common.api_client import (
    SeoulApiError,
    SeoulApiTransientError,
    fetch_rent_history_by_date_parallel,
    strip_pagination_meta,
)
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket, put_json
from common.watermark import read_watermark, write_watermark
from schema.rental_history_schema import (
    COLUMN_ALIAS_MAP,
    REQUIRED_STANDARD_COLUMNS,
    SchemaValidationError,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def bronze_table_name() -> str:
    return "bronze.rental_history"


ARROW_SCHEMA = pa.schema([
    pa.field("bike_id", pa.string()),
    pa.field("rent_dt", pa.string()),
    pa.field("rent_station_no", pa.string()),
    pa.field("rent_station_name", pa.string()),
    pa.field("rent_hold", pa.string()),
    pa.field("return_dt", pa.string()),
    pa.field("return_station_no", pa.string()),
    pa.field("return_station_name", pa.string()),
    pa.field("return_hold", pa.string()),
    pa.field("use_min", pa.string()),
    pa.field("use_distance_m", pa.string()),
    pa.field("user_class_cd", pa.string()),
    pa.field("sex_cd", pa.string()),
    pa.field("birth_year", pa.string()),
    pa.field("rent_station_id", pa.string()),
    pa.field("return_station_id", pa.string()),
    pa.field("bike_se_cd", pa.string()),
    pa.field("rent_date_partition", pa.string()),
    pa.field("source_file", pa.string()),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
])


def _build_arrow_table(rows: List[Dict[str, Any]], date_str: str, source_file: str) -> pa.Table:
    ingested_at = datetime.now(timezone.utc)

    # 모든 표준 컬럼 리스트 초기화
    cols: Dict[str, list] = {
        "bike_id": [],
        "rent_dt": [],
        "rent_station_no": [],
        "rent_station_name": [],
        "rent_hold": [],
        "return_dt": [],
        "return_station_no": [],
        "return_station_name": [],
        "return_hold": [],
        "use_min": [],
        "use_distance_m": [],
        "user_class_cd": [],
        "sex_cd": [],
        "birth_year": [],
        "rent_station_id": [],
        "return_station_id": [],
        "bike_se_cd": [],
        "rent_date_partition": [],
        "source_file": [],
        "ingested_at": [],
    }

    # 역방향 매핑 준비: 표준 컬럼 -> 소스 컬럼 후보들
    standard_to_sources: Dict[str, List[str]] = {}
    for src, dst in COLUMN_ALIAS_MAP.items():
        standard_to_sources.setdefault(dst, []).append(src)

    for r in rows:
        for dst, sources in standard_to_sources.items():
            val = None
            for src in sources:
                if src in r and r[src] is not None:
                    val = str(r[src])
                    break
            cols[dst].append(val)

        cols["rent_date_partition"].append(date_str)
        cols["source_file"].append(source_file)
        cols["ingested_at"].append(ingested_at)

    return pa.table(cols, schema=ARROW_SCHEMA)


def _process_one_day(target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")
    raw_rows = fetch_rent_history_by_date_parallel(target_date)

    # API 원본 응답을 raw zone에 그대로 보존 (감사/재처리 대비)
    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/rental_history/api/rent_dt={date_str}/payload.json",
        {"rent_dt": date_str, "row_count": len(raw_rows), "rows": raw_rows},
    )

    if not raw_rows:
        logger.info("%s: 신규 데이터 없음", date_str)
        return 0

    rows = [strip_pagination_meta(r) for r in raw_rows]
    actual_columns = list({k for r in rows for k in r.keys()})
    validate_and_report(actual_columns)

    arrow_table = _build_arrow_table(rows, date_str, f"api:{date_str}")
    row_count = len(arrow_table)

    overwrite_partition(bronze_table_name(), arrow_table, "rent_date_partition", date_str)
    logger.info("%s: %d행 PyIceberg 적재 완료", date_str, row_count)
    return row_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    last_processed = read_watermark()
    start_date = last_processed + timedelta(days=1)
    end_date = date.today() - timedelta(days=1)

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

    current = start_date
    while current <= end_date:
        try:
            _process_one_day(current)
            write_watermark(current)  # 성공한 날짜만 워터마크 갱신
        except (SchemaValidationError, SeoulApiError, SeoulApiTransientError) as e:
            logger.error("%s 처리 실패, 배치 중단: %s", current, e)
            sys.exit(1)
        current += timedelta(days=1)


if __name__ == "__main__":
    run()