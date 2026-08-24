"""
Bronze 일 배치 잡 - 서울시 공공자전거 고장신고 내역 (OA-15644)

전략: 증분 기준 = REGDTTM(등록일시)
- tbCycleFailureReport는 대여이력과 달리 시간 단위 분할이 필요 없다 (날짜 단위로 충분).
- 워터마크 다음날부터 어제까지 날짜별로 순차 처리, 성공한 날짜만 커밋.
- 재실행 시 동일 날짜 파티션을 덮어써서 멱등성 보장.
- Spark를 완전히 제거하고 PyArrow + PyIceberg(SqlCatalog)로 경량화/고속화 (Issue #142).

사용법:
    python -m jobs.daily_batch_failure_report
    MAX_DAYS_PER_RUN=1 python -m jobs.daily_batch_failure_report
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
    fetch_failure_reports_by_date,
    strip_pagination_meta,
)
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket, put_json
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import BRONZE_FAILURE_REPORT
from schema.failure_report_schema import (
    COLUMN_ALIAS_MAP,
    SchemaValidationError,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

WATERMARK_KEY = BRONZE_FAILURE_REPORT


def _table_name() -> str:
    return "bronze.failure_report"


def _build_arrow_table(rows: List[Dict[str, Any]], date_str: str) -> pa.Table:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    source_file_val = f"api:{date_str}"

    cols: Dict[str, list] = {
        "bike_no": [],
        "reg_dttm": [],
        "failure_type": [],
        "reg_date_partition": [],
        "source_file": [],
        "ingested_at": [],
    }

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

        cols["reg_date_partition"].append(date_str)
        cols["source_file"].append(source_file_val)
        cols["ingested_at"].append(now_iso)

    return pa.table(cols)


def _process_one_day(target_date: date) -> int:
    raw_rows = list(fetch_failure_reports_by_date(target_date))
    date_str = target_date.strftime("%Y-%m-%d")

    ensure_bucket(config.SETTINGS.raw_bucket)
    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/failure_report/api/reg_dt={date_str}/payload.json",
        {"reg_dt": date_str, "row_count": len(raw_rows), "rows": raw_rows},
    )

    if not raw_rows:
        logger.info("%s: 신규 데이터 없음", date_str)
        return 0

    rows = [strip_pagination_meta(r) for r in raw_rows]
    actual_columns = list({k for r in rows for k in r.keys()})
    validate_and_report(actual_columns)

    arrow_table = _build_arrow_table(rows, date_str)
    row_count = len(arrow_table)

    overwrite_partition(_table_name(), arrow_table, "reg_date_partition", date_str)
    logger.info("%s: %d행 PyIceberg 적재 완료", date_str, row_count)
    return row_count


def run() -> None:
    if config.SETTINGS.seoul_api_key == "sample":
        logger.warning(
            "SEOUL_API_KEY가 'sample'(데모 키)입니다. data.seoul.go.kr에서 발급받은 "
            "실제 인증키로 .env를 교체하지 않으면 API 호출이 계속 실패합니다."
        )

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    last_processed = read_watermark(watermark_key=WATERMARK_KEY)
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
            write_watermark(current, watermark_key=WATERMARK_KEY)
        except (SchemaValidationError, SeoulApiError, SeoulApiTransientError) as e:
            logger.error("%s 처리 실패, 배치 중단: %s", current, e)
            sys.exit(1)
        current += timedelta(days=1)


if __name__ == "__main__":
    run()
