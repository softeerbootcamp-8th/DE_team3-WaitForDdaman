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
from pyiceberg.types import StringType, TimestamptzType

import config
from common.api_client import (
    SeoulApiError,
    SeoulApiTransientError,
    fetch_failure_reports_by_date,
    strip_pagination_meta,
)
from common.cutoff_utils import parse_collection_cutoff
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket, put_json
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import BRONZE_FAILURE_REPORT
from schemas.failure_report_schema import (
    COLUMN_ALIAS_MAP,
    SchemaValidationError,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

WATERMARK_KEY = BRONZE_FAILURE_REPORT
COMPLETION_PREFIX = "_meta/completion/bronze_failure_report"

# ⚠️ reg_date_partition은 "API 요청일"이다 (실제 신고일이 아니다, #304).
# tbCycleFailureReport는 요청일 기준 최대 31일치를 함께 돌려주므로 한 파티션 안에
# 신고일이 여러 개 섞인다. requested_date는 그 의미를 이름으로 못박은 컬럼이고
# (파티션 값과 항상 같다), observed_at은 그 응답을 관측한 논리 기준시각이다.
# 실제 신고일 기준 정제는 Silver 책임 - staging/jobs/silver_failure_report.py 참고.
ARROW_SCHEMA = pa.schema([
    pa.field("bike_no", pa.string()),
    pa.field("reg_dttm", pa.string()),
    pa.field("failure_type", pa.string()),
    pa.field("reg_date_partition", pa.string()),
    pa.field("requested_date", pa.string()),
    pa.field("observed_at", pa.timestamp("us", tz="UTC")),
    pa.field("source_file", pa.string()),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
])

# 기존 테이블에 없을 수 있는 수집 메타데이터 컬럼 - _ensure_bronze_columns가 채운다.
REQUEST_METADATA_COLUMNS = ("requested_date", "observed_at")


def _table_name() -> str:
    return "bronze.failure_report"


def _build_arrow_table(
    rows: List[Dict[str, Any]], date_str: str, observed_at: datetime | None = None
) -> pa.Table:
    """API 응답을 Bronze 모양으로 옮긴다. 요청일로 행을 잘라내지 않는다(#304).

    observed_at을 안 주면 적재 시각으로 채운다 - 로컬 단독 실행/기존 호출부 호환용.
    """
    ingested_at = datetime.now(timezone.utc)
    observed_at_val = (observed_at or ingested_at).astimezone(timezone.utc)
    source_file_val = f"api:{date_str}"

    cols: Dict[str, list] = {
        "bike_no": [],
        "reg_dttm": [],
        "failure_type": [],
        "reg_date_partition": [],
        "requested_date": [],
        "observed_at": [],
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
        cols["requested_date"].append(date_str)
        cols["observed_at"].append(observed_at_val)
        cols["source_file"].append(source_file_val)
        cols["ingested_at"].append(ingested_at)

    return pa.table(cols, schema=ARROW_SCHEMA)


def _ensure_bronze_columns(catalog=None) -> None:
    """수집 메타데이터 컬럼(requested_date/observed_at)이 없으면 스키마를 진화시킨다.

    이 잡은 Spark 없이 PyIceberg로 쓰기 때문에, 기존 테이블에 없는 컬럼이 Arrow에
    들어 있으면 overwrite가 실패한다. 컬럼 추가는 하위호환 변경(기존 행은 NULL)이라
    초기 적재분과 이미 쌓인 파티션을 건드리지 않는다.
    """
    cat = catalog or build_iceberg_catalog()
    table = cat.load_table(_table_name())
    existing = set(table.schema().column_names)
    missing = [name for name in REQUEST_METADATA_COLUMNS if name not in existing]
    if not missing:
        return

    logger.info("%s 스키마 진화: %s 컬럼 추가", _table_name(), missing)
    with table.update_schema() as update:
        for name in missing:
            field = ARROW_SCHEMA.field(name)
            iceberg_type = TimestamptzType() if pa.types.is_timestamp(field.type) else StringType()
            update.add_column(name, iceberg_type)


def _process_one_day(target_date: date, cutoff: datetime | None = None) -> int:
    """요청일 하나의 API 응답을 Raw/Bronze에 그대로 적재한다.

    ⚠️ 응답은 요청일 하루치가 아니라 요청일 기준 최대 31일치다(#304). 여기서 요청일로
    잘라내면 원본이 유실되므로 자르지 않는다 - 파티션만 요청일이고, 실제 신고일 기준
    정제와 중복 제거는 Silver가 한다.
    """
    raw_rows = list(fetch_failure_reports_by_date(target_date))
    date_str = target_date.strftime("%Y-%m-%d")
    # 표기는 호출자가 준 시간대 그대로 남긴다(Raw 매니페스트와 같은 값이 되도록).
    # Arrow 컬럼으로 옮길 때만 _build_arrow_table이 UTC로 정규화한다.
    observed_at = cutoff or datetime.now(timezone.utc)

    ensure_bucket(config.SETTINGS.raw_bucket)
    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/failure_report/api/reg_dt={date_str}/payload.json",
        {
            "dataset": "failure_report",
            "target_date": date_str,
            "requested_date": date_str,
            "observed_at": observed_at.isoformat(),
            "row_count": len(raw_rows),
            "reg_dt": date_str,  # 기존 키 호환
            "rows": raw_rows,
        },
    )

    if not raw_rows:
        logger.info("%s: 신규 데이터 없음", date_str)
        return 0

    rows = [strip_pagination_meta(r) for r in raw_rows]
    actual_columns = list({k for r in rows for k in r.keys()})
    validate_and_report(actual_columns)

    arrow_table = _build_arrow_table(rows, date_str, observed_at=observed_at)
    row_count = len(arrow_table)

    _ensure_bronze_columns()
    overwrite_partition(_table_name(), arrow_table, "reg_date_partition", date_str)
    logger.info("%s: 요청일 응답 %d행 PyIceberg 적재 완료 (신고일 여럿 포함 가능)", date_str, row_count)
    return row_count


def _parse_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if not normalized:
        return default
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{name} must be 'true' or 'false': {value!r}")


def _write_completion_marker(
    bucket: str,
    target_date: date,
    status: str,
    row_count: int,
    started_at: str,
    error: str | None = None,
) -> dict:
    target_value = target_date.isoformat()
    marker = {
        "dataset": "failure_report",
        "target_date": target_value,
        "status": status,
        "row_count": row_count,
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "dag_run_id": os.getenv("DAG_RUN_ID", "unknown"),
        "source": "seoul_open_api",
        "error": error,
    }
    put_json(bucket, f"{COMPLETION_PREFIX}/target_date={target_value}/completion.json", marker)
    return marker


def run() -> None:
    api_key = getattr(config.SETTINGS, "seoul_api_key1", None) or getattr(config.SETTINGS, "seoul_api_key", "")
    if api_key == "sample":
        logger.warning(
            "SEOUL_API_KEY가 'sample'(데모 키)입니다. data.seoul.go.kr에서 발급받은 "
            "실제 인증키로 .env를 교체하지 않으면 API 호출이 계속 실패합니다."
        )

    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    cutoff = parse_collection_cutoff(os.getenv("COLLECTION_CUTOFF_AT"))
    as_of_date = cutoff.date()

    t0_enabled = _parse_bool_env("FAILURE_REPORT_T0_ENABLED", default=False)
    last_processed = read_watermark(watermark_key=WATERMARK_KEY)
    start_date = last_processed + timedelta(days=1)
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

    if start_date <= end_date:
        current = start_date
        while current <= end_date:
            started_at = datetime.now(timezone.utc).isoformat()
            try:
                row_count = _process_one_day(current, cutoff=cutoff)
                status = "COMPLETE_EMPTY" if row_count == 0 else "COMPLETE"
                _write_completion_marker(bucket, current, status, row_count, started_at)
                write_watermark(current, watermark_key=WATERMARK_KEY)
            except (SchemaValidationError, SeoulApiError, SeoulApiTransientError) as e:
                logger.error("%s 처리 실패, 배치 중단: %s", current, e)
                _write_completion_marker(bucket, current, "FAILED", 0, started_at, error=str(e))
                sys.exit(1)
            current += timedelta(days=1)
    else:
        logger.info("처리할 신규 확정 날짜 없음 (워터마크=%s)", last_processed)

    if t0_enabled:
        logger.info("FAILURE_REPORT_T0_ENABLED=true 적용 - 기준일 당일(%s) 파티션 적재 시작 (워터마크 미갱신)", as_of_date)
        try:
            _process_one_day(as_of_date, cutoff=cutoff)
            logger.info("기준일 당일(%s) 고장신고 파티션 적재 완료", as_of_date)
        except (SchemaValidationError, SeoulApiError, SeoulApiTransientError) as e:
            logger.error("기준일 당일(%s) 고장신고 T0 적재 실패: %s", as_of_date, e)
            sys.exit(1)


if __name__ == "__main__":
    run()
