"""고장신고 API 관측본을 Raw payload/manifest로 저장하는 순수 수집 잡."""

import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import config
from common.api_client import (
    SeoulApiError,
    SeoulApiTransientError,
    fetch_failure_reports_by_date,
    strip_pagination_meta,
)
from common.s3_utils import ensure_bucket, put_json
from schema.failure_report_schema import SchemaValidationError, validate_and_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
SNAPSHOT_TYPES = {"PRELIMINARY", "FINAL"}


def parse_collection_cutoff(value: str | None) -> datetime:
    if not value:
        return datetime.now(KST).replace(microsecond=0)
    cutoff = datetime.fromisoformat(value)
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("collection_cutoff_at must include timezone")
    return cutoff.astimezone(KST).replace(microsecond=0)


def snapshot_keys(
    target_date: date, observed_at: datetime, snapshot_type: str
) -> tuple[str, str]:
    normalized_type = snapshot_type.strip().upper()
    if normalized_type not in SNAPSHOT_TYPES:
        raise ValueError(f"unsupported snapshot_type: {snapshot_type}")

    observed_key = observed_at.astimezone(KST).strftime("%Y%m%dT%H%M%S%z")
    prefix = (
        f"raw/failure_report/api/target_date={target_date.isoformat()}/"
        f"observed_at={observed_key}/snapshot_type={normalized_type}/"
    )
    return f"{prefix}payload.json", f"{prefix}manifest.json"


def reg_date_span(rows: list[dict]) -> tuple[list[str], int]:
    """응답이 걸친 신고일의 [min, max]와 서로 다른 신고일 수를 구한다.

    이 응답은 요청일 하루치가 아니라 요청일 기준 최대 31일치다(#304). Raw 행 수를
    "요청일 하루치 건수"로 읽고 Silver 적재 건수와 그대로 비교하면 항상 어긋난다 -
    매니페스트에 응답이 실제로 몇 개 신고일을 덮는지 남겨 그 비교를 막는다.
    """
    dates = set()
    for row in rows:
        for key in ("REGDTTM", "regDttm", "reg_dttm", "등록일시"):
            value = row.get(key)
            if value:
                digits = "".join(ch for ch in str(value) if ch.isdigit())
                if len(digits) >= 8:
                    dates.add(f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}")
                break
    if not dates:
        return [], 0
    ordered = sorted(dates)
    return [ordered[0], ordered[-1]], len(ordered)


def build_payload(
    target_date: date, cutoff: datetime, snapshot_type: str, rows: list[dict]
) -> dict:
    """Raw payload 문서. API 응답을 자르지 않고 그대로 싣는다(#304).

    표준 메타데이터(target_date/observed_at/snapshot_type)를 쓰되, 이미 `reg_dt`를
    읽는 기존 소비자를 위해 그 키도 같은 값으로 남긴다.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    return {
        "dataset": "failure_report",
        "target_date": date_str,
        "requested_date": date_str,
        "observed_at": cutoff.isoformat(),
        "snapshot_type": snapshot_type.strip().upper(),
        "row_count": len(rows),
        # 기존 경로/키 호환 - raw/failure_report/api/reg_dt=<D>/payload.json을 읽는 쪽이 있다.
        "reg_dt": date_str,
        "rows": rows,
    }


def collect_one_day(
    target_date: date,
    cutoff: datetime,
    snapshot_type: str,
    raw_bucket: str,
) -> int:
    date_str = target_date.strftime("%Y-%m-%d")
    raw_rows = list(fetch_failure_reports_by_date(target_date))
    rows = [strip_pagination_meta(r) for r in raw_rows]

    schema_valid = True
    if rows:
        actual_columns = list({k for r in rows for k in r.keys()})
        try:
            validate_and_report(actual_columns)
        except SchemaValidationError:
            schema_valid = False

    payload = build_payload(target_date, cutoff, snapshot_type, rows)
    payload_key, manifest_key = snapshot_keys(target_date, cutoff, snapshot_type)
    put_json(raw_bucket, payload_key, payload)

    # 기존 일 배치 경로 호환성 보장 - 표준 경로와 완전히 같은 문서를 쓴다.
    put_json(raw_bucket, f"raw/failure_report/api/reg_dt={date_str}/payload.json", payload)

    span, distinct_count = reg_date_span(rows)
    manifest = {
        "dataset": "failure_report",
        "target_date": date_str,
        "requested_date": date_str,
        "snapshot_type": snapshot_type,
        "observed_at": cutoff.isoformat(),
        "row_count": len(rows),
        # 응답이 요청일 하루치가 아님을 드러내는 값 - 계층 간 행 수 비교를 막는 근거다.
        "reg_date_span": span,
        "distinct_reg_date_count": distinct_count,
        "schema_valid": schema_valid,
        "status": "COMPLETE" if schema_valid else "SCHEMA_INVALID",
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }
    put_json(raw_bucket, manifest_key, manifest)

    logger.info(
        "%s: 고장신고 Raw %d행 수집 완료 (%s, 신고일 %d개 %s)",
        date_str, len(rows), snapshot_type, distinct_count, span,
    )
    return len(rows)


def run() -> None:
    if config.SETTINGS.seoul_api_key == "sample":
        logger.warning("SEOUL_API_KEY가 'sample'입니다. 실제 인증키 교체가 필요합니다.")

    ensure_bucket(config.SETTINGS.raw_bucket)

    cutoff_str = os.getenv("COLLECTION_CUTOFF_AT")
    cutoff = parse_collection_cutoff(cutoff_str)
    snapshot_type = (os.getenv("SNAPSHOT_TYPE") or "PRELIMINARY").strip().upper()

    if snapshot_type not in SNAPSHOT_TYPES:
        logger.error("유효하지 않은 SNAPSHOT_TYPE: %s", snapshot_type)
        sys.exit(1)

    target_dates = [cutoff.date() - timedelta(days=1), cutoff.date()]
    logger.info(
        "고장신고 Raw 수집 시작: cutoff=%s snapshot_type=%s 대상=%s",
        cutoff.isoformat(),
        snapshot_type,
        [d.isoformat() for d in target_dates],
    )

    for target_date in target_dates:
        try:
            collect_one_day(target_date, cutoff, snapshot_type, config.SETTINGS.raw_bucket)
        except (SeoulApiError, SeoulApiTransientError) as e:
            logger.error("%s 수집 실패: %s", target_date, e)
            sys.exit(1)


if __name__ == "__main__":
    run()
