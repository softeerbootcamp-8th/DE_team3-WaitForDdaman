"""대여이력 API 관측본을 Raw payload/manifest로 저장하는 순수 수집 잡."""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

import config
from common.api_client import (
    SeoulApiError,
    SeoulApiTransientError,
    fetch_rent_history_pages_by_hour,
    strip_pagination_meta,
)
from common.s3_utils import ensure_bucket, get_json, put_json
from common.watermark import read_watermark
from jobs.rental_history_snapshot_policy import (
    ROLE_CONFIRMED,
    ROLE_CURRENT,
    CollectionWindow,
    build_date_backfill_windows,
    build_final_windows,
    parse_bool,
    parse_max_days,
)
from schema.rental_history_schema import SchemaValidationError, validate_and_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
SNAPSHOT_TYPES = {"PRELIMINARY", "FINAL"}
# #142와 동일한 최대 동시성. 한 snapshot 안의 시간대 호출만 병렬화하고 날짜/window 간에는
# 순차를 유지한다(#167).
MAX_HOUR_CONCURRENCY = 8


class RawCollectionError(Exception):
    """Raw snapshot이 후속 승격 후보로 사용할 수 없는 상태임을 나타낸다."""


def unusable_reason(manifest: dict) -> str:
    """운영 로그에서 0행·API 미완료·스키마 장애를 혼동하지 않게 분류한다."""
    status = manifest.get("status")
    if status == "COMPLETE_EMPTY":
        return "EMPTY_RESULT"
    if status == "INCOMPLETE":
        return "API_INCOMPLETE"
    if status == "COMPLETE" and manifest.get("schema_valid") is not True:
        return "SCHEMA_INVALID"
    return "UNUSABLE_SNAPSHOT"


def parse_collection_cutoff(value: str) -> datetime:
    """Airflow가 전달한 논리적 cutoff를 timezone-aware KST 시각으로 정규화한다.

    마이크로초는 버린다 - 이 값에서 파생되는 observed_at 키(snapshot_keys()의
    strftime("%Y%m%dT%H%M%S%z"), 초 단위)와 manifest에 저장하는 isoformat() 값이
    같은 인스턴트를 가리켜야 한다. 안 자르면 마이크로초가 있는 cutoff에서
    key와 manifest 값이 어긋나 selector의 완전 일치 비교(rental_history_
    snapshot_policy.py)가 항상 실패한다. 수동 트리거의 dag_run.conf나 실행
    환경의 data_interval_end는 마이크로초를 포함할 수 있다. 이 함수가
    select_rental_history_snapshot/promote_rental_history_raw/update_rental_
    history_confirmed_watermark의 공통 진입점이라 여기서 한 번만 자르면 된다.
    """
    cutoff = datetime.fromisoformat(value)
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("collection_cutoff_at must include timezone")
    return cutoff.astimezone(KST).replace(microsecond=0)


def build_collection_windows(cutoff: datetime) -> list[tuple[date, list[int]]]:
    """예비 복구에 필요한 전날 전체와 당일 완료 시간대의 요청 범위를 만든다."""
    previous_date = cutoff.date() - timedelta(days=1)
    windows = [(previous_date, list(range(24)))]
    if cutoff.hour > 0:
        windows.append((cutoff.date(), list(range(cutoff.hour))))
    return windows


def build_run_windows(cutoff: datetime, snapshot_type: str) -> list[CollectionWindow]:
    """snapshot type별 수집 대상 window를 만든다.

    PRELIMINARY는 #135와 동일하게 전날 전체와 당일 완료 시간대만 본다(둘 다 필수).
    FINAL은 확정 워터마크 다음 날부터의 backlog를 필수로 잡고, 당일 window는
    RENTAL_HISTORY_T0_ENABLED에 따라 필수/관측 전용으로 갈린다.
    """
    if snapshot_type == "PRELIMINARY":
        return [
            CollectionWindow(
                target_date=target_date,
                hours=hours,
                required=True,
                role=ROLE_CURRENT if target_date == cutoff.date() else ROLE_CONFIRMED,
            )
            for target_date, hours in build_collection_windows(cutoff)
        ]

    backfill_target = os.getenv("BACKFILL_TARGET_DATE")
    if backfill_target:
        target_date = date.fromisoformat(backfill_target)
        if target_date != cutoff.date():
            raise RawCollectionError(
                "BACKFILL_TARGET_DATE must equal COLLECTION_CUTOFF_AT date"
            )
        return build_date_backfill_windows(target_date)

    t0_enabled = parse_bool(os.getenv("RENTAL_HISTORY_T0_ENABLED"))
    max_days = parse_max_days(os.getenv("MAX_DAYS_PER_RUN"))
    confirmed_through = read_watermark()
    windows = build_final_windows(
        cutoff=cutoff,
        confirmed_through=confirmed_through,
        max_days=max_days,
        t0_enabled=t0_enabled,
    )
    logger.info(
        "확정 수집 window 계산: watermark=%s max_days=%s t0_enabled=%s windows=%s",
        confirmed_through,
        max_days,
        t0_enabled,
        [
            (w.target_date.isoformat(), len(w.hours), "required" if w.required else "optional")
            for w in windows
        ],
    )
    return windows


def snapshot_keys(
    target_date: date, observed_at: datetime, snapshot_type: str
) -> tuple[str, str]:
    """날짜·논리 관측시각·snapshot type별 payload/manifest S3 key를 만든다."""
    normalized_type = snapshot_type.strip().upper()
    if normalized_type not in SNAPSHOT_TYPES:
        raise ValueError(f"unsupported snapshot_type: {snapshot_type}")

    observed_key = observed_at.astimezone(KST).strftime("%Y%m%dT%H%M%S%z")
    prefix = (
        f"raw/rental_history/api/target_date={target_date.isoformat()}/"
        f"observed_at={observed_key}/snapshot_type={normalized_type}/"
    )
    return f"{prefix}payload.json", f"{prefix}manifest.json"


def _fetch_hour_pages(
    fetch_pages: Callable[[date, int], Iterable[list[dict]]],
    target_date: date,
    hour: int,
) -> tuple[int, list[list[dict]], str | None]:
    """한 시간대의 페이지네이션을 순차적으로 끝까지 소비한다.

    시간대 내부 순서(페이지네이션)는 그대로 순차이며, 이 함수 자체가 스레드 풀의
    병렬 실행 단위(시간대)가 된다. 실패 전까지 받은 페이지는 버리지 않고 반환해
    부분 결과를 숨기지 않는다.
    """
    pages: list[list[dict]] = []
    try:
        for page_rows in fetch_pages(target_date, hour):
            pages.append(page_rows)
        return hour, pages, None
    except (SeoulApiError, SeoulApiTransientError) as exc:
        return hour, pages, f"{target_date.isoformat()} {hour}시: {exc}"


def collect_snapshot(
    target_date: date,
    hours: list[int],
    observed_at: datetime,
    snapshot_type: str,
    fetch_pages: Callable[[date, int], Iterable[list[dict]]],
    write_json: Callable[[str, Any], None],
    read_json: Callable[[str], Any | None] | None = None,
) -> dict:
    """한 날짜의 API 관측본을 수집하고 payload 다음 manifest 순서로 기록한다."""
    # 마이크로초를 여기서 자른다 - 호출자가 이미 정규화된 값을 넘긴다고 가정하지 않는다.
    # key(snapshot_keys()의 초 단위 strftime)와 아래 observed_iso(isoformat())가 같은
    # 인스턴트를 가리켜야 selector의 완전 일치 비교가 깨지지 않는데, 그 보장을 호출자
    # (parse_collection_cutoff)에만 맡기면 이 함수를 직접 부르는 새 호출자가 생길 때마다
    # 같은 버그가 재발한다. 이 함수 스스로 불변식을 지킨다 (#182).
    observed_at = observed_at.replace(microsecond=0)
    started_at = time.monotonic()
    normalized_type = snapshot_type.strip().upper()
    payload_key, manifest_key = snapshot_keys(
        target_date, observed_at, normalized_type
    )
    requested_hours = list(hours)
    existing_manifest = read_json(manifest_key) if read_json is not None else None
    if (
        isinstance(existing_manifest, dict)
        and existing_manifest.get("status") == "COMPLETE"
        and existing_manifest.get("schema_valid") is True
        and existing_manifest.get("requested_hours") == requested_hours
        and existing_manifest.get("completed_hours") == requested_hours
        and existing_manifest.get("payload_key") == payload_key
    ):
        logger.info(
            "기존 COMPLETE Raw snapshot 재사용: target_date=%s observed_at=%s "
            "snapshot_type=%s",
            target_date,
            observed_at.astimezone(KST).isoformat(),
            normalized_type,
        )
        return existing_manifest

    completed_hours: list[int] = []
    raw_rows: list[dict] = []
    page_count = 0
    hour_errors: list[str] = []

    if requested_hours:
        results: dict[int, tuple[list[list[dict]], str | None]] = {}
        max_workers = min(MAX_HOUR_CONCURRENCY, len(requested_hours))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_hour_pages, fetch_pages, target_date, hour): hour
                for hour in requested_hours
            }
            for future in as_completed(futures):
                hour, pages, error = future.result()
                results[hour] = (pages, error)

        # 완료 순서와 무관하게 항상 시간순으로 결합해 payload/manifest를 결정적으로 만든다.
        for hour in sorted(results):
            pages, error = results[hour]
            for page_rows in pages:
                page_count += 1
                raw_rows.extend(page_rows)

            if error is not None:
                hour_errors.append(error)
                logger.error("대여이력 Raw 시간대 수집 실패: %s", error)
                continue

            completed_hours.append(hour)
            logger.info(
                "대여이력 Raw 시간대 수집 완료: target_date=%s hour=%d pages=%d rows=%d",
                target_date,
                hour,
                len(pages),
                sum(len(page_rows) for page_rows in pages),
            )

    collection_error = "; ".join(hour_errors) if hour_errors else None

    if collection_error is not None:
        status = "INCOMPLETE"
    elif not raw_rows:
        status = "COMPLETE_EMPTY"
    else:
        status = "COMPLETE"

    # 빈 결과는 검사할 컬럼 자체가 없으므로 스키마 실패(False)와 구분한다.
    # JSON null로 저장되어 운영 로그에서 COMPLETE_EMPTY가 스키마 장애처럼 보이지 않는다.
    schema_valid: bool | None = None
    schema_error: str | None = None
    if raw_rows:
        schema_valid = False
        columns = list(strip_pagination_meta(raw_rows[0]).keys())
        try:
            validate_and_report(columns)
            schema_valid = True
        except SchemaValidationError as exc:
            schema_error = str(exc)

    error = collection_error or schema_error
    observed_iso = observed_at.astimezone(KST).isoformat()
    payload = raw_rows
    manifest = {
        "dataset": "rental_history",
        "target_date": target_date.isoformat(),
        "observed_at": observed_iso,
        "snapshot_type": normalized_type,
        "status": status,
        "requested_hours": requested_hours,
        "completed_hours": completed_hours,
        "page_count": page_count,
        "row_count": len(raw_rows),
        "schema_valid": schema_valid,
        "payload_key": payload_key,
        "error": error,
    }

    write_json(payload_key, payload)
    write_json(manifest_key, manifest)
    logger.info(
        "대여이력 Raw snapshot 저장 완료: target_date=%s observed_at=%s "
        "snapshot_type=%s status=%s pages=%d rows=%d elapsed_seconds=%.3f",
        target_date,
        observed_iso,
        normalized_type,
        status,
        page_count,
        len(raw_rows),
        time.monotonic() - started_at,
    )
    return manifest


def run() -> list[dict]:
    """snapshot type별 수집 window를 날짜별 Raw payload/manifest로 저장한다.

    필수 window가 하나라도 쓸 수 없는 상태면 실패한다. 관측 전용 window(T0=false의 당일)는
    실패해도 경고만 남기고 run 실패로 보지 않는다.
    """
    cutoff_value = os.getenv("COLLECTION_CUTOFF_AT")
    if not cutoff_value:
        raise RawCollectionError("COLLECTION_CUTOFF_AT is required")

    snapshot_type = os.getenv("SNAPSHOT_TYPE", "").strip().upper()
    if snapshot_type not in SNAPSHOT_TYPES:
        raise RawCollectionError(
            f"SNAPSHOT_TYPE must be one of {sorted(SNAPSHOT_TYPES)}"
        )

    cutoff = parse_collection_cutoff(cutoff_value)
    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)

    def write_json(key: str, payload: dict) -> None:
        put_json(bucket, key, payload)

    def read_json(key: str) -> Any | None:
        return get_json(bucket, key)

    windows = build_run_windows(cutoff, snapshot_type)
    if not windows:
        logger.info("수집할 window 없음 (cutoff=%s)", cutoff.isoformat())
        return []

    manifests = []
    unusable = []
    for window in windows:
        manifest = collect_snapshot(
            target_date=window.target_date,
            hours=window.hours,
            observed_at=cutoff,
            snapshot_type=snapshot_type,
            fetch_pages=fetch_rent_history_pages_by_hour,
            write_json=write_json,
            read_json=read_json,
        )
        manifests.append(manifest)

        if manifest["status"] == "COMPLETE" and manifest["schema_valid"]:
            continue

        summary = {
            "target_date": manifest.get("target_date", window.target_date.isoformat()),
            "status": manifest["status"],
            "schema_valid": manifest["schema_valid"],
            "reason": unusable_reason(manifest),
        }
        if window.required:
            unusable.append(summary)
        else:
            # 관측 전용 window(T0=false의 당일)는 실패해도 확정 결과를 바꾸지 않는다.
            # 다만 degraded 관측 지표로 남기기 위해 경고는 반드시 남긴다.
            logger.warning("관측 전용 window 수집 실패 (승격 대상 아님): %s", summary)

    if unusable:
        raise RawCollectionError(f"unusable rental history Raw snapshots: {unusable}")

    return manifests


if __name__ == "__main__":
    run()
