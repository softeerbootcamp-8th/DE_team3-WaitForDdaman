"""대여이력 API 관측본을 Raw payload/manifest로 저장하는 순수 수집 잡."""

import logging
import os
import time
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
from schema.rental_history_schema import SchemaValidationError, validate_and_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
SNAPSHOT_TYPES = {"PRELIMINARY", "FINAL"}


class RawCollectionError(Exception):
    """Raw snapshot이 후속 승격 후보로 사용할 수 없는 상태임을 나타낸다."""


def parse_collection_cutoff(value: str) -> datetime:
    """Airflow가 전달한 논리적 cutoff를 timezone-aware KST 시각으로 정규화한다."""
    cutoff = datetime.fromisoformat(value)
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("collection_cutoff_at must include timezone")
    return cutoff.astimezone(KST)


def build_collection_windows(cutoff: datetime) -> list[tuple[date, list[int]]]:
    """예비 복구에 필요한 전날 전체와 당일 완료 시간대의 요청 범위를 만든다."""
    previous_date = cutoff.date() - timedelta(days=1)
    windows = [(previous_date, list(range(24)))]
    if cutoff.hour > 0:
        windows.append((cutoff.date(), list(range(cutoff.hour))))
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
    collection_error: str | None = None

    for hour in requested_hours:
        hour_page_count = 0
        hour_row_count = 0
        try:
            for page_rows in fetch_pages(target_date, hour):
                page_count += 1
                hour_page_count += 1
                hour_row_count += len(page_rows)
                raw_rows.extend(page_rows)
        except (SeoulApiError, SeoulApiTransientError) as exc:
            collection_error = f"{target_date.isoformat()} {hour}시: {exc}"
            logger.error("대여이력 Raw 시간대 수집 실패: %s", collection_error)
            break

        completed_hours.append(hour)
        logger.info(
            "대여이력 Raw 시간대 수집 완료: target_date=%s hour=%d pages=%d rows=%d",
            target_date,
            hour,
            hour_page_count,
            hour_row_count,
        )

    if collection_error is not None:
        status = "INCOMPLETE"
    elif not raw_rows:
        status = "COMPLETE_EMPTY"
    else:
        status = "COMPLETE"

    schema_valid = False
    schema_error: str | None = None
    if raw_rows:
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
    """논리 cutoff의 전날 전체와 당일 완료 시간대를 날짜별 Raw로 수집한다."""
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

    manifests = [
        collect_snapshot(
            target_date=target_date,
            hours=hours,
            observed_at=cutoff,
            snapshot_type=snapshot_type,
            fetch_pages=fetch_rent_history_pages_by_hour,
            write_json=write_json,
            read_json=read_json,
        )
        for target_date, hours in build_collection_windows(cutoff)
    ]

    unusable = [
        manifest
        for manifest in manifests
        if manifest["status"] != "COMPLETE" or not manifest["schema_valid"]
    ]
    if unusable:
        summary = [
            {
                "target_date": manifest.get("target_date"),
                "status": manifest["status"],
                "schema_valid": manifest["schema_valid"],
            }
            for manifest in unusable
        ]
        raise RawCollectionError(f"unusable rental history Raw snapshots: {summary}")

    return manifests


if __name__ == "__main__":
    run()
