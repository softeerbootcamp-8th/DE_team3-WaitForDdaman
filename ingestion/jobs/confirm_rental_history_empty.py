"""검증된 대여이력 0행 날짜를 감사 기록과 함께 수동 승인한다.

전체 24시간 API 호출이 성공했지만 0행인 날짜는 자동으로 정상 처리하지 않는다.
운영자가 원천을 확인한 뒤 이 잡을 실행하면, COMPLETE_EMPTY manifest와 빈 payload를
다시 검증하고 날짜별 completion marker를 남긴 다음 Bronze/Silver 워터마크를 정확히
하루만 전진시킨다. 임의 날짜 점프와 근거 없는 워터마크 수정을 허용하지 않는다.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import config
from common.s3_utils import ensure_bucket, get_json, list_keys, put_json
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import SILVER_RENTAL_HISTORY
from jobs.rental_history_snapshot_policy import raw_target_prefix

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

FULL_DAY_HOURS = list(range(24))
COMPLETION_PREFIX = "_meta/completion/bronze_rental_history"
KST = ZoneInfo("Asia/Seoul")


class EmptyConfirmationError(Exception):
    """0행 증거 또는 연속 워터마크 조건이 수동 승인 계약과 맞지 않는다."""


def completion_key(target_date: date) -> str:
    return f"{COMPLETION_PREFIX}/target_date={target_date.isoformat()}/completion.json"


def _validated_empty_manifest(bucket: str, target_date: date) -> tuple[str, dict]:
    """최신 관측본이 0행이고 과거에 데이터가 없었는지 검증한다."""
    manifest_keys = sorted(
        (
            key
            for key in list_keys(bucket, raw_target_prefix(target_date))
            if key.endswith("/manifest.json")
        ),
        reverse=True,
    )

    if not manifest_keys:
        raise EmptyConfirmationError(
            f"{target_date.isoformat()}의 manifest 없음"
        )

    latest_key = manifest_keys[0]
    latest = get_json(bucket, latest_key)
    if not isinstance(latest, dict):
        raise EmptyConfirmationError(f"최신 manifest를 읽을 수 없음: {latest_key}")

    if latest.get("status") != "COMPLETE_EMPTY":
        raise EmptyConfirmationError(
            f"최신 manifest가 COMPLETE_EMPTY가 아님: {latest.get('status')!r} ({latest_key})"
        )
    if latest.get("dataset") != "rental_history":
        raise EmptyConfirmationError(f"최신 manifest dataset 불일치: {latest_key}")
    if latest.get("target_date") != target_date.isoformat():
        raise EmptyConfirmationError(f"최신 manifest target_date 불일치: {latest_key}")
    if latest.get("requested_hours") != FULL_DAY_HOURS:
        raise EmptyConfirmationError(f"최신 manifest 요청 범위가 24시간이 아님: {latest_key}")
    if latest.get("completed_hours") != FULL_DAY_HOURS:
        raise EmptyConfirmationError(f"최신 manifest 완료 범위가 24시간이 아님: {latest_key}")
    if latest.get("row_count") != 0 or latest.get("error") is not None:
        raise EmptyConfirmationError(f"최신 manifest가 정상적인 0행 결과가 아님: {latest_key}")

    try:
        observed_at = datetime.fromisoformat(latest.get("observed_at"))
    except (TypeError, ValueError) as exc:
        raise EmptyConfirmationError(f"최신 manifest observed_at 형식 오류: {latest_key}") from exc
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise EmptyConfirmationError(f"최신 manifest observed_at timezone 누락: {latest_key}")
    if observed_at.astimezone(KST).date() <= target_date:
        raise EmptyConfirmationError(f"대상 날짜가 끝난 뒤의 관측본이 아님: {latest_key}")

    payload_key = latest.get("payload_key")
    if not isinstance(payload_key, str) or get_json(bucket, payload_key) != []:
        raise EmptyConfirmationError(f"최신 manifest의 빈 payload 증거 없음: {latest_key}")

    for older_key in manifest_keys[1:]:
        older = get_json(bucket, older_key)
        if (
            isinstance(older, dict)
            and older.get("status") == "COMPLETE"
            and isinstance(older.get("row_count"), int)
            and older["row_count"] > 0
        ):
            raise EmptyConfirmationError(
                f"이전에 데이터가 있는 COMPLETE manifest가 존재함: {older_key}"
            )

    return latest_key, latest


def _validate_actor_and_reason(confirmed_by: str, reason: str) -> tuple[str, str]:
    actor = confirmed_by.strip()
    normalized_reason = reason.strip()
    if not actor:
        raise EmptyConfirmationError("confirmed_by는 필수입니다")
    if not normalized_reason:
        raise EmptyConfirmationError("reason은 필수입니다")
    return actor, normalized_reason


def _validate_contiguous_watermark(name: str, current: date, target: date) -> None:
    previous = target - timedelta(days=1)
    if current not in {previous, target}:
        raise EmptyConfirmationError(
            f"{name} 워터마크는 대상의 직전 날짜여야 합니다: "
            f"current={current} expected={previous}"
        )


def run(target_date_str: str, confirmed_by: str, reason: str) -> dict:
    """검증된 빈 날짜를 승인하고 Bronze/Silver 워터마크를 멱등 전진시킨다."""
    try:
        target_date = date.fromisoformat(target_date_str)
    except (TypeError, ValueError) as exc:
        raise EmptyConfirmationError(
            f"target_date는 YYYY-MM-DD 형식이어야 합니다: {target_date_str!r}"
        ) from exc

    actor, normalized_reason = _validate_actor_and_reason(confirmed_by, reason)
    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)

    manifest_key, manifest = _validated_empty_manifest(bucket, target_date)
    marker_key = completion_key(target_date)
    existing_marker = get_json(bucket, marker_key)

    bronze_before = read_watermark()
    silver_before = read_watermark(watermark_key=SILVER_RENTAL_HISTORY)
    _validate_contiguous_watermark("Bronze", bronze_before, target_date)
    _validate_contiguous_watermark("Silver", silver_before, target_date)

    if existing_marker is not None:
        if not isinstance(existing_marker, dict) or (
            existing_marker.get("status") != "MANUALLY_CONFIRMED_EMPTY"
            or existing_marker.get("target_date") != target_date.isoformat()
            or existing_marker.get("source_manifest_key") != manifest_key
        ):
            raise EmptyConfirmationError(f"기존 completion marker와 승인 요청이 충돌함: {marker_key}")
        marker = existing_marker
    else:
        marker = {
            "dataset": "rental_history",
            "target_date": target_date.isoformat(),
            "status": "MANUALLY_CONFIRMED_EMPTY",
            "row_count": 0,
            "confirmed_by": actor,
            "confirmed_at": datetime.now(timezone.utc).isoformat(),
            "reason": normalized_reason,
            "source_manifest_key": manifest_key,
            "source_observed_at": manifest.get("observed_at"),
        }
        # 감사 근거를 워터마크보다 먼저 남긴다. 이후 쓰기가 실패해도 재실행으로 복구할 수 있다.
        put_json(bucket, marker_key, marker)

    audit = {
        "completion_status": "MANUALLY_CONFIRMED_EMPTY",
        "completion_key": marker_key,
        "confirmed_by": marker["confirmed_by"],
        "reason": marker["reason"],
    }
    if bronze_before < target_date:
        write_watermark(target_date, extra=audit)
    if silver_before < target_date:
        write_watermark(
            target_date,
            extra=audit,
            watermark_key=SILVER_RENTAL_HISTORY,
        )

    result = {
        **marker,
        "completion_key": marker_key,
        "bronze_before": bronze_before.isoformat(),
        "bronze_after": target_date.isoformat(),
        "silver_before": silver_before.isoformat(),
        "silver_after": target_date.isoformat(),
    }
    logger.warning(
        "대여이력 빈 날짜 수동 승인: target_date=%s confirmed_by=%s marker=%s",
        target_date,
        actor,
        marker_key,
    )
    return result
