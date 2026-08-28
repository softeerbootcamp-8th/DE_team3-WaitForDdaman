"""승격에 쓸 대여이력 Raw 관측본을 고르고 selection.json으로 계약을 남기는 잡.

FINAL 수집 task가 실패해도 이 잡은 ALL_DONE으로 실행된다. 성공 조건은 "FINAL task가
성공했는가"가 아니라 "필수 파티션마다 쓸 수 있는 Raw 후보가 있는가"다.

Spark를 쓰지 않고 S3 manifest만 읽는 제어면 작업이라 재시도가 싸고 부작용이 없다.
Bronze/워터마크/Asset은 절대 건드리지 않는다.
"""
import json
import logging
import os
import sys
from datetime import date

import config
from common.s3_utils import ensure_bucket, get_json, list_keys, put_json
from common.watermark import read_watermark
from jobs.rental_history_snapshot_policy import (
    DEFAULT_PRELIMINARY_MAX_AGE_MINUTES,
    Candidate,
    Selection,
    build_date_backfill_windows,
    build_final_windows,
    build_promotion_id,
    parse_bool,
    parse_max_days,
    raw_target_prefix,
    select_snapshots,
    selection_key,
)
from jobs.collect_rental_history_raw import parse_collection_cutoff

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class SnapshotSelectionError(Exception):
    """필수 파티션 중 하나라도 쓸 수 있는 후보가 없어 승격을 진행할 수 없는 상태."""


def load_candidates(bucket: str, target_dates) -> list[Candidate]:
    """대상 날짜 prefix만 조회해 manifest와 payload 존재 여부를 모은다."""
    candidates: list[Candidate] = []
    for target_date in target_dates:
        keys = list_keys(bucket, raw_target_prefix(target_date))
        existing = set(keys)
        for key in keys:
            if not key.endswith("/manifest.json"):
                continue
            candidates.append(
                Candidate(
                    manifest_key=key,
                    manifest=get_json(bucket, key),
                    payload_exists=key.replace("manifest.json", "payload.json")
                    in existing,
                )
            )
    return candidates


def build_selection_document(
    cutoff,
    promotion_id: str,
    selection: Selection,
    fallback_enabled: bool,
    t0_enabled: bool,
    source_bucket: str,
) -> dict:
    """task 간 계약으로 쓰는 결정적 selection 문서를 만든다."""
    run_date = cutoff.date()
    return {
        "dataset": "rental_history",
        "run_date": run_date.isoformat(),
        "promotion_id": promotion_id,
        "collection_cutoff_at": cutoff.isoformat(),
        "source_bucket": source_bucket,
        "fallback_enabled": fallback_enabled,
        "t0_enabled": t0_enabled,
        "required_confirmed_dates": selection.required_confirmed_dates,
        "current_date_required": selection.current_date_required,
        "selected_snapshots": selection.selected,
        "mode": selection.mode,
        "selection_key": selection_key(run_date, promotion_id),
    }


def run() -> dict:
    """필수 window마다 후보를 골라 selection.json을 쓰고 요약 한 줄을 표준출력에 남긴다."""
    cutoff_value = os.getenv("COLLECTION_CUTOFF_AT")
    if not cutoff_value:
        raise SnapshotSelectionError("COLLECTION_CUTOFF_AT is required")

    cutoff = parse_collection_cutoff(cutoff_value)
    fallback_enabled = parse_bool(os.getenv("RENTAL_HISTORY_FALLBACK_ENABLED"))
    t0_enabled = parse_bool(os.getenv("RENTAL_HISTORY_T0_ENABLED"))
    max_age_minutes = int(
        os.getenv("RENTAL_HISTORY_PRELIMINARY_MAX_AGE_MINUTES")
        or DEFAULT_PRELIMINARY_MAX_AGE_MINUTES
    )
    max_days = parse_max_days(os.getenv("MAX_DAYS_PER_RUN"))

    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)

    backfill_target = os.getenv("BACKFILL_TARGET_DATE")
    if backfill_target:
        target_date = date.fromisoformat(backfill_target)
        if target_date != cutoff.date():
            raise SnapshotSelectionError(
                "BACKFILL_TARGET_DATE must equal COLLECTION_CUTOFF_AT date"
            )
        confirmed_through = None
        windows = build_date_backfill_windows(target_date)
    else:
        confirmed_through = read_watermark()
        windows = build_final_windows(
            cutoff=cutoff,
            confirmed_through=confirmed_through,
            max_days=max_days,
            t0_enabled=t0_enabled,
        )
    required_dates = [w.target_date for w in windows if w.required]
    candidates = load_candidates(bucket, required_dates)

    selection = select_snapshots(
        cutoff=cutoff,
        windows=windows,
        candidates=candidates,
        fallback_enabled=fallback_enabled,
        max_age_minutes=max_age_minutes,
    )

    logger.info(
        "후보 선택 요약: watermark=%s required=%s fallback_enabled=%s "
        "max_age_minutes=%d rejections=%s selected=%s",
        confirmed_through,
        [d.isoformat() for d in required_dates],
        fallback_enabled,
        max_age_minutes,
        selection.rejections,
        [
            (s["target_date"], s["snapshot_type"], s["observed_at"], s["payload_key"])
            for s in selection.selected
        ],
    )

    if selection.missing:
        raise SnapshotSelectionError(
            f"승격 후보를 찾지 못한 필수 파티션: {selection.missing}"
        )

    promotion_id = build_promotion_id(cutoff)
    document = build_selection_document(
        cutoff=cutoff,
        promotion_id=promotion_id,
        selection=selection,
        fallback_enabled=fallback_enabled,
        t0_enabled=t0_enabled,
        source_bucket=bucket,
    )
    put_json(bucket, document["selection_key"], document)
    logger.info(
        "selection 기록 완료: mode=%s key=%s", document["mode"], document["selection_key"]
    )

    print(
        json.dumps(
            {
                "promotion_id": promotion_id,
                "mode": document["mode"],
                "selection_key": document["selection_key"],
                "selected_count": len(document["selected_snapshots"]),
            },
            ensure_ascii=False,
        )
    )
    return document


if __name__ == "__main__":
    try:
        run()
    except SnapshotSelectionError as exc:
        logger.error("후보 선택 실패: %s", exc)
        sys.exit(1)
