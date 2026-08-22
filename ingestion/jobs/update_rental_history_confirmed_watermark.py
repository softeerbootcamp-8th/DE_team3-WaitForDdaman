"""COMPLETE promotion marker를 읽어 Bronze 확정 워터마크만 전진시키는 잡.

Bronze 확정 워터마크를 쓰는 유일한 경로다. 승격과 분리해 둔 이유는 두 가지다.
- Iceberg commit은 성공했는데 워터마크 쓰기만 실패한 경우, 이 task만 재실행하면 복구된다.
- 워터마크가 실행일 당일로 앞서 나가는 사고를 구조적으로 막는다.

확정 여부는 snapshot type(FINAL/PRELIMINARY)이 아니라 "그 날짜가 0~23시로 완결됐는가"로
판단한다. 예비 관측본이라도 전날 하루가 통째로 들어왔으면 확정 후보이고, 반대로 당일
partial 파티션은 FINAL이어도 확정 후보가 아니다.
"""
import json
import logging
import os
import sys
from datetime import date, timedelta

import config
from common.s3_utils import ensure_bucket, get_json
from common.watermark import read_watermark, write_watermark
from jobs.collect_rental_history_raw import parse_collection_cutoff
from jobs.rental_history_snapshot_policy import build_promotion_id, promotion_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

FULL_DAY_HOURS = list(range(24))


class ConfirmedWatermarkError(Exception):
    """Bronze commit 상태가 불완전해 확정 워터마크를 갱신할 수 없는 상태."""


def full_day_partitions(promotion: dict, run_date: date) -> set[str]:
    """실제로 커밋된 파티션 중 하루가 통째로 들어온 날짜만 확정 후보로 추린다."""
    promoted = set(promotion.get("promoted_partitions") or [])
    return {
        entry["target_date"]
        for entry in promotion.get("selected_snapshots") or []
        if entry["target_date"] in promoted
        and list(entry.get("requested_hours") or []) == FULL_DAY_HOURS
        and date.fromisoformat(entry["target_date"]) < run_date
    }


def next_confirmed_watermark(
    current: date, candidates: set[str], run_date: date
) -> tuple[date, list[str]]:
    """기존 워터마크 다음 날부터 연속으로 이어지는 구간까지만 전진한다."""
    confirmed: list[str] = []
    cursor = current + timedelta(days=1)
    while cursor < run_date and cursor.isoformat() in candidates:
        confirmed.append(cursor.isoformat())
        cursor += timedelta(days=1)
    if not confirmed:
        return current, []
    return date.fromisoformat(confirmed[-1]), confirmed


def run() -> dict:
    """COMPLETE promotion을 읽어 연속 full-day 구간까지만 워터마크를 갱신한다."""
    cutoff_value = os.getenv("COLLECTION_CUTOFF_AT")
    if not cutoff_value:
        raise ConfirmedWatermarkError("COLLECTION_CUTOFF_AT is required")

    cutoff = parse_collection_cutoff(cutoff_value)
    run_date = cutoff.date()
    key = promotion_key(run_date, build_promotion_id(cutoff))

    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)
    promotion = get_json(bucket, key)
    if not isinstance(promotion, dict):
        raise ConfirmedWatermarkError(f"COMPLETE promotion marker 없음: {key}")
    if promotion.get("status") != "COMPLETE":
        raise ConfirmedWatermarkError(
            f"promotion status가 COMPLETE가 아님: {promotion.get('status')!r}"
        )

    before = read_watermark()
    candidates = full_day_partitions(promotion, run_date)
    after, confirmed_partitions = next_confirmed_watermark(before, candidates, run_date)

    noop = after <= before
    if not noop:
        write_watermark(after)

    result = {
        "before": before.isoformat(),
        "after": after.isoformat(),
        "noop": noop,
        "confirmed_partitions": confirmed_partitions,
        "promotion_key": key,
    }
    logger.info(
        "확정 워터마크 갱신 결과: before=%s after=%s noop=%s confirmed=%s candidates=%s",
        result["before"],
        result["after"],
        noop,
        confirmed_partitions,
        sorted(candidates),
    )
    print(json.dumps(result, ensure_ascii=False))
    return result


if __name__ == "__main__":
    try:
        run()
    except ConfirmedWatermarkError as exc:
        logger.error("확정 워터마크 갱신 실패: %s", exc)
        sys.exit(1)
