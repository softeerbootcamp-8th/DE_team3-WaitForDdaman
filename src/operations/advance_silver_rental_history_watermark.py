"""Silver 대여이력 초기 적재의 연속 완료 구간까지만 Silver 워터마크를 전진시킨다.

### 왜 Bronze의 advance_completion_watermark.py를 재사용하지 않는가
그쪽은 완료 단위가 "날짜"이고 marker가 없으면 그 자리에서 멈추되 실패 여부는
RECONCILIATION_FAIL_ON_INCOMPLETE 플래그로 고른다. 여기는 완료 단위가 planner가 자른
"청크 범위"이고, 공백이 있으면 그 직전까지 전진한 뒤 반드시 실패해야 한다(DAG를 성공으로
끝내면 실패한 청크가 조용히 묻힌다). marker key 구조도 실패 정책도 달라서, 억지로 한
모듈로 합치면 양쪽 다 읽기 어려워진다. 의도적으로 따로 둔다.

### 왜 all_ranges를 계획에서 받는가
finalizer가 Bronze 워터마크를 다시 읽으면 DAG 실행 중에 다른 파이프라인이 Bronze를 더
전진시킨 경우 이번 실행이 책임지지 않은 구간까지 상한에 들어온다. planner가 시작 시점에
고정한 계획(all_ranges / bronze_watermark_at_start)만 본다.

### 왜 ALL_DONE으로 도는가
mapped 청크가 하나라도 실패하면 ALL_SUCCESS인 마무리 태스크는 아예 실행되지 않아,
성공한 117개 청크의 결과가 워터마크에 전혀 반영되지 않았다(#232 이후에도 남아 있던 문제).
이 잡은 실패한 청크가 있어도 실행돼서, 연속으로 COMPLETE인 마지막 청크 끝까지만 워터마크를
올리고 스스로 실패한다 - 다음 실행은 그 공백 청크부터 다시 처리하고, 공백 뒤에 이미
COMPLETE인 marker는 그대로 재사용한다(삭제하지 않는다).

사용법:
    SILVER_BACKFILL_PLAN='{"contract_version":1,"all_ranges":[...]}' \
        python -m jobs.advance_silver_rental_history_watermark
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date

import config
from common.s3_utils import ensure_bucket
from common.silver_rental_history_completion import (
    SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
    is_range_complete,
)
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import DATASET_WATERMARK_KEYS

logger = logging.getLogger(__name__)

SILVER_WATERMARK_KEY = DATASET_WATERMARK_KEYS["silver_rental_history"]


class IncompleteBackfillError(RuntimeError):
    """연속 완료 구간에 공백이 있음 - 워터마크는 그 직전까지 전진하고 태스크는 실패시킨다."""


def load_plan() -> dict:
    raw = (os.getenv("SILVER_BACKFILL_PLAN") or "").strip()
    if not raw:
        raise ValueError("SILVER_BACKFILL_PLAN이 필요합니다 (planner의 XCom JSON)")
    try:
        plan = json.loads(raw)
    except json.JSONDecodeError as e:
        # 상류 planner가 실패해 XCom이 비었거나 stdout 마지막 줄이 JSON이 아닌 경우.
        raise ValueError(f"SILVER_BACKFILL_PLAN을 JSON으로 읽을 수 없음: {raw[:200]!r}") from e
    if not isinstance(plan, dict) or "all_ranges" not in plan:
        raise ValueError(f"SILVER_BACKFILL_PLAN 형식이 올바르지 않음: {raw[:200]!r}")
    return plan


def confirmed_ranges(
    bucket: str, all_ranges: list[dict], contract_version: int, before: date
) -> tuple[list[dict], dict | None]:
    """계획을 start 순으로 훑어 연속 COMPLETE 구간과 최초 공백 구간을 돌려준다.

    이미 워터마크가 덮은 구간(end <= before)은 건너뛴다 - 이전 실행이 부분 전진시킨
    뒤 marker가 정리된 경우에도 그 구간 때문에 공백으로 오판하지 않게 하기 위함이다.
    """
    ordered = sorted(all_ranges, key=lambda r: r["start"])
    confirmed: list[dict] = []
    for chunk in ordered:
        if date.fromisoformat(chunk["end"]) <= before:
            continue
        if not is_range_complete(bucket, chunk["start"], chunk["end"], contract_version):
            return confirmed, chunk
        confirmed.append(chunk)
    return confirmed, None


def run() -> dict:
    plan = load_plan()
    contract_version = int(plan.get("contract_version") or SILVER_RENTAL_HISTORY_CONTRACT_VERSION)
    all_ranges = plan.get("all_ranges") or []

    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)
    before = read_watermark(watermark_key=SILVER_WATERMARK_KEY)

    confirmed, first_gap = confirmed_ranges(bucket, all_ranges, contract_version, before)
    after = date.fromisoformat(confirmed[-1]["end"]) if confirmed else before
    if after > before:
        write_watermark(after, watermark_key=SILVER_WATERMARK_KEY)

    result = {
        "dataset": "silver_rental_history",
        "contract_version": contract_version,
        "before": before.isoformat(),
        "after": after.isoformat(),
        "confirmed_range_count": len(confirmed),
        "total_range_count": len(all_ranges),
        "first_incomplete_range": first_gap,
        "noop": after == before,
    }
    print(json.dumps(result, ensure_ascii=False))

    if first_gap is not None:
        # 워터마크는 이미 연속 완료 직전까지 올려뒀다 - 그 뒤에 실패시켜야 DAG가 실패
        # 상태로 남아 사람이 공백 청크를 본다. 공백 뒤의 COMPLETE marker는 손대지 않는다.
        raise IncompleteBackfillError(
            f"청크 완료 marker에 공백이 있음: {first_gap['start']}~{first_gap['end']} "
            f"(워터마크는 {before.isoformat()} -> {after.isoformat()}까지만 전진)"
        )
    logger.info(
        "Silver 워터마크 전진 완료: %s -> %s (%d/%d 청크 COMPLETE)",
        result["before"], result["after"], len(confirmed), len(all_ranges),
    )
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
