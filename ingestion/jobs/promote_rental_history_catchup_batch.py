"""Catchup 전용 대여이력 배치 승격 - prepare가 확보한 FINAL Raw를 묶어 단일 Iceberg commit.

### 왜 후보 선택도 재검증도 하지 않는가
일 배치는 같은 날짜에 여러 관측본(05시 PRELIMINARY, 06시 FINAL)이 있을 수 있어
"무엇을 승격할지" 고르는 단계(select_rental_history_snapshot -> selection.json)가 필요하다.

Catchup은 다르다. 이미 지나간 확정 날짜의 공백을 메우는 작업이라 관측본이 하나뿐이고,
그 하나의 key도 `target_date 23:59:59 KST`로 결정적이다. 고를 것이 없으므로 selection
단계를 두지 않는다.

manifest를 다시 읽어 COMPLETE 여부를 확인하지도 않는다. 그 검증은 이미 prepare task가
했다 - collect_rental_history_raw가 실패하면 prepare task 자체가 실패하고, 실패한 날짜는
애초에 이 잡으로 넘어오지 않는다. 승격 직전에 한 번 더 읽는 것은 같은 사실을 두 번
확인하는 것이고, "어디가 진실의 원천인가"를 흐린다. **수집 성공 여부는 prepare가 막고,
이 잡은 넘겨받은 날짜를 승격하는 책임만 진다.**

payload가 실제로 읽히는지는 load 단계에서 자연히 드러난다 - 없으면 Iceberg를 건드리기
전에 실패한다.

### 왜 날짜를 묶는가
Catchup은 공백 날짜가 한 번에 여러 개 나올 수 있는데, 날짜마다 커밋하면 같은
bronze.rental_history 테이블에 대한 Iceberg commit이 날짜 수만큼 반복된다. 커밋
1회당 고정비(메타데이터 작성 + 카탈로그 트랜잭션)가 행 수와 무관하게 붙으므로,
넘겨받은 날짜를 묶어 overwrite_partitions 한 번으로 반영한다.

### 왜 promotion marker는 날짜별로 남기는가
배치로 커밋하더라도 promotion.json은 날짜마다 하나씩 쓴다.
write_rental_history_completion_marker가 날짜별 promotion.json을 되읽어
COMPLETE/FAILED를 판정하고, advance_completion_watermark는 그 completion marker의
연속 구간만 워터마크에 반영하기 때문이다. 배치 단위 문서 하나로 바꾸면 이미 검증된
"날짜별 marker -> 연속 구간 워터마크" 계약을 전부 다시 설계해야 한다. 대신 문서에
batch_id/batch_size/batch_dates를 넣어 같은 커밋에 묶였다는 사실을 추적할 수 있게 한다.

사용법:
    BACKFILL_TARGET_DATES=2026-05-01,2026-05-02 python -m jobs.promote_rental_history_catchup_batch
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from typing import Callable, Optional

import config
from common.s3_utils import ensure_bucket, get_json, put_json
from jobs.collect_rental_history_raw import parse_collection_cutoff, snapshot_keys
from jobs.promote_rental_history_raw import promote
from jobs.rental_history_snapshot_policy import build_promotion_id, promotion_key

logger = logging.getLogger(__name__)

DATASET = "rental_history"
DEFAULT_BATCH_SIZE = 6

# Catchup은 "그 날짜가 끝난 시점"을 논리 cutoff로 고정한다. 이 값이 결정적이어야
# prepare가 만든 Raw와 이 잡이 읽는 Raw의 key가 같아진다.
CATCHUP_CUTOFF_SUFFIX = "T23:59:59+09:00"


class CatchupPromotionError(Exception):
    """배치 승격을 진행할 수 없는 상태."""


def catchup_cutoff(target_date: date) -> datetime:
    return parse_collection_cutoff(f"{target_date.isoformat()}{CATCHUP_CUTOFF_SUFFIX}")


def final_snapshot_keys(target_date: date) -> tuple[str, str]:
    """Catchup 논리 cutoff 기준 FINAL Raw의 (payload_key, manifest_key)."""
    return snapshot_keys(target_date, catchup_cutoff(target_date), "FINAL")


def promotion_marker_key(target_date: date) -> str:
    cutoff = catchup_cutoff(target_date)
    return promotion_key(target_date, build_promotion_id(cutoff))


def build_batches(dates: list[str], batch_size: int = DEFAULT_BATCH_SIZE) -> list[list[str]]:
    """주어진 순서를 유지한 채 batch_size개씩 자른다.

    정렬하지 않는다 - gap 목록이 불연속일 수 있고, 재실행 시 같은 그룹이 재현되려면
    호출자가 준 순서를 그대로 써야 한다.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size는 1 이상이어야 합니다: {batch_size}")
    return [dates[i : i + batch_size] for i in range(0, len(dates), batch_size)]


def load_payloads(
    bucket: str,
    dates: list[str],
    read_json: Optional[Callable[[str, str], object]] = None,
) -> list[dict]:
    """prepare가 확보한 결정적 FINAL Raw payload를 전부 먼저 읽는다.

    manifest를 다시 읽어 COMPLETE 여부를 판정하지 않는다 - 수집 성공은 prepare task가
    보장한다. 다만 payload가 실제로 읽히고 배열인지는 확인한다. 하나라도 문제가 있으면
    Iceberg를 건드리기 전에 실패해, 반쪽짜리 커밋이 생기지 않게 한다.
    """
    reader = read_json or get_json
    loaded: list[dict] = []

    for value in dates:
        target_date = date.fromisoformat(value)
        payload_key, manifest_key = final_snapshot_keys(target_date)
        rows = reader(bucket, payload_key)

        if rows is None:
            raise CatchupPromotionError(f"{value}: FINAL Raw payload 객체가 없음 - {payload_key}")
        if not isinstance(rows, list):
            raise CatchupPromotionError(f"{value}: payload 형식이 배열이 아님 - {payload_key}")

        loaded.append(
            {
                "target_date": value,
                "rent_date_partition": value,
                "source_file": payload_key,
                "manifest_key": manifest_key,
                "row_count": len(rows),
                "rows": rows,
            }
        )
    return loaded


def build_promotion_document(
    item: dict,
    partition_counts: dict[str, int],
    *,
    batch_id: str,
    batch_size: int,
    batch_dates: list[str],
) -> dict:
    """날짜별 promotion marker. 기존 소비자가 보는 필드를 그대로 유지한다.

    batch_* 필드는 감사 정보다 - 이 날짜가 어떤 단일 commit에 묶여 반영됐는지
    추적할 수 있게 한다. 소비자(write_rental_history_completion_marker)는
    status와 bronze_row_count_by_partition만 보므로 하위호환이 깨지지 않는다.
    """
    target_date = item["target_date"]
    return {
        "dataset": DATASET,
        "run_date": target_date,
        "promotion_id": build_promotion_id(catchup_cutoff(date.fromisoformat(target_date))),
        "status": "COMPLETE",
        "mode": "CATCHUP",
        "source": "catchup_final_raw",
        "promoted_partitions": [target_date],
        "bronze_row_count_by_partition": {
            target_date: partition_counts.get(target_date, item["row_count"])
        },
        "selected_snapshots": [
            {
                "target_date": target_date,
                "snapshot_type": "FINAL",
                "payload_key": item["source_file"],
                "manifest_key": item["manifest_key"],
                "row_count": item["row_count"],
            }
        ],
        "batch_id": batch_id,
        "batch_size": batch_size,
        "batch_dates": batch_dates,
        "promoted_at": datetime.now(timezone.utc).isoformat(),
    }


def promote_batch(
    bucket: str,
    dates: list[str],
    *,
    batch_id: str,
    batch_size: int,
    read_json: Optional[Callable[[str, str], object]] = None,
    write_json: Optional[Callable[[str, str, object], None]] = None,
    promote_fn: Optional[Callable[[list[dict]], dict[str, int]]] = None,
) -> dict:
    """넘겨받은 날짜를 한 번의 commit으로 반영하고, 그 뒤에 날짜별 marker를 쓴다."""
    writer = write_json or put_json
    committer = promote_fn or promote

    if not dates:
        logger.info("배치 %s: 처리할 날짜 없음", batch_id)
        return {"batch_id": batch_id, "promoted_dates": []}

    payloads = load_payloads(bucket, dates, read_json=read_json)

    # 커밋이 끝나기 전에는 어떤 marker도 쓰지 않는다. 여기서 실패하면 promotion marker가
    # 없으므로 completion marker가 FAILED로 판정되고 watermark가 그 날짜에서 멈춘다.
    partition_counts = committer(payloads)

    for item in payloads:
        document = build_promotion_document(
            item,
            partition_counts,
            batch_id=batch_id,
            batch_size=batch_size,
            batch_dates=dates,
        )
        writer(bucket, promotion_marker_key(date.fromisoformat(item["target_date"])), document)

    promoted_dates = [item["target_date"] for item in payloads]
    logger.info(
        "배치 %s 승격 완료: 커밋 1회, %d일 %s", batch_id, len(promoted_dates), promoted_dates
    )
    return {"batch_id": batch_id, "promoted_dates": promoted_dates}


def run() -> dict:
    raw = (os.getenv("BACKFILL_TARGET_DATES") or "").strip()
    if not raw:
        raise CatchupPromotionError("BACKFILL_TARGET_DATES가 필요합니다 (콤마 구분)")
    dates = [d.strip() for d in raw.split(",") if d.strip()]
    if not dates:
        raise CatchupPromotionError(f"처리할 날짜가 없습니다: {raw!r}")

    batch_size = int(os.getenv("RENTAL_HISTORY_PROMOTE_BATCH_SIZE") or DEFAULT_BATCH_SIZE)
    if batch_size < 1:
        raise CatchupPromotionError(f"RENTAL_HISTORY_PROMOTE_BATCH_SIZE는 1 이상이어야 합니다: {batch_size}")

    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    batch_id = os.getenv("RENTAL_HISTORY_BATCH_ID") or build_promotion_id(
        catchup_cutoff(date.fromisoformat(dates[0]))
    )
    result = promote_batch(bucket, dates, batch_id=batch_id, batch_size=batch_size)
    print(json.dumps(result, ensure_ascii=False))
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
