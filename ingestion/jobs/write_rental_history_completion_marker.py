"""날짜 단위 대여이력 Backfill의 완료 marker를 기록한다.

이 잡은 전역 Bronze confirmed watermark를 읽거나 갱신하지 않는다. 날짜별 DagRun의
실제 결과만 `_meta/completion/bronze_rental_history/target_date=.../completion.json`에
남겨 Historical Reconciliation이 연속 완료 구간을 계산할 수 있게 한다.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import config
from common.s3_utils import ensure_bucket, get_json, put_json
from jobs.collect_rental_history_raw import parse_collection_cutoff, snapshot_keys
from jobs.rental_history_snapshot_policy import build_promotion_id, promotion_key

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KST = ZoneInfo("Asia/Seoul")
COMPLETION_PREFIX = "_meta/completion/bronze_rental_history"


def completion_key(target_date: str) -> str:
    return f"{COMPLETION_PREFIX}/target_date={target_date}/completion.json"


def run() -> dict:
    target_date = os.getenv("BACKFILL_TARGET_DATE")
    cutoff_value = os.getenv("COLLECTION_CUTOFF_AT")
    if not target_date or not cutoff_value:
        raise ValueError("BACKFILL_TARGET_DATE and COLLECTION_CUTOFF_AT are required")

    cutoff = parse_collection_cutoff(cutoff_value)
    if cutoff.date().isoformat() != target_date:
        raise ValueError("BACKFILL_TARGET_DATE must equal COLLECTION_CUTOFF_AT date")

    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)
    _, manifest_key = snapshot_keys(cutoff.date(), cutoff, "FINAL")
    manifest = get_json(bucket, manifest_key)
    promotion = get_json(bucket, promotion_key(cutoff.date(), build_promotion_id(cutoff)))

    now = datetime.now(timezone.utc).isoformat()
    if isinstance(promotion, dict) and promotion.get("status") == "COMPLETE":
        row_count = sum((promotion.get("bronze_row_count_by_partition") or {}).values())
        status = "COMPLETE"
        error = None
    elif isinstance(manifest, dict) and manifest.get("status") == "COMPLETE_EMPTY":
        row_count = 0
        status = "COMPLETE_EMPTY"
        error = "원천 API가 0행을 반환해 수동 확인 필요"
    else:
        row_count = int((manifest or {}).get("row_count") or 0) if isinstance(manifest, dict) else 0
        status = "FAILED"
        error = (manifest or {}).get("error") if isinstance(manifest, dict) else "FINAL manifest 없음"

    marker = {
        "dataset": "rental_history",
        "target_date": target_date,
        "status": status,
        "row_count": row_count,
        "started_at": os.getenv("BACKFILL_STARTED_AT") or now,
        "completed_at": now,
        "dag_run_id": os.getenv("DAG_RUN_ID", "unknown"),
        "source": "seoul_open_api",
        "error": error,
    }
    put_json(bucket, completion_key(target_date), marker)
    logger.info("completion marker 기록: target_date=%s status=%s row_count=%d", target_date, status, row_count)

    if status != "COMPLETE":
        print(marker)
        return marker
    print(marker)
    return marker


if __name__ == "__main__":
    try:
        result = run()
        if result["status"] != "COMPLETE":
            sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - marker 실패는 DagRun 실패로 남겨야 함
        logger.error("completion marker 기록 실패: %s", exc)
        sys.exit(1)
