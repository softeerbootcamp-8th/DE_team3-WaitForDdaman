"""Bronze 원천별 Reconciliation 대상 날짜를 계산한다."""
from __future__ import annotations

import json
import os
from datetime import date, timedelta

import config
from common.s3_utils import ensure_bucket, get_json
from common.watermark import read_watermark
from config.watermark_keys import DATASET_WATERMARK_KEYS

COMPLETION_PREFIXES = {
    "rental_history": "_meta/completion/bronze_rental_history",
    "failure_report": "_meta/completion/bronze_failure_report",
}
ACCEPTED_STATUS = {
    "rental_history": {"COMPLETE", "MANUALLY_CONFIRMED_EMPTY"},
    "failure_report": {"COMPLETE", "COMPLETE_EMPTY"},
}


def marker_key(dataset: str, target_date: date) -> str:
    return (
        f"{COMPLETION_PREFIXES[dataset]}/target_date={target_date.isoformat()}"
        "/completion.json"
    )


def run() -> list[str]:
    dataset = os.getenv("DATASET", "").strip()
    target_value = os.getenv("RECONCILIATION_TARGET_DATE", "").strip()
    if dataset not in COMPLETION_PREFIXES:
        raise ValueError(f"지원하지 않는 DATASET: {dataset!r}")
    if not target_value:
        raise ValueError("RECONCILIATION_TARGET_DATE가 필요합니다")

    target_date = date.fromisoformat(target_value)
    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)
    watermark = read_watermark(watermark_key=DATASET_WATERMARK_KEYS[dataset])

    missing: list[str] = []
    current = watermark + timedelta(days=1)
    while current <= target_date:
        marker = get_json(bucket, marker_key(dataset, current))
        if not isinstance(marker, dict) or marker.get("status") not in ACCEPTED_STATUS[dataset]:
            missing.append(current.isoformat())
        current += timedelta(days=1)

    print(json.dumps(missing, ensure_ascii=False))
    return missing


if __name__ == "__main__":
    run()
