"""고장신고 하루치 API/Raw/Bronze 적재와 날짜 completion marker 기록."""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timezone

import config
from common.s3_utils import ensure_bucket, put_json
from bronze.daily_batch_failure_report import _process_one_day

COMPLETION_PREFIX = "_meta/completion/bronze_failure_report"


def run() -> dict:
    target_value = os.getenv("TARGET_DATE", "").strip()
    if not target_value:
        raise ValueError("TARGET_DATE가 필요합니다")
    target_date = date.fromisoformat(target_value)
    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        row_count = _process_one_day(target_date)
        status = "COMPLETE_EMPTY" if row_count == 0 else "COMPLETE"
        error = None
    except Exception as exc:  # noqa: BLE001 - marker 기록 후 DagRun을 실패시킨다
        status = "FAILED"
        row_count = 0
        error = str(exc)

    marker = {
        "dataset": "failure_report",
        "target_date": target_value,
        "status": status,
        "row_count": row_count,
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "dag_run_id": os.getenv("DAG_RUN_ID", "unknown"),
        "source": "seoul_open_api",
        "error": error,
    }
    put_json(bucket, f"{COMPLETION_PREFIX}/target_date={target_value}/completion.json", marker)
    print(json.dumps(marker, ensure_ascii=False))
    if status == "FAILED":
        sys.exit(1)
    return marker


if __name__ == "__main__":
    run()
