"""
초기 워터마크 설정 - 백필(파일 기반)이 커버한 마지막 날짜를 워터마크로 기록한다.

왜 필요한가: 워터마크가 한 번도 기록된 적이 없으면 daily_batch_*.py는
`.env`의 BACKFILL_START_DATE(기본 2015-01-01)부터 API로 다시 긁으려고 시도한다.
파일로 이미 백필한 기간을 API로 또 채우는 건 중복 작업이다.

백필이 끝나면 이 스크립트를 1회 실행해서, daily_batch가 백필 마지막 날짜의
다음날부터만 이어서 처리하도록 만든다. 데이터셋마다 워터마크가 분리되어 있으므로
DATASET으로 지정한다.

bikeman_event는 "백필"이 아니라 "서비스 시작일"이 기준이다 - 따맨(bikeman)은 파일
백필이 없고 6/30부터 Postgres에 데이터가 이미 존재하므로, 최초 실행 전에 이 스크립트로
2026-06-29(서비스 시작 전날)를 워터마크로 찍어야 daily_batch가 6/30부터 정확히
처리한다.

사용법:
    WATERMARK_DATE=2026-06-30 DATASET=rental_history python -m jobs.set_watermark
    WATERMARK_DATE=2026-06-30 DATASET=failure_report python -m jobs.set_watermark
    WATERMARK_DATE=2026-06-29 DATASET=bikeman_event python -m jobs.set_watermark
"""
import logging
import os
import sys
from datetime import datetime, timedelta

import config
from common.s3_utils import ensure_bucket
from common.watermark import write_watermark
from config.watermark_keys import GOLD_DIM_BIKE, SILVER_BIKE_MAN_ACTION, SILVER_RENTAL_HISTORY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 데이터셋명 -> 워터마크 키 (각 daily_batch_*.py/silver/gold 모듈에 정의된 것과 동일해야 함)
# silver_rental_history/gold_dim_bike는 이 DAG의 params 안내에는 있었지만 실제 매핑이
# 빠져있던 버그 - config/watermark_keys.py의 상수를 그대로 재사용해 값이 어긋나지 않게 한다.
WATERMARK_KEYS = {
    "rental_history": "_meta/watermark/rental_history.json",
    "failure_report": "_meta/watermark/failure_report.json",
    "bikeman_event": "_meta/watermark/bikeman_event.json",
    "silver_bike_man_action": SILVER_BIKE_MAN_ACTION,
    "silver_rental_history": SILVER_RENTAL_HISTORY,
    "gold_dim_bike": GOLD_DIM_BIKE,
}


def run(date_str: str, dataset: str) -> None:
    if dataset not in WATERMARK_KEYS:
        print(f"알 수 없는 DATASET: {dataset} (가능한 값: {list(WATERMARK_KEYS.keys())})")
        sys.exit(1)

    # 다른 잡들은 backfill 단계에서 버킷을 먼저 만들어두지만, 이 스크립트가
    # 가장 먼저(backfill 없이) 실행되는 데이터셋(예: bikeman_event)도 있으므로
    # 여기서도 명시적으로 버킷 존재를 보장한다. (NoSuchBucket 방지)
    ensure_bucket(config.SETTINGS.raw_bucket)

    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    watermark_key = WATERMARK_KEYS[dataset]
    write_watermark(target_date, watermark_key=watermark_key)
    logger.info(
        "[%s] 워터마크를 %s로 설정했습니다. 다음 daily_batch 실행은 %s부터 처리합니다.",
        dataset,
        target_date,
        target_date + timedelta(days=1),
    )


if __name__ == "__main__":
    date_str = os.getenv("WATERMARK_DATE")
    dataset = os.getenv("DATASET", "rental_history")  # 기존 사용자 호환을 위해 기본값 유지
    if not date_str:
        print("사용법: WATERMARK_DATE=YYYY-MM-DD DATASET=rental_history python -m jobs.set_watermark")
        sys.exit(1)
    run(date_str, dataset)
