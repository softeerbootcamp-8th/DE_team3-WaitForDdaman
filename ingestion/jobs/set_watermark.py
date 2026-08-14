"""
초기 워터마크 설정 - 백필(파일 기반)이 커버한 마지막 날짜를 워터마크로 기록한다.

왜 필요한가: 워터마크가 한 번도 기록된 적이 없으면 daily_batch_*.py는
`.env`의 BACKFILL_START_DATE(기본 2015-01-01)부터 API로 다시 긁으려고 시도한다.
파일로 이미 백필한 기간을 API로 또 채우는 건 중복 작업이다.

백필이 끝나면 이 스크립트를 1회 실행해서, daily_batch가 백필 마지막 날짜의
다음날부터만 이어서 처리하도록 만든다. 데이터셋마다 워터마크가 분리되어 있으므로
DATASET으로 지정한다.

사용법:
    WATERMARK_DATE=2026-06-30 DATASET=rental_history python -m jobs.set_watermark
    WATERMARK_DATE=2026-06-30 DATASET=failure_report python -m jobs.set_watermark
    WATERMARK_DATE=2026-06-30 DATASET=silver_rental_history python -m jobs.set_watermark
    WATERMARK_DATE=2026-06-30 DATASET=gold_dim_bike python -m jobs.set_watermark
"""
import logging
import os
import sys
from datetime import datetime, timedelta

from common.watermark import write_watermark
from config.watermark_keys import (
    BRONZE_FAILURE_REPORT,
    BRONZE_RENTAL_HISTORY,
    GOLD_DIM_BIKE,
    SILVER_RENTAL_HISTORY,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 데이터셋명 -> 워터마크 키. 실제 키 문자열은 config/watermark_keys.py 한 곳에서만
# 관리한다 (각 daily_batch_*.py / staging / pipeline 잡과 반드시 같은 값을 참조해야 함).
WATERMARK_KEYS = {
    "rental_history": BRONZE_RENTAL_HISTORY,
    "failure_report": BRONZE_FAILURE_REPORT,
    "silver_rental_history": SILVER_RENTAL_HISTORY,
    "gold_dim_bike": GOLD_DIM_BIKE,
}


def run(date_str: str, dataset: str) -> None:
    if dataset not in WATERMARK_KEYS:
        print(f"알 수 없는 DATASET: {dataset} (가능한 값: {list(WATERMARK_KEYS.keys())})")
        sys.exit(1)

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