"""
Gold DAG(gold_dim_fact)의 wait_for_silver 단계 - 워터마크 기반 Silver 소스
준비 확인용

### 왜 ExternalTaskSensor 대신 워터마크 직접 확인인가 (2026-08-17, #50)
rental_history/station_master/station_active Silver DAG가 Bronze 완료
Asset으로 트리거되도록 전환되면서, DagRun의 logical_date가 정해진 스케줄
그리드에 맞춰 정렬되지 않는다(수동 Asset 이벤트로 만든 DagRun은 logical_date가
아예 null인 경우도 실측 확인됨). ExternalTaskSensor는
"이 DAG의 logical_date - execution_delta = 대상 DAG의 logical_date"로
상류 DagRun을 찾는데, 이 전제(고정 스케줄) 자체가 깨져서 대상을 못 찾고
매번 타임아웃난다.

Airflow 3의 Task SDK가 태스크 실행 컨텍스트에서 메타데이터 DB 직접 접근을
막아서(`DagRun.find()` 같은 ORM 조회 시도는 RuntimeError로 실패), DagRun
상태를 직접 조회하는 대안도 쓸 수 없다. 대신 실제로 Silver가 어디까지
처리했는지를 나타내는 워터마크 파일(S3)을 직접 읽어서 판단한다.

### REQUIRED_OFFSET_DAYS
이 파이프라인의 daily_batch들은 T-1(어제까지만 확정 처리) 구조인 것들이
있어(rental_history 등), 그런 소스는 워터마크가 오늘(target_date) 값에
도달하지 못한다 - REQUIRED_OFFSET_DAYS로 필요한 만큼 과거로 보정한다.

이 스크립트는 성공/실패가 아니라 "아직 준비 안 됨"을 나타내야 하므로, 준비 안 됐을
때도 예외 없이 False/exit code 1로 조용히 끝낸다 (PythonSensor가 poke_interval마다
재시도).

### PythonSensor로 전환 (2026-08-22, #145)
판정 로직(워터마크 비교)은 그대로 두고, BashSensor의 exit-code 우회 대신
is_ready()를 PythonSensor python_callable로 직접 호출하도록 정리한다.

사용법 (PythonSensor에서):
    is_ready("rental_history", "2026-08-17", required_offset_days=1)

CLI로도 그대로 쓸 수 있다:
    TARGET_DATE=2026-08-17 DATASET=rental_history REQUIRED_OFFSET_DAYS=1 \
        python -m jobs.check_silver_watermark
"""
import logging
import os
import sys
from datetime import datetime, timedelta

from common.watermark import read_watermark
from config.watermark_keys import SILVER_FAILURE_REPORT, SILVER_RENTAL_HISTORY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 워터마크 키는 항상 config/watermark_keys.py 상수를 참조해서 다른 코드와 값이
# 어긋나지 않게 한다.
DATASET_WATERMARK_KEYS = {
    "rental_history": SILVER_RENTAL_HISTORY,
    "failure_report": SILVER_FAILURE_REPORT,
}


def is_ready(dataset: str, target_date: str, required_offset_days: int = 0) -> bool:
    if dataset not in DATASET_WATERMARK_KEYS:
        logger.error("알 수 없는 DATASET: %s (가능한 값: %s)", dataset, list(DATASET_WATERMARK_KEYS))
        return False

    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    required_date = target - timedelta(days=required_offset_days)

    watermark_key = DATASET_WATERMARK_KEYS[dataset]
    watermark = read_watermark(watermark_key=watermark_key)

    if watermark >= required_date:
        logger.info("%s 준비 완료 (워터마크=%s >= 필요일=%s)", dataset, watermark, required_date)
        return True

    logger.info("%s 아직 준비 안 됨 (워터마크=%s < 필요일=%s)", dataset, watermark, required_date)
    return False


def run() -> None:
    offset_days = int(os.getenv("REQUIRED_OFFSET_DAYS", "0"))
    ready = is_ready(os.environ["DATASET"], os.environ["TARGET_DATE"], offset_days)
    sys.exit(0 if ready else 1)


if __name__ == "__main__":
    run()
