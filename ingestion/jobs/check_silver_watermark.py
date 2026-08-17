"""
Gold DAG(dag_gold_dim_fact)의 wait_for_silver 단계 - Asset 트리거 Silver DAG 준비 확인용 (범용)

### 왜 ExternalTaskSensor / DagRun 직접 조회를 안 쓰는가
- ExternalTaskSensor: Asset 트리거로 생성된 DagRun은 logical_date가 null이라
  "logical_date - execution_delta"로 상류 DagRun을 매칭할 수 없다.
- DagRun.find() 등 airflow.models ORM 직접 조회: Airflow 3의 Task SDK가 태스크
  코드에서 메타데이터 DB에 직접 접근하는 걸 막는다
  ("RuntimeError: Direct database access via the ORM is not allowed in
  Airflow 3.0" - 2026-08-17 실측).

대신 Silver 잡이 성공적으로 커밋했을 때 직접 쓰는 워터마크 파일(S3)을 그대로 읽어
판단한다 - bike_man_action(check_silver_bike_man_action_watermark.py)이 이미 쓰던
방식과 동일하고, 확인 대상 워터마크 키만 파라미터로 받는다.

이 스크립트는 성공/실패가 아니라 "아직 준비 안 됨"을 나타내야 하므로, 준비 안 됐을
때도 예외 없이 exit code 1로 조용히 끝낸다 (BashSensor가 poke_interval마다 재시도).

사용법 (BashSensor에서):
    TARGET_DATE=2026-08-17 WATERMARK_KEY_NAME=SILVER_STATION_MASTER \
        python -m jobs.check_silver_watermark
"""
import logging
import os
import sys
from datetime import datetime

import config.watermark_keys as watermark_keys
from common.watermark import read_watermark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run() -> None:
    target_date = datetime.strptime(os.environ["TARGET_DATE"], "%Y-%m-%d").date()
    key_name = os.environ["WATERMARK_KEY_NAME"]
    watermark_key = getattr(watermark_keys, key_name)
    watermark = read_watermark(watermark_key=watermark_key)

    if watermark >= target_date:
        logger.info("%s 준비 완료 (워터마크=%s >= 대상일=%s)", key_name, watermark, target_date)
        sys.exit(0)

    logger.info("%s 아직 준비 안 됨 (워터마크=%s < 대상일=%s)", key_name, watermark, target_date)
    sys.exit(1)


if __name__ == "__main__":
    run()
