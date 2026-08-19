"""
Gold DAG(dag_gold_dim_fact)의 wait_for_silver 단계 - bikeman_action 준비 확인용

### 왜 ExternalTaskSensor를 안 쓰는가
silver_bikeman_action_daily DAG는 고정 cron이 아니라 Asset(bikeman_event_bronze)
트리거라 logical_date가 매일 정해진 시각으로 정렬되지 않는다. ExternalTaskSensor는
"이 DAG의 logical_date - execution_delta = 대상 DAG의 logical_date"로 매칭하는데,
Asset 트리거 DAG는 이 계산이 통하지 않아 대상 DagRun을 못 찾고 계속 실패할 위험이
있다. 대신 실제로 Silver가 어디까지 처리했는지를 나타내는 워터마크 파일을 직접
읽어서 "Gold DAG의 오늘(ds)까지 Silver가 끝났는가"만 확인한다.

이 스크립트는 성공/실패가 아니라 "아직 준비 안 됨"을 나타내야 하므로, 준비 안 됐을
때도 예외 없이 exit code 1로 조용히 끝낸다 (BashSensor가 poke_interval마다 재시도).

### TARGET_DATE는 호출부에서 이미 하루 전으로 넘어온다 (2026-08-17 수정, #52)
ingestion/jobs/daily_batch_bikeman_event.py는 "작업자가 몰아서 제출하는 경우가
많아 오늘은 항상 미확정"이라는 이유로 항상 `end_date = date.today() - 1`까지만
처리한다(파일 docstring 참고) - 즉 silver_bikeman_action의 워터마크는 구조적으로
실행일보다 항상 하루 늦다. 이 스크립트가 예전에 `TARGET_DATE={{ ds }}`(오늘)를
그대로 받아 비교했을 때는, 워터마크가 절대 오늘에 도달할 수 없어 센서가 영원히
"준비 안 됨"만 반복하다 타임아웃났다. 그래서 호출부(gold_dim_fact_dag.py)가
`{{ macros.ds_add(ds, -1) }}`로 이미 하루 전 날짜를 넘기도록 고쳤고, 이 스크립트는
그 값을 그대로(추가 보정 없이) 비교하면 된다.

사용법 (BashSensor에서, 호출부가 이미 하루 전 날짜를 넘김):
    TARGET_DATE=2026-08-16 python -m jobs.check_silver_bikeman_action_watermark
"""
import logging
import os
import sys
from datetime import datetime

from common.watermark import read_watermark
from config.watermark_keys import SILVER_BIKEMAN_ACTION

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run() -> None:
    target_date = datetime.strptime(os.environ["TARGET_DATE"], "%Y-%m-%d").date()
    watermark = read_watermark(watermark_key=SILVER_BIKEMAN_ACTION)

    if watermark >= target_date:
        logger.info("silver.bikeman_action 준비 완료 (워터마크=%s >= 대상일=%s)", watermark, target_date)
        sys.exit(0)

    logger.info("silver.bikeman_action 아직 준비 안 됨 (워터마크=%s < 대상일=%s)", watermark, target_date)
    sys.exit(1)


if __name__ == "__main__":
    run()
