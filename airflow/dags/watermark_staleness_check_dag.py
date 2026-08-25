"""
watermark_staleness_check - 전체 데이터셋 워터마크가 정체됐는지 매일 확인 (Issue #180)

Lambda 실행 에러/DLQ 알림(infra/lambdas/notify_slack)이나 DAG 태스크 실패 알림
(dag_common.notify_slack_on_failure)은 모두 "무언가 실행되다 실패"를 감지한다.
반대로 스케줄러 장애 등으로 DAG 자체가 트리거되지 않거나 계속 조용히 스킵되는
경우는 그 두 경로 어디에도 안 걸린다. 이 DAG는 결과 데이터(워터마크)를 직접
읽어서 "파이프라인 전체가 며칠째 멈춰 있는지"를 감지하는 별도 안전망이다.

check_watermark_staleness.run()이 정체를 감지하면 예외를 던져 태스크가 실패하고,
dag_common.notify_slack_on_failure(on_failure_callback)가 Slack 알림을 보낸다 -
이 체크 자체를 위한 별도 알림 경로를 새로 만들지 않고 기존 경로를 그대로 재사용한다.
"""
import os
import sys

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

from dag_common import notify_slack_on_failure

PYLIB_DIR = "/opt/airflow/pylib"
INGESTION_DIR = "/opt/airflow/ingestion"


def _load_ingestion_env(env_path: str) -> None:
    """gold_dim_fact_dag.py와 동일한 패턴 - ingestion/.env 값을 컨테이너 환경변수
    위에 그대로 덮어쓴다. 그 컨테이너 밖(CI/로컬 DagBag 파싱 등)에서는 파일이
    없는 게 정상이라 조용히 스킵한다."""
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip()


_load_ingestion_env(f"{INGESTION_DIR}/.env")
if PYLIB_DIR not in sys.path:
    sys.path.insert(0, PYLIB_DIR)
if INGESTION_DIR not in sys.path:
    sys.path.insert(0, INGESTION_DIR)

from jobs.check_watermark_staleness import run as check_watermark_staleness  # noqa: E402

default_args = {
    "retries": 1,  # S3 일시 장애 대비 - 그 이상 재시도해도 워터마크 값 자체는 안 바뀐다
    "on_failure_callback": notify_slack_on_failure,
}


@dag(
    dag_id="watermark_staleness_check",
    # 매일 09:00 KST - bronze_daily_batch_all_sources(06:00)/gold_dim_fact(08:00) 등
    # 그날의 배치가 대부분 끝났을 시점 이후로 잡아 오탐(아직 안 끝났을 뿐인데 정체로
    # 오판)을 줄인다.
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 8, 23, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["independent", "monitoring"],
    doc_md=__doc__,
)
def watermark_staleness_check():
    PythonOperator(
        task_id="check_watermark_staleness",
        python_callable=check_watermark_staleness,
    )


watermark_staleness_check()
