"""
bikeman_event_generator - Gold 마트 동기화 결과(serving.bike_risk_daily.action='수거')를
근거로 bikeman(현장 작업자)의 수거·배치 행동을 시뮬레이션해 bikeman.fact_worker_event에
이벤트를 적재한다. gold_to_serving_sync의 verify_bike_risk_daily_sync가 끝나면
TriggerDagRunOperator로 트리거된다 (station_daily 브랜치와는 무관 - 이 DAG가 읽는 건
bike_risk_daily뿐이라 그 완료를 기다리지 않는다).

generate_collect_events/deploy_returned_bikes 두 태스크는 서로 다른 event_type/자전거
집합을 다루는 독립 작업이라 병렬 실행한다 (gold_to_serving_sync의 bike_risk_daily/
station_daily 브랜치 병렬 설계와 동일한 이유).

### 왜 BashOperator가 아니라 PythonOperator + PostgresHook인가
이 저장소의 다른 모든 job은 psycopg2 + .env 직접 연결 + `python -m jobs.X` 단독 실행
컨벤션을 따르지만, 이 DAG는 사용자 확정에 따라 Airflow UI Connection(bikeman_postgres)
+ PostgresHook을 쓴다 (docs/superpowers/specs/2026-08-18-bikeman-event-generator-design.md 참고).
"""
import sys
from datetime import timedelta

import pendulum
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

JOBS_DIR = "/opt/airflow/pipeline/bikeman_event_generator/jobs"
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

import deploy_returned_bikes  # noqa: E402
import generate_collect_events  # noqa: E402

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _notify_slack_on_failure(context: dict) -> None:
    import os

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    ti = context["task_instance"]
    message = f":x: *{ti.dag_id}.{ti.task_id}* 실패\n실행일: {context['ds']}\n로그: {ti.log_url}"
    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
    except requests.RequestException:
        pass


default_args["on_failure_callback"] = _notify_slack_on_failure


def _target_date(context: dict) -> str:
    return context["dag_run"].conf.get("snapshot_date") or context["ds"]


def _run_generate_collect_events(**context) -> None:
    generate_collect_events.run(_target_date(context))


def _run_deploy_returned_bikes(**context) -> None:
    deploy_returned_bikes.run(_target_date(context))


@dag(
    dag_id="bikeman_event_generator",
    schedule=None,  # gold_to_serving_sync의 TriggerDagRunOperator로만 실행
    start_date=pendulum.datetime(2026, 8, 18, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["daily_batch", "bikeman"],
    doc_md=__doc__,
)
def bikeman_event_generator():
    PythonOperator(
        task_id="generate_collect_events",
        python_callable=_run_generate_collect_events,
        execution_timeout=timedelta(minutes=10),
    )
    PythonOperator(
        task_id="deploy_returned_bikes",
        python_callable=_run_deploy_returned_bikes,
        execution_timeout=timedelta(minutes=10),
    )


bikeman_event_generator()
