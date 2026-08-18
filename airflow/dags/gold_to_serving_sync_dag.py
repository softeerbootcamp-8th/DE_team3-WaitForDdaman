# airflow/dags/gold_to_serving_sync_dag.py
"""
gold_to_serving_sync - Gold Iceberg 마트를 서빙 Postgres(station_daily/bike_risk_daily)로
동기화한다. dag_risk_decision의 마지막 태스크(build_fact_bike_decision)가 끝나면
TriggerDagRunOperator로 이 DAG를 트리거한다 (dag_gold_dim_fact가 아님 - bike_risk_daily가
필요로 하는 fact_bike_risk/fact_bike_decision은 dag_risk_decision의 산출물이라 그게
끝나야 두 마트 모두 만들 재료가 갖춰짐).

두 브랜치(bike_risk_daily / station_daily)는 서로 의존하지 않아 병렬 실행한다.

### 실패 전파를 끊는 이유
트리거는 wait_for_completion=False다 - 이 DAG가 실패해도 이미 만들어진 gold 데이터
자체는 유효하므로 dag_risk_decision을 실패로 만들 이유가 없다. 대신 각 태스크에
Slack 알림을 건다 (CloudWatch/SNS는 이 프로젝트에 대응 AWS 인프라가 없어 스코프 제외 -
spec §2/§3 참고).

### trigger_bikeman_event_generator (2026-08-18 추가)
verify_bike_risk_daily_sync가 끝나면 bikeman_event_generator를 트리거한다
(station_daily 브랜치와는 무관 - 그 DAG가 읽는 건 bike_risk_daily뿐이라 완료를
기다리지 않는다). 세부 설계는
docs/superpowers/specs/2026-08-18-bikeman-event-generator-design.md 참고.
"""
from datetime import timedelta

import pendulum
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag

SERVING_SYNC_DIR = "/opt/airflow/pipeline/serving_sync"
INGESTION_DIR = "/opt/airflow/ingestion"
PYTHON = "python"

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


def _bash(job_module: str, extra_env: str = "") -> str:
    return (
        f"cd {SERVING_SYNC_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:{SERVING_SYNC_DIR}/jobs:$PYTHONPATH "
        f"PYTHONDONTWRITEBYTECODE=1 {extra_env}{PYTHON} -m jobs.{job_module}"
    )


def _verify_bash(iceberg_table: str, postgres_table: str) -> str:
    # dag_risk_decision이 conf로 넘긴 snapshot_date가 있으면 그걸 우선한다 (아래
    # SNAPSHOT_DATE 주석 참고) - 없으면(수동 트리거 등) 이 DAG 자신의 ds로 폴백.
    return _bash(
        "verify_serving_sync",
        f"ICEBERG_TABLE=bike_catalog.gold.{iceberg_table} POSTGRES_TABLE={postgres_table} "
        "SNAPSHOT_DATE='{{ dag_run.conf.get(\"snapshot_date\") or ds }}' ",
    )


@dag(
    dag_id="gold_to_serving_sync",
    schedule=None,  # dag_risk_decision의 TriggerDagRunOperator로만 실행
    start_date=pendulum.datetime(2026, 8, 18, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["daily_batch", "serving"],
    doc_md=__doc__,
)
def gold_to_serving_sync():
    # 아래 네 태스크의 SNAPSHOT_DATE는 모두 dag_run.conf.get("snapshot_date")를 자기
    # ds보다 우선한다 - dag_risk_decision이 conf로 trigger_serving_sync에서 넘긴 날짜가
    # 있으면 그걸 쓰고(백필/미래 스케줄 실행에서 두 DAG의 날짜가 어긋나지 않게), 수동
    # 트리거처럼 conf가 없으면 기존과 동일하게 이 DAG 자신의 ds로 폴백한다.
    build_mart_bike_risk_daily = BashOperator(
        task_id="build_mart_bike_risk_daily",
        bash_command=_bash("build_mart_bike_risk_daily", "SNAPSHOT_DATE='{{ dag_run.conf.get(\"snapshot_date\") or ds }}' "),
        execution_timeout=timedelta(minutes=20),
    )
    write_bike_risk_daily = BashOperator(
        task_id="write_bike_risk_daily",
        bash_command=_bash("write_bike_risk_daily", "SNAPSHOT_DATE='{{ dag_run.conf.get(\"snapshot_date\") or ds }}' "),
        execution_timeout=timedelta(minutes=15),
    )
    verify_bike_risk_daily_sync = BashOperator(
        task_id="verify_bike_risk_daily_sync",
        bash_command=_verify_bash("mart_bike_risk_daily", "bike_risk_daily"),
        execution_timeout=timedelta(minutes=10),
    )
    trigger_bikeman_event_generator = TriggerDagRunOperator(
        task_id="trigger_bikeman_event_generator",
        trigger_dag_id="bikeman_event_generator",
        # logical_date는 일부러 지정하지 않는다 - "{{ logical_date }}"로 명시했더니 이
        # DAG가 schedule=None이라 conf만 넘기고 --logical-date 없이 트리거된 실행에서는
        # dag_run.logical_date가 None이 되고, 그 경우 Jinja 컨텍스트에 logical_date
        # 키 자체가 주입되지 않아 UndefinedError로 매번 실패했다(airflow 3.3, 실측 확인).
        # bikeman_event_generator는 날짜를 conf.snapshot_date로만 받으므로(아래 conf
        # 참고) 트리거되는 DAG run 자체의 logical_date는 어떤 값이어도 무방하다 -
        # 파라미터를 아예 생략하면 TriggerDagRunOperator가 기본값(NOTSET)으로 두고
        # 실행 시점에 timezone.utcnow()를 자동으로 채워 넣는다.
        conf={"snapshot_date": "{{ dag_run.conf.get(\"snapshot_date\") or ds }}"},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    build_mart_station_daily = BashOperator(
        task_id="build_mart_station_daily",
        bash_command=_bash("build_mart_station_daily", "SNAPSHOT_DATE='{{ dag_run.conf.get(\"snapshot_date\") or ds }}' "),
        execution_timeout=timedelta(minutes=20),
    )
    write_station_daily = BashOperator(
        task_id="write_station_daily",
        bash_command=_bash("write_station_daily", "SNAPSHOT_DATE='{{ dag_run.conf.get(\"snapshot_date\") or ds }}' "),
        execution_timeout=timedelta(minutes=15),
    )
    verify_station_daily_sync = BashOperator(
        task_id="verify_station_daily_sync",
        bash_command=_verify_bash("mart_station_daily", "station_daily"),
        execution_timeout=timedelta(minutes=10),
    )

    build_mart_bike_risk_daily >> write_bike_risk_daily >> verify_bike_risk_daily_sync >> trigger_bikeman_event_generator
    build_mart_station_daily >> write_station_daily >> verify_station_daily_sync


gold_to_serving_sync()
