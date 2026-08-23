"""
bikeman_event_generator - Gold 마트 동기화 결과를 근거로 bikeman(현장 작업자)의
수거·배치 행동을 시뮬레이션해 bikeman.fact_worker_event에 이벤트를 적재한다.
gold_to_serving_sync의 verify_bike_risk_daily_sync가 끝나면 TriggerDagRunOperator로
트리거된다.

serving.bike_risk_daily에서 action 컬럼이 제거된 뒤에는 최신 snapshot의 risk_score
상위 500대를 generate_collect_events의 COLLECT 대상으로 삼는다. DEPLOY 이벤트는
기존 bikeman.fact_worker_event의 전날 COLLECT 이력 기준으로 계속 생성된다.

generate_collect_events/deploy_returned_bikes는 애초에 서로 다른 event_type/자전거
집합을 다루는 독립 작업으로 보고 병렬 실행했다. Task 9 E2E 백필 검증 중 deploy_
returned_bikes가 매번 대상 0건을 반환하는 문제를 발견했는데, 근본 원인은 이번
백필보다 먼저 적재돼 있던 2026-09-01 COLLECT 배치(Task 5)였다 - fetch_deploy_targets
가 "가장 최근 이벤트"를 occurred_at(비즈니스 날짜) 기준으로 판별하다 보니, 실제
삽입 시각과 무관하게 날짜값이 미래인 09-01 COLLECT가 07-18~08-17 전 구간에서
계속 "최신"으로 잡혀 매일의 "어제 COLLECT" 조회를 가려버렸다(자세한 검증은
E2E_VERIFICATION.md 참고). 그와 별개로, 두 태스크를 병렬로 두면 "수거" 스냅샷이
여러 날 재사용되는 상황에서 generate_collect_events가 오늘자 COLLECT를 deploy_
returned_bikes의 "어제 COLLECT" 조회보다 먼저 커밋해 같은 방식으로 대상을 놓칠
잠재적 여지도 있어, 재발 방지 차원에서 deploy_returned_bikes >> generate_collect_events
로 순서를 강제한다.

### 왜 BashOperator가 아니라 PythonOperator + PostgresHook인가
이 저장소의 다른 모든 job은 psycopg2 + .env 직접 연결 + `python -m jobs.X` 단독 실행
컨벤션을 따르지만, 이 DAG는 사용자 확정에 따라 Airflow UI Connection(bikeman_postgres)
+ PostgresHook을 쓴다 (docs/superpowers/specs/2026-08-18-bikeman-event-generator-design.md 참고).
"""
import sys
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

from dag_common import notify_slack_on_failure

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
    "on_failure_callback": notify_slack_on_failure,
}


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
    tags=["serving", "trigger_only"],
    doc_md=__doc__,
)
def bikeman_event_generator():
    generate_collect_events_task = PythonOperator(
        task_id="generate_collect_events",
        python_callable=_run_generate_collect_events,
        execution_timeout=timedelta(minutes=10),
    )
    deploy_returned_bikes_task = PythonOperator(
        task_id="deploy_returned_bikes",
        python_callable=_run_deploy_returned_bikes,
        execution_timeout=timedelta(minutes=10),
    )

    # Task 9 E2E 백필 중 발견: 같은 "수거" 대상 자전거 목록이 매일 재사용되는 상황(gold
    # 스냅샷이 하나뿐이거나 백필처럼 연속 실행할 때)에서 두 태스크를 병렬로 두면
    # deploy_returned_bikes가 "어제 COLLECT"를 찾는 조회(fetch_deploy_targets, latest
    # event 기준)가 같은 실행의 generate_collect_events가 "오늘" COLLECT를 커밋한
    # 뒤에 실행될 경우 그 자전거의 최신 이벤트가 이미 오늘 COLLECT로 바뀌어버려
    # 대상을 0건으로 놓친다. deploy_returned_bikes를 먼저 끝내 오늘자 COLLECT가
    # 커밋되기 전에 어제자 조회를 마치도록 순서를 강제한다.
    deploy_returned_bikes_task >> generate_collect_events_task


bikeman_event_generator()
