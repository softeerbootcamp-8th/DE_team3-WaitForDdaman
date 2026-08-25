# airflow/dags/gold_to_serving_sync_dag.py
"""
gold_to_serving_sync - Gold Iceberg 마트를 서빙 Postgres(station_daily/bike_risk_daily)로
동기화한다. gold_risk_decision의 마지막 태스크(build_fact_bike_decision)가 끝나면
TriggerDagRunOperator로 이 DAG를 트리거한다 (gold_dim_fact가 아님 - bike_risk_daily가
필요로 하는 fact_bike_risk/fact_bike_decision은 gold_risk_decision의 산출물이라 그게
끝나야 두 마트 모두 만들 재료가 갖춰짐).

두 브랜치(bike_risk_daily / station_daily)는 서로 의존하지 않아 병렬 실행한다.

### 실패 전파를 끊는 이유
트리거는 wait_for_completion=False다 - 이 DAG가 실패해도 이미 만들어진 gold 데이터
자체는 유효하므로 gold_risk_decision을 실패로 만들 이유가 없다. 대신 각 태스크에
Slack 알림을 건다 (CloudWatch/SNS는 이 프로젝트에 대응 AWS 인프라가 없어 스코프 제외 -
spec §2/§3 참고).

### trigger_bikeman_event_generator (2026-08-18 추가)
verify_bike_risk_daily_sync가 끝나면 bikeman_event_generator를 트리거한다.
serving.bike_risk_daily에서 action 컬럼이 제거된 뒤에는 최신 snapshot의 risk_score
상위 500대를 COLLECT 대상으로 삼는다. 세부 설계는
docs/superpowers/specs/2026-08-18-bikeman-event-generator-design.md 참고.
"""
import json
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag

from dag_common import notify_slack_on_failure

PYLIB_DIR = "/opt/airflow/pylib"
SERVING_SYNC_DIR = "/opt/airflow/pipeline/serving_sync"
INGESTION_DIR = "/opt/airflow/ingestion"

# write_*/verify_* 4개 태스크가 부르는 Lambda 함수 이름 - infra/terraform/serving_sync.tf의
# aws_lambda_function.function_name과 반드시 같아야 한다 (#172).
WRITE_BIKE_RISK_DAILY_LAMBDA = "serving-sync-write-bike-risk-daily"
WRITE_STATION_DAILY_LAMBDA = "serving-sync-write-station-daily"
VERIFY_SERVING_SYNC_LAMBDA = "serving-sync-verify"


def _load_ingestion_env(env_path: str) -> None:
    """`source .env`(BashOperator 시절)와 동일하게, ingestion/.env의 값을 컨테이너
    환경변수 위에 그대로 덮어쓴다 (gold_dim_fact_dag.py와 동일한 패턴 - 안 하면
    컨테이너 루트 .env(배포용)가 그대로 새어 들어와 LocalStack 대신 실제 AWS로
    나가는 문제가 재현된다). 이 파일은 docker-compose.local.yml 컨테이너에만
    존재해서 DagBag이 이 DAG를 로드하는 다른 환경(CI 등)에서는 조용히 건너뛴다."""
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip()


def _build_mart_bike_risk_daily(snapshot_date: str) -> None:
    _load_ingestion_env(f"{INGESTION_DIR}/.env")
    if PYLIB_DIR not in sys.path:
        sys.path.insert(0, PYLIB_DIR)
    if INGESTION_DIR not in sys.path:
        sys.path.insert(0, INGESTION_DIR)
    if f"{SERVING_SYNC_DIR}/jobs" not in sys.path:
        sys.path.insert(0, f"{SERVING_SYNC_DIR}/jobs")

    from build_mart_bike_risk_daily import run as _run_build_mart_bike_risk_daily

    os.environ["SNAPSHOT_DATE"] = snapshot_date
    _run_build_mart_bike_risk_daily()


def _build_mart_station_daily(snapshot_date: str) -> None:
    _load_ingestion_env(f"{INGESTION_DIR}/.env")
    if PYLIB_DIR not in sys.path:
        sys.path.insert(0, PYLIB_DIR)
    if INGESTION_DIR not in sys.path:
        sys.path.insert(0, INGESTION_DIR)
    if f"{SERVING_SYNC_DIR}/jobs" not in sys.path:
        sys.path.insert(0, f"{SERVING_SYNC_DIR}/jobs")

    from build_mart_station_daily import run as _run_build_mart_station_daily

    os.environ["SNAPSHOT_DATE"] = snapshot_date
    _run_build_mart_station_daily()

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_slack_on_failure,
}


def _lambda_payload(**extra: str) -> str:
    """LambdaInvokeFunctionOperator의 payload(JSON 문자열)를 만든다. snapshot_date는
    항상 포함하고, gold_risk_decision이 conf로 넘긴 값을 자기 ds보다 우선한다(아래
    태스크 정의 주석과 동일한 규칙) - Jinja 렌더링은 Airflow가 payload 필드 전체를
    템플릿으로 처리할 때 일어나므로, 여기서는 렌더 전 원본 표현식을 그대로 넣는다."""
    payload = dict(extra)
    payload["snapshot_date"] = "{{ dag_run.conf.get('snapshot_date') or ds }}"
    return json.dumps(payload)


@dag(
    dag_id="gold_to_serving_sync",
    schedule=None,  # gold_risk_decision의 TriggerDagRunOperator로만 실행
    start_date=pendulum.datetime(2026, 8, 18, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["serving", "trigger_only"],
    doc_md=__doc__,
)
def gold_to_serving_sync():
    # 아래 네 태스크의 SNAPSHOT_DATE는 모두 dag_run.conf.get("snapshot_date")를 자기
    # ds보다 우선한다 - gold_risk_decision이 conf로 trigger_serving_sync에서 넘긴 날짜가
    # 있으면 그걸 쓰고(백필/미래 스케줄 실행에서 두 DAG의 날짜가 어긋나지 않게), 수동
    # 트리거처럼 conf가 없으면 기존과 동일하게 이 DAG 자신의 ds로 폴백한다.
    build_mart_bike_risk_daily = PythonOperator(
        task_id="build_mart_bike_risk_daily",
        python_callable=_build_mart_bike_risk_daily,
        op_kwargs={"snapshot_date": "{{ dag_run.conf.get(\"snapshot_date\") or ds }}"},
        execution_timeout=timedelta(minutes=20),
    )
    write_bike_risk_daily = LambdaInvokeFunctionOperator(
        task_id="write_bike_risk_daily",
        function_name=WRITE_BIKE_RISK_DAILY_LAMBDA,
        invocation_type="RequestResponse",  # 비동기면 Lambda 자체 재시도가 붙어 DELETE+INSERT가 두 번 돌 수 있음
        payload=_lambda_payload(),
        execution_timeout=timedelta(minutes=15),
    )
    verify_bike_risk_daily_sync = LambdaInvokeFunctionOperator(
        task_id="verify_bike_risk_daily_sync",
        function_name=VERIFY_SERVING_SYNC_LAMBDA,
        invocation_type="RequestResponse",
        payload=_lambda_payload(
            iceberg_table="bike_catalog.gold.mart_bike_risk_daily",
            postgres_table="bike_risk_daily",
        ),
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

    build_mart_station_daily = PythonOperator(
        task_id="build_mart_station_daily",
        python_callable=_build_mart_station_daily,
        op_kwargs={"snapshot_date": "{{ dag_run.conf.get(\"snapshot_date\") or ds }}"},
        execution_timeout=timedelta(minutes=20),
    )
    write_station_daily = LambdaInvokeFunctionOperator(
        task_id="write_station_daily",
        function_name=WRITE_STATION_DAILY_LAMBDA,
        invocation_type="RequestResponse",
        payload=_lambda_payload(),
        execution_timeout=timedelta(minutes=15),
    )
    verify_station_daily_sync = LambdaInvokeFunctionOperator(
        task_id="verify_station_daily_sync",
        function_name=VERIFY_SERVING_SYNC_LAMBDA,
        invocation_type="RequestResponse",
        payload=_lambda_payload(
            iceberg_table="bike_catalog.gold.mart_station_daily",
            postgres_table="station_daily",
        ),
        execution_timeout=timedelta(minutes=10),
    )

    build_mart_bike_risk_daily >> write_bike_risk_daily >> verify_bike_risk_daily_sync >> trigger_bikeman_event_generator
    build_mart_station_daily >> write_station_daily >> verify_station_daily_sync


gold_to_serving_sync()
