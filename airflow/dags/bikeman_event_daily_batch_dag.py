"""
따맨(bikeman) 이벤트 일 배치 DAG - 매일 실행, 별도 DAG로 유지

### 왜 bronze_daily_batch_all_sources에 합치지 않았는가
따맨은 우리가 직접 만든 내부 시스템이지만, Bronze 레이어 관점에서는 "수거/배치
이벤트를 생성하는 원천"으로 다른 3개 원천(대여이력/고장신고/대여소정보)과 동등하게
취급한다. 다만 실패 원인의 성격이 다르다 - 공공 API 장애가 아니라 우리 자체 DB
연결/스키마 문제이므로, 알림과 오너십을 분리해서 보고 싶다는 팀 결정에 따라
별도 DAG로 둔다. 스케줄은 다른 3개와 동일한 06:00으로 맞춰서 같은 시점에
Bronze 레이어 전체가 갱신되도록 한다.

### 3일 lookback 재처리
따맨은 오프라인 작업 후 몰아서 제출하는 게 정상 케이스라(source_data 문서 참고),
"어제"만 처리하면 이미 확정한 날짜에 늦게 도착한 이벤트를 놓친다. daily_batch_ttamaeng_event.py
안에서 매 실행마다 처리 시작점을 3일 앞당겨 재계산한다 - overwritePartitions라
같은 occurred_at 파티션을 여러 번 덮어써도 멱등적으로 안전하다.

### 최초 실행 전 필수 절차
set_watermark DAG(또는 CLI)로 dataset=bike_man_event, watermark_date=2026-06-29를
1회 찍어야 한다. 안 찍으면 config 기본 백필 시작일부터 처리를 시도해서 낭비가 생긴다
(rental_history/failure_report와 동일한 절차).
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

INGESTION_DIR = "/opt/airflow/ingestion"
INGESTION_PYTHON = "python"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="ttamaeng_event_daily_batch",
    schedule="0 6 * * *",  # 매일 06:00 KST - 다른 3개 원천과 동일 시각
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 같은 파티션에 두 실행이 동시에 덮어쓰기 시도하는 것 방지
    default_args=default_args,
    tags=["bikeman", "daily_batch", "bronze"],
    params={
        "max_days_per_run": "",
    },
    doc_md=__doc__,
)
def ttamaeng_event_daily_batch():
    BashOperator(
        task_id="daily_batch_ttamaeng_event",
        bash_command=(
            f"cd {INGESTION_DIR} && set -a && source .env && set +a && "
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' "
            f"{INGESTION_PYTHON} -m jobs.daily_batch_ttamaeng_event"
        ),
        execution_timeout=timedelta(minutes=30),
    )


ttamaeng_event_daily_batch()
