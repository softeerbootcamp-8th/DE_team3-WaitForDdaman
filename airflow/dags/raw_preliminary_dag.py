"""
통합 Raw 예비 수집 DAG (대여이력 + 고장신고)

업무 마감 전 복구 지점을 확보하기 위해 API 원본 payload와 manifest만 S3 Raw에 저장한다.
Spark/Iceberg를 실행하지 않고 Bronze·워터마크·Asset을 변경하지 않는다.

예약 실행의 수집 기준시각은 실제 태스크 시작 시각이 아니라 data_interval_end다.
수동 실행에서 더 최신 논리 시각이 필요할 때만 dag_run.conf의
collection_cutoff_at을 명시적으로 전달한다. 같은 DAGRun의 재시도는 같은 값을 사용한다.
"""
import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_common import COLLECTION_CUTOFF_AT_TEMPLATE, DEFAULT_ARGS, bash_job

PRELIMINARY_SCHEDULE = os.getenv(
    "PRELIMINARY_SCHEDULE", "0 5 * * *"
)


@dag(
    dag_id="raw_preliminary",
    schedule=PRELIMINARY_SCHEDULE,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["raw", "daily_batch"],
    doc_md=__doc__,
)
def raw_preliminary():
    collect_rental_history = BashOperator(
        task_id="collect_rental_history_preliminary_raw",
        bash_command=bash_job("collect_rental_history_raw"),
        env={
            "COLLECTION_CUTOFF_AT": COLLECTION_CUTOFF_AT_TEMPLATE,
            "SNAPSHOT_TYPE": "PRELIMINARY",
        },
        append_env=True,
        execution_timeout=timedelta(hours=2),
    )

    collect_failure_report = BashOperator(
        task_id="collect_failure_report_preliminary_raw",
        bash_command=bash_job("collect_failure_report_raw"),
        env={
            "COLLECTION_CUTOFF_AT": COLLECTION_CUTOFF_AT_TEMPLATE,
            "SNAPSHOT_TYPE": "PRELIMINARY",
        },
        append_env=True,
        execution_timeout=timedelta(hours=1),
    )

    [collect_rental_history, collect_failure_report]


raw_preliminary()
