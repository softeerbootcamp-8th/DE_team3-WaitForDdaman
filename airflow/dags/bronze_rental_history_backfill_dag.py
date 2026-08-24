"""대여이력 날짜 단위 확정 Backfill DAG (#195).

한 DagRun은 Airflow logical date(`{{ ds }}`)의 하루만 처리한다. 따라서 Airflow Backfill로
과거 날짜를 여러 DagRun으로 만들 수 있고, 한 날짜 실패가 다른 날짜의 성공 데이터나
completion marker를 덮어쓰지 않는다.

이 DAG는 전역 Bronze confirmed watermark를 읽거나 갱신하지 않는다. 날짜별 결과는
completion marker로만 기록하며, 후속 Historical Reconciliation이 연속 성공 구간을
확인한 뒤 전역 워터마크를 전진시킨다.

운영 예시:
    airflow backfill create --dag-id bronze_rental_history_backfill \
      --from-date 2026-07-01 --to-date 2026-07-03
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.task.trigger_rule import TriggerRule

from dag_common import BRONZE_POOL, DEFAULT_ARGS, bash_job

TARGET_DATE = "{{ ds }}"
CUTOFF_AT = "{{ ds }}T23:59:59+09:00"


@dag(
    dag_id="bronze_rental_history_backfill",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    # 명시적인 `airflow backfill create`만 과거 날짜를 생성한다.
    # DAG 활성화만으로 start_date 이후 전체 구간이 자동 실행되면 안 된다.
    catchup=False,
    max_active_runs=2,
    max_active_tasks=2,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "rental_history", "backfill"],
    doc_md=__doc__,
)
def bronze_rental_history_backfill():
    common_env = {
        "BACKFILL_TARGET_DATE": TARGET_DATE,
        "COLLECTION_CUTOFF_AT": CUTOFF_AT,
        "DAG_RUN_ID": "{{ run_id }}",
        "BACKFILL_STARTED_AT": "{{ ts }}",
    }

    collect = BashOperator(
        task_id="collect_final_raw_for_date",
        bash_command=bash_job(
            "collect_rental_history_raw",
            "SNAPSHOT_TYPE='FINAL' RENTAL_HISTORY_T0_ENABLED='false' ",
        ),
        env=common_env,
        append_env=True,
        retries=0,
        execution_timeout=timedelta(minutes=30),
        pool=BRONZE_POOL,
    )

    select = BashOperator(
        task_id="select_final_raw_for_date",
        bash_command=bash_job(
            "select_rental_history_snapshot",
            "RENTAL_HISTORY_FALLBACK_ENABLED='false' RENTAL_HISTORY_T0_ENABLED='false' ",
        ),
        env=common_env,
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=10),
    )

    promote = BashOperator(
        task_id="promote_date_to_bronze",
        bash_command=bash_job("promote_rental_history_raw"),
        env=common_env,
        append_env=True,
        execution_timeout=timedelta(minutes=30),
        pool=BRONZE_POOL,
    )

    marker = BashOperator(
        task_id="write_completion_marker",
        bash_command=bash_job("write_rental_history_completion_marker"),
        env=common_env,
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    collect >> select >> promote >> marker


bronze_rental_history_backfill()
