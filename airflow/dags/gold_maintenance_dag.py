"""
Iceberg 테이블 유지보수 DAG - 컴팩션(rewrite_data_files) + 스냅샷 만료
(expire_snapshots) + 고아 파일 정리(remove_orphan_files)

### 대상: Gold TEMP 4개 + Bronze/Silver rental_history (#173)
gold.bike_location/station_active/fact_station_inventory/bike_last_action은
매일 전체 덮어쓰기(overwritePartitions)라 실행마다 새 데이터 파일이 쌓이고
(small file 문제), bronze/silver.rental_history는 초기 적재가 작은 파일을
대량 생성해서 대상에 포함된다(#173).

### 왜 gold_dim_fact와 분리된 별도 DAG인가
컴팩션은 daily 배치의 SLA와 무관하고, 하루에 1~2개씩만 늘어나는 파일을 매일
정리할 필요는 없으므로 주간 1회로 충분하다 - gold_dim_fact에 태스크를 얹으면
daily 배치가 컴팩션 실패에 영향받게 되므로 분리했다(자세한 이유는
pipeline/collection_priority/jobs/compact_gold_tables.py docstring 참고).

### 스케줄
매주 일요일 03:00 KST - daily 배치(08:00 KST)와 겹치지 않는 새벽 시간대,
트래픽이 가장 적은 주기로 선택.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from dag_common import is_aws_env, notify_slack_on_failure, run_emr_serverless_spark_job

COLLECTION_PRIORITY_DIR = "/opt/airflow/pipeline/collection_priority"
INGESTION_DIR = "/opt/airflow/ingestion"
PYTHON = "python"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify_slack_on_failure,
}


def _compact_bash() -> str:
    # collection_priority 잡은 자체 common 패키지가 없다 -
    # ingestion/common(config, spark_session 등)을 그대로 재사용한다 (gold_dim_fact_dag.py와 동일 패턴).
    return (
        f"cd {COLLECTION_PRIORITY_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:$PYTHONPATH PYTHONDONTWRITEBYTECODE=1 "
        f"{PYTHON} -m jobs.compact_gold_tables"
    )


@dag(
    dag_id="gold_maintenance",
    schedule="0 3 * * 0",  # 매주 일요일 03:00 KST
    start_date=pendulum.datetime(2026, 8, 17, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gold", "independent", "weekly_batch"],
    doc_md=__doc__,
)
def gold_maintenance():
    if is_aws_env():
        @task(task_id="compact_gold_tables", execution_timeout=timedelta(hours=1))
        def compact_gold_tables_emr() -> str:
            return run_emr_serverless_spark_job(
                entry_point="local:///opt/app/pipeline/collection_priority/jobs/compact_gold_tables.py",
                name="gold-compact-tables",
                log_group_name="/emr-serverless/gold-maintenance",
                log_stream_name_prefix="compact-gold-tables",
                tags={
                    "dag_id": "gold_maintenance",
                    "task_id": "compact_gold_tables",
                },
            )

        compact_gold_tables_emr()
    else:
        BashOperator(
            task_id="compact_gold_tables",
            bash_command=_compact_bash(),
            execution_timeout=timedelta(hours=1),
        )


gold_maintenance()
