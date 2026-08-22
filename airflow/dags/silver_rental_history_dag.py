"""
Silver 일 배치 DAG - 대여이력 (bronze.rental_history -> silver.rental_history)
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import RENTAL_HISTORY_BRONZE
from dag_common import DEFAULT_ARGS, SILVER_POOL

INGESTION_DIR = "/opt/airflow/ingestion"
STAGING_DIR = "/opt/airflow/staging"
PYTHON = "python"


def _bash(job_dir: str, job_module: str, extra_env: str = "") -> str:
    # staging 잡은 자체 common 패키지가 없다 - ingestion/common(iceberg_catalog,
    # iceberg_io, sql_assert, watermark 등)을 그대로 재사용해 중복을 피한다. PYTHONPATH에
    # ingestion을 추가하면 `from common import ...`가 ingestion/common으로 해석된다.
    return (
        f"cd {job_dir} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:$PYTHONPATH {extra_env}{PYTHON} -m jobs.{job_module}"
    )


@dag(
    dag_id="silver_rental_history",
    schedule=[RENTAL_HISTORY_BRONZE],  # 고정 시간이 아니라 Bronze 완료 이벤트로 트리거
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 같은 파티션에 두 실행이 동시에 MERGE/덮어쓰기 시도하는 것 방지
    default_args=DEFAULT_ARGS,
    tags=["silver", "asset_triggered"],
    params={
        "max_days_per_run": "",
    },
    doc_md=__doc__,
)
def silver_rental_history():
    BashOperator(
        task_id="transform_silver_rental_history",
        bash_command=_bash(
            STAGING_DIR,
            "transform_silver_rental_history",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(hours=1),
        pool=SILVER_POOL,
    )


silver_rental_history()
