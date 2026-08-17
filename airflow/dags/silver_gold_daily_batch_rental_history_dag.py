from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

INGESTION_DIR = "/opt/airflow/ingestion"
STAGING_DIR = "/opt/airflow/staging"
RISK_MODEL_DIR = "/opt/airflow/pipeline/risk_model"
PYTHON = "python"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _bash(job_dir: str, job_module: str, extra_env: str = "") -> str:
    # staging/pipeline 잡은 자체 common 패키지가 없다 - ingestion/common(config,
    # spark_session, watermark 등)을 그대로 재사용해 중복을 피한다. PYTHONPATH에
    # ingestion을 추가하면 `from common import ...`가 ingestion/common으로 해석된다.
    return (
        f"cd {job_dir} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:$PYTHONPATH {extra_env}{PYTHON} -m jobs.{job_module}"
    )


@dag(
    dag_id="silver_gold_daily_batch_rental_history",
    schedule="30 7 * * *",  # 매일 07:30 KST - Bronze(06:00 시작)가 보통 끝난 뒤
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 같은 파티션에 두 실행이 동시에 MERGE/덮어쓰기 시도하는 것 방지
    default_args=default_args,
    tags=["daily_batch", "silver", "gold", "rental_history"],
    params={
        "max_days_per_run": "",
    },
    doc_md=__doc__,
)
def silver_gold_daily_batch_rental_history():
    transform_silver = BashOperator(
        task_id="transform_silver_rental_history",
        bash_command=_bash(
            STAGING_DIR,
            "transform_silver_rental_history",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(hours=1),
    )

    build_dim_bike = BashOperator(
        task_id="build_gold_dim_bike",
        bash_command=_bash(
            RISK_MODEL_DIR,
            "build_dim_bike",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(minutes=30),
    )

    transform_silver >> build_dim_bike


silver_gold_daily_batch_rental_history()
