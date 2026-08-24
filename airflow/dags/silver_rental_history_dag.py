"""
Silver 일 배치 DAG - 대여이력 (bronze.rental_history -> silver.rental_history)

### #137 - 당일 promotion 처리
06:00 일 배치의 publish_bronze_asset이 Asset event에 run_date/promotion_id/promotion_key를
실어 보낸다. 이 DAG은 그 값을 triggering_asset_events에서 그대로 꺼내 전달할 뿐,
S3의 "최신 COMPLETE promotion"을 추측하지 않는다.

bronze_catchup_all_sources처럼 metadata 없이 같은 Asset을 발행하는 경로에서 트리거되면
(수동 트리거 포함) 세 값이 빈 문자열이 되고, transform_silver_rental_history가 그걸
"metadata 없음"으로 보고 기존 확정 워터마크 구간만 처리한다 - 이 DAG에서는 분기가 필요
없다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from dag_assets import RENTAL_HISTORY_BRONZE, RENTAL_HISTORY_SILVER
from dag_common import DEFAULT_ARGS, SILVER_POOL

INGESTION_DIR = "/opt/airflow/ingestion"
STAGING_DIR = "/opt/airflow/staging"
PYTHON = "python"

# daily_batch DAG과 같은 Variable을 공유한다 (bronze_daily_batch_all_sources_dag.py 참고).
T0_ENABLED_TEMPLATE = "{{ var.value.get('RENTAL_HISTORY_T0_ENABLED', 'false') }}"


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
    @task(task_id="resolve_bronze_promotion_metadata")
    def resolve_bronze_promotion_metadata(**context) -> dict:
        """Asset이 실어 보낸 metadata만 신뢰한다 - S3의 최신 marker를 추측하지 않는다.

        metadata 없는 트리거(Catchup, 수동 트리거)는 빈 문자열 3종을 돌려주고,
        transform_silver_rental_history가 그걸 "확정 구간만 처리"로 해석한다.
        """
        events = context["triggering_asset_events"].get(RENTAL_HISTORY_BRONZE, [])
        if not events:
            return {"run_date": "", "promotion_id": "", "promotion_key": ""}
        latest = max(events, key=lambda event: event.timestamp)
        extra = latest.extra or {}
        return {
            "run_date": str(extra.get("run_date", "")),
            "promotion_id": str(extra.get("promotion_id", "")),
            "promotion_key": str(extra.get("promotion_key", "")),
        }

    metadata = resolve_bronze_promotion_metadata()

    transform_silver_rental_history = BashOperator(
        task_id="transform_silver_rental_history",
        bash_command=_bash(
            STAGING_DIR,
            "transform_silver_rental_history",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        env={
            "RENTAL_HISTORY_T0_ENABLED": T0_ENABLED_TEMPLATE,
            "RENTAL_HISTORY_BRONZE_RUN_DATE": (
                "{{ ti.xcom_pull(task_ids='resolve_bronze_promotion_metadata')['run_date'] }}"
            ),
            "RENTAL_HISTORY_BRONZE_PROMOTION_ID": (
                "{{ ti.xcom_pull(task_ids='resolve_bronze_promotion_metadata')['promotion_id'] }}"
            ),
            "RENTAL_HISTORY_BRONZE_PROMOTION_KEY": (
                "{{ ti.xcom_pull(task_ids='resolve_bronze_promotion_metadata')['promotion_key'] }}"
            ),
        },
        append_env=True,
        execution_timeout=timedelta(hours=1),
        pool=SILVER_POOL,
        outlets=[RENTAL_HISTORY_SILVER],  # #217 dq_rental_history_dag의 트리거
    )

    metadata >> transform_silver_rental_history


silver_rental_history()
