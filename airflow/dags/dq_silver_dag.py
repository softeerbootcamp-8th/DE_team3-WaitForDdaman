"""
Silver DQ 어써션 DAG 팩토리 (rental_history 이외 4개 소스)

dq_bronze_dag.py와 완전히 같은 패턴(run_dq_assertions >> log_dq_check_result,
해석 에이전트/GitHub 이슈/Slack 없음)을 Silver 4개 소스에 적용한다. rental_history는
dq_rental_history_dag.py(#217 파일럿, 5태스크 전체 자동화)가 이미 담당하므로 여기
목록에서 제외한다.

이 4개 소스는 지금까지 Silver 완료를 나타내는 Asset이 없었다(Bronze Asset으로
트리거만 받고 자기 완료를 outlets로 발행하지 않음) - dag_assets.py에 신규로
BIKEMAN_ACTION_SILVER 등 4개를 추가하고, 각 Silver DAG의 마지막 태스크에
outlets로 걸어서 이 DQ DAG들의 트리거로 쓴다. Bronze Asset이 아니라 이 Asset을
구독해야 그 실행 시점에 대상 Silver 테이블이 최신 상태다(RENTAL_HISTORY_SILVER와
동일한 이유, dag_assets.py 참고).

run_dq_assertions.py/log_dq_check_result.py는 ingestion/jobs에 있고 소스가
Silver든 Bronze든 무관하게 동일 실행 방식(ingestion/.env + PYTHONPATH)을 쓰므로
dq_bronze_dag.py와 같은 bash 실행 방식을 그대로 쓴다 - staging 잡 실행 방식과는
다르다(staging_bash가 아니다).
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import (
    BIKEMAN_ACTION_SILVER,
    FAILURE_REPORT_SILVER,
    STATION_ACTIVE_SILVER,
    STATION_MASTER_SILVER,
)
from dag_common import DEFAULT_ARGS, SILVER_POOL

INGESTION_DIR = "/opt/airflow/src"
PYTHON = "python"
CONFIG_DIR = "/opt/airflow/pylib/config/dq"

# (dag_id 접미사, 구독할 Silver Asset, source_name, YAML 파일명)
SILVER_SOURCES = [
    ("bikeman_action", BIKEMAN_ACTION_SILVER, "silver_bikeman_action", "silver_bikeman_action.yaml"),
    ("station_master", STATION_MASTER_SILVER, "silver_station_master", "silver_station_master.yaml"),
    ("station_active", STATION_ACTIVE_SILVER, "silver_station_active", "silver_station_active.yaml"),
    ("failure_report", FAILURE_REPORT_SILVER, "silver_failure_report", "silver_failure_report.yaml"),
]


def _bash(job_module: str, source_name: str, config_filename: str) -> str:
    return (
        f"cd {INGESTION_DIR} && set -a && source /opt/airflow/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:$PYTHONPATH "
        "EXECUTION_DATE='{{ ds }}' "
        f"DQ_SOURCE_NAME={source_name} "
        f"DQ_ASSERTIONS_CONFIG={CONFIG_DIR}/{config_filename} "
        f"{PYTHON} -m jobs.{job_module}"
    )


def _make_silver_dq_dag(dag_id_suffix, asset, source_name, config_filename):
    @dag(
        dag_id=f"dq_silver_{dag_id_suffix}",
        schedule=[asset],  # 고정 시간이 아니라 해당 Silver Asset 발행 이벤트로 트리거
        start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["dq", "silver", "asset_triggered"],
        doc_md=__doc__,
    )
    def _dag():
        run_assertions = BashOperator(
            task_id="run_dq_assertions",
            bash_command=_bash("run_dq_assertions", source_name, config_filename),
            execution_timeout=timedelta(minutes=15),
            pool=SILVER_POOL,
        )

        log_result = BashOperator(
            task_id="log_dq_check_result",
            bash_command=_bash("log_dq_check_result", source_name, config_filename),
            execution_timeout=timedelta(minutes=10),
            pool=SILVER_POOL,
        )

        run_assertions >> log_result

    return _dag()


for _suffix, _asset, _source_name, _config_filename in SILVER_SOURCES:
    globals()[f"dq_silver_{_suffix}"] = _make_silver_dq_dag(
        _suffix, _asset, _source_name, _config_filename
    )
