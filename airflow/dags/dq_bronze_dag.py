"""
Bronze DQ 어써션 DAG 팩토리 (5개 원천 공통)

dq_rental_history_dag.py(#217, Silver 파일럿)와 같은 2-태스크 패턴
(run_dq_assertions >> log_dq_check_result)을 Bronze 5개 원천에 재사용한다. 소스마다
새 DAG 파일을 만드는 대신, (asset, source_name, config_filename)만 다른 목록으로
두고 DAG을 찍어낸다 - 소스가 늘어도 이 파일이 아니라 BRONZE_SOURCES 목록만 늘리면 된다.

Silver 파일럿과 다른 점 두 가지:
  - 해석 에이전트/GitHub 이슈/Slack 자동화는 이번 범위에 없다. 어써션 계산과
    dq.check_result_history 적재까지만 한다 - 추이를 보고 다음 확장을 판단한다.
  - FAIL이어도 배치를 막지 않는 것은 동일하다(dq_assertions.py의 설계 원칙 그대로).
    Bronze 승격 자체는 이미 스키마 계약(ingestion/schema/*.py)이 하드 게이트로
    막고 있으므로, 이 어써션은 그 위에 값 수준 결측/이상치 비율을 관찰하는 용도다.

source_name은 Silver 파일럿의 "rental_history"와 겹치지 않도록 전부 "bronze_" 접두를
쓴다(config/dq/bronze_*.yaml과 동일 규칙) - dq.check_result_history에서 계층을
구분해서 볼 수 있어야 한다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import (
    BIKEMAN_EVENT_BRONZE,
    FAILURE_REPORT_BRONZE,
    RENTAL_HISTORY_BRONZE,
    STATION_ACTIVE_BRONZE,
    STATION_MASTER_BRONZE,
)
from dag_common import DEFAULT_ARGS, BRONZE_POOL

INGESTION_DIR = "/opt/airflow/ingestion"
PYTHON = "python"
CONFIG_DIR = "/opt/airflow/pylib/config/dq"

# (dag_id 접미사, 구독할 Bronze Asset, source_name, YAML 파일명)
BRONZE_SOURCES = [
    ("rental_history", RENTAL_HISTORY_BRONZE, "bronze_rental_history", "bronze_rental_history.yaml"),
    ("bikeman_event", BIKEMAN_EVENT_BRONZE, "bronze_bikeman_event", "bronze_bikeman_event.yaml"),
    ("station_master", STATION_MASTER_BRONZE, "bronze_station_master", "bronze_station_master.yaml"),
    ("station_active", STATION_ACTIVE_BRONZE, "bronze_station_active", "bronze_station_active.yaml"),
    ("failure_report", FAILURE_REPORT_BRONZE, "bronze_failure_report", "bronze_failure_report.yaml"),
]


def _bash(job_module: str, source_name: str, config_filename: str) -> str:
    return (
        f"cd {INGESTION_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:$PYTHONPATH "
        "EXECUTION_DATE='{{ ds }}' "
        f"DQ_SOURCE_NAME={source_name} "
        f"DQ_ASSERTIONS_CONFIG={CONFIG_DIR}/{config_filename} "
        f"{PYTHON} -m jobs.{job_module}"
    )


def _make_bronze_dq_dag(dag_id_suffix, asset, source_name, config_filename):
    @dag(
        dag_id=f"dq_bronze_{dag_id_suffix}",
        schedule=[asset],  # 고정 시간이 아니라 해당 Bronze Asset 발행 이벤트로 트리거
        start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["dq", "bronze", "asset_triggered"],
        doc_md=__doc__,
    )
    def _dag():
        run_assertions = BashOperator(
            task_id="run_dq_assertions",
            bash_command=_bash("run_dq_assertions", source_name, config_filename),
            execution_timeout=timedelta(minutes=15),
            pool=BRONZE_POOL,
        )

        log_result = BashOperator(
            task_id="log_dq_check_result",
            bash_command=_bash("log_dq_check_result", source_name, config_filename),
            execution_timeout=timedelta(minutes=10),
            pool=BRONZE_POOL,
        )

        run_assertions >> log_result

    return _dag()


for _suffix, _asset, _source_name, _config_filename in BRONZE_SOURCES:
    globals()[f"dq_bronze_{_suffix}"] = _make_bronze_dq_dag(
        _suffix, _asset, _source_name, _config_filename
    )
