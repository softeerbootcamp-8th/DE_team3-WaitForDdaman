"""
Gold DQ 어써션 DAG 팩토리 (dq_bronze_dag.py/dq_silver_dag.py와 동일 패턴)

Gold 7개 잡(build_dim_bike 등)은 이미 sql_assert.py QualityCheck 하드 게이트가
완전성/enum/범위/uniqueness(threshold=0.99)를 막고 있다(#164). 그 threshold
0.99가 뜻하는 건 "중복률 1%까지는 통과"인데, 그 이하 값이 지금까지 어디에도
남지 않았다 - 여기서는 하드 게이트보다 낮은 임계(0.5%)로 먼저 걸어서 실제로
배치가 막히기 전에 추이로 조기경보를 볼 수 있게 한다. fact_bike_decision은
참조 무결성까지 이미 하드 게이트로 막혀있고 uniqueness 체크 자체가 없어(주문
데이터 성격상 bike_id 중복이 정상) 이번 목록에서 제외한다.

Gold DAG(gold_dim_fact/gold_risk_decision)는 Bronze/Silver와 달리 원래 고정
스케줄 + PythonSensor 대기 구조였다. DQ만큼은 "그 Gold 테이블이 실제로 갱신된
시점"에 정확히 맞아야 하므로, 이번에 build_* 태스크 6개에 한해 신규 Gold Asset을
추가해 outlets로 걸었다(dag_assets.py 참고) - Gold DAG 자체의 스케줄/센서 구조는
바꾸지 않았다. gold.bike_last_action은 build_fact_station_inventory 태스크가
같은 실행에서 함께 쓰므로 FACT_STATION_INVENTORY_GOLD Asset을 공유한다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import (
    BIKE_FEATURES_DAILY_GOLD,
    BIKE_LOCATION_GOLD,
    DIM_BIKE_GOLD,
    FACT_BIKE_RISK_GOLD,
    FACT_STATION_INVENTORY_GOLD,
    STATION_ACTIVE_GOLD,
)
from dag_common import DEFAULT_ARGS, GOLD_POOL

INGESTION_DIR = "/opt/airflow/ingestion"
PYTHON = "python"
CONFIG_DIR = "/opt/airflow/pylib/config/dq"

# (dag_id 접미사, 구독할 Gold Asset, source_name, YAML 파일명)
GOLD_SOURCES = [
    ("dim_bike", DIM_BIKE_GOLD, "gold_dim_bike", "gold_dim_bike.yaml"),
    ("bike_location", BIKE_LOCATION_GOLD, "gold_bike_location", "gold_bike_location.yaml"),
    ("station_active", STATION_ACTIVE_GOLD, "gold_station_active", "gold_station_active.yaml"),
    (
        "fact_station_inventory",
        FACT_STATION_INVENTORY_GOLD,
        "gold_fact_station_inventory",
        "gold_fact_station_inventory.yaml",
    ),
    (
        "bike_last_action",
        FACT_STATION_INVENTORY_GOLD,
        "gold_bike_last_action",
        "gold_bike_last_action.yaml",
    ),
    (
        "bike_features_daily",
        BIKE_FEATURES_DAILY_GOLD,
        "gold_bike_features_daily",
        "gold_bike_features_daily.yaml",
    ),
    ("fact_bike_risk", FACT_BIKE_RISK_GOLD, "gold_fact_bike_risk", "gold_fact_bike_risk.yaml"),
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


def _make_gold_dq_dag(dag_id_suffix, asset, source_name, config_filename):
    @dag(
        dag_id=f"dq_gold_{dag_id_suffix}",
        schedule=[asset],  # 고정 시간이 아니라 해당 Gold Asset 발행 이벤트로 트리거
        start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["dq", "gold", "asset_triggered"],
        doc_md=__doc__,
    )
    def _dag():
        run_assertions = BashOperator(
            task_id="run_dq_assertions",
            bash_command=_bash("run_dq_assertions", source_name, config_filename),
            execution_timeout=timedelta(minutes=15),
            pool=GOLD_POOL,
        )

        log_result = BashOperator(
            task_id="log_dq_check_result",
            bash_command=_bash("log_dq_check_result", source_name, config_filename),
            execution_timeout=timedelta(minutes=10),
            pool=GOLD_POOL,
        )

        run_assertions >> log_result

    return _dag()


for _suffix, _asset, _source_name, _config_filename in GOLD_SOURCES:
    globals()[f"dq_gold_{_suffix}"] = _make_gold_dq_dag(
        _suffix, _asset, _source_name, _config_filename
    )
