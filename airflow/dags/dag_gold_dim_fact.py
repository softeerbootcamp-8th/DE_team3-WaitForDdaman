"""
Gold DAG - Silver 스키마를 사용해 Gold 스키마(dim_bike/bike_location/
station_active/fact_station_inventory)를 만들어 S3(LocalStack)에 적재한다.

### 태스크 그래프
각 build 태스크는 실제로 읽는 Silver 소스의 센서에만 연결한다 (센서를 하나의
리스트로 묶어 모든 태스크 앞에 붙이면, 실제로는 필요 없는 소스까지 기다리는
것처럼 보이고 그래프도 불필요하게 얽힌다 - 2026-08-17 수정).

    silver.rental_history  --> gold.dim_bike
    silver.rental_history  --> gold.bike_location
    silver.station_master + silver.station_active --> gold.station_active
    silver.bike_man_action + gold.bike_location + gold.station_active
        --> gold.fact_station_inventory

    wait_rental_history ──┬──> build_dim_bike
                          └──> build_bike_location ─────────┐
    wait_station_master ──┬──> build_station_active ────────┼──> build_fact_station_inventory
    wait_station_active ──┘                                 │
    wait_bike_man_action ────────────────────────────────────┘

### wait_for_silver가 4개인 이유와 방식이 갈리는 이유
이 DAG가 필요로 하는 Silver 소스는 rental_history / station_master /
station_active / bike_man_action, 총 4개다(failure_report는 bike_features
전용이라 이 DAG 범위에서 제외됨). 이 4개는 서로 다른 3개 DAG + 1개 Asset
트리거 DAG에서 나온다.

    - rental_history / station_master / station_active: 고정 cron DAG라
      ExternalTaskSensor(execution_delta)로 정확히 매칭 가능
    - bike_man_action: silver_bike_man_action_daily가 Asset(bikeman_event_bronze)
      트리거라 logical_date가 매일 정해진 시각으로 정렬되지 않는다.
      ExternalTaskSensor의 execution_delta 매칭 전제(고정 스케줄)가 깨지므로,
      대신 실제 워터마크 파일을 직접 확인하는 BashSensor를 쓴다
      (ingestion/jobs/check_silver_bike_man_action_watermark.py).

### execution_delta 계산 (ExternalTaskSensor)
이 DAG는 08:00 KST에 스케줄된다. `external_execution_date = logical_date -
execution_delta` 공식이므로, "같은 날짜의 데이터"를 가리키는 상류 DAG의
logical_date와 맞추려면 두 DAG의 스케줄 시각 차이를 그대로 execution_delta로
넣어야 한다.
    - rental_history(30 7 * * *): 08:00 - 07:30 = 30분
    - station_master(0 7 * * *) / station_active(0 7 * * *): 08:00 - 07:00 = 1시간

### silver.station_active (2026-08-17, 담당 팀원 작업 반영)
더 이상 더미가 아니다 - `silver_station_active_daily` DAG(`staging/jobs/
silver_station_active.py`)가 `bronze.station_active`에서 station_id만 추려
매일 적재한다. `build_station_active`(gold)의 조인 로직은 station_active에서
station_id만 쓰므로 이 잡의 실제 컬럼 스키마(snapshot_date, station_id 2개뿐)와
그대로 맞는다 - 코드 변경 없이 정상 동작한다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag

INGESTION_DIR = "/opt/airflow/ingestion"
COLLECTION_PRIORITY_DIR = "/opt/airflow/pipeline/collection_priority"
PYTHON = "python"

SENSOR_TIMEOUT = timedelta(hours=3).total_seconds()
BIKE_MAN_ACTION_SENSOR_TIMEOUT = timedelta(hours=6).total_seconds()  # Asset 트리거라 더 여유
POKE_INTERVAL = 300  # 5분

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _ingestion_bash(job_module: str, extra_env: str = "") -> str:
    # PYTHONDONTWRITEBYTECODE=1: 이 DAG도 여러 태스크가 동시에 같은 ingestion/common
    # 모듈을 처음 import한다 - dag_common.py의 bash_job()과 동일한 이유로 .pyc
    # 쓰기 경합("EOFError: marshal data too short")을 피하려고 캐시를 아예 안 만든다.
    return (
        f"cd {INGESTION_DIR} && set -a && source .env && set +a && "
        f"PYTHONDONTWRITEBYTECODE=1 {extra_env}{PYTHON} -m jobs.{job_module}"
    )


def _collection_priority_bash(job_module: str, extra_env: str = "") -> str:
    # collection_priority 잡은 자체 common 패키지가 없다 -
    # ingestion/common(config, spark_session, watermark 등)을 그대로 재사용한다.
    return (
        f"cd {COLLECTION_PRIORITY_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:$PYTHONPATH PYTHONDONTWRITEBYTECODE=1 "
        f"{extra_env}{PYTHON} -m jobs.{job_module}"
    )


@dag(
    dag_id="dag_gold_dim_fact",
    schedule="0 8 * * *",  # 매일 08:00 KST - 상류 Silver DAG(07:00~07:30)가 보통 끝난 뒤
    start_date=pendulum.datetime(2026, 8, 17, tz="Asia/Seoul"),  # silver_station_active_daily 최초 가용일과 동일
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["daily_batch", "gold"],
    params={
        "max_days_per_run": "",  # build_dim_bike 백필용
    },
    doc_md=__doc__,
)
def dag_gold_dim_fact():
    wait_rental_history = ExternalTaskSensor(
        task_id="wait_for_silver_rental_history",
        external_dag_id="silver_gold_daily_batch_rental_history",
        external_task_id="transform_silver_rental_history",
        execution_delta=timedelta(minutes=30),
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
    )
    wait_station_master = ExternalTaskSensor(
        task_id="wait_for_silver_station_master",
        external_dag_id="silver_station_master_daily",
        external_task_id="silver_station_master",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
    )
    wait_station_active = ExternalTaskSensor(
        task_id="wait_for_silver_station_active",
        external_dag_id="silver_station_active_daily",
        external_task_id="silver_station_active",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
    )
    wait_bike_man_action = BashSensor(
        task_id="wait_for_silver_bike_man_action",
        bash_command=_ingestion_bash(
            "check_silver_bike_man_action_watermark",
            "TARGET_DATE='{{ ds }}' ",
        ),
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=BIKE_MAN_ACTION_SENSOR_TIMEOUT,
    )
    build_dim_bike = BashOperator(
        task_id="build_dim_bike",
        bash_command=_collection_priority_bash(
            "build_dim_bike",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(minutes=30),
    )
    build_bike_location = BashOperator(
        task_id="build_bike_location",
        bash_command=_collection_priority_bash("build_bike_location", "SNAPSHOT_DATE='{{ ds }}' "),
        execution_timeout=timedelta(minutes=20),
    )
    build_station_active = BashOperator(
        task_id="build_station_active",
        bash_command=_collection_priority_bash("build_station_active", "SNAPSHOT_DATE='{{ ds }}' "),
        execution_timeout=timedelta(minutes=20),
    )
    build_fact_station_inventory = BashOperator(
        task_id="build_fact_station_inventory",
        bash_command=_collection_priority_bash("build_fact_station_inventory", "SNAPSHOT_DATE='{{ ds }}' "),
        execution_timeout=timedelta(minutes=20),
    )

    # rental_history: dim_bike/bike_location 둘 다 이 소스만 직접 읽는다
    wait_rental_history >> build_dim_bike
    wait_rental_history >> build_bike_location

    # station_active: station_master + station_active 둘 다 이 태스크만 직접 읽는다
    wait_station_master >> build_station_active
    wait_station_active >> build_station_active

    # fact_station_inventory: rental_history/station_master/station_active는
    # build_bike_location/build_station_active를 거쳐 간접 보장되므로 직접 연결하지 않음
    build_bike_location >> build_fact_station_inventory
    build_station_active >> build_fact_station_inventory
    wait_bike_man_action >> build_fact_station_inventory


dag_gold_dim_fact()
