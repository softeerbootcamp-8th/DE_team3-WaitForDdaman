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
    silver.bikeman_action + gold.bike_location + gold.station_active
        --> gold.fact_station_inventory

    wait_rental_history ──┬──> build_dim_bike
                          └──> build_bike_location ─────────┐
    wait_station_master ──┬──> build_station_active ────────┼──> build_fact_station_inventory
    wait_station_active ──┘                                 │
    wait_bikeman_action ────────────────────────────────────┘

### wait_for_silver가 4개인 이유와 방식이 갈리는 이유
이 DAG가 필요로 하는 Silver 소스는 rental_history / station_master /
station_active / bikeman_action, 총 4개다(failure_report는 bike_features
전용이라 이 DAG 범위에서 제외됨). 이 4개는 서로 다른 3개 DAG + 1개 Asset
트리거 DAG에서 나온다.

    - bikeman_action: silver_bikeman_action가 Asset(bikeman_event_bronze)
      트리거라 logical_date가 매일 정해진 시각으로 정렬되지 않는다.
      ExternalTaskSensor의 execution_delta 매칭 전제(고정 스케줄)가 깨지므로,
      대신 실제 워터마크 파일을 직접 확인하는 BashSensor를 쓴다
      (ingestion/jobs/check_silver_bikeman_action_watermark.py).

### rental_history / station_master / station_active도 BashSensor로 전환 (2026-08-17, #50)
이 3개 Silver DAG도 Bronze 완료 Asset 트리거로 전환되면서 bikeman_action과
동일한 문제를 겪는다 - DagRun의 logical_date가 고정 스케줄 그리드에 맞춰
정렬되지 않고(수동 Asset 이벤트로 만든 DagRun은 logical_date가 아예 null인
경우도 실측 확인됨), ExternalTaskSensor(execution_delta)가 상류 DagRun을 못
찾아 매번 타임아웃난다. `DagRun.find()`로 메타데이터 DB를 직접 조회하는 대안도
시도했으나 Airflow 3의 Task SDK가 태스크 코드의 ORM 직접 접근을 막아
`RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0`로
실패한다(실측). 그래서 bikeman_action과 같은 방식(실제 상태를 직접 확인하는
BashSensor)으로 통일한다.

    - rental_history: 워터마크가 있는 증분 소스라 check_silver_watermark.py로
      워터마크 값을 직접 확인 (T-1 구조라 REQUIRED_OFFSET_DAYS=1)
    - station_master / station_active: 워터마크가 없는 스냅샷 소스라
      check_silver_snapshot_date.py로 테이블의 MAX(snapshot_date)를 직접 확인
      (T-0 구조라 오프셋 없음)
### TARGET_DATE를 어제로 넘기는 이유 (2026-08-17 수정, #52)
daily_batch_bikeman_event.py는 항상 "어제까지"만 처리하므로(오늘 데이터는
작업자가 몰아서 제출하는 경우가 많아 항상 미확정) silver_bikeman_action의
워터마크는 구조적으로 실행일보다 하루 늦다. 예전엔 TARGET_DATE에 오늘({{ ds }})을
그대로 넘겨서 이 센서가 워터마크를 영원히 못 따라잡고 매번 타임아웃났다
(팀원 리뷰로 발견). `macros.ds_add(ds, -1)`로 하루 전 날짜를 넘겨서 고쳤다.

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

### BashSensor(exit code) → PythonSensor 전환 (2026-08-22, #145)
위 절들이 설명하는 "왜 BashSensor로 전환했는가"(ExternalTaskSensor의
execution_delta 전제가 Asset 트리거로 깨짐)는 그대로 유효하다. 다만 BashSensor는
poke마다 새 `python -m jobs.X` 서브프로세스를 띄웠고, 그중 station_master/
station_active용 check_silver_snapshot_date.py는 그 서브프로세스 안에서 Spark
세션까지 새로 띄웠다(poke_interval=300s/timeout=6h 기준 센서당 최대 72회). 판정
로직 자체(워터마크 비교, 스냅샷 파티션 존재 확인)는 그대로 두고, `bikeman_event_
generator_dag.py`와 같은 방식으로 ingestion 잡 모듈을 sys.path에 얹어 Airflow
워커 프로세스 안에서 직접 호출하는 PythonSensor로 바꿔 서브프로세스/Spark 세션
기동을 없앤다. station_master/station_active 판정은 Spark의 MAX(snapshot_date)
대신, Iceberg Hadoop 카탈로그가 identity 파티션마다 만드는 Hive 스타일 S3
디렉터리(`data/snapshot_date=YYYY-MM-DD/`)를 boto3로 나열해서 구한다
(check_silver_snapshot_date.py 참고) - 이 테이블들에 파티션을 지우는 잡이 없어
오탐이 없다.

### ingestion/.env를 여기서 직접 로드하는 이유 (2026-08-22, #145)
`_ingestion_bash`(BashSensor 시절)는 매 poke마다 `cd ingestion && source .env`로
서브프로세스를 띄워, 컨테이너 자체의 환경변수(루트 `.env` - "AWS 배포 시 바꾸는
파일"로 문서화됨, 배포 시 APP_ENV=aws + 실 AWS 키가 됨)와 무관하게 ingestion 잡을
항상 `ingestion/.env`(APP_ENV=local + LocalStack 엔드포인트) 기준으로 실행해왔다.
PythonSensor는 서브프로세스를 띄우지 않고 이 태스크 프로세스 안에서 바로
`is_ready()`를 호출하므로, `config.SETTINGS`가 평가되는 시점(첫 `import config`)에
컨테이너가 물려받은 환경변수를 그대로 쓴다 - `source .env` 단계가 없으면 루트
`.env`가 APP_ENV=aws인 상태에서 컨테이너가 뜬 경우 LocalStack 대신 진짜 AWS S3로
나가버린다(#145 검증 중 AccessDenied로 실측 확인). 그래서 `jobs.check_silver_*`를
import(=config 첫 평가)하기 전에 `ingestion/.env`를 직접 읽어 os.environ에
덮어써서, 서브프로세스 시절과 동일한 값으로 config.SETTINGS가 만들어지게 한다.
Task SDK가 태스크마다 새 프로세스를 띄우므로(다른 DAG 태스크와 프로세스를 공유하지
않음) 여기서 os.environ을 덮어써도 다른 DAG에 영향이 없다.
"""
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import dag

from dag_common import notify_slack_on_failure

PYLIB_DIR = "/opt/airflow/pylib"
INGESTION_DIR = "/opt/airflow/ingestion"
COLLECTION_PRIORITY_DIR = "/opt/airflow/pipeline/collection_priority"
PYTHON = "python"


def _load_ingestion_env(env_path: str) -> None:
    """`source .env`(BashSensor 시절)와 동일하게, ingestion/.env의 값을 컨테이너
    환경변수 위에 그대로 덮어쓴다 (export 없는 단순 KEY=VALUE 라인만 있는 파일).

    이 파일은 docker-compose.local.yml 컨테이너에만 존재한다 - DagBag이 DAG
    폴더 전체를 import하는 CI/로컬 테스트(예: 다른 DAG의 회귀 테스트가 같은
    폴더를 통째로 로드하는 경우) 등 그 컨테이너 밖에서는 없는 게 정상이라,
    없으면 조용히 건너뛴다(그 환경에서는 config.SETTINGS가 컨테이너/러너 자체
    환경변수 기준으로 평가되어도 DAG import 자체는 깨지면 안 된다).
    """
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip()


_load_ingestion_env(f"{INGESTION_DIR}/.env")

# PythonSensor가 Spark/서브프로세스 없이 판정 함수를 직접 호출할 수 있도록,
# pylib(config)와 ingestion을 네임스페이스 패키지 루트로 sys.path에 얹는다
# (bikeman_event_generator_dag.py와 동일한 패턴). config가 위에서 로드한
# ingestion/.env 값으로 평가되도록 반드시 _load_ingestion_env 다음에 import한다.
if PYLIB_DIR not in sys.path:
    sys.path.insert(0, PYLIB_DIR)
if INGESTION_DIR not in sys.path:
    sys.path.insert(0, INGESTION_DIR)

from jobs.check_silver_bikeman_action_watermark import is_ready as bikeman_action_ready  # noqa: E402
from jobs.check_silver_snapshot_date import is_ready as snapshot_date_ready  # noqa: E402
from jobs.check_silver_watermark import is_ready as watermark_ready  # noqa: E402

SENSOR_TIMEOUT = timedelta(hours=6).total_seconds()  # 전부 Asset 트리거라 여유 있게
POKE_INTERVAL = 300  # 5분

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_slack_on_failure,
}

T0_ENABLED_TEMPLATE = "{{ var.value.get('RENTAL_HISTORY_T0_ENABLED', 'false') }}"


def _collection_priority_bash(job_module: str, extra_env: str = "") -> str:
    # collection_priority 잡은 자체 common 패키지가 없다 -
    # ingestion/common(config, spark_session, watermark 등)을 그대로 재사용한다.
    return (
        f"cd {COLLECTION_PRIORITY_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:$PYTHONPATH PYTHONDONTWRITEBYTECODE=1 "
        f"{extra_env}{PYTHON} -m jobs.{job_module}"
    )


@dag(
    dag_id="gold_dim_fact",
    schedule="0 8 * * *",  # 매일 08:00 KST - 상류 Silver DAG(07:00~07:30)가 보통 끝난 뒤
    start_date=pendulum.datetime(2026, 8, 17, tz="Asia/Seoul"),  # silver_station_active_daily 최초 가용일과 동일
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gold", "daily_batch"],
    params={
        "max_days_per_run": "",  # build_dim_bike 백필용
    },
    doc_md=__doc__,
)
def gold_dim_fact():
    wait_rental_history = PythonSensor(
        task_id="wait_for_silver_rental_history",
        python_callable=watermark_ready,
        op_kwargs={"dataset": "rental_history", "target_date": "{{ ds }}", "required_offset_days": 1},
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
    )
    wait_station_master = PythonSensor(
        task_id="wait_for_silver_station_master",
        python_callable=snapshot_date_ready,
        op_kwargs={"table_name": "silver.station_master", "target_date": "{{ ds }}"},
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
    )
    wait_station_active = PythonSensor(
        task_id="wait_for_silver_station_active",
        python_callable=snapshot_date_ready,
        op_kwargs={"table_name": "silver.station_active", "target_date": "{{ ds }}"},
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
    )
    wait_bikeman_action = PythonSensor(
        task_id="wait_for_silver_bikeman_action",
        python_callable=bikeman_action_ready,
        op_kwargs={"target_date": "{{ macros.ds_add(ds, -1) }}"},
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
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
        env={
            "RENTAL_HISTORY_T0_ENABLED": T0_ENABLED_TEMPLATE,
        },
        append_env=True,
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

    trigger_risk_decision = TriggerDagRunOperator(
        task_id="trigger_risk_decision",
        trigger_dag_id="gold_risk_decision",
        logical_date="{{ logical_date }}",
        conf={"snapshot_date": "{{ ds }}"},
        wait_for_completion=False,
        reset_dag_run=True,
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
    wait_bikeman_action >> build_fact_station_inventory

    build_fact_station_inventory >> trigger_risk_decision


gold_dim_fact()
