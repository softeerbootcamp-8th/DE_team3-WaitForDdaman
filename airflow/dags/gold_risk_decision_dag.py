"""
gold_risk_decision - 위험도 추론 + 대여중단 결정

gold_dim_fact(gold.bike_location, gold.fact_station_inventory 등)가 끝난 뒤, 저장된
위험도 모델로 매일 자전거별 risk_score/risk_grade를 추론하고(gold.fact_bike_risk), 대여소
재고 상황을 반영해 대여중단/보류를 결정한다(gold.fact_bike_decision).

gold.bike_features_daily(추론용 feature)는 gold_dim_fact 소관이 아니라 이 DAG이
직접 만든다(build_bike_features_daily) - Silver(rental_history/failure_report)만
있으면 되고 순수 이 모델 전용 입력이라 risk_model 스코프로 판단했다.

### 콜드스타트 분기 제거 (2026-08-19, #90)
원안은 "최초 실행일엔 어제 fact_bike_decision이 없으니 필터를 스킵한다"는 걸
wait_for_yesterday_decision(센서) + check_cold_start(분기) +
skip_filter_first_run/apply_lagged_filter(둘 다 EmptyOperator)로 구현하려 했다.
그런데 4-a/4-b가 실행해도 아무 일도 안 하는 EmptyOperator라 분기 결과를 쓰는
곳이 실제로는 없었다 - 어느 쪽으로 가든 다음 태스크(run_risk_scoring_model)는
동일하게 실행됨. 콜드스타트는 대신 각 job 안에서 tableExists 체크로 안전하게
처리된다: bikeman_action 콜드스타트는 build_fact_bike_risk.py의
_currently_collected_bike_ids, fact_bike_decision 콜드스타트는
build_bike_features_daily.py의 _suspended_bike_days(#85)가 센서 없이 직접
처리한다. 그래서 4개 태스크(및 이걸 위해 있던 trigger_rule=
none_failed_min_one_success)를 전부 제거했다.

### trigger_serving_sync (2026-08-18 추가)
build_fact_bike_decision이 끝나면 gold_to_serving_sync(gold_to_serving_sync_dag.py)를
TriggerDagRunOperator로 트리거한다. bike_risk_daily 마트가 필요로 하는
fact_bike_risk/fact_bike_decision은 이 DAG의 산출물이라, gold_dim_fact가 아니라
여기서 트리거해야 두 마트(station_daily 포함) 모두 만들 재료가 갖춰진다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import dag

from dag_common import notify_slack_on_failure

RISK_MODEL_DIR = "/opt/airflow/pipeline/risk_model"
INGESTION_DIR = "/opt/airflow/ingestion"
AIRFLOW_HOME_DIR = "/opt/airflow"
PYTHON = "python"

DAG_ID = "gold_risk_decision"

# gold_dim_fact의 wait_for_silver_rental_history와 동일한 값 - 상류가 이미
# 검증된 타임아웃/간격이라 여기서도 그대로 맞춘다.
SENSOR_TIMEOUT = timedelta(hours=6).total_seconds()
POKE_INTERVAL = 300  # 5분

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_slack_on_failure,
}


def _bash(job_module: str, extra_env: str = "") -> str:
    # /opt/airflow 를 넣어야 jobs/*.py 에서 pipeline.train_risk_model.* 을 import 할 수 있다
    # (train_risk_model 계약 - registry/score/features 공유) - 컨테이너에서 직접 검증함.
    return (
        f"cd {RISK_MODEL_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:{AIRFLOW_HOME_DIR}:$PYTHONPATH {extra_env}{PYTHON} -m jobs.{job_module}"
    )


def _ingestion_bash(job_module: str, extra_env: str = "") -> str:
    # check_silver_watermark.py는 ingestion/jobs 소속 - gold_dim_fact_dag.py의
    # wait_for_silver_rental_history와 동일한 커맨드 형태를 그대로 쓴다.
    return (
        f"cd {INGESTION_DIR} && set -a && source .env && set +a && "
        f"PYTHONDONTWRITEBYTECODE=1 {extra_env}{PYTHON} -m jobs.{job_module}"
    )


@dag(
    dag_id=DAG_ID,
    schedule=None,  # gold_dim_fact.trigger_risk_decision(TriggerDagRunOperator)로만 실행
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gold", "trigger_only"],
    params={
        "snapshot_date": "",  # 수동 재처리용. 미지정 시 dag_run.conf.snapshot_date 또는 ds 사용
    },
    doc_md=__doc__,
)
def gold_risk_decision():
    snapshot_date_expr = '{{ dag_run.conf.get("snapshot_date") or params.snapshot_date or ds }}'

    # 1. wait_for_silver_failure_report - rental_history는 gold_dim_fact가 이미
    # 대기해줬지만 failure_report는 그 DAG 스코프 밖이라 여기서 직접 확인한다.
    wait_for_silver_failure_report = BashSensor(
        task_id="wait_for_silver_failure_report",
        bash_command=_ingestion_bash(
            "check_silver_watermark",
            f"DATASET=failure_report REQUIRED_OFFSET_DAYS=1 TARGET_DATE='{snapshot_date_expr}' ",
        ),
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
    )

    # 2. build_bike_features_daily - Silver(rental_history/failure_report)만 있으면 됨.
    # rental_history는 트리거 소스인 gold_dim_fact가 이미 대기했으므로, 여기선
    # failure_report 대기 뒤에만 실행하면 된다.
    build_bike_features_daily = BashOperator(
        task_id="build_bike_features_daily",
        bash_command=_bash("build_bike_features_daily", f"SNAPSHOT_DATE='{snapshot_date_expr}' "),
        execution_timeout=timedelta(minutes=30),
    )

    # 3. run_risk_scoring_model + build_fact_bike_risk
    # 원안엔 별도 태스크였지만, 모델 로드/추론 -> UPSERT가 하나의 Spark 세션 안에서
    # 자연스럽게 이어지는 처리라(build_fact_bike_risk.py) 태스크도 하나로 합쳤다.
    run_risk_scoring_model = BashOperator(
        task_id="run_risk_scoring_model",
        bash_command=_bash("build_fact_bike_risk", f"SNAPSHOT_DATE='{snapshot_date_expr}' "),
        execution_timeout=timedelta(minutes=30),
    )

    # 4. build_fact_bike_decision
    build_fact_bike_decision = BashOperator(
        task_id="build_fact_bike_decision",
        bash_command=_bash("build_fact_bike_decision", f"SNAPSHOT_DATE='{snapshot_date_expr}' "),
        execution_timeout=timedelta(minutes=30),
    )

    # 5. gold_to_serving_sync 트리거 - wait_for_completion=False로 걸어 sync 실패가
    # 이 DAG(이미 만들어진 gold 데이터는 그 자체로 유효함)를 실패로 만들지 않게 한다.
    #
    # conf로 이 DAG가 실제로 처리한 날짜(params.snapshot_date, 미지정이면 ds)를 명시적으로
    # 넘긴다 - gold_to_serving_sync는 자기 logical_date에서 파생한 ds로 파티션을 잡는데,
    # 이 DAG는 params.snapshot_date(백필 시 임의의 날짜)로 파티션을 잡아 둘이 서로 다른
    # 경로로 날짜를 정하므로, 명시 지정 없이는 백필/미래 스케줄 실행에서 어긋난다.
    trigger_serving_sync = TriggerDagRunOperator(
        task_id="trigger_serving_sync",
        trigger_dag_id="gold_to_serving_sync",
        logical_date="{{ logical_date }}",
        conf={"snapshot_date": snapshot_date_expr},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    wait_for_silver_failure_report >> build_bike_features_daily >> run_risk_scoring_model >> build_fact_bike_decision >> trigger_serving_sync


gold_risk_decision()
