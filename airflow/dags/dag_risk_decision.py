"""
dag_risk_decision - 위험도 추론 + 대여중단 결정

dag_gold_dim_fact(gold.bike_location, gold.fact_station_inventory 등)가 끝난 뒤, 저장된
위험도 모델로 매일 자전거별 risk_score/risk_grade를 추론하고(gold.fact_bike_risk), 대여소
재고 상황을 반영해 대여중단/보류를 결정한다(gold.fact_bike_decision).

gold.bike_features_daily(추론용 feature)는 dag_gold_dim_fact 소관이 아니라 이 DAG이
직접 만든다(build_bike_features_daily) - Silver(rental_history/failure_report)만
있으면 되고 순수 이 모델 전용 입력이라 risk_model 스코프로 판단했다.

### wait_for_gold_facts를 왜 ExternalTaskSensor로 뒀는가
같은 리포의 silver_bikeman_action_daily_dag는 이미 Asset 기반 트리거(schedule=[Asset])로
가있고, dag_gold_dim_fact -> dag_risk_decision도 구조적으로 같은 이유(업스트림 완료
시각이 매일 일정하지 않음)로 Asset이 더 안전하다. 다만 지금은 계획 문서에 적힌 대로
ExternalTaskSensor로 먼저 만들어두고, 필요해지면 Asset으로 바꾼다.

### check_cold_start
wait_for_yesterday_decision을 soft_fail=True로 뒀다 - 최초 실행일(T-1 파티션이
아예 없음)엔 타임아웃이 나도 DAG 전체를 실패시키지 않고 skipped로 남기고, 그
상태를 check_cold_start가 보고 분기한다.

### 필터가 cold start를 안 타는 이유
apply_lagged_filter는 실제로는 bike_man_action 최신 이벤트만 보고 fact_bike_decision
내용 자체는 안 읽는다 (매일 재계산이라 "어제 대여중단이었다"는 사실이 오늘 결과를
바꾸지 않음 - 대여소 재고가 바뀌면 오늘 다시 대여중단/보류가 갈릴 수 있어서). 그래서
4-a/4-b가 실제로는 같은 job을 부르지만, 계획 문서에 있는 이름 그대로 태스크는
남겨뒀다.

### trigger_serving_sync (2026-08-18 추가)
build_fact_bike_decision이 끝나면 gold_to_serving_sync(gold_to_serving_sync_dag.py)를
TriggerDagRunOperator로 트리거한다. bike_risk_daily 마트가 필요로 하는
fact_bike_risk/fact_bike_decision은 이 DAG의 산출물이라, dag_gold_dim_fact가 아니라
여기서 트리거해야 두 마트(station_daily 포함) 모두 만들 재료가 갖춰진다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag

RISK_MODEL_DIR = "/opt/airflow/pipeline/risk_model"
INGESTION_DIR = "/opt/airflow/ingestion"
PYTHON = "python"

DAG_ID = "dag_risk_decision"
UPSTREAM_DAG_ID = "dag_gold_dim_fact"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _bash(job_module: str, extra_env: str = "") -> str:
    return (
        f"cd {RISK_MODEL_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:$PYTHONPATH {extra_env}{PYTHON} -m jobs.{job_module}"
    )


def _branch_on_cold_start(**context) -> str:
    # Airflow 3 Task SDK에서는 dag_run이 Pydantic 모델이라 get_task_instance() 같은
    # 2.x ORM 메서드가 없고, 태스크 실행 중 직접 DB(ORM) 접근도 금지돼 있다
    # (RuntimeError: Direct database access via the ORM is not allowed).
    # 공식 대안은 ti.get_task_states() - Execution API를 통해 형제 태스크 상태를 조회한다.
    dag_run = context["dag_run"]
    ti = context["ti"]
    states = ti.get_task_states(
        dag_id=DAG_ID,
        task_ids=["wait_for_yesterday_decision"],
        run_ids=[dag_run.run_id],
    )
    if states.get(dag_run.run_id, {}).get("wait_for_yesterday_decision") == "skipped":
        return "skip_filter_first_run"
    return "apply_lagged_filter"


@dag(
    dag_id=DAG_ID,
    schedule=None,  # TODO: dag_gold_dim_fact 완료 후 트리거 (고정 시간 없음 - 센서가 대기)
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["daily_batch", "gold", "risk_decision"],
    params={
        "snapshot_date": "",  # 미지정 시 각 job이 오늘 날짜로 기본 처리
    },
    doc_md=__doc__,
)
def dag_risk_decision():
    # 1. wait_for_gold_facts
    wait_for_gold_facts = ExternalTaskSensor(
        task_id="wait_for_gold_facts",
        external_dag_id=UPSTREAM_DAG_ID,
        external_task_id=None,  # 그 DAG 전체 성공을 기다림 (태스크 이름 변경에 안전)
        execution_date_fn=lambda dt: dt,
        mode="reschedule",
        timeout=timedelta(hours=3).total_seconds(),
    )

    # 2. wait_for_yesterday_decision
    wait_for_yesterday_decision = ExternalTaskSensor(
        task_id="wait_for_yesterday_decision",
        external_dag_id=DAG_ID,
        external_task_id="build_fact_bike_decision",
        execution_date_fn=lambda dt: dt - timedelta(days=1),
        mode="reschedule",
        timeout=timedelta(minutes=10).total_seconds(),
        soft_fail=True,  # 최초 실행일엔 T-1이 없어서 타임아웃 -> skipped로 처리 (DAG 안 죽음)
    )

    # 3. check_cold_start
    # 기본 trigger_rule(all_success)이면 바로 위(wait_for_yesterday_decision)가 cold
    # start로 skipped됐을 때 이 태스크까지 자동으로 skip되어 분기 판단 자체가 안 된다.
    # skip도 "실패는 아님"으로 취급해 계속 진행해야 하므로 none_failed로 바꾼다.
    check_cold_start = BranchPythonOperator(
        task_id="check_cold_start",
        python_callable=_branch_on_cold_start,
        trigger_rule="none_failed",
    )

    # 4-a / 4-b. 둘 다 EmptyOperator - 실제 필터 분기는 없고(아래 5+6 참고),
    # 원안의 태스크 이름/구조만 유지하기 위한 자리표시.
    skip_filter_first_run = EmptyOperator(task_id="skip_filter_first_run")
    apply_lagged_filter = EmptyOperator(task_id="apply_lagged_filter")

    # bike_features_daily는 Silver(rental_history/failure_report)만 있으면 되고
    # dag_gold_dim_fact의 Gold 산출물엔 의존하지 않아, gold_facts 대기와 별도로 병렬 실행한다.
    build_bike_features_daily = BashOperator(
        task_id="build_bike_features_daily",
        bash_command=_bash("build_bike_features_daily", "SNAPSHOT_DATE='{{ params.snapshot_date }}' "),
        execution_timeout=timedelta(minutes=30),
    )

    # 5+6. run_risk_scoring_model + build_fact_bike_risk
    # 원안엔 별도 태스크였지만, 모델 로드/추론 -> UPSERT가 하나의 Spark 세션 안에서
    # 자연스럽게 이어지는 처리라(build_fact_bike_risk.py) 태스크도 하나로 합쳤다.
    #
    # trigger_rule=none_failed_min_one_success: 이 태스크는 check_cold_start 분기의
    # 합류 지점이다 - skip_filter_first_run/apply_lagged_filter 중 하나는 분기 설계상
    # 항상 skipped라서, 기본 trigger_rule(all_success)이면 이 조건이 영원히 안 맞아
    # run_risk_scoring_model 이하가 전부 자동 skip되고 DAG는 그대로 success로 표시된다
    # (실제로는 스코어링도 결정도 안 만들어졌는데 겉보기엔 성공 - 가장 위험한 실패 모드).
    run_risk_scoring_model = BashOperator(
        task_id="run_risk_scoring_model",
        bash_command=_bash("build_fact_bike_risk", "SNAPSHOT_DATE='{{ params.snapshot_date }}' "),
        execution_timeout=timedelta(minutes=30),
        trigger_rule="none_failed_min_one_success",
    )

    # 7. build_fact_bike_decision
    build_fact_bike_decision = BashOperator(
        task_id="build_fact_bike_decision",
        bash_command=_bash("build_fact_bike_decision", "SNAPSHOT_DATE='{{ params.snapshot_date }}' "),
        execution_timeout=timedelta(minutes=30),
    )

    # 8. gold_to_serving_sync 트리거 - wait_for_completion=False로 걸어 sync 실패가
    # 이 DAG(이미 만들어진 gold 데이터는 그 자체로 유효함)를 실패로 만들지 않게 한다.
    trigger_serving_sync = TriggerDagRunOperator(
        task_id="trigger_serving_sync",
        trigger_dag_id="gold_to_serving_sync",
        logical_date="{{ logical_date }}",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    wait_for_gold_facts >> wait_for_yesterday_decision >> check_cold_start
    check_cold_start >> [skip_filter_first_run, apply_lagged_filter]
    [skip_filter_first_run, apply_lagged_filter, build_bike_features_daily] >> run_risk_scoring_model >> build_fact_bike_decision
    build_fact_bike_decision >> trigger_serving_sync


dag_risk_decision()
