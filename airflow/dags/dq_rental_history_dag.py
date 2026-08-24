"""
Silver 대여이력 DQ 파일럿 DAG (#217)

어써션 실행 -> 히스토리 적재 -> 해석 에이전트 -> GitHub 이슈 생성/코멘트 -> Slack
알림, 5개 태스크로 분리한다(하나로 합치지 않는다는 요구사항 - 각 단계가 독립적으로
재시도/재실행 가능해야 함).

뒤 2개 태스크(GitHub/Slack)는 Airflow 레벨의 분기 오퍼레이터가 아니라 "할 일 없으면
조용히 스킵"하는 잡 내부 로직으로 short-circuit한다 - interpret_dq_results가 FAIL
없어서 스킵되면 해석 결과 파일 자체가 없고, report_dq_issue는 그 경우 곧바로
빈 리스트를 반환하며, is_anomaly=true인 체크가 하나도 없어도 마찬가지다. 두
태스크 다 외부 API(GitHub/Slack) 호출이 재시도까지 실패해도 태스크를 실패시키지
않는다 - 품질 "알림"이 안 됐다고 배치 전체가 막히면 안 된다는 게 팀 정책(#217).
이슈를 자동으로 닫는 로직은 절대 없다 - 사람이 직접 닫아야 한다.

RENTAL_HISTORY_SILVER Asset(silver_rental_history_dag.py의 transform_silver_rental_history
완료)으로 트리거된다 - Bronze Asset이 아니라 Silver 완료를 구독해야 어써션 대상
(silver.rental_history)이 그 실행 시점에 최신 상태다.

실행일은 항상 {{ ds }}(논리 실행일)를 그대로 쓴다 - "지금 시각"을 쓰면 재실행마다
다른 파티션을 보게 되어 결정론이 깨진다. 같은 {{ ds }}로 재실행해도 pending 결과
파일을 덮어쓰고 히스토리 테이블엔 append만 하므로 재실행 자체는 안전하지만, 중복
행이 쌓이는 건 막지 않는다(파일럿 범위 밖 - 필요해지면 log_dq_check_result에
run_id 기준 삭제-후-append를 추가한다).
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import RENTAL_HISTORY_SILVER
from dag_common import DEFAULT_ARGS, SILVER_POOL

INGESTION_DIR = "/opt/airflow/ingestion"
PYTHON = "python"
SOURCE_NAME = "rental_history"


def _bash(job_module: str) -> str:
    return (
        f"cd {INGESTION_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:$PYTHONPATH "
        "EXECUTION_DATE='{{ ds }}' "
        f"DQ_SOURCE_NAME={SOURCE_NAME} "
        f"{PYTHON} -m jobs.{job_module}"
    )


@dag(
    dag_id="dq_rental_history",
    schedule=[RENTAL_HISTORY_SILVER],  # 고정 시간이 아니라 Silver 완료 이벤트로 트리거
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dq", "asset_triggered", "silver"],
    doc_md=__doc__,
)
def dq_rental_history():
    run_assertions = BashOperator(
        task_id="run_dq_assertions",
        bash_command=_bash("run_dq_assertions_rental_history"),
        execution_timeout=timedelta(minutes=30),
        pool=SILVER_POOL,
    )

    log_result = BashOperator(
        task_id="log_dq_check_result",
        bash_command=_bash("log_dq_check_result"),
        execution_timeout=timedelta(minutes=10),
        pool=SILVER_POOL,
    )

    interpret_result = BashOperator(
        task_id="interpret_dq_results",
        bash_command=_bash("interpret_dq_results"),
        execution_timeout=timedelta(minutes=10),
        pool=SILVER_POOL,
    )

    report_issue = BashOperator(
        task_id="report_dq_issue",
        bash_command=_bash("report_dq_issue"),
        execution_timeout=timedelta(minutes=5),
        pool=SILVER_POOL,
    )

    notify_slack = BashOperator(
        task_id="notify_dq_slack",
        bash_command=_bash("notify_dq_slack"),
        execution_timeout=timedelta(minutes=5),
        pool=SILVER_POOL,
    )

    run_assertions >> log_result >> interpret_result >> report_issue >> notify_slack


dq_rental_history()
