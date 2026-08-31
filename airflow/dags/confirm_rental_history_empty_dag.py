"""대여이력 0행 날짜 수동 승인 DAG.

확정 날짜의 전체 24시간 API 호출이 성공했지만 0행인 경우 자동 승격하지 않는다.
운영자가 원천 상태를 확인한 뒤 이 DAG에 대상 날짜, 확인자, 사유를 입력한다.

잡은 COMPLETE_EMPTY manifest와 빈 payload, Bronze/Silver의 연속 워터마크를 다시
검증한다. 검증을 통과하면 감사 가능한 completion marker를 먼저 기록한 뒤 두
워터마크를 해당 날짜까지 한 칸 전진시킨다. 임의 날짜 점프에는 사용할 수 없다.

Airflow UI의 Trigger DAG w/ config 입력 예시:

    {
      "target_date": "2026-08-21",
      "confirmed_by": "ezzkimm",
      "reason": "서울 API 24시간 조회 결과 실제 0행 확인"
    }
"""

import sys

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

PYLIB_DIR = "/opt/airflow/pylib"
INGESTION_DIR = "/opt/airflow/src"


def _confirm_empty_callable(target_date_str: str, confirmed_by: str, reason: str):
    if PYLIB_DIR not in sys.path:
        sys.path.insert(0, PYLIB_DIR)
    if INGESTION_DIR not in sys.path:
        sys.path.insert(0, INGESTION_DIR)

    from bronze.confirm_rental_history_empty import run

    return run(
        target_date_str=target_date_str,
        confirmed_by=confirmed_by,
        reason=reason,
    )


@dag(
    dag_id="confirm_rental_history_empty",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "manual"],
    params={
        "target_date": "",
        "confirmed_by": "",
        "reason": "",
    },
    doc_md=__doc__,
)
def confirm_rental_history_empty():
    PythonOperator(
        task_id="confirm_empty_date",
        python_callable=_confirm_empty_callable,
        op_kwargs={
            "target_date_str": "{{ params.target_date }}",
            "confirmed_by": "{{ params.confirmed_by }}",
            "reason": "{{ params.reason }}",
        },
    )


confirm_rental_history_empty()
