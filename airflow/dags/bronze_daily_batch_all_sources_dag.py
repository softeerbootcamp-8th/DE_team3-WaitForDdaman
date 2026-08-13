"""
Bronze 일 배치 DAG (3개 원천 통합) - 매일 실행

대여이력 / 고장신고 / 대여소정보 3개 원천의 API 증분 수집을 한 DAG에서 실행한다.

### 태스크 의존성 설계
    station_master ─┬─> rental_history
                    └─> failure_report

대여소정보를 먼저 적재하는 이유: 마스터(참조) 데이터라서 이벤트 데이터보다 먼저
최신 상태여야 한다. 신설 대여소가 마스터에 반영되기 전에 이벤트가 먼저 들어오면
Silver 조인 단계에서 고아(orphan) 레코드가 생기는데, 순서를 이렇게 두면 그 창을 줄일 수 있다.
대여이력과 고장신고는 서로 의존관계가 없어 병렬로 둔다.

### 각 소스의 증분 전략이 서로 다르다
| 소스 | API | 증분 기준 | 워터마크 |
|---|---|---|---|
| 대여이력 | tbCycleRentData | RENT_DT (날짜+시간 0~23시) | O |
| 고장신고 | tbCycleFailureReport | REGDTTM (날짜) | O |
| 대여소정보 | tbCycleStationInfo | 없음 (매번 전체 스냅샷) | X |

대여소정보만 워터마크가 없는 이유: 이 API는 날짜 파라미터를 받지 않고 호출 시점의
전체 스냅샷만 준다. 그래서 매일 찍어서 snapshot_date 파티션으로 쌓는 방식이고,
과거 날짜를 소급 조회할 수 없다 (Airflow backfill로 과거를 채울 수 없는 소스).

### catchup=False인 이유
대여이력/고장신고는 밀린 날짜를 스크립트 내부 워터마크 로직이 알아서 이어서 처리한다.
Airflow의 catchup에 의존하지 않고, "하루 한 번 트리거"만 Airflow가 책임진다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

INGESTION_DIR = "/opt/airflow/ingestion"
INGESTION_PYTHON = "python"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _bash(job_module: str, extra_env: str = "") -> str:
    return (
        f"cd {INGESTION_DIR} && set -a && source .env && set +a && "
        f"{extra_env}{INGESTION_PYTHON} -m jobs.{job_module}"
    )


@dag(
    dag_id="bronze_daily_batch_all_sources",
    schedule="0 6 * * *",  # 매일 06:00 KST - 전날 데이터가 확정된 뒤 수집
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 같은 파티션에 두 실행이 동시에 덮어쓰기 시도하는 것 방지
    max_active_tasks=2,  # 로컬 LocalStack 동시 쓰기 부하 제한
    default_args=default_args,
    tags=["daily_batch", "bronze", "all_sources"],
    params={
        # 로컬 검증 시 하루치만 처리하고 싶을 때 사용 (빈 문자열이면 밀린 날짜 전부 처리)
        "max_days_per_run": "",
    },
    doc_md=__doc__,
)
def bronze_daily_batch_all_sources():
    # 대여소정보: 워터마크 없음, 매일 전체 스냅샷을 그날 파티션으로 적재
    station_master = BashOperator(
        task_id="daily_batch_station_master",
        bash_command=_bash("daily_batch_station_master"),
        execution_timeout=timedelta(minutes=30),
    )

    # 대여이력: 하루치가 시간 단위 24회 호출 + 페이징이라 가장 오래 걸림
    rental_history = BashOperator(
        task_id="daily_batch_rental_history",
        bash_command=_bash(
            "daily_batch_rental_history",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(hours=2),
    )

    failure_report = BashOperator(
        task_id="daily_batch_failure_report",
        bash_command=_bash(
            "daily_batch_failure_report",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(hours=1),
    )

    station_master >> [rental_history, failure_report]


bronze_daily_batch_all_sources()
