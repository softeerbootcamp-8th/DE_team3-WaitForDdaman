"""
Bronze 일 배치 DAG (4개 원천 통합) - 매일 실행

대여이력 / 고장신고 / 대여소정보 / 따맨(bikeman) 이벤트, 4개 원천의 증분 수집을
한 DAG에서 실행한다.

### 태스크 의존성 설계
    station_master ─┬─> rental_history
                    ├─> failure_report
                    └─> bikeman_event

대여소정보를 먼저 적재하는 이유: 마스터(참조) 데이터라서 이벤트 데이터보다 먼저
최신 상태여야 한다. 신설 대여소가 마스터에 반영되기 전에 이벤트가 먼저 들어오면
Silver 조인 단계에서 고아(orphan) 레코드가 생기는데, 순서를 이렇게 두면 그 창을 줄일 수 있다.
이 원리는 bikeman_event의 station_id에도 동일하게 적용되므로 같은 위치(station_master 뒤,
나머지와 병렬)에 둔다. 대여이력/고장신고/bikeman_event 세 개는 서로 의존관계가 없어
병렬로 둔다.

### bikeman_event가 다른 3개와 다른 점 (구조는 동일, 소스만 다름)
| 소스 | 조회 방식 | 증분 기준 | 재처리 |
|---|---|---|---|
| 대여이력 | 공공 API | RENT_DT | 없음 |
| 고장신고 | 공공 API | REGDTTM | 없음 |
| 대여소정보 | 공공 API | 없음(전체 스냅샷) | 없음 |
| bikeman_event | 우리 Postgres 직접 조회 | occurred_at | **3일 lookback** |

bikeman만 재처리(lookback)가 있는 이유: 오프라인 작업 후 몰아서 제출하는 게
정상 케이스라, "어제"만 보면 이미 확정된 날짜에 늦게 도착한 이벤트를 놓친다.
daily_batch_bikeman_event.py 내부에서 매 실행마다 3일치를 다시 계산해서
overwritePartitions로 안전하게 덮어쓴다 - DAG 레벨에서는 신경 쓸 게 없다.

### 최초 실행 전 필수 절차
rental_history/failure_report와 마찬가지로, bikeman_event도 최초 실행 전에
set_watermark DAG(또는 CLI)로 워터마크를 찍어야 한다. bikeman은 파일 백필이
아니라 서비스 시작일 전날(2026-06-29)을 찍는다:
    dataset=bikeman_event, watermark_date=2026-06-29

### catchup=False인 이유
대여이력/고장신고/bikeman_event는 밀린 날짜를 스크립트 내부 워터마크 로직이
알아서 이어서 처리한다. Airflow의 catchup에 의존하지 않고, "하루 한 번 트리거"만
Airflow가 책임진다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import BIKEMAN_EVENT_BRONZE

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
    max_active_tasks=2,  # 로컬 LocalStack 동시 쓰기 부하 제한 (4개 태스크가 됐어도 동일하게 유지)
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

    # bikeman(따맨) 이벤트: 우리 자체 Postgres에서 직접 조회. 다른 3개는 공공 API지만
    # Bronze 레이어 관점에서는 "이벤트를 생성하는 원천"으로 동등하게 취급해 여기 포함.
    # outlets=[BIKEMAN_EVENT_BRONZE] - 이 태스크가 성공적으로 끝나면 Silver DAG가
    # 고정 시간이 아니라 이 이벤트를 받아서 자동으로 트리거된다 (실패/스킵 시엔 발생 안 함).
    bikeman_event = BashOperator(
        task_id="daily_batch_bikeman_event",
        bash_command=_bash(
            "daily_batch_bikeman_event",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(minutes=30),
        outlets=[BIKEMAN_EVENT_BRONZE],
    )

    station_master >> [rental_history, failure_report, bikeman_event]


bronze_daily_batch_all_sources()
