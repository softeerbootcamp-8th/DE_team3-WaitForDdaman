"""
Bronze 백필 DAG (2개 원천) - 1회성, 수동 트리거

대여이력 / 고장신고 두 원천의 파일 기반 백필을 한 DAG에서 실행한다.

### 대여소정보(station_master)가 여기 없는 이유
이 원천은 파일 백필 대상이 아니다. tbCycleStationInfo는 날짜 파라미터를 받지 않고
호출 시점의 전체 스냅샷만 주므로 과거를 소급 적재할 수 없고, 반기 파일에는 골드
조인 키인 station_id가 없다 (jobs/daily_batch_station_master.py 참고).
대여소정보 적재는 bronze_daily_batch_all_sources DAG가 매일 스냅샷으로 담당한다.

### 태스크 의존성 설계
대여이력과 고장신고는 서로 의존관계가 없어서 병렬로 둔다.

    rental_history
    failure_report

### 왜 병렬을 2개로 제한하는가
로컬(LocalStack) 환경에서 여러 Spark 잡이 동시에 대량 PutObject를 보내면
"read of closed file" 레이스 컨디션이 발생하는 게 실측으로 확인됐다. 각 잡이 이미
내부적으로 로컬 병렬도를 제한하고 있으므로, DAG 레벨에서도 동시 실행을 제한한다.

### 실행 방법
Airflow UI에서 "Trigger DAG w/ config"로 각 소스의 input_dir / 파일 패턴을 조정할 수 있다.
예) 로컬 검증 시 대여이력만 1개월치: rental_history_pattern = "*2601*"
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

# docker-compose에서 ingestion 프로젝트를 이 경로로 마운트해야 한다.
# airflow/Dockerfile에 Java+pyspark+ingestion 의존성이 설치되어 있으므로 컨테이너의
# 시스템 python을 그대로 쓴다 (컨테이너 자체가 Spark 실행 환경).
INGESTION_DIR = "/opt/airflow/ingestion"
INGESTION_PYTHON = "python"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}


def _bash(job_module: str, extra_env: str = "") -> str:
    """공통 실행 커맨드. .env를 로드해 각 잡이 설정값을 읽을 수 있게 한다."""
    return (
        f"cd {INGESTION_DIR} && set -a && source .env && set +a && "
        f"{extra_env}{INGESTION_PYTHON} -m jobs.{job_module}"
    )


@dag(
    dag_id="bronze_backfill_all_sources",
    schedule=None,  # 백필은 반복 실행이 아니라 필요할 때 한 번 트리거하는 작업
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,  # 로컬 LocalStack 동시 쓰기 부하 제한
    default_args=default_args,
    tags=["backfill", "bronze", "all_sources"],
    params={
        "rental_history_dir": f"{INGESTION_DIR}/data/rental_history",
        "rental_history_pattern": "*",
        "failure_report_dir": f"{INGESTION_DIR}/data/failure_report",
        "failure_report_pattern": "*",
    },
    doc_md=__doc__,
)
def bronze_backfill_all_sources():
    # 대여이력: 반기 파일이 최대 700MB대라 가장 오래 걸림
    rental_history = BashOperator(
        task_id="backfill_rental_history",
        bash_command=_bash(
            "backfill_rental_history",
            "INPUT_DIR='{{ params.rental_history_dir }}' "
            "INPUT_FILE_PATTERN='{{ params.rental_history_pattern }}' ",
        ),
        execution_timeout=timedelta(hours=3),
    )

    # 고장신고: zip/csv/xlsx 혼합 입력, 볼륨은 작음
    failure_report = BashOperator(
        task_id="backfill_failure_report",
        bash_command=_bash(
            "backfill_failure_report",
            "INPUT_DIR='{{ params.failure_report_dir }}' "
            "INPUT_FILE_PATTERN='{{ params.failure_report_pattern }}' ",
        ),
        execution_timeout=timedelta(hours=1),
    )

    # 두 태스크 사이에 의존관계를 걸지 않는다.
    # 서로 참조하지 않는 원천이고, 동시 실행 부하는 max_active_tasks=2가 제한한다.


bronze_backfill_all_sources()
