"""
Bronze DAG 공통 설정 - 일 배치/백필 DAG가 import해서 쓴다.

실행 커맨드 헬퍼와 ingestion 경로, 동시성 풀이 DAG마다 복제되는 걸 막는다.
dag_assets.py와 같은 방식으로, Airflow가 dags 폴더를 sys.path에 넣어 파싱하므로
`from dag_common import ...`를 별도 패키징 없이 그대로 쓸 수 있다.
"""
import os
from datetime import timedelta

import requests

INGESTION_DIR = "/opt/airflow/ingestion"
STAGING_DIR = "/opt/airflow/staging"  # staging/jobs/ 잡(Silver 등) 실행 위치
INGESTION_PYTHON = "python"

# ==============================================================================
# 워커 자원 가드 (Worker Resource Guard) - Issue #144
# ==============================================================================
# Spark를 제거하고 PyArrow / DuckDB 기반 인프로세스 연산으로 전환됨에 따라,
# 연산 부하가 Airflow 워커 프로세스로 집중된다.
#
# 워커 프로세스가 여러 대용량 파티션 연산을 동시에 수행하다가 OOM으로 사망하는 것을
# 방지하기 위해, 전역 풀(Pool)을 통해 동시 실행 태스크 수를 제한하는 "워커 메모리 가드"로 동작한다.
#
# 풀 생성은 docker-compose*.yml의 airflow-init이 기동 때마다 수행한다(있으면 갱신).
# 풀이 없으면 이 풀을 지정한 태스크는 스케줄되지 못하고 대기 상태에 머문다.
BRONZE_POOL = "bronze_ingest"

# Silver DAG 5개(station_master/rental_history/failure_report/station_active/
# bikeman_action)는 전부 Bronze Asset 트리거라 언제 몇 개가 동시에 도는지
# 서로 모른다. DuckDB 윈도우/조인 연산의 동시 메모리 점유를 제어하기 위해
# SILVER_POOL로 전역 동시 실행 수를 제한한다 (워커 메모리 보호).
SILVER_POOL = "silver_process"

# 대여이력 Raw 수집/선택/승격은 실제 프로세스 시작 시각이 아니라 "논리 실행 시각"으로
# 판단해야 한다 (같은 DAGRun의 재시도가 같은 window/같은 key를 보게 하기 위함).
# 예약 실행은 data_interval_end, 수동 실행은 dag_run.conf.collection_cutoff_at을 쓴다.
# 예비 DAG와 일 배치 DAG가 서로 다른 규칙을 쓰면 selection이 조용히 어긋나므로 여기서 공유한다.
COLLECTION_CUTOFF_AT_TEMPLATE = (
    '{{ dag_run.conf.get("collection_cutoff_at") '
    'if dag_run and dag_run.conf.get("collection_cutoff_at") '
    'else data_interval_end.in_timezone("Asia/Seoul").isoformat() }}'
)

# ==============================================================================
# 태스크 최종 실패 알림 (Slack) - Issue #180
# ==============================================================================
# 원래 bikeman_event_generator_dag.py / gold_to_serving_sync_dag.py 두 곳에
# 동일한 함수가 각자 복제돼 있었다. 여기 하나로 모아서 다른 DAG들도 재시도가
# 전부 소진돼 최종 실패로 확정된 태스크에 대해 Slack 알림을 받을 수 있게 한다.
#
# SLACK_WEBHOOK_URL은 Airflow 워커 프로세스의 환경변수다 - infra/terraform의
# notify_slack Lambda가 읽는 SLACK_WEBHOOK_URL(AWS Lambda 환경변수)과 이름은
# 같지만 완전히 별개의 값이다. 여기서 알림을 받으려면 Airflow 쪽 .env/compose
# 환경변수로 따로 설정해야 한다.
def notify_slack_on_failure(context: dict) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    ti = context["task_instance"]
    message = f":x: *{ti.dag_id}.{ti.task_id}* 실패\n실행일: {context['ds']}\n로그: {ti.log_url}"
    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
    except requests.RequestException:
        pass


# 일반 잡 기본 재시도 설정
# (단, ML Spark 학습 잡인 build_train_samples는 Spark 자원 낭비 방지를 위해 retries=0 유지)
DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_slack_on_failure,
}


def bash_job(job_module: str, extra_env: str = "") -> str:
    """
    공통 실행 커맨드. .env를 로드해 각 잡이 설정값을 읽을 수 있게 한다.

    PYTHONDONTWRITEBYTECODE=1: 브론즈 일 배치는 여러 태스크가 동시에(BRONZE_POOL
    안에서 병렬로) 같은 ingestion/common 모듈을 처음 import하는데, 이때 여러
    프로세스가 동시에 같은 .pyc 캐시 파일에 쓰다가 파일이 잘려서
    "EOFError: marshal data too short"로 실패하는 것을 방지한다.
    """
    return (
        f"cd {INGESTION_DIR} && set -a && source .env && set +a && "
        f"PYTHONDONTWRITEBYTECODE=1 {extra_env}{INGESTION_PYTHON} -m jobs.{job_module}"
    )


def bash_staging_job(job_module: str, extra_env: str = "") -> str:
    """
    staging/jobs/(Silver 등) 잡 실행 커맨드. staging에는 자체 common/이 없어
    ingestion/common/(config, watermark, iceberg_io, sql_assert)을 PYTHONPATH로
    그대로 재사용한다.
    """
    return (
        f"cd {STAGING_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:{STAGING_DIR}:$PYTHONPATH "
        f"PYTHONDONTWRITEBYTECODE=1 {extra_env}{INGESTION_PYTHON} -m jobs.{job_module}"
    )
