"""
Bronze DAG 공통 설정 - 일 배치/백필 DAG가 import해서 쓴다.

실행 커맨드 헬퍼와 ingestion 경로, 동시성 풀이 DAG마다 복제되는 걸 막는다.
dag_assets.py와 같은 방식으로, Airflow가 dags 폴더를 sys.path에 넣어 파싱하므로
`from dag_common import ...`를 별도 패키징 없이 그대로 쓸 수 있다.
"""
from datetime import timedelta

INGESTION_DIR = "/opt/airflow/ingestion"
STAGING_DIR = "/opt/airflow/staging"  # staging/jobs/ 잡(Silver 등) 실행 위치
INGESTION_PYTHON = "python"

# 로컬(LocalStack)에서 여러 Spark 잡이 동시에 대량 PutObject를 보내면
# "read of closed file" 레이스가 발생하는 게 실측으로 확인됐다.
# 일 배치 DAG 안에서는 max_active_tasks=2로 막히지만, 백필은 별도 DAG라 그 제한을
# 공유하지 않는다. 백필은 몇 시간짜리라 일 배치 스케줄과 겹치는 게 정상 케이스이므로,
# 두 DAG의 Spark 태스크를 이 풀로 묶어 전역 동시 실행 수를 제한한다.
#
# 풀 생성은 docker-compose*.yml의 airflow-init이 기동 때마다 해준다(있으면 갱신).
# 손으로 만들 필요는 없지만, 이 이름을 바꾸면 compose 쪽도 같이 바꿔야 한다.
# 풀이 없으면 이 풀을 지정한 태스크는 스케줄되지 못하고 DAG가 조용히 멈춘다.
BRONZE_POOL = "bronze_ingest"

# Silver DAG 5개(station_master/rental_history/failure_report/station_active/
# bikeman_action)는 전부 Bronze Asset 트리거라 언제 몇 개가 동시에 도는지
# 서로 모른다. BRONZE_POOL과 같은 이유(LocalStack 동시 쓰기 경합)로 전역
# 동시 실행 수를 제한한다 (#71).
#
# 풀 생성은 BRONZE_POOL과 동일하게 docker-compose*.yml의 airflow-init이
# 기동 때마다 해준다. 풀이 없으면 이 풀을 지정한 태스크는 스케줄되지 못하고
# DAG가 조용히 멈춘다.
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

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def bash_job(job_module: str, extra_env: str = "") -> str:
    """
    공통 실행 커맨드. .env를 로드해 각 잡이 설정값을 읽을 수 있게 한다.

    PYTHONDONTWRITEBYTECODE=1: 브론즈 일 배치는 여러 태스크가 동시에(BRONZE_POOL
    안에서 병렬로) 같은 ingestion/common 모듈을 처음 import하는데, 이때 여러
    프로세스가 동시에 같은 .pyc 캐시 파일에 쓰다가 파일이 잘려서
    "EOFError: marshal data too short"로 실패하는 게 실측으로 확인됐다
    (2026-08-17). 바이트코드 캐시 자체를 안 만들게 해서 이 경합을 없앤다 -
    이 정도 배치 잡에서 컴파일 오버헤드는 무시할 수준.
    """
    return (
        f"cd {INGESTION_DIR} && set -a && source .env && set +a && "
        f"PYTHONDONTWRITEBYTECODE=1 {extra_env}{INGESTION_PYTHON} -m jobs.{job_module}"
    )


def bash_staging_job(job_module: str, extra_env: str = "") -> str:
    """
    staging/jobs/(Silver 등) 잡 실행 커맨드. staging에는 자체 common/이 없어
    ingestion/common/(config, spark_session, watermark)을 PYTHONPATH로 그대로
    재사용한다 - PYTHONPATH에 ingestion과 staging을 같이 잡으면 `jobs`는 두
    디렉터리의 jobs/가 네임스페이스 패키지로 합쳐지고(PEP 420), `common`은
    ingestion 쪽에서만 찾힌다. .env도 ingestion/.env를 그대로 source한다.

    PYTHONDONTWRITEBYTECODE=1: bash_job()과 동일한 이유 - Silver DAG 여러 개가
    SILVER_POOL 안에서 동시에 같은 ingestion/common을 처음 import할 때의 .pyc
    캐시 쓰기 경합을 막는다.
    """
    return (
        f"cd {STAGING_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:{STAGING_DIR}:$PYTHONPATH "
        f"PYTHONDONTWRITEBYTECODE=1 {extra_env}{INGESTION_PYTHON} -m jobs.{job_module}"
    )
