"""
Bronze DAG 공통 설정 - 일 배치/백필 DAG가 import해서 쓴다.

실행 커맨드 헬퍼와 ingestion 경로, 동시성 풀이 DAG마다 복제되는 걸 막는다.
dag_assets.py와 같은 방식으로, Airflow가 dags 폴더를 sys.path에 넣어 파싱하므로
`from dag_common import ...`를 별도 패키징 없이 그대로 쓸 수 있다.
"""
from datetime import timedelta

INGESTION_DIR = "/opt/airflow/ingestion"
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
