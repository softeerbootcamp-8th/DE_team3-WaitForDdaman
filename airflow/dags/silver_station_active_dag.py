"""
Silver 일 배치 DAG - 실시간 대여정보 필터 (station_active)

`bronze.station_active`의 당일 스냅샷에서 station_id 집합만 추려 `silver.station_active`에
적재한다. silver_station_master_dag.py와 이유가 동일하다.

### catchup=False인 이유

bikeList API는 날짜 파라미터를 받지 않고 호출 시점의 전체 스냅샷만 반환한다. 브론즈
스냅샷이 없는 날은 실버도 만들 수 없고, API로 소급 조회할 방법이 없다. Airflow의
catchup으로 채울 수 있는 과거가 애초에 존재하지 않는다.

브론즈에 그 날짜 스냅샷이 있다면 SNAPSHOT_DATE로 수동 재처리할 수 있다.

### schedule
브론즈 일 배치가 06:00 KST에 돌므로 07:00으로 둔다 (silver_station_master와 동일 오프셋).

### PYTHONPATH
staging/에는 자체 common/이 없어 ingestion/common/(spark_session)을 재사용하고,
config는 레포 최상위 config 패키지를 쓴다(/opt/airflow/pylib/config로 마운트됨).
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

PYLIB_DIR = "/opt/airflow/pylib"          # config 패키지 (docker-compose가 ./config를 마운트)
INGESTION_DIR = "/opt/airflow/ingestion"  # common/(spark_session), .env 출처
STAGING_DIR = "/opt/airflow/staging"      # 잡 실행 위치
PYTHON_BIN = "python"

SILVER_MODULE = "silver_station_active"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _staging_bash(job_module: str, extra_env: str = "") -> str:
    """staging/ 잡 실행 커맨드. 설정값은 ingestion/.env를 그대로 source한다."""
    return (
        f"cd {STAGING_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={PYLIB_DIR}:{INGESTION_DIR}:{STAGING_DIR} "
        f"{extra_env}{PYTHON_BIN} -m jobs.{job_module}"
    )


@dag(
    dag_id="silver_station_active_daily",
    schedule="0 7 * * *",  # 매일 07:00 KST - bronze_daily_batch_all_sources(06:00) 뒤
    start_date=pendulum.datetime(2026, 8, 17, tz="Asia/Seoul"),
    catchup=False,  # 과거 스냅샷을 API로 소급 조회할 수 없다 - 위 doc 참고
    max_active_runs=1,  # 같은 파티션에 두 실행이 동시에 쓰는 것 방지
    default_args=default_args,
    tags=["daily_batch", "silver", "station_active"],
    doc_md=__doc__,
)
def silver_station_active_daily():
    BashOperator(
        task_id="silver_station_active",
        bash_command=_staging_bash(SILVER_MODULE),
        execution_timeout=timedelta(minutes=20),
    )


silver_station_active_daily()
