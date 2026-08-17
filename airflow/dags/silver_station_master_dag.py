"""
Silver 일 배치 DAG - 대여소 마스터 (station_master)

`bronze.station_master`의 당일 스냅샷을 정제해 `silver.station_master`에 적재한다.
정제 내용은 타입 캐스팅 / region(강남·강북) 파생 / 대여소명 공백 정규화 세 가지다.

### catchup=False인 이유 (고장신고·대여이력 실버와 다른 점)

고장신고·대여이력은 `{{ ds }}` 1일치를 처리하므로 밀린 날짜를 Airflow가 채우는 게
의미가 있다(catchup=True). 대여소는 그렇지 않다.

    tbCycleStationInfo API는 날짜 파라미터를 받지 않고 호출 시점의 전체 스냅샷만
    반환한다. 브론즈 스냅샷이 없는 날은 실버도 만들 수 없고, API로 소급 조회할
    방법이 없다. 즉 catchup으로 채울 수 있는 과거가 애초에 존재하지 않는다.

브론즈에 그 날짜 스냅샷이 있다면 SNAPSHOT_DATE로 수동 재처리할 수 있다.

### schedule
브론즈 일 배치가 06:00 KST에 돌므로 07:00으로 둔다. 대여소 브론즈 태스크는
실측 23초에 끝나지만, 같은 DAG의 대여이력이 오래 걸릴 수 있어 여유를 둔다.

### PYTHONPATH
staging/에는 자체 common/이 없어 ingestion/common/(spark_session)을 재사용하고,
config는 레포 최상위 config 패키지를 쓴다(/opt/airflow/pylib/config로 마운트됨).
세 경로를 모두 잡아야 `import config`와 `from common.spark_session import ...`이
동시에 동작한다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

PYLIB_DIR = "/opt/airflow/pylib"          # config 패키지 (docker-compose가 ./config를 마운트)
INGESTION_DIR = "/opt/airflow/ingestion"  # common/(spark_session), .env 출처
STAGING_DIR = "/opt/airflow/staging"      # 잡 실행 위치
PYTHON_BIN = "python"

SILVER_MODULE = "silver_station_master"

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
    dag_id="silver_station_master_daily",
    schedule="0 7 * * *",  # 매일 07:00 KST - bronze_daily_batch_all_sources(06:00) 뒤
    start_date=pendulum.datetime(2026, 8, 14, tz="Asia/Seoul"),
    catchup=False,  # 과거 스냅샷을 API로 소급 조회할 수 없다 - 위 doc 참고
    max_active_runs=1,  # 같은 파티션에 두 실행이 동시에 쓰는 것 방지
    default_args=default_args,
    tags=["daily_batch", "silver", "station_master"],
    doc_md=__doc__,
)
def silver_station_master_daily():
    BashOperator(
        task_id="silver_station_master",
        bash_command=_staging_bash(SILVER_MODULE),
        execution_timeout=timedelta(minutes=20),
    )


silver_station_master_daily()
