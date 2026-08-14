"""
Silver DAG - 공공자전거 고장신고 내역 (bronze.failure_report -> silver.failure_report)

### 확정 스키마 (변경 금지)
    bike_no       STRING
    reg_dttm      TIMESTAMP   (브론즈 STRING -> 캐스팅)
    failure_type  STRING      (trim)

파티션: days(reg_dttm)  <- 브론즈의 적재일 파티션(reg_date_partition)이 아니다.
유일키: (bike_no, reg_dttm, failure_type)
그레인: 고장부위 신고 1건 = 1행 (이벤트 단위로 접지 않는다)

### daily vs backfill
| DAG | schedule | 처리 범위 | 적재 방식 |
|---|---|---|---|
| silver_failure_report_daily | 매일 | {{ ds }} 1일치 | MERGE INTO |
| silver_failure_report_backfill | None (수동 트리거) | params 기간 (start_date~end_date) | INSERT OVERWRITE |

daily는 catchup=True로 둔다 - {{ ds }} 파티션 단위 MERGE라 밀린 날짜를 (bronze처럼
잡 내부 워터마크가 아니라) Airflow 스케줄러가 직접 채워야 하기 때문이다.

### jobs 모듈
staging/README.md에 이미 "Bronze / Silver 생성 - Spark ETL" 자리로 명시돼 있어
staging/jobs/ 밑에 flat하게 담는다 (ingestion/jobs/와 동일한 파일당 1잡 구조).
staging/에는 자체 common/이 없고 ingestion/common/(config, spark_session)을
PYTHONPATH로 그대로 재사용한다 - 아래 _staging_bash() 참고.
아래 SILVER_* 상수가 가리키는 staging/jobs/silver_failure_report_*.py 는
check/transform/validate/metrics 4단계를 daily/backfill이 동일 모듈로 공유하되,
TARGET_DS 또는 START_DATE/END_DATE로 처리 범위만 다르게 받는 것을 가정한다.
적재 단계만 daily(MERGE)/backfill(INSERT OVERWRITE)가 별도 모듈이다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

INGESTION_DIR = "/opt/airflow/ingestion"  # common/(config, spark_session), .env 출처
STAGING_DIR = "/opt/airflow/staging"      # 잡 실행 위치
PYTHON_BIN = "python"

# ---- 잡 모듈 경로 상수 (staging/jobs/silver_failure_report_*.py) ----
SILVER_CHECK_MODULE = "silver_failure_report_check"
SILVER_TRANSFORM_MODULE = "silver_failure_report_transform"
SILVER_VALIDATE_MODULE = "silver_failure_report_validate"
SILVER_MERGE_MODULE = "silver_failure_report_merge"          # daily 전용 - MERGE INTO
SILVER_OVERWRITE_MODULE = "silver_failure_report_overwrite"  # backfill 전용 - INSERT OVERWRITE
SILVER_METRICS_MODULE = "silver_failure_report_metrics"

daily_default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

backfill_default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}


def _staging_bash(job_module: str, extra_env: str = "") -> str:
    """
    staging/ 잡 실행 커맨드. staging에는 common/이 없어 ingestion/common/을 그대로
    재사용한다 - PYTHONPATH에 ingestion과 staging을 같이 잡으면 `jobs`는 두 디렉터리의
    jobs/ 가 네임스페이스 패키지로 합쳐지고(PEP 420), `common`은 ingestion 쪽에서만
    찾긴다. 설정값(.env)도 ingestion/.env를 그대로 source한다.
    """
    return (
        f"cd {STAGING_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:{STAGING_DIR} "
        f"{extra_env}{PYTHON_BIN} -m jobs.{job_module}"
    )


@dag(
    dag_id="silver_failure_report_daily",
    schedule="0 7 * * *",  # 매일 07:00 KST - bronze_daily_batch_all_sources(06:00) 뒤에 여유를 두고 실행
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=True,  # {{ ds }} 단위 MERGE라 밀린 날짜는 Airflow가 직접 채워야 함
    max_active_runs=1,  # 같은 reg_dttm 파티션에 두 실행이 동시에 MERGE 시도하는 것 방지
    default_args=daily_default_args,
    tags=["silver", "failure_report", "daily"],
    doc_md=__doc__,
)
def silver_failure_report_daily():
    target_ds_env = "TARGET_DS='{{ ds }}' "

    check = BashOperator(
        task_id="check",
        bash_command=_staging_bash(SILVER_CHECK_MODULE, target_ds_env),
        execution_timeout=timedelta(minutes=10),
    )
    transform = BashOperator(
        task_id="transform",
        bash_command=_staging_bash(SILVER_TRANSFORM_MODULE, target_ds_env),
        execution_timeout=timedelta(minutes=30),
    )
    validate = BashOperator(
        task_id="validate",
        bash_command=_staging_bash(SILVER_VALIDATE_MODULE, target_ds_env),
        execution_timeout=timedelta(minutes=10),
    )
    merge = BashOperator(
        task_id="merge",
        bash_command=_staging_bash(SILVER_MERGE_MODULE, target_ds_env),
        execution_timeout=timedelta(minutes=30),
    )
    metrics = BashOperator(
        task_id="metrics",
        bash_command=_staging_bash(SILVER_METRICS_MODULE, target_ds_env),
        execution_timeout=timedelta(minutes=10),
    )

    check >> transform >> validate >> merge >> metrics


@dag(
    dag_id="silver_failure_report_backfill",
    schedule=None,  # 백필은 반복 실행이 아니라 필요할 때 한 번 트리거하는 작업
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=backfill_default_args,
    tags=["silver", "failure_report", "backfill"],
    params={
        # 둘 다 필수(YYYY-MM-DD, end_date 포함) - 비어 있으면 check 태스크에서 즉시 실패시켜야 한다.
        "start_date": "",
        "end_date": "",
    },
    doc_md=__doc__,
)
def silver_failure_report_backfill():
    period_env = "START_DATE='{{ params.start_date }}' END_DATE='{{ params.end_date }}' "

    check = BashOperator(
        task_id="check",
        bash_command=_staging_bash(SILVER_CHECK_MODULE, period_env),
        execution_timeout=timedelta(minutes=30),
    )
    transform = BashOperator(
        task_id="transform",
        bash_command=_staging_bash(SILVER_TRANSFORM_MODULE, period_env),
        execution_timeout=timedelta(hours=1),
    )
    validate = BashOperator(
        task_id="validate",
        bash_command=_staging_bash(SILVER_VALIDATE_MODULE, period_env),
        execution_timeout=timedelta(minutes=30),
    )
    overwrite = BashOperator(
        task_id="overwrite",
        bash_command=_staging_bash(SILVER_OVERWRITE_MODULE, period_env),
        execution_timeout=timedelta(hours=1),
    )
    metrics = BashOperator(
        task_id="metrics",
        bash_command=_staging_bash(SILVER_METRICS_MODULE, period_env),
        execution_timeout=timedelta(minutes=30),
    )

    check >> transform >> validate >> overwrite >> metrics


silver_failure_report_daily()
silver_failure_report_backfill()
