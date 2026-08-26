"""
Bronze DAG 공통 설정 - 일 배치/백필 DAG가 import해서 쓴다.

실행 커맨드 헬퍼와 ingestion 경로, 동시성 풀이 DAG마다 복제되는 걸 막는다.
dag_assets.py와 같은 방식으로, Airflow가 dags 폴더를 sys.path에 넣어 파싱하므로
`from dag_common import ...`를 별도 패키징 없이 그대로 쓸 수 있다.
"""
import logging
import os
import shlex
import time
from datetime import timedelta

import requests

logger = logging.getLogger(__name__)

INGESTION_DIR = "/opt/airflow/ingestion"
STAGING_DIR = "/opt/airflow/staging"  # staging/jobs/ 잡(Silver 등) 실행 위치
INGESTION_PYTHON = "python"


def load_env_file(env_path: str = "/opt/airflow/.env") -> None:
    """BashOperator가 source하던 .env 값을 TaskFlow/Python 태스크에서도 볼 수 있게 한다."""
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            if not os.environ.get(key):
                os.environ[key] = value.strip()


load_env_file()

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

# EMR Serverless로 제출하는 초기 적재 배치 전용 풀 (#249). BRONZE_POOL과 분리하는 이유:
# BRONZE_POOL은 "Airflow 워커 프로세스 자체의 PyArrow 메모리"를 보호하는 가드인데,
# EMR 제출 태스크는 워커에서 boto3로 start_job_run 후 polling만 할 뿐 무거운 연산을
# 워커에서 하지 않는다 - 진짜 자원 제약은 EMR Serverless 애플리케이션의 pre-initialized
# capacity(Driver 1개 + Executor 3개, 각 4vCPU/16GB)다. 슬롯 수를 이 pre-initialized
# Executor 개수에 맞춰, 매핑된 배치 태스크들이 지나치게 많이 동시 제출돼 매번 콜드스타트
# Executor를 새로 띄우는 상황(=오버헤드 재발)을 피한다. 그 이상 필요하면 EMR Serverless가
# on-demand로 최대 400vCPU까지 자동 확장하므로 슬롯을 넘는 배치는 대기했다가 순서대로 돈다.
EMR_INITIAL_LOAD_POOL = "emr_initial_load"

# 초기 적재 S3 스테이징 업로드 전용 풀 (#255). BRONZE_POOL/EMR_INITIAL_LOAD_POOL과 분리하는
# 이유: 이 태스크는 Airflow 워커(EC2 t4g.large, 2vCPU/8GB) 프로세스 안에서 boto3로 직접
# 로컬 MD5 계산 + PutObject/CopyObject를 수행한다 - EMR 제출 태스크(가벼운 polling)와도,
# PyArrow 연산(BRONZE_POOL)과도 자원 성격이 다르다. 슬롯을 낮게 잡아, 완전히 빈 S3에
# ~40~47GB/114개 파일을 한 번에 올려야 하는 최초 적재 시나리오에서도 여러 배치가 동시에
# MD5 해시+업로드를 돌려 워커 CPU/메모리/네트워크를 잠식하지 않게 한다.
S3_STAGING_POOL = "s3_initial_load_staging"

# 서울시 Open API 키별 동시성을 제어하는 전역 풀. rental_history는 키 1~3,
# failure_report는 키 4를 사용하므로 최대 4개 날짜 Task만 동시에 API를 호출한다.
SEOUL_API_POOL = "seoul_api"

# rental_history 날짜별 backfill/reconciliation의 promote(Bronze Iceberg commit) 전용 풀.
# prepare(수집/선택) 단계는 SEOUL_API_POOL로 최대 3개 날짜가 동시에 도는 게 정상이지만,
# 같은 bronze.rental_history 테이블에 여러 날짜가 동시에 PyIceberg overwrite commit을
# 시도하면 snapshot 충돌(CommitFailedException)이 난다. slot=1로 고정해 Bronze commit
# 순서만 직렬화하고, API 호출 병렬성은 그대로 유지한다.
BRONZE_RENTAL_HISTORY_COMMIT_POOL = "bronze_rental_history_commit"

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

    try:
        ti = context["task_instance"]
        log_url = ti.log_url
        base_url = os.getenv("AIRFLOW_BASE_URL")
        if base_url and log_url and log_url.startswith("http://localhost:8080"):
            log_url = base_url.rstrip("/") + log_url[len("http://localhost:8080"):]
        message = f":x: *{ti.dag_id}.{ti.task_id}* 실패\n실행일: {context['ds']}\n로그: {log_url}"
        resp = requests.post(webhook_url, json={"text": message}, timeout=10)
        resp.raise_for_status()
    except Exception:
        # 알림 자체가 실패해도 태스크 실패 처리(콜백 호출부)를 방해하면 안 되므로 항상 삼킨다.
        # 대신 로그로는 반드시 남겨서 웹훅 만료/rate limit 등을 나중에라도 추적할 수 있게 한다.
        logger.exception("Slack 실패 알림 전송 실패")


def is_aws_env() -> bool:
    return os.getenv("APP_ENV", "local") == "aws"


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes")


def chunk_list(items: list, batch_size: int) -> list[list]:
    """items를 batch_size개씩 연속 구간으로 자른다 (순서 보존, 마지막 배치는 나머지만).

    #249: 파일 하나당 EMR Serverless JobRun을 하나씩 새로 시작하면 pre-initialized
    capacity를 매번 다시 요청하는 시작 오버헤드가 파일 개수만큼 반복된다. 배치로 묶어
    배치 하나 = JobRun 하나가 되게 하면 오버헤드가 (파일 수 / batch_size)번으로 준다.
    batch_size가 작을수록 오버헤드 절감은 줄지만 배치 하나가 실패했을 때 재시도 비용
    (이미 성공한 파일도 같은 JobRun 안에서 재처리 - 멱등이라 결과는 같지만 시간이 더 듦)
    이 작아진다 - compute_silver_rental_history_backfill_ranges.py의 chunk_days와
    동일한 트레이드오프 축이라 이름도 맞췄다."""
    if batch_size < 1:
        raise ValueError(f"batch_size는 1 이상이어야 합니다: {batch_size}")
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} 환경변수가 필요합니다.")
    return value


def _emr_spark_submit_parameters(extra_env: dict[str, str] | None = None) -> str:
    env = {
        "APP_ENV": "aws",
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", ""),
        "RISK_MODEL_CONFIG": os.getenv("EMR_RISK_MODEL_CONFIG", "/opt/app/config/risk_model.yaml"),
    }
    for key in (
        "ICEBERG_CATALOG_TYPE",
        "ICEBERG_CATALOG_NAME",
        "ICEBERG_WAREHOUSE_PATH",
        "ICEBERG_JDBC_CATALOG_URI",
        "ICEBERG_CATALOG_SECRET_ARN",
        "RAW_BUCKET",
        "WAREHOUSE_BUCKET",
    ):
        value = os.getenv(key)
        if value:
            env[key] = value

    # Secrets Manager ARN이 아직 배포 환경에 없으면 기존 .env 값을 쓰는 과도기
    # 경로를 허용한다. ARN이 있으면 비밀번호는 job 파라미터에 싣지 않는다.
    if not env.get("ICEBERG_CATALOG_SECRET_ARN"):
        for key in ("ICEBERG_JDBC_CATALOG_USER", "ICEBERG_JDBC_CATALOG_PASSWORD"):
            value = os.getenv(key)
            if value:
                env[key] = value

    for key, value in (extra_env or {}).items():
        if value is not None:
            env[key] = str(value)

    params = []
    for key, value in env.items():
        if value == "":
            continue
        params.extend(
            [
                "--conf",
                f"spark.emr-serverless.driverEnv.{key}={value}",
                "--conf",
                f"spark.executorEnv.{key}={value}",
            ]
        )
    return " ".join(shlex.quote(p) for p in params)


def run_emr_serverless_spark_job(
    *,
    entry_point: str,
    name: str,
    entry_point_arguments: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
    log_group_name: str = "/emr-serverless/airflow-spark",
    log_stream_name_prefix: str | None = None,
    tags: dict[str, str] | None = None,
) -> str:
    """EMR Serverless Spark job을 제출하고 terminal 상태까지 polling한다."""
    # AWS 분기 DAG의 staging/인자 전달 흐름을 로컬에서 확인할 때 사용한다.
    # 이 모드에서는 boto3를 만들거나 EMR API를 호출하지 않는다.
    if _env_flag("EMR_SERVERLESS_DRY_RUN"):
        print(
            "EMR Serverless dry-run: "
            f"name={name} entry_point={entry_point} "
            f"entry_point_arguments={entry_point_arguments or []}"
        )
        return f"dry-run:{name}"

    import boto3

    application_id = _required_env("EMR_SPARK_APPLICATION_ID")
    execution_role_arn = _required_env("EMR_SPARK_EXECUTION_ROLE_ARN")
    region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")
    client = boto3.client("emr-serverless", region_name=region)

    response = client.start_job_run(
        applicationId=application_id,
        executionRoleArn=execution_role_arn,
        name=name,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_point,
                "entryPointArguments": entry_point_arguments or [],
                "sparkSubmitParameters": _emr_spark_submit_parameters(extra_env),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "cloudWatchLoggingConfiguration": {
                    "enabled": True,
                    "logGroupName": log_group_name,
                    "logStreamNamePrefix": log_stream_name_prefix or name,
                }
            }
        },
        tags=tags or {},
    )
    job_run_id = response["jobRunId"]
    print(f"EMR Serverless job submitted: application={application_id} job_run_id={job_run_id}")

    terminal = {"SUCCESS", "FAILED", "CANCELLED"}
    poll_seconds = int(os.getenv("EMR_SPARK_POLL_INTERVAL_SECONDS", "30"))
    max_seconds = int(os.getenv("EMR_SPARK_POLL_MAX_SECONDS", str(6 * 60 * 60)))
    deadline = time.monotonic() + max_seconds
    last_state = None

    while True:
        job = client.get_job_run(applicationId=application_id, jobRunId=job_run_id)["jobRun"]
        state = job["state"]
        if state != last_state:
            print(f"EMR Serverless job {job_run_id} state={state}: {job.get('stateDetails', '')}")
            last_state = state
        if state == "SUCCESS":
            return job_run_id
        if state in terminal:
            raise RuntimeError(
                f"EMR Serverless job {job_run_id} failed with state={state}: "
                f"{job.get('stateDetails', '')}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"EMR Serverless job {job_run_id} did not finish within {max_seconds} seconds "
                f"(last_state={state})"
            )
        time.sleep(poll_seconds)


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
