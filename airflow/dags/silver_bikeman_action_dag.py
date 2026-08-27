"""
Silver 변환 DAG - bikeman_event(Bronze) -> bikeman_action(Silver)

### 왜 고정 시간이 아니라 Asset 기반 스케줄인가
Silver는 Bronze 결과물에 의존하는데, Bronze 완료 시각이 매일 일정하지 않다
(원천 API 응답 지연, 재시도, 로컬 LocalStack 부하 등으로 몇 분~몇십 분 유동적).
고정 시간(예: "매일 07:00")으로 스케줄하면 "Bronze가 그 전에 반드시 끝난다"는
보장 없는 가정에 의존하게 되고, 어기면 어제자(또는 그 이전) Bronze 데이터로
Silver가 조용히 돌아버린다 - 에러도 안 나서 제일 위험한 실패 모드다.

Airflow Asset 스케줄링을 쓰면 bronze_daily_batch_all_sources DAG의
daily_batch_bikeman_event 태스크가 "성공적으로 끝났을 때"(실패/스킵 시에는
이벤트가 발생하지 않음) 자동으로 이 DAG가 트리거된다 - 시간 추측이 필요 없다.

### Asset 정의는 dag_assets.py에서 공유
Bronze/Silver 양쪍에서 각자 Asset("bikeman_event_bronze")를 새로 선언해도 이름이
같으면 동작은 하지만, 오타로 이름이 어긋나면 "조용히 안 트리거되는" 사고가 난다.
공용 모듈(dag_assets.py)에서 하나로 관리해 이 위험을 없앤다.

### Bronze가 여러 번 성공해도 안전한 이유
bikeman_event는 3일 lookback으로 하루 실행에도 같은 날짜 파티션이 여러 번 덮어써질
수 있는데(그래도 outlets는 태스크 "성공" 1회당 1번 발생), 혹시 Bronze가 하루에
여러 번 성공하면(예: 수동 재실행) Silver도 그만큼 다시 트리거된다. Silver 잡
자체가 `occurred_date_partition` identity 파티션을 날짜 기준으로 덮어쓰므로 여러 번
돌아도 결과는 같다 (멱등). 변환은 Spark 없이 DuckDB/PyArrow/PyIceberg로 실행한다.

사용법 (수동 테스트):
    python -m jobs.silver_bikeman_action
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import BIKEMAN_ACTION_SILVER, BIKEMAN_EVENT_BRONZE
from dag_common import COLLECTION_CUTOFF_AT_TEMPLATE, DEFAULT_ARGS, SILVER_POOL

INGESTION_DIR = "/opt/airflow/ingestion"  # common, .env 출처
STAGING_DIR = "/opt/airflow/staging"      # 잡 실행 위치
PYTHON_BIN = "python"


def _staging_bash(job_module: str, extra_env_str: str = "") -> str:
    return (
        f"cd {STAGING_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:{STAGING_DIR}:$PYTHONPATH "
        f"{extra_env_str}"
        f"{PYTHON_BIN} -m jobs.{job_module}"
    )


@dag(
    dag_id="silver_bikeman_action",
    schedule=[BIKEMAN_EVENT_BRONZE],  # 고정 시간이 아니라 Bronze 완료 이벤트로 트리거
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 같은 파티션에 두 실행이 동시에 덮어쓰기 시도하는 것 방지
    default_args=DEFAULT_ARGS,
    tags=["silver", "asset_triggered", "main"],
    params={
        "max_days_per_run": "",
    },
    doc_md=__doc__,
)
def silver_bikeman_action():
    BashOperator(
        task_id="silver_bikeman_action",
        bash_command=_staging_bash("silver_bikeman_action", "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' "),
        env={
            "COLLECTION_CUTOFF_AT": COLLECTION_CUTOFF_AT_TEMPLATE,
        },
        append_env=True,
        execution_timeout=timedelta(minutes=30),
        pool=SILVER_POOL,
        outlets=[BIKEMAN_ACTION_SILVER],
    )


silver_bikeman_action()
