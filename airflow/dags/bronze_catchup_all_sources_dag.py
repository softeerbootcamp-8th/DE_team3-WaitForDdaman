"""
Bronze 고장신고 Catchup DAG - 수동 복구 전용

### 왜 필요한가
대여이력은 `bronze_rental_history_backfill`이 logical date별로 처리하므로 이 legacy
Catchup DAG에서는 제거했다. 이 DAG은 failure_report의 수동 복구만 유지한다.

### bronze_initial_load_all_sources와의 관계
초기 적재나 일 배치와 독립적인 수동 DAG이다. failure_report 누락 구간을
`MAX_DAYS_PER_RUN`으로 제한하거나 무제한 처리할 때 사용한다.

### 워터마크
failure_report 워터마크는 기존과 동일하게 이어받는다. 실행 후 failure_report Silver
Asset을 발행한다.

사용법 (수동 트리거):
    airflow dags trigger bronze_catchup_all_sources
    # 구간을 제한하고 싶으면 conf로 override
    airflow dags trigger bronze_catchup_all_sources --conf '{"max_days_per_run": "10"}'

### 트리거 시점 주의 (#74)
이 DAG은 bronze_daily_batch_all_sources와 같은 BRONZE_POOL(슬롯 2)을 공유한다.
백필 구간이 크면 몇 시간씩 걸리는데(실측 약 2시간 35분), 그 사이 06:00 KST
daily_batch 크론이 걸리면 daily_batch의 5개 태스크가 슬롯을 못 잡고 이 DAG이
끝날 때까지 대기한다 - execution_timeout은 태스크 실행 시작 후부터 재므로
타임아웃 실패는 안 나지만, 그날 Silver/Gold/Risk 파이프라인 전체가 그만큼
밀린다. BRONZE_POOL 슬롯을 늘리거나 이 DAG만 별도 pool로 분리하는 것도
검토했으나, 애초에 슬롯을 2로 제한한 이유(LocalStack 동시 쓰기 3개 이상 시
"read of closed file" 레이스, 실측 확인)가 재발할 수 있어 보류했다. 대신
06:00 KST 전후로는 이 DAG을 새로 트리거하지 않는 것으로 회피한다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import FAILURE_REPORT_BRONZE
from dag_common import BRONZE_POOL, DEFAULT_ARGS, bash_job


@dag(
    dag_id="bronze_catchup_all_sources",
    schedule=None,  # 수동 트리거 전용, 크론 없음
    # 넉넉히 과거로 잡는다 - 과거 logical_date로 수동 트리거해도 태스크 없이
    # 조용히 success 처리되는 걸 피하기 위함 (start_date보다 이른 logical_date는
    # Airflow가 범위 밖으로 보고 태스크를 아예 안 만든다 - 2026-08-18 실측).
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "manual"],
    params={
        # 빈 문자열 = 무제한 (각 잡의 `if max_days:` 판정이 빈 문자열을 falsy로 봄).
        # 구간을 제한하고 싶으면 트리거 시 conf로 override.
        "max_days_per_run": "",
    },
    doc_md=__doc__,
)
def bronze_catchup_all_sources():
    BashOperator(
        task_id="catchup_failure_report",
        bash_command=bash_job(
            "daily_batch_failure_report",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(hours=3),
        outlets=[FAILURE_REPORT_BRONZE],
        pool=BRONZE_POOL,
    )


bronze_catchup_all_sources()
