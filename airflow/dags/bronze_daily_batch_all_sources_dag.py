"""
Bronze 일 배치 DAG (5개 원천 통합) - 매일 실행

대여소정보 / 대여이력 / 고장신고 / 따맨(bikeman) 이벤트 / 실시간 대여정보를 한 DAG에서
**전부 병렬로** 실행한다. 원천 사이에 태스크 의존성을 두지 않는다.

### 왜 의존성을 두지 않는가
Bronze는 원천을 가공 없이 그대로 적재하는 단계라, 대여이력·고장신고·bikeman·실시간
대여정보 잡은 station_master를 **읽지 않는다**. 실제 데이터 의존이 없다.

예전에는 `station_master >> [나머지 3개]` 체인이 걸려 있었는데, 근거였던
"마스터가 먼저 있어야 고아(orphan) 레코드를 판별할 수 있다"는 Silver 조인 단계의
문제이고 현재 Silver 코드에도 구현돼 있지 않다. 반면 부작용은 실제였다 - 공공 API가
한 번 삐끗해서 station_master가 실패하면 나머지 3개가 전부 upstream_failed로 막히고,
워터마크가 안 밀려서 다음 날 2일치를 몰아서 처리하게 됐다.

순서 선호는 hard dependency 대신 priority_weight로 표현한다. 마스터 데이터라 슬롯이
나면 먼저 잡지만, 실패해도 나머지로 전파되지 않는다.

### ⚠️ max_active_runs=1과 station_master 스냅샷
한 DAG로 묶으면 `max_active_runs`를 5개 원천이 공유한다. 이전 run이 안 끝나면 다음 날
run이 큐에서 대기하는데, 이게 24시간을 넘기면 그날 station_master 스냅샷이 통째로 빈다.
tbCycleStationInfo는 과거를 소급 조회할 수 없어서 **영구 손실**이다 (대여이력·고장신고·
bikeman은 워터마크로 며칠 밀려도 따라잡는다).

그래서 `max_days_per_run` 기본값을 유한값으로 둔다. 밀린 날짜가 많아도 한 run이 3일치만
처리하고 끝나므로 run이 하루를 넘길 수 없다. 나머지는 다음 날 run이 워터마크를 이어받아
처리한다. 빠르게 따라잡아야 하면 이 값을 UI에서 키우되, run이 길어지는 동안 대여소
스냅샷이 밀린다는 걸 알고 올려야 한다.

`max_active_runs=1` 자체는 유지한다. 두 run이 동시에 같은 파티션을 덮어쓰는 것을 막는다.

### 원천별 성격
| 소스 | 조회 방식 | 증분 기준 | 재처리 |
|---|---|---|---|
| 대여소정보 | 공공 API | 없음(전체 스냅샷) | 없음, 소급 불가 |
| 실시간 대여정보 | 공공 API | 없음(전체 스냅샷) | 없음, 소급 불가 |
| 대여이력 | 공공 API | RENT_DT | 없음 |
| 고장신고 | 공공 API | REGDTTM | 없음 |
| bikeman_event | 우리 Postgres 직접 조회 | occurred_at | 3일 lookback |

bikeman만 lookback이 있는 이유: 오프라인 작업 후 몰아서 제출하는 게 정상 케이스라
"어제"만 보면 이미 확정된 날짜에 늦게 도착한 이벤트를 놓친다. 잡 내부에서 매 실행마다
3일치를 다시 계산해 overwritePartitions로 덮어쓰므로 DAG 레벨에서는 신경 쓸 게 없다.

### 최초 실행 전 필수 절차
- 대여이력/고장신고: 초기 적재 DAG(bronze_initial_load_all_sources)가 성공 직후 워터마크를 찍는다
- bikeman_event: 파일 백필이 없으므로 set_watermark DAG로 서비스 시작 전날을 1회 찍는다
      dataset=bikeman_event, watermark_date=2026-06-29
- 대여소정보: 워터마크 자체가 없다 (증분 기준이 될 컬럼이 없음)
- 실시간 대여정보: 대여소정보와 동일하게 워터마크가 없다

### catchup=False인 이유
밀린 날짜는 각 잡의 워터마크 로직이 알아서 이어서 처리한다. Airflow의 catchup에
의존하지 않고 "하루 한 번 트리거"만 Airflow가 책임진다.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import (
    BIKEMAN_EVENT_BRONZE,
    FAILURE_REPORT_BRONZE,
    RENTAL_HISTORY_BRONZE,
    STATION_ACTIVE_BRONZE,
    STATION_MASTER_BRONZE,
)
from dag_common import BRONZE_POOL, DEFAULT_ARGS, bash_job

# 밀린 날짜가 많아도 한 run이 이만큼만 처리하고 끝낸다. run이 하루를 넘겨
# 다음 날 station_master 스냅샷을 굶기지 않게 하는 안전장치 (위 doc 참고).
DEFAULT_MAX_DAYS_PER_RUN = "3"


@dag(
    dag_id="bronze_daily_batch_all_sources",
    schedule="0 6 * * *",  # 매일 06:00 KST - 전날 데이터가 확정된 뒤 수집
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 두 run이 같은 파티션을 동시에 덮어쓰는 것 방지
    max_active_tasks=2,  # 로컬 LocalStack 동시 쓰기 부하 제한
    default_args=DEFAULT_ARGS,
    tags=["daily_batch", "bronze", "all_sources"],
    params={
        "max_days_per_run": DEFAULT_MAX_DAYS_PER_RUN,
    },
    doc_md=__doc__,
)
def bronze_daily_batch_all_sources():
    # 대여소정보: 워터마크 없음, 매일 전체 스냅샷을 그날 파티션으로 적재.
    # priority_weight를 높여 슬롯이 나면 먼저 잡게 한다 - 마스터 데이터라 최신인 편이
    # 낫고, 유일하게 소급이 불가능한 원천이라 하루 안에 반드시 성공해야 한다.
    BashOperator(
        task_id="daily_batch_station_master",
        bash_command=bash_job("daily_batch_station_master"),
        execution_timeout=timedelta(minutes=30),
        outlets=[STATION_MASTER_BRONZE],
        pool=BRONZE_POOL,
        priority_weight=10,
    )

    # 대여이력: 하루치가 시간 단위 24회 호출 + 페이징이라 가장 오래 걸린다
    BashOperator(
        task_id="daily_batch_rental_history",
        bash_command=bash_job(
            "daily_batch_rental_history",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(hours=2),
        outlets=[RENTAL_HISTORY_BRONZE],
        pool=BRONZE_POOL,
    )

    BashOperator(
        task_id="daily_batch_failure_report",
        bash_command=bash_job(
            "daily_batch_failure_report",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(hours=1),
        outlets=[FAILURE_REPORT_BRONZE],
        pool=BRONZE_POOL,
    )

    # bikeman(따맨) 이벤트: 우리 자체 Postgres에서 직접 조회. 공공 API가 아니지만
    # Bronze 관점에서는 "이벤트를 생성하는 원천"으로 동등하게 취급해 여기 포함한다.
    # outlets - 이 태스크가 성공하면 silver_bikeman_action_daily가 고정 시간이 아니라
    # 이 이벤트를 받아 트리거된다 (실패/스킵 시에는 발생하지 않는다).
    BashOperator(
        task_id="daily_batch_bikeman_event",
        bash_command=bash_job(
            "daily_batch_bikeman_event",
            "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' ",
        ),
        execution_timeout=timedelta(minutes=30),
        outlets=[BIKEMAN_EVENT_BRONZE],
        pool=BRONZE_POOL,
    )

    # 실시간 대여정보: 워터마크 없음, station_master와 동일하게 매일 전체 스냅샷을
    # 그날 파티션으로 적재. gold.fact_station_inventory가 이 데이터를 필요로 한다.
    BashOperator(
        task_id="daily_batch_station_active",
        bash_command=bash_job("daily_batch_station_active"),
        execution_timeout=timedelta(minutes=30),
        outlets=[STATION_ACTIVE_BRONZE],
        pool=BRONZE_POOL,
    )

    # 태스크 간 의존성을 의도적으로 두지 않는다 (위 doc 참고).
    # 순서 선호는 station_master의 priority_weight로만 표현한다.


bronze_daily_batch_all_sources()
