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

### 대여이력만 TaskGroup인 이유
대여이력은 06:00 FINAL 관측이 실패해도 05:00 예비 관측본으로 그날의 Bronze를 살릴 수
있어야 한다(#136). 그래서 이 원천만 수집 / 후보 선택 / 승격 / 확정 워터마크 / Asset
발행을 별도 태스크로 쪼갠다.

    collect_final_raw
        >> select_final_or_preliminary   (ALL_DONE - 수집이 실패해도 반드시 실행)
        >> promote_to_bronze             (PyIceberg 단일 snapshot commit, BRONZE_POOL)
        >> update_confirmed_watermark
        >> publish_bronze_asset          (RENTAL_HISTORY_BRONZE의 유일한 producer)

- 수집 실패는 Airflow에서 그대로 보이되, 예비 관측본으로 복구되면 파이프라인은 성공한다.
  따라서 이 원천의 외부 성공 조건은 "FINAL 수집 성공"이 아니라 "Bronze commit 완료"다.
- Asset을 마지막 빈 태스크에만 두는 이유는, 승격이나 워터마크가 실패한 상태가 Silver로
  전파되는 걸 막기 위함이다. 앞 단계에 outlet이 있으면 부분 상태가 그대로 하류에 흘러간다.
- 기본값은 RENTAL_HISTORY_FALLBACK_ENABLED=false, RENTAL_HISTORY_T0_ENABLED=false다.
  이 상태의 처리 범위·실패 의미는 기존 단일 태스크와 같고, 예비 관측본은 쓰지 않는다.
- 대여이력과 고장신고의 대규모 gap은 00:30
  bronze_historical_reconciliation에서 날짜별 mapped task로 처리한다.
  bronze_catchup_all_sources는 레거시 수동 복구 경로로만 유지한다.

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
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup, dag, task
from airflow.task.trigger_rule import TriggerRule

from dag_assets import (
    BIKEMAN_EVENT_BRONZE,
    FAILURE_REPORT_BRONZE,
    RENTAL_HISTORY_BRONZE,
    STATION_ACTIVE_BRONZE,
    STATION_MASTER_BRONZE,
)
from dag_common import (
    BRONZE_POOL,
    COLLECTION_CUTOFF_AT_TEMPLATE,
    DEFAULT_ARGS,
    bash_job,
)

# 밀린 날짜가 많아도 한 run이 이만큼만 처리하고 끝낸다. run이 하루를 넘겨
# 다음 날 station_master 스냅샷을 굶기지 않게 하는 안전장치 (위 doc 참고).
DEFAULT_MAX_DAYS_PER_RUN = "3"

# select_rental_history_snapshot/promote_rental_history_raw와 같은 KST 기준 논리 시각.
KST = ZoneInfo("Asia/Seoul")

MAX_DAYS_PER_RUN_TEMPLATE = "{{ params.max_days_per_run }}"

# 운영 플래그는 Airflow Variable로 두고 DAG가 문자열 env로만 주입한다.
# ingestion 잡은 Airflow를 import하지도, Variable을 직접 읽지도 않는다.
FALLBACK_ENABLED_TEMPLATE = (
    "{{ var.value.get('RENTAL_HISTORY_FALLBACK_ENABLED', 'false') }}"
)
T0_ENABLED_TEMPLATE = "{{ var.value.get('RENTAL_HISTORY_T0_ENABLED', 'false') }}"
PRELIMINARY_MAX_AGE_MINUTES_TEMPLATE = (
    "{{ var.value.get('RENTAL_HISTORY_PRELIMINARY_MAX_AGE_MINUTES', '120') }}"
)


@dag(
    dag_id="bronze_daily_batch_all_sources",
    schedule="0 6 * * *",  # 매일 06:00 KST - 전날 데이터가 확정된 뒤 수집
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 두 run이 같은 파티션을 동시에 덮어쓰는 것 방지
    max_active_tasks=2,  # 로컬 LocalStack 동시 쓰기 부하 제한
    default_args=DEFAULT_ARGS,
    tags=["bronze"],
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

    # 대여이력: 하루치가 시간 단위 24회 호출 + 페이징이라 가장 오래 걸린다.
    # 이 원천만 예비 관측본 fallback이 필요해 단계별 태스크로 쪼갠다 (위 doc 참고).
    with TaskGroup(group_id="rental_history"):
        collect_final_raw = BashOperator(
            task_id="collect_final_raw",
            bash_command=bash_job("collect_rental_history_raw"),
            env={
                "COLLECTION_CUTOFF_AT": COLLECTION_CUTOFF_AT_TEMPLATE,
                "SNAPSHOT_TYPE": "FINAL",
                "MAX_DAYS_PER_RUN": MAX_DAYS_PER_RUN_TEMPLATE,
                "RENTAL_HISTORY_T0_ENABLED": T0_ENABLED_TEMPLATE,
            },
            append_env=True,
            # HTTP/페이지 단위 일시 오류는 api_client의 tenacity가 이미 재시도한다.
            # 여기에 Airflow 지수 백오프 3회를 더하면 fallback 진입이 06:30을 넘겨
            # 예비 관측본을 쓸 수 있는 시간대를 놓친다.
            retries=0,
            execution_timeout=timedelta(minutes=15),
        )

        # 수집이 실패해도 반드시 실행돼야 예비 관측본으로 복구할 수 있다.
        select_final_or_preliminary = BashOperator(
            task_id="select_final_or_preliminary",
            bash_command=bash_job("select_rental_history_snapshot"),
            env={
                "COLLECTION_CUTOFF_AT": COLLECTION_CUTOFF_AT_TEMPLATE,
                "MAX_DAYS_PER_RUN": MAX_DAYS_PER_RUN_TEMPLATE,
                "RENTAL_HISTORY_FALLBACK_ENABLED": FALLBACK_ENABLED_TEMPLATE,
                "RENTAL_HISTORY_T0_ENABLED": T0_ENABLED_TEMPLATE,
                "RENTAL_HISTORY_PRELIMINARY_MAX_AGE_MINUTES": (
                    PRELIMINARY_MAX_AGE_MINUTES_TEMPLATE
                ),
            },
            append_env=True,
            trigger_rule=TriggerRule.ALL_DONE,
            execution_timeout=timedelta(minutes=15),
        )

        # PyIceberg(SqlCatalog)로 선택된 날짜 파티션들을 한 번의 snapshot commit으로
        # 승격한다 - Spark/JVM을 쓰지 않는다(#194). BRONZE_POOL은 다른 Bronze 태스크와 공유.
        promote_to_bronze = BashOperator(
            task_id="promote_to_bronze",
            bash_command=bash_job("promote_rental_history_raw"),
            env={"COLLECTION_CUTOFF_AT": COLLECTION_CUTOFF_AT_TEMPLATE},
            append_env=True,
            execution_timeout=timedelta(minutes=30),
            pool=BRONZE_POOL,
        )

        update_confirmed_watermark = BashOperator(
            task_id="update_confirmed_watermark",
            bash_command=bash_job("update_rental_history_confirmed_watermark"),
            env={"COLLECTION_CUTOFF_AT": COLLECTION_CUTOFF_AT_TEMPLATE},
            append_env=True,
            execution_timeout=timedelta(minutes=15),
        )

        # Bronze commit과 확정 워터마크가 모두 정합한 뒤에만 Asset을 발행한다.
        # #137: Silver가 S3의 "최신 COMPLETE promotion"을 추측하지 않도록, 이 promotion을
        # 유일하게 식별하는 run_date/promotion_id/promotion_key를 Asset event에 실어
        # 보낸다. promotion_id/key 포맷은 promote_to_bronze와 같은 cutoff로 계산하므로
        # 항상 그 태스크가 실제로 쓴 marker와 일치한다 (jobs.rental_history_snapshot_policy.
        # build_promotion_id/promotion_key와 동일 규칙 - dag-processor 환경은 ingestion을
        # import할 수 없어 여기서 다시 계산한다. dag_common.py 참고).
        @task(task_id="publish_bronze_asset", outlets=[RENTAL_HISTORY_BRONZE])
        def publish_bronze_asset(**context):
            dag_run = context["dag_run"]
            conf_cutoff = (
                dag_run.conf.get("collection_cutoff_at")
                if dag_run is not None and dag_run.conf
                else None
            )
            cutoff = (
                datetime.fromisoformat(conf_cutoff)
                if conf_cutoff
                else context["data_interval_end"]
            )
            cutoff = cutoff.astimezone(KST)

            run_date = cutoff.date()
            promotion_id = cutoff.strftime("%Y%m%dT%H%M%S%z")
            promotion_key = (
                "_meta/promotion/bronze_rental_history/"
                f"run_date={run_date.isoformat()}/promotion_id={promotion_id}/promotion.json"
            )
            context["outlet_events"][RENTAL_HISTORY_BRONZE].extra = {
                "run_date": run_date.isoformat(),
                "promotion_id": promotion_id,
                "promotion_key": promotion_key,
            }

        (
            collect_final_raw
            >> select_final_or_preliminary
            >> promote_to_bronze
            >> update_confirmed_watermark
            >> publish_bronze_asset()
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
