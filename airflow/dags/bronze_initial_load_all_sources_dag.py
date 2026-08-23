"""
Bronze 초기 적재(initial load) DAG (2개 원천) - 1회성, 수동 트리거

대여이력 / 고장신고 두 원천의 파일 기반 초기 적재를 한 DAG에서 실행한다. 각 소스
디렉터리에 파일이 없으면 열린데이터광장에서 자동으로 받는다(common/file_downloader.py,
jobs/initial_load_*.py 참고) - 사람이 미리 내려받아 둘 필요가 없다.

### 대여소정보(station_master)가 여기 없는 이유
이 원천은 파일 기반 초기 적재 대상이 아니다. tbCycleStationInfo는 날짜 파라미터를 받지
않고 호출 시점의 전체 스냅샷만 주므로 과거를 소급 적재할 수 없고, 반기 파일에는 골드
조인 키인 station_id가 없다 (jobs/daily_batch_station_master.py 참고).
대여소정보 적재는 bronze_daily_batch_all_sources DAG가 매일 스냅샷으로 담당한다.

### 태스크 의존성 설계
대여이력과 고장신고는 서로 의존관계가 없어서 병렬로 둔다. 각 소스는 초기 적재 성공
직후 자기 워터마크를 찍고, 그 뒤에 Silver까지 이어서 승격한다 - 초기 적재 시점에
Silver를 채워두지 않으면 daily_batch/backfill을 별도로 한 번 더 돌려야 한다.

    rental_history ─┬─> check_watermark_date_rental_history ─> set_bronze_ingestion_watermark_rental_history ─┐
                     └─> bootstrap_silver_watermark_rental_history ──────────────────────────────────────────┴─> load_silver_rental_history
    failure_report ──> check_watermark_date_failure_report ─> set_bronze_ingestion_watermark_failure_report ──> load_silver_failure_report

check_watermark_date_*는 사람이 입력한 *_watermark_date가 실제 Bronze 최대 적재일과
다르면 경고 로그만 남기는 안전망이다 (DAG를 막지 않음 - ⚠️ 문단 참고).

### 왜 set_*과 bootstrap_*으로 동사가 다른가
값을 "어떻게 얻는지"가 다르다. set_bronze_ingestion_watermark_*는 사람이 지정한
고정 날짜(*_watermark_date 파라미터)를 그대로 기록해서, daily_batch(Bronze API 수집)가
그 다음날부터 이어받게 한다. bootstrap_silver_watermark_rental_history는 입력 날짜가
없고 Bronze 테이블의 실제 MIN(partition)을 직접 쿼리해서 Silver 처리 시작점을 자동으로
계산한다 - 이름이 다른 이유를 유지해야 "이거 날짜를 내가 넣어줘야 하나?"를 task_id만
보고 구분할 수 있다. task_id는 실행하는 jobs/<모듈명>.py와 최대한 맞춘다
(set_bronze_ingestion_watermark_* -> jobs/set_watermark.py, bootstrap_silver_watermark_*
-> jobs/bootstrap_silver_watermark.py) - 로그에서 실패를 봤을 때 바로 어떤 파일을
찾아야 하는지 알 수 있게 하기 위함.

### 왜 워터마크 태스크가 DAG 안에 있는가
워터마크가 없으면 daily_batch가 .env의 BACKFILL_START_DATE(기본 2015-01-01)부터 API로
다시 긁는다. 파일로 방금 채운 기간을 중복 처리하게 되므로, 초기 적재와 워터마크는 한
DAG에서 이어져야 한다. 별도 set_watermark DAG를 손으로 트리거하는 방식은 빠뜨리기 쉽다.

업스트림 성공에만 걸어두는 게 중요하다. 워터마크는 "성공적으로 커밋된 마지막 날짜"만
기록해야 하고 부분 실패 시 갱신하면 데이터 누락이 생긴다 (common/watermark.py 참고).

⚠️ *_pattern으로 적재 범위를 좁히면 *_watermark_date도 그 범위의 마지막 날짜로 같이
   바꿔야 한다. 안 그러면 실제로 적재하지 않은 기간을 처리 완료로 표시한다.

### 왜 병렬을 2개로 제한하는가
로컬(LocalStack) 환경에서 여러 Spark 잡이 동시에 대량 PutObject를 보내면
"read of closed file" 레이스 컨디션이 발생하는 게 실측으로 확인됐다. 각 잡이 이미
내부적으로 로컬 병렬도를 제한하고 있으므로, DAG 레벨에서도 동시 실행을 제한한다.

max_active_tasks=2는 이 DAG 안에서만 유효하다. 초기 적재는 몇 시간짜리라 도중에 일 배치
스케줄(06:00)과 겹치는 게 정상 케이스인데, DAG 단위 제한은 서로를 알지 못한다.
그래서 Spark를 쓰는 태스크는 각 레이어의 daily 잡과 같은 풀에 넣어 전역으로 묶는다 -
Bronze 초기 적재 태스크는 BRONZE_POOL, Silver 승격 태스크(load_silver_*)는 daily
silver_*_dag.py와 같은 SILVER_POOL. 워터마크 태스크는 S3에 JSON 한 개만 쓰고 Spark를
안 띄우므로 풀에서 제외한다(슬롯을 잡으면 정작 초기 적재가 밀린다).

### 실행 방법
Airflow UI에서 "Trigger DAG w/ config"로 각 소스의 input_dir / 파일 패턴을 조정할 수 있다.
예) 로컬 검증 시 대여이력만 1개월치: rental_history_pattern = "*2601*"
"""
import json
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from dag_common import (
    BRONZE_POOL,
    INGESTION_DIR,
    SILVER_POOL,
    bash_job,
    bash_staging_job,
    is_aws_env,
    notify_slack_on_failure,
    run_emr_serverless_spark_job,
)

# 일 배치의 DEFAULT_ARGS를 그대로 쓰지 않는다. 초기 적재는 수동 1회성이라 사람이 붙어서
# 보고 있고, 실패하면 빨리 알고 원인을 봐야 한다. 일 배치처럼 길게 백오프하면
# 손으로 트리거해놓고 30분씩 기다리게 된다.
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "on_failure_callback": notify_slack_on_failure,
}


@dag(
    dag_id="bronze_initial_load_all_sources",
    schedule=None,  # 초기 적재는 반복 실행이 아니라 필요할 때 한 번 트리거하는 작업
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,  # 로컬 LocalStack 동시 쓰기 부하 제한
    default_args=default_args,
    tags=["bronze", "independent", "manual"],
    params={
        "rental_history_dir": f"{INGESTION_DIR}/data/rental_history",
        "rental_history_pattern": "*",
        "rental_history_watermark_date": "2026-06-30",
        "failure_report_dir": f"{INGESTION_DIR}/data/failure_report",
        "failure_report_pattern": "*",
        "failure_report_watermark_date": "2026-06-30",
        # transform_silver_rental_history.py(다른 DAG인 silver_rental_history_dag.py와
        # 공유하는 잡)는 손대지 않는다 - 대신 이 DAG의 load_silver_rental_history 태스크가
        # 그 잡을 여러 번 순차 호출해서 몇 년치를 나눠 처리한다. 청크 크기(chunk_days)는
        # daily가 이미 안전하다고 검증한 기본값(31일)과 동일하게 두고, total_days_cap만큼
        # 반복 호출한다 - 한 번의 Spark 잡이 다년치를 한 번에 캐시/처리하다 OOM 나는 걸
        # 피하기 위함(Bronze 초기 적재를 파일 단위로 쪼갠 것과 같은 이유).
        "rental_history_silver_chunk_days": "31",
        "rental_history_silver_total_days_cap": "3650",
    },
    doc_md=__doc__,
)
def bronze_initial_load_all_sources():
    # 대여이력: 반기 파일이 최대 700MB대라, 폴더 전체를 세션 하나로 순회하면 임시 파일과
    # 힙이 누적돼 OOM이 났다(#94). 그래서 "목록 나열(다운로드 포함) -> 파일별로 별도
    # 프로세스(=새 JVM) 실행"으로 나눈다. list_input_files는 Spark를 안 띄우는 순수
    # 다운로드+glob 작업이라 BRONZE_POOL에 넣지 않는다(워터마크 태스크와 동일한 이유).
    list_rental_history_files = BashOperator(
        task_id="list_rental_history_files",
        bash_command=bash_job(
            "list_input_files",
            "DATASET=rental_history "
            "INPUT_DIR='{{ params.rental_history_dir }}' "
            "INPUT_FILE_PATTERN='{{ params.rental_history_pattern }}' ",
        ),
        do_xcom_push=True,
        execution_timeout=timedelta(hours=1),  # 로컬에 캐시가 없으면 반기 파일 여러 개를 순차 다운로드
    )

    @task(task_id="parse_rental_history_files")
    def parse_rental_history_files(raw_json: str) -> list[str]:
        return json.loads(raw_json)

    rental_history_files = parse_rental_history_files(list_rental_history_files.output)

    @task(
        task_id="initial_load_rental_history_file",
        execution_timeout=timedelta(minutes=30),
        pool=BRONZE_POOL,
    )
    def initial_load_rental_history_file_emr(input_file: str) -> str:
        return run_emr_serverless_spark_job(
            entry_point="local:///opt/app/ingestion/jobs/initial_load_rental_history.py",
            name="bronze-initial-load-rental-history",
            extra_env={"INPUT_FILE": input_file},
            log_group_name="/emr-serverless/bronze-initial-load",
            log_stream_name_prefix="rental-history",
            tags={
                "dag_id": "bronze_initial_load_all_sources",
                "task_id": "initial_load_rental_history_file",
                "dataset": "rental_history",
            },
        )

    # 파일 개수만큼 태스크 인스턴스가 동적으로 생성된다(Dynamic Task Mapping) - 파일
    # 하나 = spark-submit 프로세스 하나 = 새 JVM. 하나가 OOM으로 죽어도 그 인스턴스만
    # 재시도되고 나머지 파일에는 영향이 없다.
    if is_aws_env():
        rental_history = initial_load_rental_history_file_emr.expand(input_file=rental_history_files)
    else:
        rental_history = BashOperator.partial(
            task_id="initial_load_rental_history_file",
            execution_timeout=timedelta(minutes=30),  # 폴더 전체 기준(3시간) -> 파일 1개 기준으로 축소
            pool=BRONZE_POOL,
        ).expand(
            bash_command=rental_history_files.map(
                lambda f: bash_job("initial_load_rental_history", f"INPUT_FILE='{f}' ")
            )
        )

    # 고장신고: zip/csv/xlsx 혼합 입력, 볼륨은 작지만 동일한 이유로 파일 단위로 나눈다.
    list_failure_report_files = BashOperator(
        task_id="list_failure_report_files",
        bash_command=bash_job(
            "list_input_files",
            "DATASET=failure_report "
            "INPUT_DIR='{{ params.failure_report_dir }}' "
            "INPUT_FILE_PATTERN='{{ params.failure_report_pattern }}' ",
        ),
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=30),
    )

    @task(task_id="parse_failure_report_files")
    def parse_failure_report_files(raw_json: str) -> list[str]:
        return json.loads(raw_json)

    failure_report_files = parse_failure_report_files(list_failure_report_files.output)

    @task(
        task_id="initial_load_failure_report_file",
        execution_timeout=timedelta(minutes=20),
        pool=BRONZE_POOL,
    )
    def initial_load_failure_report_file_emr(input_file: str) -> str:
        return run_emr_serverless_spark_job(
            entry_point="local:///opt/app/ingestion/jobs/initial_load_failure_report.py",
            name="bronze-initial-load-failure-report",
            extra_env={"INPUT_FILE": input_file},
            log_group_name="/emr-serverless/bronze-initial-load",
            log_stream_name_prefix="failure-report",
            tags={
                "dag_id": "bronze_initial_load_all_sources",
                "task_id": "initial_load_failure_report_file",
                "dataset": "failure_report",
            },
        )

    if is_aws_env():
        failure_report = initial_load_failure_report_file_emr.expand(input_file=failure_report_files)
    else:
        failure_report = BashOperator.partial(
            task_id="initial_load_failure_report_file",
            execution_timeout=timedelta(minutes=20),  # 폴더 전체 기준(1시간) -> 파일 1개 기준으로 축소
            pool=BRONZE_POOL,
        ).expand(
            bash_command=failure_report_files.map(
                lambda f: bash_job("initial_load_failure_report", f"INPUT_FILE='{f}' ")
            )
        )

    # *_watermark_date는 사람이 직접 입력하는 값이라 *_pattern으로 적재 범위를 좁히고
    # 이 값을 안 맞추면(#41-42 경고 참고) 실제로 적재 안 한 기간이 daily_batch/Silver에
    # "처리 완료"로 영구히 잘못 기록된다. set_watermark 자체를 실데이터 기반으로 바꾸는
    # 건 추후 작업이라, 지금은 실제 Bronze MAX(partition)과 비교해 어긋나면 경고만 남기는
    # 안전망을 먼저 둔다 (DAG를 막지는 않음 - 로컬에서는 데이터가 일부만 있어도 진행해야
    # 해서 강제 실패시키면 안 된다).
    check_watermark_date_rental_history = BashOperator(
        task_id="check_watermark_date_rental_history",
        bash_command=bash_job(
            "check_watermark_date",
            "DATASET=rental_history "
            "WATERMARK_DATE='{{ params.rental_history_watermark_date }}' ",
        ),
        retries=0,
        execution_timeout=timedelta(minutes=10),
        pool=BRONZE_POOL,  # Spark로 테이블을 읽으므로 풀에 넣음 (bootstrap_silver_watermark와 동일 이유)
    )

    check_watermark_date_failure_report = BashOperator(
        task_id="check_watermark_date_failure_report",
        bash_command=bash_job(
            "check_watermark_date",
            "DATASET=failure_report "
            "WATERMARK_DATE='{{ params.failure_report_watermark_date }}' ",
        ),
        retries=0,
        execution_timeout=timedelta(minutes=10),
        pool=BRONZE_POOL,
    )

    # 워터마크는 해당 소스의 초기 적재가 성공했을 때만 찍힌다 (재시도 소진 후 실패면 안 찍힘).
    # Bronze의 다음 API 수집(daily_batch)이 이어받을 지점 - 값은 *_watermark_date 파라미터를 그대로 기록.
    set_bronze_ingestion_watermark_rental_history = BashOperator(
        task_id="set_bronze_ingestion_watermark_rental_history",
        bash_command=bash_job(
            "set_watermark",
            "DATASET=rental_history "
            "WATERMARK_DATE='{{ params.rental_history_watermark_date }}' ",
        ),
        retries=0,  # S3에 JSON 한 개 쓰는 작업 - 실패하면 원인을 봐야 한다
        execution_timeout=timedelta(minutes=5),
    )

    set_bronze_ingestion_watermark_failure_report = BashOperator(
        task_id="set_bronze_ingestion_watermark_failure_report",
        bash_command=bash_job(
            "set_watermark",
            "DATASET=failure_report "
            "WATERMARK_DATE='{{ params.failure_report_watermark_date }}' ",
        ),
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )

    # Silver 워터마크 부트스트랩 (#76) - Bronze rental_history의 실제 MIN(partition)을
    # 직접 읽어서 자동 계산한다 (사람이 날짜를 세어 넘길 필요 없음). failure_report는
    # silver 쪽이 워터마크 자체가 없는 구조라(매번 전체 재처리) 대상이 아니다.
    # Spark로 테이블을 읽으므로 BRONZE_POOL에 넣는다 (set_bronze_ingestion_watermark_*와
    # 달리 JSON 한 개만 쓰는 게 아니라 Iceberg 테이블을 직접 조회한다).
    bootstrap_silver_watermark_rental_history = BashOperator(
        task_id="bootstrap_silver_watermark_rental_history",
        bash_command=bash_job("bootstrap_silver_watermark", "DATASET=rental_history "),
        retries=0,
        execution_timeout=timedelta(minutes=10),
        pool=BRONZE_POOL,
    )

    # Silver 승격 (초기 적재 시점에 바로 Silver까지 채워, 이후 daily_batch/backfill을
    # 다시 돌리지 않아도 되게 한다). 잡 자체는 daily용 silver_rental_history_dag.py /
    # silver_failure_report_dag.py와 동일하다 - 여기서는 Asset 트리거 대신 초기 적재
    # 완료 직후 명시적으로 한 번 실행한다.
    #
    # rental_history: transform_silver_rental_history.py가 read_watermark()(상한, 기본
    # 키=Bronze rental_history)와 SILVER_RENTAL_HISTORY(하한)를 직접 읽으므로, 두 값이
    # 모두 이번 초기 적재 결과를 반영한 뒤에 실행해야 한다 - set_bronze_ingestion_watermark_*
    # (상한)과 bootstrap_silver_watermark_*(하한) 둘 다에 의존해야 한다.
    # transform_silver_rental_history.py 한 번 호출은 daily와 동일하게 chunk_days(기본
    # 31일)까지만 처리한다 - 몇 년치를 한 Spark 세션에서 캐시+윈도우dedup+커밋하다 OOM 나는
    # 걸 막기 위함(Bronze를 파일 단위로 쪼갠 것과 같은 이유). 대신 이 태스크가 그 잡을
    # total_days_cap/chunk_days번 순차 반복 호출해서 몇 년치 백로그를 한 태스크 안에서
    # 다 처리한다. 잡 자체(다른 DAG와 공유)는 손대지 않고, 반복 호출만 이 DAG 쪽에서 한다.
    # 잡의 로그 문구("처리할 신규 날짜 없음")로 Bronze 워터마크까지 다 따라잡았음을 감지하면
    # 남은 반복을 조기 종료한다 - 안 잡혀도(로그 문구가 바뀌는 등) 정답이 달라지진 않고
    # 남은 반복이 전부 no-op으로 끝날 뿐이라 안전하다.
    load_silver_rental_history = BashOperator(
        task_id="load_silver_rental_history",
        bash_command=f"""
set -e
CHUNK_DAYS="{{{{ params.rental_history_silver_chunk_days }}}}"
TOTAL_DAYS_CAP="{{{{ params.rental_history_silver_total_days_cap }}}}"
ITERATIONS=$(( (TOTAL_DAYS_CAP + CHUNK_DAYS - 1) / CHUNK_DAYS ))
for i in $(seq 1 "$ITERATIONS"); do
    echo "[load_silver_rental_history] 청크 $i/$ITERATIONS 시작 (MAX_DAYS_PER_RUN=$CHUNK_DAYS)"
    OUTPUT=$({bash_staging_job("transform_silver_rental_history", "MAX_DAYS_PER_RUN=$CHUNK_DAYS ")} 2>&1) || {{ echo "$OUTPUT"; exit 1; }}
    echo "$OUTPUT"
    if echo "$OUTPUT" | grep -q "처리할 신규 날짜 없음"; then
        echo "[load_silver_rental_history] Bronze 워터마크까지 모두 처리됨 - 청크 $i에서 조기 종료"
        break
    fi
done
""",
        execution_timeout=timedelta(hours=6),  # 순차 반복이라 daily(1시간)보다 훨씬 넉넉히
        pool=SILVER_POOL,
    )

    # failure_report: silver_failure_report.py는 워터마크로 구간을 자르지 않고 매번 Bronze
    # 전체를 재처리한다(#76 문서 참고) - bootstrap 대상이 아니다. set_bronze_ingestion_watermark_*
    # 이후에 실행해, 처리 후 기록되는 Silver 워터마크(Bronze 워터마크의 미러)가 이번 초기
    # 적재 값을 반영하게 한다.
    load_silver_failure_report = BashOperator(
        task_id="load_silver_failure_report",
        bash_command=bash_staging_job("silver_failure_report"),
        execution_timeout=timedelta(hours=1),
        pool=SILVER_POOL,
    )

    # 두 소스 사이에는 의존관계를 걸지 않는다. 서로 참조하지 않는 원천이고,
    # 동시 실행 부하는 max_active_tasks=2가 제한한다.
    rental_history >> check_watermark_date_rental_history >> set_bronze_ingestion_watermark_rental_history
    rental_history >> bootstrap_silver_watermark_rental_history
    [set_bronze_ingestion_watermark_rental_history, bootstrap_silver_watermark_rental_history] >> load_silver_rental_history
    failure_report >> check_watermark_date_failure_report >> set_bronze_ingestion_watermark_failure_report
    set_bronze_ingestion_watermark_failure_report >> load_silver_failure_report


bronze_initial_load_all_sources()
