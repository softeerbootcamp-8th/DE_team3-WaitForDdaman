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
모든 초기 적재 파일 탐색보다 먼저 `create_bronze_tables`가 멱등적으로 필수 Bronze
테이블을 생성한다. 따라서 신규 환경에서 별도 Bootstrap DAG를 먼저 실행하지 않아도
이 DAG를 트리거하면 테이블 준비가 보장된다.

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

### 왜 병렬을 2개로 제한하는가 (로컬에서만)
로컬(LocalStack) 환경에서 여러 Spark 잡이 동시에 대량 PutObject를 보내면
"read of closed file" 레이스 컨디션이 발생하는 게 실측으로 확인됐다. 각 잡이 이미
내부적으로 로컬 병렬도를 제한하고 있으므로, DAG 레벨에서도 동시 실행을 제한한다.
이건 LocalStack 전용 제약이라 AWS 환경(is_aws_env())에서는 적용하지 않는다 -
그대로 두면 EMR Serverless 초기 적재 배치들이 실제로는 원격 애플리케이션에서
독립 실행되는데도 로컬 레이스 컨디션 방지용 상한에 불필요하게 발목 잡힌다.

max_active_tasks는 이 DAG 안에서만 유효하다. 초기 적재는 몇 시간짜리라 도중에 일 배치
스케줄(06:00)과 겹치는 게 정상 케이스인데, DAG 단위 제한은 서로를 알지 못한다.
그래서 Spark를 쓰는 태스크는 각 레이어의 daily 잡과 같은 풀에 넣어 전역으로 묶는다 -
로컬 Bronze 초기 적재 태스크는 BRONZE_POOL, Silver 승격 태스크(load_silver_*)는 daily
silver_*_dag.py와 같은 SILVER_POOL. 워터마크 태스크는 S3에 JSON 한 개만 쓰고 Spark를
안 띄우므로 풀에서 제외한다(슬롯을 잡으면 정작 초기 적재가 밀린다).

### EMR Serverless 초기 적재: 파일당 JobRun -> 배치당 JobRun (#249)
EMR Serverless 애플리케이션은 pre-initialized capacity(Driver 1개 + Executor 3개,
각 4vCPU/16GB)를 미리 띄워 두지만, JobRun 하나마다 애플리케이션 큐잉/드라이버 초기화
오버헤드가 있다. 기존에는 파일 하나 = EMR JobRun 하나였다 - 파일이 수십 개면 이
오버헤드도 수십 번 반복됐다.

이제 list_input_files.py가 만든 파일 목록을 dag_common.chunk_list()로 미리
batch_size개씩 묶고(*_emr_batch_size 파라미터), Dynamic Task Mapping은 파일이 아니라
"배치"를 펼친다(.expand(input_files=...)). 배치 하나 = EMR JobRun 하나 = Spark 세션
하나이고, initial_load_rental_history.py / initial_load_failure_report.py가 그 안에서
파일을 순차 처리한다 - 파일마다 독립된 TemporaryDirectory를 열고 닫아 파일 단위 메모리
안전성은 그대로 유지하고, 전체 파일을 한 DataFrame으로 합치지 않는다(각 잡 파일의
모듈 docstring 참고).

배치 크기는 트레이드오프다: 크게 잡을수록 JobRun 수(=오버헤드)가 줄지만, 배치 하나가
실패하면 Airflow 태스크 재시도가 그 배치 전체를 다시 돌린다(이미 성공한 파일도 같이 -
overwritePartitions가 멱등이라 결과는 같지만 시간이 더 든다). 대여이력은 파일이 커서
(최대 700MB급) 배치를 작게, 고장신고는 파일이 작아서 배치를 크게 잡는다 - 두 값 모두
Trigger DAG w/ config로 조정 가능하다.

EMR JobRun을 제출하는 배치 태스크는 BRONZE_POOL이 아니라 EMR_INITIAL_LOAD_POOL을 쓴다
(dag_common.py 참고) - 워커 로컬 메모리가 아니라 EMR pre-initialized capacity가 진짜
제약이기 때문이다.

### AWS 초기 적재 S3 스테이징: 다운로드/압축해제와 업로드를 분리 (#255)
list_*_files 태스크는 다운로드(캐시 포함)+압축 해제까지만 하고 로컬 파일 경로를
반환한다. S3 업로드는 stage_*_files_batch가 별도로 맡는다 - 완전히 빈 S3에서
~40~47GB/114개 파일을 전부 올려야 하는 상황을 list_*_files 태스크 하나가 몇 시간
동안 순차로 떠안으면, 재시도할 때마다 이미 올라간 파일까지 처음부터 다시 순회해야
한다. 대신 로컬 파일 목록을 배치로 잘라(chunk_*_staging_files, *_staging_batch_size
파라미터) 배치 단위로 Dynamic Task Mapping을 편다 - "파일 하나 = 태스크 하나"는
만들지 않으면서도, 배치 하나가 실패해도 그 배치만 재시도된다. 이 배치 태스크는
BRONZE_POOL/EMR_INITIAL_LOAD_POOL이 아니라 S3_STAGING_POOL을 쓴다(dag_common.py
참고) - 워커(EC2 t4g.large, 2vCPU/8GB) 프로세스 안에서 boto3로 직접 로컬 MD5 계산과
PutObject/CopyObject를 수행하는 태스크라, 다른 두 풀이 보호하는 자원(PyArrow 힙,
EMR pre-initialized capacity)과는 다른 자원(워커 CPU/네트워크)을 보호해야 한다.

업로드 자체의 멱등성/재사용(예전 한글/공백 key를 서버사이드 CopyObject로 재사용하는
것 포함)은 jobs/stage_initial_load_files.py와 common/s3_utils.reuse_or_upload_staging_file
문서를 참고한다.

### EMR Serverless로의 INPUT_FILES 전달: entryPointArguments 우선 (#255)
기존에는 파일 목록을 extra_env={"INPUT_FILES": ...}로 넘겨 EMR Serverless의
sparkSubmitParameters(--conf ...driverEnv.INPUT_FILES=값)에 실었다. 이 파서는 셸이
아니라서 공백에서 토큰을 잘라먹는다(#218 실측) - 스테이징 파일명을 ASCII로 안전하게
바꿔서 회피했지만(#247, #255), 전달 경로 자체의 근본 문제는 남아 있었다. 이제
entryPointArguments(--input-files-json)로 JSON 배열을 그대로 한 토큰으로 드라이버에
전달한다 - 파서가 값 안의 공백/특수문자를 건드릴 일이 없다. INPUT_FILES 환경변수는
initial_load_rental_history.py/initial_load_failure_report.py에 하위호환 fallback으로
남아 있다(로컬 BashOperator 등 entryPointArguments를 쓰지 않는 호출부용).

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
    EMR_INITIAL_LOAD_POOL,
    INGESTION_DIR,
    S3_STAGING_POOL,
    SILVER_POOL,
    bash_job,
    bash_staging_job,
    chunk_list,
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
    # 로컬은 LocalStack 동시 쓰기 레이스 컨디션 방지용 상한(2), AWS는 EMR Serverless가
    # 원격에서 JobRun을 독립 실행하므로 이 상한을 적용할 이유가 없다 - EMR_INITIAL_LOAD_POOL
    # 슬롯 수가 실제 동시성 제약 역할을 한다 (#249, "왜 병렬을 2개로 제한하는가" 문단 참고).
    max_active_tasks=2 if not is_aws_env() else 10,
    default_args=default_args,
    tags=["bronze", "independent", "manual"],
    params={
        "rental_history_dir": f"{INGESTION_DIR}/data/rental_history",
        "rental_history_pattern": "*",
        "rental_history_watermark_date": "2026-06-30",
        # 배치 하나 = EMR JobRun 하나 (#249). 대여이력은 반기 파일이 최대 700MB급이라
        # 배치를 작게 잡아 배치 실패 시 재시도 비용(이미 성공한 파일 재처리)을 낮춘다.
        "rental_history_emr_batch_size": "3",
        # S3 스테이징 업로드 배치 크기 (#255) - 대여이력은 연 12개 파일이므로
        # 반기 단위(6개)로 묶는다. S3_STAGING_POOL 슬롯 수(2)에 맞춰 배치
        # 하나(파일 최대 700MB급 x 이 값)가 워커 메모리/네트워크를 과도하게 잡지 않게 한다.
        "rental_history_staging_batch_size": "6",
        "failure_report_dir": f"{INGESTION_DIR}/data/failure_report",
        "failure_report_pattern": "*",
        "failure_report_watermark_date": "2026-06-30",
        # 고장신고는 파일이 작아서 배치를 더 크게 잡아 JobRun 수를 더 줄인다.
        "failure_report_emr_batch_size": "6",
        # 고장신고는 파일이 작아서 스테이징 배치도 더 크게 잡는다.
        "failure_report_staging_batch_size": "10",
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
    # 신규 환경에서도 초기 적재 태스크가 테이블 없음으로 실패하지 않도록
    # 모든 파일 탐색/적재보다 먼저 Bronze Iceberg 테이블을 멱등 생성한다.
    create_bronze_tables = BashOperator(
        task_id="create_bronze_tables",
        bash_command=bash_job("bootstrap_iceberg_tables"),
        pool=BRONZE_POOL,
    )

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

    if is_aws_env():
        # S3 업로드는 list_rental_history_files가 아니라 여기서 배치 단위로 한다(#255) -
        # "AWS 초기 적재 S3 스테이징" 문단 참고. rental_history_files는 이 시점에는 아직
        # 로컬 파일 경로 목록이다.
        @task(task_id="chunk_rental_history_staging_files")
        def chunk_rental_history_staging_files(files: list[str], batch_size: str) -> list[list[str]]:
            return chunk_list(files, int(batch_size))

        rental_history_staging_batches = chunk_rental_history_staging_files(
            rental_history_files, "{{ params.rental_history_staging_batch_size }}"
        )

        stage_rental_history_files_batch = BashOperator.partial(
            task_id="stage_rental_history_files_batch",
            # 스테이징 배치 기본 크기(5) x 파일 1개당 넉넉한 상한(700MB급 파일 감안).
            execution_timeout=timedelta(hours=2),
            pool=S3_STAGING_POOL,
            do_xcom_push=True,
        ).expand(
            bash_command=rental_history_staging_batches.map(
                lambda batch: bash_job(
                    "stage_initial_load_files",
                    f"DATASET=rental_history INPUT_FILES='{json.dumps(batch)}' ",
                )
            )
        )

        @task(task_id="parse_rental_history_staging_uris")
        def parse_rental_history_staging_uris(raw_json_per_batch: list[str]) -> list[str]:
            """배치별 XCom(JSON 배열 문자열)을 평평하게 이어붙여 하나의 S3 URI 목록으로 만든다."""
            return [uri for raw_json in raw_json_per_batch for uri in json.loads(raw_json)]

        rental_history_staged_uris = parse_rental_history_staging_uris(
            stage_rental_history_files_batch.output
        )

        # 파일 목록을 배치로 미리 잘라서(dag_common.chunk_list, #249) 배치 단위로
        # Dynamic Task Mapping을 편다 - 배치 하나 = EMR JobRun 하나. 이렇게 해야 파일이
        # 수십 개여도 EMR JobRun 시작 오버헤드가 배치 수만큼만 발생한다("EMR Serverless
        # 초기 적재: 파일당 JobRun -> 배치당 JobRun" 문단 참고).
        @task(task_id="chunk_rental_history_files")
        def chunk_rental_history_files(files: list[str], batch_size: str) -> list[list[str]]:
            return chunk_list(files, int(batch_size))

        rental_history_file_batches = chunk_rental_history_files(
            rental_history_staged_uris, "{{ params.rental_history_emr_batch_size }}"
        )

        @task(
            task_id="initial_load_rental_history_batch",
            # 배치 기본 크기(3) x 파일 1개당 기존 상한(30분) + 여유. 배치 크기 파라미터를
            # 크게 바꾸면 이 상한도 같이 늘려야 한다.
            execution_timeout=timedelta(hours=3),
            pool=EMR_INITIAL_LOAD_POOL,
        )
        def initial_load_rental_history_batch_emr(input_files: list[str]) -> str:
            return run_emr_serverless_spark_job(
                entry_point="local:///opt/app/ingestion/jobs/initial_load_rental_history.py",
                name="bronze-initial-load-rental-history",
                # entryPointArguments가 1차 전달 경로다(#255) - sparkSubmitParameters의
                # 자체 파서가 공백에서 토큰을 잘라먹는 문제를 피한다("EMR Serverless로의
                # INPUT_FILES 전달" 문단 참고). INPUT_FILES 환경변수는 잡 쪽에 하위호환
                # fallback으로만 남아 있다.
                entry_point_arguments=["--input-files-json", json.dumps(input_files)],
                log_group_name="/emr-serverless/bronze-initial-load",
                log_stream_name_prefix="rental-history",
                tags={
                    "dag_id": "bronze_initial_load_all_sources",
                    "task_id": "initial_load_rental_history_batch",
                    "dataset": "rental_history",
                },
            )

        # 배치 개수만큼 태스크 인스턴스가 동적으로 생성된다(Dynamic Task Mapping) - 배치
        # 하나가 실패해도 그 배치 인스턴스만 재시도되고 나머지 배치에는 영향이 없다
        # (단, 재시도는 배치 전체 단위 - 배치 안 개별 파일 단위는 아니다. #249 문단 참고).
        rental_history = initial_load_rental_history_batch_emr.expand(
            input_files=rental_history_file_batches
        )
    else:
        # 로컬은 배치로 묶지 않는다 - EMR JobRun 시작 오버헤드가 없는 환경이라 묶을
        # 이유가 없고, 기존 "파일 하나 = 새 프로세스" 격리를 그대로 유지한다.
        rental_history = BashOperator.partial(
            task_id="initial_load_rental_history_file",
            execution_timeout=timedelta(minutes=30),  # 폴더 전체 기준(3시간) -> 파일 1개 기준으로 축소
            pool=BRONZE_POOL,
        ).expand(
            bash_command=rental_history_files.map(
                lambda f: bash_job("initial_load_rental_history", f"INPUT_FILES='{json.dumps([f])}' ")
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

    if is_aws_env():
        # S3 업로드는 list_failure_report_files가 아니라 여기서 배치 단위로 한다(#255) -
        # rental_history와 동일한 스테이징 구조("AWS 초기 적재 S3 스테이징" 문단 참고).
        @task(task_id="chunk_failure_report_staging_files")
        def chunk_failure_report_staging_files(files: list[str], batch_size: str) -> list[list[str]]:
            return chunk_list(files, int(batch_size))

        failure_report_staging_batches = chunk_failure_report_staging_files(
            failure_report_files, "{{ params.failure_report_staging_batch_size }}"
        )

        stage_failure_report_files_batch = BashOperator.partial(
            task_id="stage_failure_report_files_batch",
            execution_timeout=timedelta(hours=1),
            pool=S3_STAGING_POOL,
            do_xcom_push=True,
        ).expand(
            bash_command=failure_report_staging_batches.map(
                lambda batch: bash_job(
                    "stage_initial_load_files",
                    f"DATASET=failure_report INPUT_FILES='{json.dumps(batch)}' ",
                )
            )
        )

        @task(task_id="parse_failure_report_staging_uris")
        def parse_failure_report_staging_uris(raw_json_per_batch: list[str]) -> list[str]:
            return [uri for raw_json in raw_json_per_batch for uri in json.loads(raw_json)]

        failure_report_staged_uris = parse_failure_report_staging_uris(
            stage_failure_report_files_batch.output
        )

        # rental_history와 동일한 배치 구조(#249) - 고장신고는 파일이 작아 배치를
        # 더 크게 잡는다(failure_report_emr_batch_size 기본값 참고).
        @task(task_id="chunk_failure_report_files")
        def chunk_failure_report_files(files: list[str], batch_size: str) -> list[list[str]]:
            return chunk_list(files, int(batch_size))

        failure_report_file_batches = chunk_failure_report_files(
            failure_report_staged_uris, "{{ params.failure_report_emr_batch_size }}"
        )

        @task(
            task_id="initial_load_failure_report_batch",
            # 배치 기본 크기(6) x 파일 1개당 기존 상한(20분) + 여유.
            execution_timeout=timedelta(hours=2),
            pool=EMR_INITIAL_LOAD_POOL,
        )
        def initial_load_failure_report_batch_emr(input_files: list[str]) -> str:
            return run_emr_serverless_spark_job(
                entry_point="local:///opt/app/ingestion/jobs/initial_load_failure_report.py",
                name="bronze-initial-load-failure-report",
                # entryPointArguments가 1차 전달 경로다(#255) - rental_history와 동일한
                # 이유("EMR Serverless로의 INPUT_FILES 전달" 문단 참고).
                entry_point_arguments=["--input-files-json", json.dumps(input_files)],
                log_group_name="/emr-serverless/bronze-initial-load",
                log_stream_name_prefix="failure-report",
                tags={
                    "dag_id": "bronze_initial_load_all_sources",
                    "task_id": "initial_load_failure_report_batch",
                    "dataset": "failure_report",
                },
            )

        failure_report = initial_load_failure_report_batch_emr.expand(
            input_files=failure_report_file_batches
        )
    else:
        failure_report = BashOperator.partial(
            task_id="initial_load_failure_report_file",
            execution_timeout=timedelta(minutes=20),  # 폴더 전체 기준(1시간) -> 파일 1개 기준으로 축소
            pool=BRONZE_POOL,
        ).expand(
            bash_command=failure_report_files.map(
                lambda f: bash_job("initial_load_failure_report", f"INPUT_FILES='{json.dumps([f])}' ")
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
    # #232: 기존엔 bash for-loop가 transform_silver_rental_history.py를 반복 호출하며
    # 매번 스스로 다음 구간을 정했다 - 한 반복 실패가 루프 전체를 멈추고, 반복들이
    # 서로 의존해 병렬화도 안 됐다. 이제는 (1) 전체 청크 목록을 한 번에 계산하고
    # (2) 청크별로 독립된 Airflow 태스크 인스턴스로 펼쳐서(.expand()) 실행하고
    # (3) 전부 성공했을 때만 워터마크를 한 번에 전진시킨다 - 실패한 청크만 골라
    # 재시도할 수 있고, 나머지 청크는 그 실패에 영향받지 않는다.
    compute_rental_history_backfill_ranges = BashOperator(
        task_id="compute_rental_history_backfill_ranges",
        bash_command=bash_job(
            "compute_silver_rental_history_backfill_ranges",
            "CHUNK_DAYS='{{ params.rental_history_silver_chunk_days }}' "
            "TOTAL_DAYS_CAP='{{ params.rental_history_silver_total_days_cap }}' ",
        ),
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=10),
        pool=BRONZE_POOL,  # Bronze/Silver 워터마크를 Iceberg에서 읽으므로 다른 워터마크 태스크와 동일 풀
    )

    @task(task_id="parse_rental_history_backfill_ranges")
    def parse_rental_history_backfill_ranges(raw_json: str) -> list[dict]:
        return json.loads(raw_json)

    rental_history_backfill_ranges = parse_rental_history_backfill_ranges(
        compute_rental_history_backfill_ranges.output
    )

    load_silver_rental_history_chunk = BashOperator.partial(
        task_id="load_silver_rental_history_chunk",
        execution_timeout=timedelta(hours=1),  # 청크 1개(최대 31일) 기준 - 순차 6시간에서 축소
        pool=SILVER_POOL,
        # pyiceberg SqlCatalog는 커밋 재시도가 없다 - 매핑 인스턴스를 동시에 띄우면
        # silver.rental_history/silver.rental_history_quarantine에 대한 동시
        # overwrite_partition 커밋이 충돌해 CommitFailedException으로 재시도를
        # 소진할 수 있다. 일회성 백필이라 청크 wall-clock 시간이 병목이 아니므로
        # 매핑 인스턴스를 직렬화해 동시 쓰기 경합 자체를 없앤다 (#232).
        max_active_tis_per_dag=1,
    ).expand(
        bash_command=rental_history_backfill_ranges.map(
            lambda r: bash_staging_job(
                "transform_silver_rental_history",
                f"BACKFILL_RANGE_START='{r['start']}' BACKFILL_RANGE_END='{r['end']}' ",
            )
        )
    )

    @task(task_id="max_rental_history_backfill_range_end", trigger_rule="all_success")
    def max_rental_history_backfill_range_end(ranges: list[dict]) -> str | None:
        """청크가 하나도 없으면(이미 다 처리됨) None - 마무리 태스크가 건너뛴다."""
        if not ranges:
            return None
        return max(r["end"] for r in ranges)

    rental_history_backfill_max_end = max_rental_history_backfill_range_end(
        rental_history_backfill_ranges
    )

    # trigger_rule="all_success": load_silver_rental_history_chunk의 모든 매핑 인스턴스가
    # 성공해야만 실행된다 - common/watermark.py의 "부분 실패 시 갱신 금지" 불변식을
    # 유지한다. 청크가 0개(처리할 신규 구간 없음)면 WATERMARK_DATE가 빈 문자열이 되므로
    # set_watermark.py가 그대로 실패한다 - 그 경우엔 애초에 이 태스크가 워터마크를
    # 바꿀 필요가 없으므로 skip 조건을 bash에서 직접 처리한다.
    finalize_rental_history_backfill_watermark = BashOperator(
        task_id="finalize_rental_history_backfill_watermark",
        bash_command=f"""
set -e
END_DATE="{{{{ ti.xcom_pull(task_ids='max_rental_history_backfill_range_end') }}}}"
if [ -z "$END_DATE" ]; then
    echo "[finalize_rental_history_backfill_watermark] 처리할 신규 구간 없음 - 워터마크 변경 없이 종료"
    exit 0
fi
{bash_job("set_watermark", "DATASET=silver_rental_history WATERMARK_DATE=$END_DATE ")}
""",
        trigger_rule="all_success",
        execution_timeout=timedelta(minutes=5),
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
    [
        set_bronze_ingestion_watermark_rental_history, bootstrap_silver_watermark_rental_history,
    ] >> compute_rental_history_backfill_ranges
    load_silver_rental_history_chunk >> rental_history_backfill_max_end >> finalize_rental_history_backfill_watermark
    failure_report >> check_watermark_date_failure_report >> set_bronze_ingestion_watermark_failure_report
    set_bronze_ingestion_watermark_failure_report >> load_silver_failure_report
    create_bronze_tables >> [list_rental_history_files, list_failure_report_files]


bronze_initial_load_all_sources()
