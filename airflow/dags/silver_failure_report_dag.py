"""
Silver DAG - 공공자전거 고장신고 내역 (bronze.failure_report -> silver.failure_report)

### 확정 스키마 (변경 금지)
    bike_no       STRING
    reg_dttm      TIMESTAMP   (브론즈 STRING -> 캐스팅)
    failure_type  STRING      (trim)

파티션: reg_date_partition (identity, yyyy-MM-dd = date(reg_dttm))
        ⚠️ 이름은 브론즈와 같지만 의미가 다르다 - 브론즈는 'API 요청일', 실버는 '고장 등록일'.
        API가 요청일 기준 최대 31일치를 돌려주므로(#304) 브론즈 요청일 파티션 하나에
        실버 등록일 파티션이 여러 개 섞여 나오고 같은 신고가 요청일마다 중복으로 온다.
        Gold 담당자와의 인터페이스 계약이다 (#143에서 days(reg_dttm) hidden 파티션에서
        바뀜 - pyiceberg가 transform 파티션 쓰기에 제약이 있어 identity로 통일).
        자세한 배경은 staging/jobs/silver_failure_report.py docstring 참고.
유일키: (bike_no, reg_dttm, failure_type)
그레인: 고장부위 신고 1건 = 1행 (이벤트 단위로 접지 않는다)

### 왜 고정 시간이 아니라 Asset 기반 스케줄인가
Bronze 완료 시각은 매일 일정하지 않다(API 응답 지연, 재시도 등). 고정 시간(예: 07:00)으로
스케줄하면 "Bronze가 그 전에 끝난다"는 보장 없는 가정에 의존하게 되고, 어기면 어제자
Bronze 데이터로 Silver가 조용히 돈다 - 에러도 안 나서 제일 위험한 실패 모드다.

bronze_daily_batch_all_sources DAG의 daily_batch_failure_report 태스크가 성공하면
(실패/스킵 시에는 발생하지 않음) outlets로 FAILURE_REPORT_BRONZE Asset을 갱신하고,
이 DAG는 그 갱신을 스케줄 트리거로 사용한다.

### 단일 DAG (sliding window 재처리 + backlog 증분)
daily/backfill을 나누지 않는다 - silver는 결국 브론즈에 쌓인 걸 정제만 하면 되는
레이어다(catchup=False, 날짜 구간 파라미터 없음). 잡은 매 실행

  - 실제 신고일 기준 최근 31일 sliding window를 다시 계산해 파티션을 교체하고,
  - 그 아래에 남은 backlog 구간만 증분으로 처리하며 Silver 워터마크를 전진시킨다.

최근 구간을 매번 다시 만들기 때문에, 브론즈가 나중에 정정되거나 신고가 늦게
도착해도 실버에 반영된다 - self-healing. 자세한 이유는
staging/jobs/silver_failure_report.py docstring 참고 (#288, #304).

### 단일 태스크 (#82: 5단계 분할 -> 통합)
원래 check -> transform -> validate -> overwrite -> metrics 5단계로 분할돼 있었으나,
이 데이터 규모(3.9MB, 229개 파일)에서 실측한 결과 분할 안 했을 때보다 3.8배 느렸다
(50.5s vs 13.4s, 2026-08-19 측정). 태스크당 세션 기동 비용(3~4초 x 5)만으로는
설명이 안 되고, 각 단계가 staging Iceberg 테이블에 썼다가 다시 읽는 구조라 매 단계
S3 I/O 왕복이 추가로 붙는 게 원인이었다. 분할이 주던 이점(단계별 실패 지점 구분,
재시도 단위 세분화)보다 이 오버헤드가 훨씬 커서 하나의 태스크로 합쳤다 - #143에서
Spark를 걷어낸 뒤에도 단계별 S3 왕복 비용은 그대로라 이 판단은 유지한다.
check/validate 방어 로직 자체는 staging/jobs/silver_failure_report.py 안에
그대로 남아있고, 실패 시 로그로 어느 단계인지 구분할 수 있다. 재시도 단위가 전체
잡으로 커지지만, 리포트성 배치라 재실행 비용이 크지 않아 감수할 만한 트레이드오프다.

### jobs 모듈
staging/README.md에 이미 "Bronze / Silver 생성" 자리로 명시돼 있어 staging/jobs/
밑에 flat하게 담는다 (ingestion/jobs/와 동일한 파일당 1잡 구조). staging/에는 자체
common/이 없고 ingestion/common/(iceberg_catalog, iceberg_io, sql_assert 등)을
PYTHONPATH로 그대로 재사용한다 - 아래 _staging_bash() 참고.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from dag_assets import FAILURE_REPORT_BRONZE
from dag_common import DEFAULT_ARGS, SILVER_POOL

INGESTION_DIR = "/opt/airflow/ingestion"  # common/(iceberg_catalog, iceberg_io 등), .env 출처
STAGING_DIR = "/opt/airflow/staging"      # 잡 실행 위치
PYTHON_BIN = "python"

SILVER_MODULE = "silver_failure_report"  # staging/jobs/silver_failure_report.py


def _staging_bash(job_module: str, extra_env: str = "") -> str:
    """
    staging/ 잡 실행 커맨드. staging에는 common/이 없어 ingestion/common/을 그대로
    재사용한다 - PYTHONPATH에 ingestion과 staging을 같이 잡으면 `jobs`는 두 디렉터리의
    jobs/ 가 네임스페이스 패키지로 합쳐지고(PEP 420), `common`은 ingestion 쪽에서만
    찾긴다. 설정값(.env)도 ingestion/.env를 그대로 source한다.
    """
    return (
        f"cd {STAGING_DIR} && set -a && source {INGESTION_DIR}/.env && set +a && "
        f"PYTHONPATH={INGESTION_DIR}:{STAGING_DIR}:$PYTHONPATH "
        f"{extra_env}{PYTHON_BIN} -m jobs.{job_module}"
    )


@dag(
    dag_id="silver_failure_report",
    schedule=[FAILURE_REPORT_BRONZE],  # 고정 시간이 아니라 Bronze 완료 이벤트로 트리거
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    # Airflow catchup은 쓰지 않는다 - 밀린 날짜는 잡이 자기 워터마크로 따라잡는다(#288).
    catchup=False,
    max_active_runs=1,  # 같은 구간에 두 실행이 동시에 replace_range를 시도하는 것 방지
    default_args=DEFAULT_ARGS,
    tags=["silver", "asset_triggered", "main"],
    params={
        # 미지정이면 잡의 DEFAULT_MAX_DAYS_PER_RUN(70)을 쓴다. 오래 밀린 워터마크를
        # 한 번에 소화시키고 싶을 때만 수동 실행에서 올린다.
        "max_days_per_run": "",
    },
    doc_md=__doc__,
)
def silver_failure_report():
    BashOperator(
        task_id="silver_failure_report",
        bash_command=_staging_bash(
            SILVER_MODULE, "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}' "
        ),
        execution_timeout=timedelta(hours=1),
        pool=SILVER_POOL,
    )


silver_failure_report()
