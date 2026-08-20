"""
Silver - 공공자전거 고장신고 내역 (bronze.failure_report -> silver.failure_report)

### 확정 스키마 (변경 금지)
    bike_no       STRING
    reg_dttm      TIMESTAMP   (브론즈 STRING -> 캐스팅)
    failure_type  STRING      (trim)

파티션: days(reg_dttm)  <- 브론즈의 적재일 파티션(reg_date_partition)이 아니다.
유일키: (bike_no, reg_dttm, failure_type)
그레인: 고장부위 신고 1건 = 1행 (이벤트 단위로 접지 않는다)

### 단일 태스크로 통합한 이유 (#82)
원래 check -> transform -> validate -> overwrite -> metrics 5단계로 분할돼 있었으나,
실측 결과 이 데이터 규모(3.9MB, 229개 파일)에서는 분할 안 했을 때보다 3.8배 느렸다
(50.5s vs 13.4s). 태스크당 Spark 세션 기동 비용(3~4초 x 5)만으로는 설명이 안 되고,
각 단계가 staging Iceberg 테이블에 썼다가 다시 읽는 구조라 매 단계 S3 I/O 왕복이
추가로 붙는 게 원인이었다. 분할이 주던 이점(단계별 실패 지점 구분)은 로그 메시지로
대체하고, staging 테이블 없이 하나의 Spark 세션 안에서 DataFrame을 그대로 넘긴다.

### check: 브론즈 부분 적재 방어
단일 DAG(매일 브론즈 전체 재처리 + INSERT OVERWRITE)에서는 이 검증이 실버를 지키는
첫 번째 방어선이다(두 번째는 validate). ingestion/jobs/initial_load_failure_report.py의
브론즈 적재는 파일 단위로 개별 커밋되고 배치 전체를 감싸는 트랜잭션이 없어서, 배치
도중 실패하면 브론즈가 부분 적재 상태로 남을 수 있다. 이 상태로 그대로 재처리하면
INSERT OVERWRITE가 멀쩡하던 실버를 부분 데이터로 통째로 교체해버리므로, 브론즈
현재 행수가 직전 실버 행수의 MIN_BRONZE_TO_PREV_SILVER_RATIO 미만이면 중단한다.
직전 실버가 없거나(최초 실행) 0행이면 비교 기준이 없으므로 통과시킨다.

### transform
- reg_dttm: STRING -> TIMESTAMP. 원본 포맷이 'yyyy-MM-dd HH:mm:ss'(19자, 2026-01~06
  API 수집분 실측 100%)와 'yyyyMMdd'(일부 초기 적재 파일, ingestion/jobs/
  initial_load_failure_report.py:_derive_date_partition 주석 참고) 둘 다 나타날 수
  있어 순서대로 시도한다. 원본 시각 표기가 파일마다 제각각(0패딩 유무, 구분자
  -/./없음, 초/시각 유무 자체가 다름)이라 모든 변형에 안전한 timestamp 패턴을 다
  나열하는 대신 날짜 부분만 뽑아 자정(00:00:00) TIMESTAMP로 통일한다. 다운스트림
  (risk_model)이 reg_dttm을 날짜 단위로만 쓰고 있어 시각 정밀도가 필요 없고, bronze가
  원본 문자열을 그대로 보존하므로 나중에 시각까지 필요해지면 거기서 다시 뽑아 쓸 수
  있다.
- failure_type: 뒤쪽 공백 제거(trim) - bronze CSV 파서가 ignoreTrailingWhiteSpace=false라
  '기타 ', '타이어 ' 같은 원본 공백이 그대로 살아있다.
- bike_no: 원본 그대로 (형식 100% 검증된 값이라 추가 정제 불필요).

집계/조인 없음 - 그레인은 bronze 1행 = silver 1행 그대로 유지한다.

### validate
- 필수 컬럼(bike_no, reg_dttm, failure_type) null 없음 (실패 시 파이프라인 중단).
  reg_dttm null은 위 STRING->TIMESTAMP 캐스팅이 못 잡은 포맷 편차 의심.
- 유일키(bike_no, reg_dttm, failure_type) 중복은 경고만 하고 통과시킨다 - reg_dttm을
  날짜 단위(자정)로 통일하기로 하면서 같은 날 같은 자전거가 같은 고장유형으로 여러 번
  신고되면 자연스럽게 발생하는 중복이라 더 이상 이례적인 실패 조건이 아니다.

### overwrite
common/spark_session.py의 build_spark_session()이 partitionOverwriteMode=dynamic을
이미 설정해두므로 writeTo(...).overwritePartitions()는 이번에 쓰는 days(reg_dttm)
파티션(=전체)만 덮어쓴다. bronze 잡들과 동일한 멱등성 패턴이라 재실행해도 안전하다.

### 단일 DAG (전체 재처리), Asset 기반 스케줄
자세한 이유는 airflow/dags/silver_failure_report_dag.py 참고.

사용법:
    python -m jobs.silver_failure_report
"""
import logging
import sys

from pyspark.sql import functions as F

import config
from common.spark_session import build_spark_session
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import BRONZE_FAILURE_REPORT, SILVER_FAILURE_REPORT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CATALOG = config.SETTINGS.iceberg_catalog_name
BRONZE_TABLE = f"{CATALOG}.bronze.failure_report"
SILVER_TABLE = f"{CATALOG}.silver.failure_report"

MIN_BRONZE_TO_PREV_SILVER_RATIO = 0.95  # 잠정치 - 실측 후 조정

REQUIRED_COLUMNS = ("bike_no", "reg_dttm", "failure_type")


def evaluate_partial_load(
    bronze_count: int,
    prev_silver_count: int,
    threshold: float = MIN_BRONZE_TO_PREV_SILVER_RATIO,
) -> tuple[bool, str]:
    """브론즈가 직전 실버 대비 threshold 미만으로 줄었으면 (True, 사유)를 반환한다."""
    if prev_silver_count == 0:
        return False, "직전 silver 데이터 없음 - 비율 체크 스킵"

    ratio = bronze_count / prev_silver_count
    reason = f"브론즈 {bronze_count}행 / 직전 실버 {prev_silver_count}행 = {ratio:.1%}"
    if ratio < threshold:
        return True, f"{reason} (임계값 {threshold:.0%} 미만 - 브론즈 부분 적재 의심)"
    return False, reason


def transform(bronze_df):
    """bronze.failure_report 전체를 확정 스키마로 변환한다. 읽기/쓰기를 하지 않는다."""
    date_only = F.regexp_replace(
        F.regexp_extract(F.col("reg_dttm"), r"(\d{4}[-.]\d{1,2}[-.]\d{1,2})", 1),
        r"\.",
        "-",
    )
    reg_dttm_ts = F.coalesce(
        F.to_date(date_only, "yyyy-M-d"),
        F.to_date(F.col("reg_dttm"), "yyyyMMdd"),
    ).cast("timestamp")

    return bronze_df.select(
        F.col("bike_no"),
        reg_dttm_ts.alias("reg_dttm"),
        F.trim(F.col("failure_type")).alias("failure_type"),
    )


def validate(silver_df) -> list[str]:
    """필수 컬럼 null을 검사하고 에러 목록을 반환한다. 유일키 중복은 경고만 로그로 남긴다."""
    errors = []

    null_counts = (
        silver_df.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in REQUIRED_COLUMNS])
        .collect()[0]
        .asDict()
    )
    for col_name, null_count in null_counts.items():
        if null_count:
            suffix = " (캐스팅 실패 의심)" if col_name == "reg_dttm" else ""
            errors.append(f"{col_name} null {null_count}행{suffix}")

    dup_count = silver_df.groupBy(*REQUIRED_COLUMNS).count().where(F.col("count") > 1).count()
    if dup_count:
        logger.warning("유일키%s 중복 %d건 (날짜 단위 통일에 따른 예상된 중복 - 통과)", REQUIRED_COLUMNS, dup_count)

    return errors


def _silver_row_count(spark) -> int:
    if not spark.catalog.tableExists(SILVER_TABLE):
        return 0
    return spark.table(SILVER_TABLE).count()


def _ensure_silver_table(spark) -> None:
    silver_db = SILVER_TABLE.rsplit(".", 1)[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            bike_no STRING,
            reg_dttm TIMESTAMP,
            failure_type STRING
        )
        USING iceberg
        PARTITIONED BY (days(reg_dttm))
        """
    )


def run() -> None:
    spark = build_spark_session("silver-failure-report")

    bronze_df = spark.table(BRONZE_TABLE)
    bronze_count = bronze_df.count()
    logger.info("bronze.failure_report: %d행", bronze_count)

    if bronze_count == 0:
        logger.error("브론즈에 데이터가 없음 - 브론즈 적재 완료 여부 확인 필요")
        sys.exit(1)

    prev_silver_count = _silver_row_count(spark)
    is_partial, reason = evaluate_partial_load(bronze_count, prev_silver_count)
    logger.info(reason)
    if is_partial:
        logger.error("파이프라인 중단: %s", reason)
        sys.exit(1)

    silver_df = transform(bronze_df).cache()

    errors = validate(silver_df)
    if errors:
        for e in errors:
            logger.error("검증 실패: %s", e)
        silver_df.unpersist()
        sys.exit(1)

    _ensure_silver_table(spark)
    row_count = silver_df.count()
    silver_df.writeTo(SILVER_TABLE).overwritePartitions()
    silver_df.unpersist()

    # overwrite가 끝난 뒤에만 갱신 - 이 시점의 Bronze 워터마크까지 Silver에
    # 반영됐다는 의미로 그대로 이어받는다
    bronze_watermark = read_watermark(watermark_key=BRONZE_FAILURE_REPORT)
    write_watermark(bronze_watermark, watermark_key=SILVER_FAILURE_REPORT)

    logger.info(
        "INSERT OVERWRITE 완료: %d행 (Silver 워터마크=%s)", row_count, bronze_watermark
    )


if __name__ == "__main__":
    run()
