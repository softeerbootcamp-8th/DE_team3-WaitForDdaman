"""
Silver validate - staging(silver.failure_report_staging)이 확정 스키마의 유일키/
그레인 규칙을 지키는지 검증한다. 여기서 실패하면 merge/overwrite로 넘어가지 않는다
(태스크 체인상 validate가 적재 앞).

검증 항목:
- 필수 컬럼(bike_no, reg_dttm, failure_type) null 없음 (실패 시 파이프라인 중단)
- reg_dttm null은 transform의 STRING->TIMESTAMP 캐스팅이 못 잡은 포맷 편차 의심
- 유일키(bike_no, reg_dttm, failure_type) 중복은 경고만 하고 통과시킨다 - reg_dttm을
  날짜 단위(자정)로 통일하기로 하면서(원본 시각 표기가 파일마다 제각각이라 전부
  안전하게 파싱할 방법이 없고, downstream인 risk_model도 날짜 단위만 씀) 같은 날
  같은 자전거가 같은 고장유형으로 여러 번 신고되면 자연스럽게 발생하는 중복이라
  더 이상 이례적인 실패 조건이 아니다.

사용법:
    python -m jobs.silver_failure_report_validate
"""
import logging
import sys

from pyspark.sql import functions as F

from common import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SILVER_STAGING_TABLE = f"{config.SETTINGS.iceberg_catalog_name}.silver.failure_report_staging"

REQUIRED_COLUMNS = ("bike_no", "reg_dttm", "failure_type")


def run() -> None:
    spark = build_spark_session("silver-validate-failure-report")
    staging_df = spark.table(SILVER_STAGING_TABLE)

    errors = []

    null_counts = (
        staging_df.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in REQUIRED_COLUMNS])
        .collect()[0]
        .asDict()
    )
    for col_name, null_count in null_counts.items():
        if null_count:
            suffix = " (캐스팅 실패 의심)" if col_name == "reg_dttm" else ""
            errors.append(f"{col_name} null {null_count}행{suffix}")

    dup_count = staging_df.groupBy(*REQUIRED_COLUMNS).count().where(F.col("count") > 1).count()
    if dup_count:
        logger.warning("유일키%s 중복 %d건 (날짜 단위 통일에 따른 예상된 중복 - 통과)", REQUIRED_COLUMNS, dup_count)

    if errors:
        for e in errors:
            logger.error("검증 실패: %s", e)
        sys.exit(1)

    logger.info("검증 통과: %d행", staging_df.count())


if __name__ == "__main__":
    run()
