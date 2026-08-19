"""
Silver check - 전체 재처리 전, 브론즈가 부분 적재 상태가 아닌지 확인한다.

단일 DAG(매일 브론즈 전체 재처리 + INSERT OVERWRITE)에서는 이 검증이 실버를 지키는
첫 번째 방어선이다(두 번째는 validate). ingestion/jobs/initial_load_failure_report.py의
브론즈 적재는 파일 단위로 개별 커밋되고 배치 전체를 감싸는 트랜잭션이 없어서, 배치
도중 실패하면 브론즈가 부분 적재 상태로 남을 수 있다. 이 상태로 그대로 재처리하면
INSERT OVERWRITE가 멀쩡하던 실버를 부분 데이터로 통째로 교체해버린다(신규 키만
추가하는 MERGE와 달리 기존 데이터를 지운다).

방어 로직: 브론즈 현재 행수가 직전 실버 행수의 MIN_BRONZE_TO_PREV_SILVER_RATIO
미만이면 실패. 직전 실버가 없거나(최초 실행) 0행이면 비교 기준이 없으므로 통과시킨다.

사용법:
    python -m jobs.silver_failure_report_check
"""
import logging
import sys

import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CATALOG = config.SETTINGS.iceberg_catalog_name
BRONZE_TABLE = f"{CATALOG}.bronze.failure_report"
SILVER_TABLE = f"{CATALOG}.silver.failure_report"

MIN_BRONZE_TO_PREV_SILVER_RATIO = 0.95  # 잠정치 - 실측 후 조정


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


def _silver_row_count(spark) -> int:
    if not spark.catalog.tableExists(SILVER_TABLE):
        return 0
    return spark.table(SILVER_TABLE).count()


def run() -> None:
    spark = build_spark_session("silver-check-failure-report")

    bronze_count = spark.table(BRONZE_TABLE).count()
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


if __name__ == "__main__":
    run()
