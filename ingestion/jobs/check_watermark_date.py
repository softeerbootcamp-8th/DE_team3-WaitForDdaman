"""
초기 적재 워터마크 값 검증 - 사람이 입력한 *_watermark_date가 실제 Bronze에 적재된
최대 날짜와 다르면 경고를 남긴다 (DAG를 실패시키지는 않음).

왜 필요한가: bronze_initial_load_all_sources_dag.py의 *_watermark_date는 사람이 직접
입력하는 값이다. *_pattern으로 적재 범위를 좁히면서 이 값을 같이 안 바꾸면,
daily_batch/Silver가 실제로 적재하지 않은 기간을 "처리 완료"로 잘못 기록해서 그
구간이 영구히 누락된다 (bronze_initial_load_all_sources_dag.py의 ⚠️ 참고). 이 잡은
그 어긋남을 사람이 놓치지 않도록 로그에 경고를 남기는 안전망이다.

로컬 테스트에서는 데이터셋 자체가 비어 있을 수 있다(다른 팀원 소스만 적재하는 등) -
이 경우는 비교할 실제 값이 없으므로 조용히 스킵한다 (경고 대상이 아니라 정상 케이스).

⚠️ 아직 DAG를 실패시키지 않는다 - 값이 어긋나도 계속 진행된다. 사람이 로그를 보고
판단해야 한다. set_watermark.py/bootstrap_silver_watermark.py 자체를 실제 데이터
기반으로 바꾸는 건 추후 작업 - 지금은 경고만 추가한다.

사용법:
    DATASET=rental_history WATERMARK_DATE=2026-06-30 python -m jobs.check_watermark_date
"""
import logging
import os
import sys
from datetime import date

import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 데이터셋명 -> (Bronze 테이블, 파티션 컬럼)
DATASETS = {
    "rental_history": ("bronze.rental_history", "rent_date_partition"),
    "failure_report": ("bronze.failure_report", "reg_date_partition"),
}


def run(dataset: str, watermark_date_str: str) -> None:
    if dataset not in DATASETS:
        print(f"알 수 없는 DATASET: {dataset} (가능한 값: {list(DATASETS.keys())})")
        sys.exit(1)

    table, partition_col = DATASETS[dataset]
    catalog = config.SETTINGS.iceberg_catalog_name

    spark = build_spark_session(f"check-watermark-date-{dataset}")
    try:
        # bootstrap_silver_watermark.py와 동일한 이유로 빈 문자열을 걸러낸다 -
        # rent_dt/reg_dttm이 NULL인 원본 행은 파티션 컬럼이 ""로 떨어져서
        # MAX 계산을 오염시킬 수 있다.
        row = spark.sql(
            f"SELECT MAX({partition_col}) AS max_date FROM {catalog}.{table} "
            f"WHERE {partition_col} IS NOT NULL AND {partition_col} != ''"
        ).collect()[0]
    finally:
        spark.stop()

    max_date_str = row["max_date"]
    if not max_date_str:
        logger.info(
            "%s: Bronze 테이블에 비교할 데이터가 없음 - 검증 스킵 (로컬 부분 적재 등 정상 케이스)",
            table,
        )
        return

    actual_max = date.fromisoformat(max_date_str)
    given = date.fromisoformat(watermark_date_str)

    if actual_max != given:
        logger.warning(
            "⚠️ %s: 입력한 watermark_date(%s)가 실제 Bronze 최대 적재일(%s)과 다릅니다. "
            "*_pattern을 좁혀놓고 watermark_date를 안 맞추면 daily_batch/Silver가 실제로 "
            "적재하지 않은 기간을 처리 완료로 잘못 기록해 영구 누락됩니다 - 의도한 게 아니면 "
            "다시 트리거하기 전에 확인하세요.",
            table, given, actual_max,
        )
    else:
        logger.info("%s: watermark_date(%s)가 실제 Bronze 최대 적재일과 일치", table, given)


if __name__ == "__main__":
    run(os.environ["DATASET"], os.environ["WATERMARK_DATE"])
