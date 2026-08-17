"""
Gold DAG(dag_gold_dim_fact)의 wait_for_silver 단계 - 스냅샷 기반 Silver 소스
(station_master / station_active) 준비 확인용

### 왜 워터마크가 아니라 테이블의 snapshot_date를 직접 보는가 (2026-08-17, #50)
station_master/station_active는 날짜 범위 증분이 아니라 "그날의 전체 스냅샷
1개"만 적재하는 구조라 워터마크 파일 자체가 없다. 이 두 Silver DAG도
Bronze 완료 Asset 트리거로 전환되면서 ExternalTaskSensor(execution_delta)가
더 이상 상류 DagRun을 못 찾는 문제(#50)를 똑같이 겪는다. 워터마크가 없으니
check_silver_watermark.py를 그대로 쓸 수 없어서, 대신 Silver 테이블에 실제로
오늘자 스냅샷이 있는지 MAX(snapshot_date)로 직접 확인한다.

rental_history와 달리 T-1이 아니라 그날 즉시(T-0) 스냅샷을 만드는 원천이므로
오프셋 없이 오늘 날짜 그대로 비교한다.

이 스크립트도 준비 안 됐을 때 예외 없이 exit code 1로 끝난다 (BashSensor가
poke_interval마다 재시도).

사용법 (BashSensor에서):
    TARGET_DATE=2026-08-17 TABLE_NAME=silver.station_master \
        python -m jobs.check_silver_snapshot_date
    TARGET_DATE=2026-08-17 TABLE_NAME=silver.station_active \
        python -m jobs.check_silver_snapshot_date
"""
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import functions as F

import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run() -> None:
    target_date = datetime.strptime(os.environ["TARGET_DATE"], "%Y-%m-%d").date()
    table_name = f"{config.SETTINGS.iceberg_catalog_name}.{os.environ['TABLE_NAME']}"

    spark = build_spark_session("check-silver-snapshot-date")
    if spark.catalog.tableExists(table_name):
        latest = spark.read.table(table_name).agg(F.max("snapshot_date")).collect()[0][0]
    else:
        # 테이블이 아직 없는 경우(최초 실행 전)도 "아직 준비 안 됨"으로 취급.
        # 그 외 예외(진짜 버그)는 여기서 삼키지 않고 그대로 올려서 태스크가
        # 실패로 보이게 한다 - 조용히 계속 재시도하다 timeout으로 묻히지 않도록.
        latest = None

    if latest is not None and latest >= target_date:
        logger.info("%s 준비 완료 (최신 스냅샷=%s >= 대상일=%s)", table_name, latest, target_date)
        sys.exit(0)

    logger.info("%s 아직 준비 안 됨 (최신 스냅샷=%s < 대상일=%s)", table_name, latest, target_date)
    sys.exit(1)


if __name__ == "__main__":
    run()
