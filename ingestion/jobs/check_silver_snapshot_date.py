"""
Gold DAG(gold_dim_fact)의 wait_for_silver 단계 - 스냅샷 기반 Silver 소스
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

### Spark 대신 boto3로 파티션 디렉터리만 나열 (2026-08-22, #145)
이전 버전은 이 판정을 위해 매번 Spark 세션을 새로 띄웠다(poke_interval=300s/
timeout=6h 기준 센서당 최대 72회). station_master/station_active는
`PARTITIONED BY (snapshot_date)` identity 파티션 Iceberg 테이블(hadoop
카탈로그)이라 실제 데이터가 `{ICEBERG_WAREHOUSE_PATH}/{namespace}/{table}/
data/snapshot_date=YYYY-MM-DD/` Hive 스타일 디렉터리에 쌓인다. Spark로 테이블을
읽는 대신 이 디렉터리 목록만 boto3로 나열해 MAX(snapshot_date)를 구하면 결과는
동일하면서 Spark 세션을 전혀 띄우지 않는다. 이 파이프라인에 파티션을 지우는
잡이 없어 "가장 늦은 파티션 디렉터리 = 최신 스냅샷"이라는 전제에 오탐이 없다.

이 스크립트도 준비 안 됐을 때 예외 없이 exit code 1로 끝난다 (PythonSensor가
poke_interval마다 재시도).

사용법 (PythonSensor에서):
    is_ready("silver.station_master", "2026-08-17")

CLI로도 그대로 쓸 수 있다:
    TARGET_DATE=2026-08-17 TABLE_NAME=silver.station_master \
        python -m jobs.check_silver_snapshot_date
"""
import logging
import os
import sys
from datetime import date, datetime
from urllib.parse import urlparse

from botocore.exceptions import ClientError

import config
from common.s3_utils import get_s3_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _table_data_prefix(namespace: str, table: str) -> tuple[str, str]:
    """Iceberg Hadoop 카탈로그가 identity 파티션 테이블에 실제로 쓰는 S3 (bucket, prefix)."""
    parsed = urlparse(config.SETTINGS.iceberg_warehouse_path)
    root = parsed.path.lstrip("/")
    prefix = f"{root}/{namespace}/{table}/data/" if root else f"{namespace}/{table}/data/"
    return parsed.netloc, prefix


def get_max_snapshot_date(namespace: str, table: str) -> date | None:
    """스냅샷 파티션 디렉터리 목록만 나열해 MAX(snapshot_date)를 구한다 (Spark 세션 미사용)."""
    bucket, prefix = _table_data_prefix(namespace, table)
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    latest = None
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
        for page in pages:
            for common_prefix in page.get("CommonPrefixes", []):
                dir_name = common_prefix["Prefix"][len(prefix):].rstrip("/")
                if not dir_name.startswith("snapshot_date="):
                    continue
                partition_date = datetime.strptime(dir_name.split("=", 1)[1], "%Y-%m-%d").date()
                if latest is None or partition_date > latest:
                    latest = partition_date
    except ClientError as e:
        # warehouse 버킷 자체가 아직 없는 완전 초기 상태(첫 Silver ETL 이전)도
        # "테이블 없음"과 같은 "아직 준비 안 됨"으로 취급한다. 그 외 에러(권한 등
        # 진짜 설정 문제)는 삼키지 않고 그대로 올려서 태스크가 실패로 보이게 한다.
        if e.response.get("Error", {}).get("Code") != "NoSuchBucket":
            raise
    return latest


def is_ready(table_name: str, target_date: str) -> bool:
    """table_name(예: 'silver.station_master')에 target_date 스냅샷이 도착했는지 확인한다."""
    namespace, table = table_name.split(".", 1)
    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    latest = get_max_snapshot_date(namespace, table)

    if latest is not None and latest >= target:
        logger.info("%s 준비 완료 (최신 스냅샷=%s >= 대상일=%s)", table_name, latest, target)
        return True

    logger.info("%s 아직 준비 안 됨 (최신 스냅샷=%s < 대상일=%s)", table_name, latest, target)
    return False


def run() -> None:
    ready = is_ready(os.environ["TABLE_NAME"], os.environ["TARGET_DATE"])
    sys.exit(0 if ready else 1)


if __name__ == "__main__":
    run()
