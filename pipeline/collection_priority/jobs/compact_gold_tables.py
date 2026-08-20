"""
Gold(TEMP) 테이블 Iceberg 컴팩션 + 스냅샷 만료 - 주간 유지보수 잡

### 왜 필요한가 - "매번 덮어쓰기"인데도 파일이 쌓이는 이유
gold.bike_location / gold.station_active / gold.fact_station_inventory /
gold.bike_last_action은 파티션 없이 매일 전체를 overwritePartitions()로 새로
쓰는 TEMP류 테이블이다. "덮어쓴다"는 건 쿼리 결과(현재 스냅샷) 기준이지 물리
파일 기준이 아니다 - Iceberg는 기존 파일을 고쳐 쓰지 않고, 매 실행마다 완전히
새 데이터 파일 + 새 스냅샷을 만든다. 어제 스냅샷이 가리키던 파일은 삭제되지
않고 그대로 남는다(타임트래블/롤백을 위해 스냅샷 이력을 계속 보관하는 게
Iceberg의 기본 동작). 그래서 매일 "오늘 것만 보이는" 전체 사본을 새로 쓰면서도,
스토리지에는 지나간 날짜 수만큼의 사본이 계속 쌓인다 - 이게 두 유지보수
프로시저가 각각 다루는, 서로 다른 두 문제다:

    1. rewrite_data_files - 지금 보이는(현재) 스냅샷 안에서 데이터가 여러 개
       작은 파일로 쪼개져 있는 걸 큰 파일로 합침(Spark 쓰기 병렬도 때문에 한
       번의 실행도 여러 파일로 나뉠 수 있음). 스캔 시 파일 오픈 오버헤드 감소.
    2. expire_snapshots - 이미 안 보이는(과거) 스냅샷 자체를 만료시켜서, 그
       스냅샷만 참조하던 파일들을 실제로 삭제 대상으로 만듦. TEMP류 테이블처럼
       매번 전체를 새로 쓰는 경우, 사실 이쪽이 스토리지 증가를 막는 핵심이다
       (rewrite_data_files만으로는 과거 스냅샷의 파일이 안 지워짐).

### 보존 정책
SNAPSHOT_RETENTION_DAYS(기본 7일)보다 오래된 스냅샷을 만료 대상으로 하되,
MIN_SNAPSHOTS_TO_RETAIN(기본 3개)은 나이와 무관하게 항상 남겨서 문제 발생 시
최근 며칠로 롤백할 여지를 남긴다.

### 왜 gold_dim_fact가 아니라 별도 DAG인가
daily 배치마다 매번 돌리면(파일/스냅샷이 하루에 1개씩만 늘어나는데) 배보다
배꼽이 커진다. 주간 1회면 충분하므로 별도 스케줄(gold_maintenance, 매주 일요일)로
분리한다 - 실패해도 다음 주에 다시 시도하면 되고, daily 배치의 SLA에 영향을 주지 않는다.

### 멱등성 / 안전성
rewrite_data_files/expire_snapshots 둘 다 데이터 내용(쿼리 결과)은 바꾸지 않고
물리 파일/스냅샷 이력만 정리하는 Iceberg 표준 유지보수 프로시저라 언제 몇 번을
다시 돌려도 안전하다. 테이블이 아직 생성 전이면(최초 배포 직후 등) 조용히 건너뛴다.

사용법:
    python -m jobs.compact_gold_tables
"""
import logging
from datetime import datetime, timedelta, timezone

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GOLD_TABLES_TO_COMPACT = [
    "bike_location",
    "station_active",
    "fact_station_inventory",
    "bike_last_action",
]

SNAPSHOT_RETENTION_DAYS = 7
MIN_SNAPSHOTS_TO_RETAIN = 3


def expire_snapshots(spark, table_short_name: str) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    table_name = f"{catalog}.gold.{table_short_name}"

    if not spark.catalog.tableExists(table_name):
        logger.info("%s: 테이블이 아직 없음 - 스냅샷 만료 건너뜀", table_name)
        return

    cutoff = datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    result = spark.sql(
        f"CALL {catalog}.system.expire_snapshots("
        f"table => '{table_name}', "
        f"older_than => TIMESTAMP '{cutoff_str}', "
        f"retain_last => {MIN_SNAPSHOTS_TO_RETAIN})"
    ).collect()[0]
    logger.info(
        "%s: 스냅샷 만료 완료 (deleted_data_files_count=%s, deleted_manifest_files_count=%s)",
        table_name, result["deleted_data_files_count"], result["deleted_manifest_files_count"],
    )


def compact_table(spark, table_short_name: str) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    table_name = f"{catalog}.gold.{table_short_name}"

    if not spark.catalog.tableExists(table_name):
        logger.info("%s: 테이블이 아직 없음 - 건너뜀", table_name)
        return

    result = spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{table_name}')").collect()[0]
    logger.info(
        "%s: 컴팩션 완료 (rewritten_data_files_count=%s, added_data_files_count=%s)",
        table_name, result["rewritten_data_files_count"], result["added_data_files_count"],
    )


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("gold-compact-tables")
    for table_short_name in GOLD_TABLES_TO_COMPACT:
        # 먼저 과거 스냅샷을 만료시켜 참조 없는 파일을 정리한 뒤, 남은(현재) 데이터를 컴팩션한다.
        expire_snapshots(spark, table_short_name)
        compact_table(spark, table_short_name)


if __name__ == "__main__":
    run()
