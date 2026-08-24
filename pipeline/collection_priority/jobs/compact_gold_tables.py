"""
Iceberg 테이블 유지보수(컴팩션 + 스냅샷 만료 + 고아 파일 정리) - 주간 유지보수 잡

### 왜 필요한가 - "매번 덮어쓰기"인데도 파일이 쌓이는 이유
gold.bike_location / gold.station_active / gold.fact_station_inventory /
gold.bike_last_action은 파티션 없이 매일 전체를 overwritePartitions()로 새로
쓰는 TEMP류 테이블이다. "덮어쓴다"는 건 쿼리 결과(현재 스냅샷) 기준이지 물리
파일 기준이 아니다 - Iceberg는 기존 파일을 고쳐 쓰지 않고, 매 실행마다 완전히
새 데이터 파일 + 새 스냅샷을 만든다. 어제 스냅샷이 가리키던 파일은 삭제되지
않고 그대로 남는다(타임트래블/롤백을 위해 스냅샷 이력을 계속 보관하는 게
Iceberg의 기본 동작). 그래서 매일 "오늘 것만 보이는" 전체 사본을 새로 쓰면서도,
스토리지에는 지나간 날짜 수만큼의 사본이 계속 쌓인다.

bronze.rental_history / silver.rental_history는 이유가 다르다 - 초기 적재가
파일 하나(최대 700MB)를 여러 CSV/파티션으로 쪼개 쓰면서 작은 파일을 대량
생성한다(#173). 이쪽은 TEMP가 아니라 누적(append) 테이블이지만, 세 프로시저
모두 동일하게 적용한다 - expire_snapshots 없이 rewrite_data_files만 하면
컴팩션 이전의 작은 파일들을 예전 스냅샷이 계속 붙잡고 있어 스토리지가 실제로는
안 줄어든다(조회 성능만 개선). 누적 테이블이라 "현재 조회되는 행"은 스냅샷
만료와 무관하게 그대로 남고, 잃는 건 SNAPSHOT_RETENTION_DAYS(7일)보다 이전
시점으로 타임트래블/롤백하는 능력뿐이다.

이 세 문제를 서로 다른 유지보수 프로시저가 각각 다룬다:

    1. rewrite_data_files - 지금 보이는(현재) 스냅샷 안에서 데이터가 여러 개
       작은 파일로 쪼개져 있는 걸 큰 파일로 합침(Spark 쓰기 병렬도 때문에 한
       번의 실행도 여러 파일로 나뉠 수 있음, 초기 적재의 소스 파일 분할도 동일).
       스캔 시 파일 오픈 오버헤드 감소.
    2. expire_snapshots - 이미 안 보이는(과거) 스냅샷 자체를 만료시켜서, 그
       스냅샷만 참조하던 파일들을 실제로 삭제 대상으로 만듦. TEMP류 테이블처럼
       매번 전체를 새로 쓰는 경우, 사실 이쪽이 스토리지 증가를 막는 핵심이다
       (rewrite_data_files만으로는 과거 스냅샷의 파일이 안 지워짐).
    3. remove_orphan_files - 위 둘과 달리 스냅샷 이력에 아예 없던 파일을 다룬다.
       pyiceberg 커밋은 "데이터 파일 쓰기 -> 카탈로그 커밋" 2단계라, 앞 단계
       이후 뒤 단계가 실패하면(네트워크 끊김 등) 어떤 스냅샷에도 참조되지 않는
       파일이 S3에 그대로 남는다(#173) - Spark writeTo().overwritePartitions()
       시절엔 이런 중간 상태가 드물었지만, Bronze/Silver/일부 Gold가 pyiceberg로
       전환되며 이론상 가능해졌다. 파일 목록을 실제로 스캔해서 메타데이터
       어디서도 참조 안 되는 파일만 골라 지운다 - 기본적으로 3일 이내 생성된
       파일은 건드리지 않아(진행 중인 커밋과 혼동 방지) 안전하다.

### 보존 정책
SNAPSHOT_RETENTION_DAYS(기본 7일)보다 오래된 스냅샷을 만료 대상으로 하되,
MIN_SNAPSHOTS_TO_RETAIN(기본 3개)은 나이와 무관하게 항상 남겨서 문제 발생 시
최근 며칠로 롤백할 여지를 남긴다. TABLES_TO_COMPACT의 테이블 전부 예외 없이
이 정책을 쓴다 - bronze/silver.rental_history도 동일(위 참고).
remove_orphan_files는 Iceberg 기본값(3일 이내 생성 파일 보호)을 그대로 쓴다 -
별도로 완화할 이유가 없다.

### #205 - 컴팩션 대상 최종 확정 (TEMP류/초기적재 이외 테이블도 포함)
처음엔 "TEMP 전체 덮어쓰기"와 "초기 적재 스몰파일" 두 케이스만 컴팩션이
필요하다고 봤지만, 세 프로시저가 다루는 문제를 다시 보면 write 패턴과
무관하게 거의 모든 활성 테이블에 적용된다:

    - expire_snapshots: 커밋 방식(overwrite_partition이든 append든)과
      무관하게 커밋마다 새 스냅샷 + 매니페스트가 쌓인다. 데이터가 안
      겹쳐도 메타데이터는 계속 늘어나고, 같은 파티션을 재실행/백필하면
      그 파티션의 이전 버전 파일이 orphan처럼 남는데 이것도 expire_snapshots
      로만 정리된다.
    - rewrite_data_files: 초기 대량적재의 스몰파일뿐 아니라, 매일 소량만
      append하는 테이블(quarantine, dq_check_result 등)은 하루에 파일
      하나씩 쌓이는 것 자체가 스몰파일 누적이다. 정상 볼륨 테이블도 쓰기
      병렬도 때문에 파티션당 여러 파일로 쪼개질 수 있다.
    - remove_orphan_files: write 패턴과 무관하게 pyiceberg 커밋 중간 실패
      가능성은 동일하므로 전 테이블에 안전하게 적용 가능.

그래서 포함 여부를 가르는 실질적 기준은 write 패턴의 종류가 아니라
"지금도 정기적으로 쓰기가 일어나는가"다. 활성 파이프라인 테이블은 전부
포함하고, 더 이상 쓰기가 없는 마이그레이션 산출물만 명시적으로 제외한다
(EXCLUDED_TABLES 참고).

### 왜 gold_dim_fact가 아니라 별도 DAG인가
daily 배치마다 매번 돌리면(파일/스냅샷이 하루에 1개씩만 늘어나는데) 배보다
배꼽이 커진다. 주간 1회면 충분하므로 별도 스케줄(gold_maintenance, 매주 일요일)로
분리한다 - 실패해도 다음 주에 다시 시도하면 되고, daily 배치의 SLA에 영향을 주지 않는다.

### 멱등성 / 안전성
세 프로시저 모두 데이터 내용(쿼리 결과)은 바꾸지 않고 물리 파일/스냅샷 이력만
정리하는 Iceberg 표준 유지보수 프로시저라 언제 몇 번을 다시 돌려도 안전하다.
테이블이 아직 생성 전이면(최초 배포 직후 등) 조용히 건너뛴다.

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

# "namespace.table" 전체 식별자로 적는다 - gold 밖의 테이블이 섞여 있어 더
# 이상 namespace를 고정할 수 없다(#173). #205로 활성 파이프라인 테이블 전체로
# 대상을 확장 - 카테고리는 위 "#205" 절 참고.
TABLES_TO_COMPACT = [
    # TEMP류 - 파티션 없이 매일 전체를 새로 쓰는 테이블 (스냅샷 만료가 핵심)
    "gold.bike_location",
    "gold.station_active",
    "gold.fact_station_inventory",
    "gold.bike_last_action",

    # 초기 적재 스몰파일 - 누적(append) 테이블이지만 초기 적재가 작은 파일을
    # 대량 생성 (#173)
    "bronze.rental_history",
    "silver.rental_history",

    # #205 - 전체 덮어쓰기 (silver_failure_report.py: overwrite_all(), 매번
    # 브론즈 전체 재처리)
    "silver.failure_report",

    # #205 - 매일 파티션 덮어쓰기 (overwrite_partition, 오늘 파티션만 갱신하지만
    # 커밋마다 스냅샷/매니페스트가 쌓이고 파티션당 여러 파일로 쪼개질 수 있음)
    "silver.station_master",
    "silver.station_active",
    "gold.dim_bike",
    "gold.mart_bike_risk_daily",
    "gold.mart_station_daily",

    # #205 - #171(PR #190)로 Spark에서 DuckDB+pyiceberg로 전환된 gold 테이블
    "gold.bike_features_daily",
    "gold.fact_bike_risk",
    "gold.fact_bike_decision",

    # #205 - Append-only (정규 파이프라인 테이블 + 저볼륨 append로 스몰파일이
    # 쌓이는 quarantine/dq_check_result)
    "bronze.station_master",
    "bronze.station_active",
    "bronze.failure_report",
    "bronze.bikeman_event",
    "bronze.bikeman_event_quarantine",
    "silver.bikeman_action_quarantine",
    "silver.dq_check_result",
]

# #205 - 제외 확정: 둘 다 PR #166 T4(파티션 컬럼명 변경) 마이그레이션의
# 일회성 산출물이고, 정상 운영 중에는 다시 쓰이지 않는다
# (ingestion/jobs/silver_bikeman_action.py 참고).
#   - silver.bikeman_action_hidden_partition_backup: 구 hidden-partition
#     테이블을 rename으로 보존한 백업, 의도적으로 유지
#   - silver.bikeman_action_identity_rebuild: identity 파티션 재구축용
#     스크래치 테이블, 재구축 완료 후 idle 상태
EXCLUDED_TABLES = [
    "silver.bikeman_action_hidden_partition_backup",
    "silver.bikeman_action_identity_rebuild",
]

SNAPSHOT_RETENTION_DAYS = 7
MIN_SNAPSHOTS_TO_RETAIN = 3


def expire_snapshots(spark, table_name: str) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    full_table_name = f"{catalog}.{table_name}"

    if not spark.catalog.tableExists(full_table_name):
        logger.info("%s: 테이블이 아직 없음 - 스냅샷 만료 건너뜀", full_table_name)
        return

    cutoff = datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    result = spark.sql(
        f"CALL {catalog}.system.expire_snapshots("
        f"table => '{full_table_name}', "
        f"older_than => TIMESTAMP '{cutoff_str}', "
        f"retain_last => {MIN_SNAPSHOTS_TO_RETAIN})"
    ).collect()[0]
    logger.info(
        "%s: 스냅샷 만료 완료 (deleted_data_files_count=%s, deleted_manifest_files_count=%s)",
        full_table_name, result["deleted_data_files_count"], result["deleted_manifest_files_count"],
    )


def compact_table(spark, table_name: str) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    full_table_name = f"{catalog}.{table_name}"

    if not spark.catalog.tableExists(full_table_name):
        logger.info("%s: 테이블이 아직 없음 - 건너뜀", full_table_name)
        return

    result = spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{full_table_name}')").collect()[0]
    logger.info(
        "%s: 컴팩션 완료 (rewritten_data_files_count=%s, added_data_files_count=%s)",
        full_table_name, result["rewritten_data_files_count"], result["added_data_files_count"],
    )


def remove_orphan_files(spark, table_name: str) -> None:
    """메타데이터 어디에서도 참조되지 않는 파일(pyiceberg 커밋 중간 실패의 잔여물
    등)을 지운다. older_than을 안 넘기면 Iceberg 기본값(3일 이내 생성 파일 보호)이
    그대로 적용된다 - 진행 중인 커밋과 혼동하지 않기 위한 안전장치라 완화하지 않는다.
    결과는 지워진 파일마다 한 행(orphan_file_location)이라 카운트만 로그로 남긴다."""
    catalog = config.SETTINGS.iceberg_catalog_name
    full_table_name = f"{catalog}.{table_name}"

    if not spark.catalog.tableExists(full_table_name):
        logger.info("%s: 테이블이 아직 없음 - 고아 파일 정리 건너뜀", full_table_name)
        return

    result = spark.sql(
        f"CALL {catalog}.system.remove_orphan_files(table => '{full_table_name}')"
    ).collect()
    logger.info("%s: 고아 파일 정리 완료 (삭제 %d건)", full_table_name, len(result))


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("gold-compact-tables")
    for table_name in TABLES_TO_COMPACT:
        # 순서: 과거 스냅샷을 먼저 만료시켜 참조 없는 파일을 정리 -> 남은(현재)
        # 데이터를 컴팩션 -> 그래도 스냅샷 이력에 없던 고아 파일을 마지막에 정리.
        expire_snapshots(spark, table_name)
        compact_table(spark, table_name)
        remove_orphan_files(spark, table_name)


if __name__ == "__main__":
    run()
