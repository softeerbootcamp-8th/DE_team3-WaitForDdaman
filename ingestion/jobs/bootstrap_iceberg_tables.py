"""
Iceberg 신규 테이블 Bootstrap (Issue #216)

신규 LocalStack/AWS 환경에서는 Iceberg warehouse와 JDBC 카탈로그가 완전히 비어 있어,
일 배치 잡이 `catalog.load_table()` 단계에서 "테이블 없음"으로 실패한다. 특히
`bronze.station_active`는 initial_load_*.py 같은 별도 초기 적재 경로가 없어(daily_batch만
있음) 테이블 생성이 보장되지 않는다.

이 잡과 register_tables_in_jdbc_catalog.py의 역할 분리:
    - register_tables_in_jdbc_catalog.py: 이미 Hadoop Catalog(S3 warehouse)에 존재하는
      테이블의 최신 metadata.json 위치를 JDBC 카탈로그에 포인터로만 등록한다
      (1회성 마이그레이션, 기존 데이터/메타데이터 그대로 재사용).
    - 이 잡(bootstrap_iceberg_tables.py): 아직 어디에도 존재하지 않는 테이블을
      JDBC 카탈로그 + S3 warehouse에 새로 만든다 (신규 환경 초기화용).

두 잡은 서로 겹치지 않는다 - register는 "이미 있는 걸 등록"하고, 이 잡은
load_table()로 존재을 먼저 확인한 뒤 없을 때만 create_table()을 호출하므로, 이미
등록/생성된 테이블은 절대 다시 만들지 않는다(스키마도, 데이터도 건드리지 않음).
그래서 실행 순서와 무관하게 몇 번을 재실행해도 안전하다(멱등).

권장 실행 순서(Issue #216):
    인프라 배포
      -> (기존 Hadoop metadata가 있는 환경) register_tables_in_jdbc_catalog
      -> bootstrap_iceberg_tables (이 잡)
      -> Initial Load
      -> Daily Batch

대상 - Bronze 5개 원천 + bikeman_event 격리 테이블:
    bronze.rental_history, bronze.failure_report, bronze.station_master,
    bronze.station_active, bronze.bikeman_event, bronze.bikeman_event_quarantine

스키마/파티션 스펙은 각 잡이 이미 쓰고 있는 정의를 그대로 옮긴 것이다 - 새로 정하지
않았다:
    - rental_history: jobs/initial_load_rental_history.py의 CREATE TABLE DDL
    - failure_report: jobs/initial_load_failure_report.py의 CREATE TABLE DDL
    - station_master: jobs/daily_batch_station_master.py의 ARROW_SCHEMA
    - station_active: jobs/daily_batch_station_active.py의 ARROW_SCHEMA
    - bikeman_event(+quarantine): jobs/daily_batch_bikeman_event.py의 ARROW_SCHEMA

사용법:
    python -m jobs.bootstrap_iceberg_tables
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField, StringType, TimestamptzType

import config
from common.iceberg_catalog import build_iceberg_catalog
from common.s3_utils import ensure_bucket

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# rental_history/failure_report의 기존 Spark DDL과 동일한 이유(#139) - Iceberg가
# 파티션별로 파일을 여러 개 동시에 여는 FanoutWriter 대신 직접 분산+정렬하게 해서
# 메모리 사용을 줄인다. 파티션이 있는 테이블에만 적용한다.
HASH_DISTRIBUTION_PROPERTIES = {"write.distribution-mode": "hash"}


def _identity_partition(source_id: int, name: str) -> PartitionSpec:
    return PartitionSpec(
        PartitionField(source_id=source_id, field_id=1000, transform=IdentityTransform(), name=name)
    )


@dataclass(frozen=True)
class BronzeTableSpec:
    identifier: str  # "bronze.<table>" - 네임스페이스 포함, 카탈로그 이름은 제외
    schema: Schema
    partition_spec: PartitionSpec | None = None
    properties: dict | None = None


RENTAL_HISTORY_SCHEMA = Schema(
    NestedField(1, "bike_id", StringType(), required=False),
    NestedField(2, "rent_dt", StringType(), required=False),
    NestedField(3, "rent_station_no", StringType(), required=False),
    NestedField(4, "rent_station_name", StringType(), required=False),
    NestedField(5, "rent_hold", StringType(), required=False),
    NestedField(6, "return_dt", StringType(), required=False),
    NestedField(7, "return_station_no", StringType(), required=False),
    NestedField(8, "return_station_name", StringType(), required=False),
    NestedField(9, "return_hold", StringType(), required=False),
    NestedField(10, "use_min", StringType(), required=False),
    NestedField(11, "use_distance_m", StringType(), required=False),
    NestedField(12, "user_class_cd", StringType(), required=False),
    NestedField(13, "sex_cd", StringType(), required=False),
    NestedField(14, "birth_year", StringType(), required=False),
    NestedField(15, "rent_station_id", StringType(), required=False),
    NestedField(16, "return_station_id", StringType(), required=False),
    NestedField(17, "bike_se_cd", StringType(), required=False),
    NestedField(18, "rent_date_partition", StringType(), required=False),
    NestedField(19, "source_file", StringType(), required=False),
    NestedField(20, "ingested_at", TimestamptzType(), required=False),
)

FAILURE_REPORT_SCHEMA = Schema(
    NestedField(1, "bike_no", StringType(), required=False),
    NestedField(2, "reg_dttm", StringType(), required=False),
    NestedField(3, "failure_type", StringType(), required=False),
    NestedField(4, "reg_date_partition", StringType(), required=False),
    NestedField(5, "source_file", StringType(), required=False),
    NestedField(6, "ingested_at", TimestamptzType(), required=False),
)

STATION_MASTER_SCHEMA = Schema(
    NestedField(1, "station_no", StringType(), required=False),
    NestedField(2, "station_id", StringType(), required=False),
    NestedField(3, "station_name", StringType(), required=False),
    NestedField(4, "station_id_name", StringType(), required=False),
    NestedField(5, "district", StringType(), required=False),
    NestedField(6, "hold_num", StringType(), required=False),
    NestedField(7, "address1", StringType(), required=False),
    NestedField(8, "address2", StringType(), required=False),
    NestedField(9, "latitude", StringType(), required=False),
    NestedField(10, "longitude", StringType(), required=False),
    NestedField(11, "snapshot_date", StringType(), required=False),
    NestedField(12, "source_file", StringType(), required=False),
    NestedField(13, "ingested_at", TimestamptzType(), required=False),
)

STATION_ACTIVE_SCHEMA = Schema(
    NestedField(1, "station_id", StringType(), required=False),
    NestedField(2, "station_name", StringType(), required=False),
    NestedField(3, "rack_tot_cnt", StringType(), required=False),
    NestedField(4, "parking_bike_tot_cnt", StringType(), required=False),
    NestedField(5, "shared", StringType(), required=False),
    NestedField(6, "latitude", StringType(), required=False),
    NestedField(7, "longitude", StringType(), required=False),
    NestedField(8, "snapshot_date", StringType(), required=False),
    NestedField(9, "source_file", StringType(), required=False),
    NestedField(10, "ingested_at", TimestamptzType(), required=False),
)

BIKEMAN_EVENT_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=False),
    NestedField(2, "event_type", StringType(), required=False),
    NestedField(3, "bike_id", StringType(), required=False),
    NestedField(4, "station_id", StringType(), required=False),
    NestedField(5, "worker_id", StringType(), required=False),
    NestedField(6, "occurred_at", TimestamptzType(), required=False),
    NestedField(7, "received_at", TimestamptzType(), required=False),
    NestedField(8, "occurred_date_partition", StringType(), required=False),
    NestedField(9, "source_file", StringType(), required=False),
    NestedField(10, "ingested_at", TimestamptzType(), required=False),
)

# bronze.bikeman_event와 동일한 컬럼 - daily_batch_bikeman_event.py가 허용되지 않는
# event_type을 append()로만 격리한다(파티션 필터 없이 전체 append). 그래서 이 테이블은
# 파티션을 두지 않는다 - silver.bikeman_action_quarantine과 동일한 이유(#216 조사 시 확인).
BIKEMAN_EVENT_QUARANTINE_SCHEMA = BIKEMAN_EVENT_SCHEMA

BRONZE_TABLE_SPECS: list[BronzeTableSpec] = [
    BronzeTableSpec(
        identifier="bronze.rental_history",
        schema=RENTAL_HISTORY_SCHEMA,
        partition_spec=_identity_partition(18, "rent_date_partition"),
        properties=HASH_DISTRIBUTION_PROPERTIES,
    ),
    BronzeTableSpec(
        identifier="bronze.failure_report",
        schema=FAILURE_REPORT_SCHEMA,
        partition_spec=_identity_partition(4, "reg_date_partition"),
        properties=HASH_DISTRIBUTION_PROPERTIES,
    ),
    BronzeTableSpec(
        identifier="bronze.station_master",
        schema=STATION_MASTER_SCHEMA,
        partition_spec=_identity_partition(11, "snapshot_date"),
        properties=HASH_DISTRIBUTION_PROPERTIES,
    ),
    BronzeTableSpec(
        identifier="bronze.station_active",
        schema=STATION_ACTIVE_SCHEMA,
        partition_spec=_identity_partition(8, "snapshot_date"),
        properties=HASH_DISTRIBUTION_PROPERTIES,
    ),
    BronzeTableSpec(
        identifier="bronze.bikeman_event",
        schema=BIKEMAN_EVENT_SCHEMA,
        partition_spec=_identity_partition(8, "occurred_date_partition"),
        properties=HASH_DISTRIBUTION_PROPERTIES,
    ),
    BronzeTableSpec(
        identifier="bronze.bikeman_event_quarantine",
        schema=BIKEMAN_EVENT_QUARANTINE_SCHEMA,
    ),
]


def _bootstrap_table(catalog: Catalog, spec: BronzeTableSpec) -> bool:
    """
    spec.identifier 테이블이 없으면 만든다. 이미 있으면 절대 건드리지 않고
    (스키마/속성 변경 없음, 데이터 없음) 그대로 스킵한다.

    Returns:
        True면 이번 호출에서 새로 생성, False면 이미 존재해서 스킵.
    """
    try:
        catalog.load_table(spec.identifier)
        return False
    except NoSuchTableError:
        pass

    catalog.create_table(
        spec.identifier,
        schema=spec.schema,
        partition_spec=spec.partition_spec or PartitionSpec(),
        properties=spec.properties or {},
    )
    return True


def run() -> None:
    settings = config.SETTINGS
    ensure_bucket(settings.warehouse_bucket)

    catalog = build_iceberg_catalog()
    catalog.create_namespace_if_not_exists("bronze")

    created, skipped = [], []
    for spec in BRONZE_TABLE_SPECS:
        if _bootstrap_table(catalog, spec):
            created.append(spec.identifier)
            logger.info("신규 생성: %s", spec.identifier)
        else:
            skipped.append(spec.identifier)
            logger.info("이미 존재 - 스킵: %s", spec.identifier)

    logger.info(
        "Bootstrap 종료 - 생성 %d개(%s), 스킵 %d개(%s)",
        len(created), created, len(skipped), skipped,
    )


if __name__ == "__main__":
    run()
