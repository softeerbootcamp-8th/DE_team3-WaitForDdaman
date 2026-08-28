"""
Silver - 실시간 대여정보 필터 테이블

이 원천(bikeList)을 수집하는 목적은 재고 수치(거치대 수/주차된 자전거 수/거치율)가
아니라 "오늘 실제로 운영 중인 대여소가 어디인지" 판별하는 것이다(bronze의
schema/station_active_schema.py 참고). 대여소명·위경도·자치구 등 서술 속성은
silver.station_master가 이미 갖고 있으므로, 여기서는 그 날 API 응답에 실제로
존재했던 station_id 집합만 남긴다.

운영/미운영 자체의 최종 판정(Gold의 build_station_active)은 이 테이블을 넘겨받는
담당 4가 한다. 담당 2는 그 판정 로직을 구현하지 않는다.

### Spark 제거 (#143)
계산량은 스냅샷 하루치 2.7천 행의 `dropDuplicates(["station_id"])` 하나뿐이라
Spark 세션 기동(3~4초 + JVM 메모리)이 계산보다 몇 배 비쌌다. 읽기/쓰기는 pyiceberg,
변환은 DuckDB SQL로 옮긴다. DuckDB를 쓰는 이유는 볼륨이 아니라 기존 Spark 표현을
번역 오류 없이 옮기기 위해서다(silver_station_master.py 주석 참고).
파티션 스펙(snapshot_date identity)과 컬럼 스키마는 그대로 둔다.

사용법:
    python -m jobs.silver_station_active
    SNAPSHOT_DATE=2026-08-14 python -m jobs.silver_station_active   # 특정 날짜 재처리
"""
import logging
import os
import sys

import duckdb
import pyarrow as pa
import config
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, NestedField, StringType

from common.duckdb_io import connect, query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BRONZE_TABLE = "bronze.station_active"
SILVER_TABLE = "silver.station_active"
PARTITION_COLUMN = "snapshot_date"

SILVER_COLUMNS = ["snapshot_date", "station_id"]

BRONZE_FIELDS = ("snapshot_date", "station_id")

# pyiceberg로 테이블을 새로 만들 때만 쓰는 정의. 기존 테이블의 스키마/파티션 스펙과
# 정확히 같아야 한다(변경 금지 - #143).
SILVER_SCHEMA = Schema(
    NestedField(1, "snapshot_date", DateType(), required=False, doc="스냅샷 기준일, 파티션 키"),
    NestedField(2, "station_id", StringType(), required=False, doc="대여소 ID (ST-4), station_master 조인 키"),
)
SILVER_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)
SILVER_PROPERTIES = {"write.distribution-mode": "hash"}

# dropDuplicates(["station_id"])의 1:1 번역.
# Spark는 그룹당 "아무 행 하나"를 남기므로 정렬 없는 row_number()가 같은 의미다.
# snapshot_date는 이 스냅샷 안에서 상수라 어떤 행이 뽑히든 결과가 같다.
NORMALIZE_SQL = """
    SELECT snapshot_date, station_id
    FROM (
        SELECT
            -- Spark의 cast()는 실패 시 null이므로 CAST가 아니라 TRY_CAST로 옮긴다.
            TRY_CAST(snapshot_date AS DATE) AS snapshot_date,
            station_id
        FROM {source}
        WHERE station_id IS NOT NULL
    )
    QUALIFY row_number() OVER (PARTITION BY station_id) = 1
"""


def normalize(bronze_table: pa.Table, con: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
    """
    브론즈 PyArrow Table을 실버 2컬럼(snapshot_date, station_id)으로 정제한다.
    읽기/쓰기를 하지 않는다.

    station_id가 null인 행은 드롭한다. 같은 스냅샷 내 station_id 중복은
    하나만 남긴다.
    """
    conn = con or connect()
    conn.register("bronze_station_active", bronze_table)
    result = query_arrow(conn, NORMALIZE_SQL.format(source="bronze_station_active"))
    return result.select(SILVER_COLUMNS)


def _log_quality(bronze_table: pa.Table, silver_table: pa.Table) -> None:
    """드롭/중복 건수를 남긴다. 조용히 넘기면 원천 이상을 놓친다."""
    con = connect()
    con.register("bronze_station_active", bronze_table)

    total = len(bronze_table)
    null_count = con.execute(
        "SELECT count(*) FROM bronze_station_active WHERE station_id IS NULL"
    ).fetchone()[0]
    kept = len(silver_table)
    dup_count = total - null_count - kept

    logger.info("브론즈 %d행 -> 실버 %d행", total, kept)
    if null_count:
        logger.warning("station_id null: %d건 (드롭)", null_count)
    if dup_count > 0:
        logger.warning("station_id 중복: %d건 (제거)", dup_count)


def _ensure_silver_table(catalog):
    """실버 테이블이 없으면 만든다. 이미 있으면 스키마/스펙을 건드리지 않고 그대로 쓴다."""
    catalog.create_namespace_if_not_exists("silver")
    try:
        return catalog.load_table(SILVER_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", SILVER_TABLE)
        location = f"{config.SETTINGS.iceberg_warehouse_path.rstrip('/')}/silver/station_active"
        return catalog.create_table(
            SILVER_TABLE,
            schema=SILVER_SCHEMA,
            location=location,
            partition_spec=SILVER_PARTITION_SPEC,
            properties=SILVER_PROPERTIES,
        )


def _read_bronze(catalog, snapshot_date: str | None) -> tuple[pa.Table, str]:
    """station_active는 파일 백필이 없으므로 station_master와 달리 source_file 필터가 필요 없다."""
    table = catalog.load_table(BRONZE_TABLE)

    if not snapshot_date:
        # 파티션 값은 매니페스트(메타데이터)에만 있어도 되므로 데이터 파일을 안 읽는다.
        partitions = [
            row["partition"][PARTITION_COLUMN]
            for row in table.inspect.partitions().to_pylist()
            if row["partition"][PARTITION_COLUMN]
        ]
        if not partitions:
            raise ValueError("브론즈에 station_active 스냅샷이 없습니다. 일 배치를 먼저 실행하세요.")
        snapshot_date = max(partitions)

    arrow = table.scan(
        row_filter=EqualTo(PARTITION_COLUMN, snapshot_date),
        selected_fields=BRONZE_FIELDS,
    ).to_arrow()
    return arrow, snapshot_date


def run() -> None:
    catalog = build_iceberg_catalog()
    silver_table = _ensure_silver_table(catalog)

    bronze_table, snapshot_date = _read_bronze(catalog, os.getenv("SNAPSHOT_DATE"))
    logger.info("브론즈 스냅샷 %s 처리 시작", snapshot_date)

    silver_arrow = normalize(bronze_table)
    _log_quality(bronze_table, silver_arrow)

    if len(silver_arrow) == 0:
        logger.error("%s: 정제 후 남은 행이 0건 - silver 적재 중단", snapshot_date)
        sys.exit(1)

    overwrite_partition(silver_table, silver_arrow, PARTITION_COLUMN, snapshot_date)

    logger.info("%s: 실버 적재 완료", snapshot_date)


if __name__ == "__main__":
    run()
