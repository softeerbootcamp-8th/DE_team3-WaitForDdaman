"""
Silver - 대여소 마스터 정제

브론즈는 원본 보존 원칙에 따라 전부 STRING이고 값 정제를 하지 않는다.
실버는 세 가지만 한다.

    1. 타입 캐스팅   위경도 -> double, 거치대수 -> int, 스냅샷일 -> date
    2. region 파생   강남/강북. 어떤 원천에도 없는 값이라 자치구 상수 매핑으로 만든다
    3. 문자열 정규화 대여소명 공백/개행 정리

골드·프론트·백엔드에서 쓰지 않는 컬럼(station_no, station_id_name, address1/2)은
떨어뜨린다. 브론즈에 그대로 남아 있으므로 필요해지면 다시 꺼낼 수 있다.

SCD Type 2 이력화는 이번 범위가 아니다. 브론즈가 일일 스냅샷을 계속 보존하므로
실버가 이력을 만들지 않아도 정보 손실이 없고, 필요해지면 소급 생성할 수 있다.
판단 근거는 docs/superpowers/specs/2026-08-14-silver-station-master-design.md 참고.

### Spark 제거 (#143)
계산량은 스냅샷 하루치 3.2천 행의 `.distinct()` 수준이라 어떤 분산 엔진도 필요 없다.
Spark 세션 기동(3~4초 + JVM 메모리)만 순수 오버헤드였다. 읽기/쓰기는 pyiceberg,
변환은 DuckDB SQL, 품질 검증은 common/sql_assert.py로 옮긴다.
- DuckDB를 쓰는 이유: 이 볼륨에 엔진이 필요해서가 아니라, 기존 Spark SQL 표현
  (cast / regexp_replace / trim)을 번역 오류 없이 1:1로 옮기고 sql_assert와
  같은 SQL 언어를 쓰기 위해서다. pyarrow compute로 흩어 쓰면 표현이 갈라진다.
- 캐스팅은 전부 TRY_CAST로 쓴다. Spark의 `cast()`는 실패 시 null을 주는데
  DuckDB의 `CAST`는 예외를 던지므로, 그대로 옮기면 원천 이상값 하나에
  배치가 죽는다(기존 동작은 null + 경고 로그).
- 파티션 스펙(snapshot_date identity)과 컬럼 스키마는 그대로 둔다.

사용법:
    python -m jobs.silver_station_master
    SNAPSHOT_DATE=2026-08-14 python -m jobs.silver_station_master   # 특정 날짜 재처리
"""
import logging
import os
import sys

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, StartsWith
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, DoubleType, IntegerType, NestedField, StringType

from common.duckdb_io import query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition
from common.sql_assert import QualityCheck

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BRONZE_TABLE = "bronze.station_master"
SILVER_TABLE = "silver.station_master"
PARTITION_COLUMN = "snapshot_date"

# 한강 남/북 기준. 서울 자치구는 25개로 고정이다.
# 좌표 기반 판정(한강 폴리곤)은 경계 대여소를 오분류할 위험이 있어 쓰지 않는다.
REGION_BY_DISTRICT = {
    # 강남 11개구
    "강서구": "강남", "양천구": "강남", "구로구": "강남", "금천구": "강남",
    "영등포구": "강남", "동작구": "강남", "관악구": "강남", "서초구": "강남",
    "강남구": "강남", "송파구": "강남", "강동구": "강남",
    # 강북 14개구
    "종로구": "강북", "중구": "강북", "용산구": "강북", "성동구": "강북",
    "광진구": "강북", "동대문구": "강북", "중랑구": "강북", "성북구": "강북",
    "강북구": "강북", "도봉구": "강북", "노원구": "강북", "은평구": "강북",
    "서대문구": "강북", "마포구": "강북",
}

SILVER_COLUMNS = [
    "snapshot_date",
    "station_id",
    "station_name",
    "district",
    "region",
    "latitude",
    "longitude",
    "hold_num",
]

# 브론즈에서 실제로 읽어오는 컬럼만 스캔한다 - 나머지 6개는 실버가 안 쓴다.
BRONZE_FIELDS = (
    "snapshot_date", "station_id", "station_name", "district",
    "latitude", "longitude", "hold_num", "source_file",
)

# pyiceberg로 테이블을 새로 만들 때만 쓰는 정의. 기존 테이블의 스키마/파티션 스펙과
# 정확히 같아야 한다(컬럼 스키마와 snapshot_date identity 파티션은 변경 금지 - #143).
SILVER_SCHEMA = Schema(
    NestedField(1, "snapshot_date", DateType(), required=False, doc="스냅샷 기준일, 파티션 키"),
    NestedField(2, "station_id", StringType(), required=False, doc="대여소 ID (ST-10), 골드 조인 키"),
    NestedField(3, "station_name", StringType(), required=False, doc="대여소명, 공백 정규화됨"),
    NestedField(4, "district", StringType(), required=False, doc="자치구"),
    NestedField(5, "region", StringType(), required=False, doc="강남 / 강북 (자치구에서 파생)"),
    NestedField(6, "latitude", DoubleType(), required=False, doc="위도"),
    NestedField(7, "longitude", DoubleType(), required=False, doc="경도"),
    NestedField(8, "hold_num", IntegerType(), required=False, doc="거치대 수 (null 가능)"),
)
SILVER_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)
# Spark로 이 테이블에 쓰는 잡이 남아 있을 때를 대비해 sibling 테이블과 같은 값을 유지한다
# (pyiceberg 쓰기 경로에서는 무시되는 속성이다).
SILVER_PROPERTIES = {"write.distribution-mode": "hash"}


class UnknownDistrictError(Exception):
    """REGION_BY_DISTRICT에 없는 자치구 - 원천 이상이므로 즉시 실패시킨다."""


def _region_case_sql(column: str = "district") -> str:
    """
    REGION_BY_DISTRICT를 CASE 식으로 펼친다.

    DuckDB의 MAP 리터럴 인덱싱은 버전에 따라 값이 아니라 리스트를 돌려주는 등
    반환 형태가 흔들려서, 상수 25개짜리 CASE로 고정한다 (조인이 아니라 식이므로
    행 순서에도 영향이 없다).
    """
    whens = " ".join(
        f"WHEN '{district}' THEN '{region}'" for district, region in REGION_BY_DISTRICT.items()
    )
    return f"CASE {column} {whens} END"


def _normalize_sql(source: str) -> str:
    return f"""
        SELECT
            -- Spark의 cast()는 실패 시 null이므로 CAST가 아니라 TRY_CAST로 옮긴다.
            TRY_CAST(snapshot_date AS DATE)                          AS snapshot_date,
            station_id,
            -- 연속 공백·개행을 공백 하나로 모은 뒤 앞뒤를 자른다
            trim(regexp_replace(station_name, '\\s+', ' ', 'g'))     AS station_name,
            district,
            {_region_case_sql()}                                     AS region,
            TRY_CAST(latitude AS DOUBLE)                             AS latitude,
            TRY_CAST(longitude AS DOUBLE)                            AS longitude,
            TRY_CAST(hold_num AS INTEGER)                            AS hold_num
        FROM {source}
    """


def normalize(bronze_table: pa.Table, con: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
    """
    브론즈 PyArrow Table을 실버 8컬럼으로 정제한다. 읽기/쓰기를 하지 않는다.

    자치구가 매핑에 없으면 UnknownDistrictError를 던진다. 조용히 null로 두면
    프론트의 지역 필터에서 그 대여소가 사라지는데 아무도 알아차리지 못한다.
    """
    conn = con or duckdb.connect(":memory:")
    conn.register("bronze_station_master", bronze_table)
    result = query_arrow(conn, _normalize_sql("bronze_station_master"))

    conn.register("silver_station_master", result)
    unmapped = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT district FROM silver_station_master "
            "WHERE district IS NOT NULL AND region IS NULL"
        ).fetchall()
    ]
    if unmapped:
        raise UnknownDistrictError(
            f"REGION_BY_DISTRICT에 없는 자치구: {sorted(unmapped)} "
            "(원천 스키마 변경 또는 값 오류 점검 필요)"
        )

    return result.select(SILVER_COLUMNS)


def _log_quality(silver_table: pa.Table) -> None:
    """
    캐스팅 실패·이상값 건수를 남긴다. 조용히 넘기면 원천 변경을 놓친다.

    sql_assert의 제약을 그대로 쓰되 raise_if_failed()는 부르지 않는다 - 이 항목들은
    기존에도 "경고만 남기고 통과"였고(위경도 0인 대여소가 실제로 3곳 있다), 여기서
    배치를 죽이면 원천의 알려진 결함으로 매일 실패한다.
    """
    result = (
        QualityCheck("silver_station_master")
        .is_complete("latitude")
        .is_complete("longitude")
        .is_complete("hold_num")
        # NULL 좌표가 섞여도 "0인 쪽이 하나라도 있으면 이상"으로 세도록 OR 형태를 유지한다
        # (`latitude <> 0 AND longitude <> 0`으로 쓰면 NULL 때문에 안 세어진다).
        .satisfies("NOT (latitude = 0 OR longitude = 0)", "위경도가 0")
        .run(silver_table)
    )

    logger.info("실버 %d행", result.total_rows)
    labels = {
        "isComplete(latitude)": "위도 캐스팅 실패/결측",
        "isComplete(longitude)": "경도 캐스팅 실패/결측",
        "isComplete(hold_num)": "거치대수 없음",
    }
    for constraint in result.failed_constraints:
        label = labels.get(constraint.name, constraint.description)
        logger.warning("%s: %d건", label, constraint.violation_count)


def _ensure_silver_table(catalog):
    """실버 테이블이 없으면 만든다. 이미 있으면 스키마/스펙을 건드리지 않고 그대로 쓴다."""
    catalog.create_namespace_if_not_exists("silver")
    try:
        return catalog.load_table(SILVER_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", SILVER_TABLE)
        return catalog.create_table(
            SILVER_TABLE,
            schema=SILVER_SCHEMA,
            partition_spec=SILVER_PARTITION_SPEC,
            properties=SILVER_PROPERTIES,
        )


def _read_bronze(catalog, snapshot_date: str | None) -> tuple[pa.Table, str]:
    """
    API 파티션만 읽는다. 원천 조사 과정에서 적재한 반기 파일 파티션이
    브론즈에 남아 있고, 그 행들은 hold_num이 전부 null이다.

    최신 스냅샷 판정도 "api: 행이 있는 파티션 중 최대"여야 한다 - 파티션 디렉터리
    목록(partition_listing)만 보면 파일 백필 파티션까지 후보로 잡히기 때문에,
    매니페스트의 파티션 값을 큰 것부터 훑으며 api: 행이 실제로 있는 첫 파티션을 쓴다.
    """
    table = catalog.load_table(BRONZE_TABLE)

    def scan(value: str) -> pa.Table:
        # snapshot_date는 identity 파티션이라 EqualTo가 그대로 파티션 프루닝에 쓰인다.
        return table.scan(
            row_filter=And(EqualTo(PARTITION_COLUMN, value), StartsWith("source_file", "api:")),
            selected_fields=BRONZE_FIELDS,
        ).to_arrow()

    if snapshot_date:
        return scan(snapshot_date), snapshot_date

    partitions = sorted(
        {
            row["partition"][PARTITION_COLUMN]
            for row in table.inspect.partitions().to_pylist()
            if row["partition"][PARTITION_COLUMN]
        },
        reverse=True,
    )
    for value in partitions:
        arrow = scan(value)
        if len(arrow) > 0:
            return arrow, value

    raise ValueError("브론즈에 API 스냅샷이 없습니다. 일 배치를 먼저 실행하세요.")


def run() -> None:
    catalog = build_iceberg_catalog()
    silver_table = _ensure_silver_table(catalog)

    bronze_table, snapshot_date = _read_bronze(catalog, os.getenv("SNAPSHOT_DATE"))
    logger.info("브론즈 스냅샷 %s 처리 시작 (%d행)", snapshot_date, len(bronze_table))

    try:
        silver_arrow = normalize(bronze_table)
    except UnknownDistrictError as e:
        logger.error("정제 실패: %s", e)
        sys.exit(1)

    _log_quality(silver_arrow)
    overwrite_partition(silver_table, silver_arrow, PARTITION_COLUMN, snapshot_date)

    logger.info("%s: 실버 적재 완료", snapshot_date)


if __name__ == "__main__":
    run()
