"""
Gold - 자전거 마스터 (silver.rental_history -> gold.dim_bike)

### Spark 제거 (#170)
groupBy(bike_id).min() + left_anti 조인은 pyarrow 윈도우 없이도 DuckDB SQL로
그대로 옮길 수 있다 - Iceberg 쓰기 어휘가 overwritePartitions()/append() 둘뿐이라는
전제는 #139에서 이미 검증됨. 읽기/쓰기는 pyiceberg, 계산은 DuckDB, 품질 검증은
common/sql_assert.py(#146에서 PyDeequ 제거)로 옮긴다.

### 멀티데이 range(MAX_DAYS_PER_RUN 백필) 안전성
한 번에 여러 날을 처리해도, 새로 등장한 자전거를 first_seen_at의 날짜(snapshot_date)별로
나눠 파티션마다 따로 쓴다(staging/jobs/transform_silver_rental_history.py와 동일
idiom) - 콜드 스타트 대응이 따로 필요 없는 이유(원 요구사항 문서 참고): 워터마크
증분 + left_anti 구조라 청크 반복이 수학적으로 정답이다(MIN()의 결합법칙 + 최초
등장 청크가 항상 승리).

### 검증 순서는 기존 Spark 잡과 동일하게 유지
build_bike_location/build_station_active 등 다른 Gold 잡은 쓰기 "전"에 검증하지만,
이 잡은 원래 Spark 코드부터 쓰기 "후" gold 테이블 전체를 다시 읽어 검증했다(증분분만이
아니라 누적 전체의 bike_id 유일성을 확인해야 하므로). 동작 동등성을 위해 그대로 둔다.

사용법:
    python -m jobs.build_dim_bike
    MAX_DAYS_PER_RUN=30 python -m jobs.build_dim_bike
"""
import logging
import os
import sys
from datetime import date, timedelta

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, IntegerType, NestedField, StringType, TimestamptzType

import config
from common.duckdb_io import connect, query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket
from common.sql_assert import QualityCheck, QualityCheckError
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import GOLD_DIM_BIKE, SILVER_RENTAL_HISTORY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SILVER_WATERMARK_KEY = SILVER_RENTAL_HISTORY
GOLD_WATERMARK_KEY = GOLD_DIM_BIKE

SILVER_TABLE = "silver.rental_history"
GOLD_TABLE = "gold.dim_bike"
PARTITION_COLUMN = "snapshot_date"

GOLD_COLUMNS = ["snapshot_date", "bike_id", "first_seen_at", "start_year"]

# 기존 테이블이 없을 때만 쓰는 정의. 스키마/파티션 스펙(snapshot_date identity)은
# 기존 Spark DDL(PARTITIONED BY (snapshot_date))과 동일해야 한다.
GOLD_SCHEMA = Schema(
    NestedField(1, "snapshot_date", DateType(), required=False),
    NestedField(2, "bike_id", StringType(), required=False),
    NestedField(3, "first_seen_at", TimestamptzType(), required=False),
    NestedField(4, "start_year", IntegerType(), required=False),
)
GOLD_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)

# 자전거별 첫 등장(MIN(rent_dt))을 구하고, 이미 gold.dim_bike에 있는 자전거는
# LEFT JOIN + IS NULL로 제외한다 (Spark left_anti 조인과 동일 의미).
# start_year는 따릉이 자전거 번호 대역별 실제 도입연도로 매핑하고, 매핑 대역에 없는
# 번호나 비정형 ID는 최초 등장 시점(first_seen_at)의 연도를 폴백으로 사용한다.
_NEW_BIKES_SQL = """
    WITH first_seen AS (
        SELECT bike_id, MIN(rent_dt) AS first_seen_at
        FROM silver_rental_history
        GROUP BY bike_id
    ),
    numbered AS (
        SELECT
            fs.bike_id,
            fs.first_seen_at,
            TRY_CAST(regexp_extract(fs.bike_id, '([0-9]+)', 1) AS BIGINT) AS bike_num
        FROM first_seen fs
    )
    SELECT
        CAST(n.first_seen_at AS DATE) AS snapshot_date,
        n.bike_id AS bike_id,
        n.first_seen_at AS first_seen_at,
        CAST(
            CASE
                WHEN n.bike_num BETWEEN 1 AND 10000 THEN 2015
                WHEN n.bike_num BETWEEN 10001 AND 20000 THEN 2017
                WHEN n.bike_num BETWEEN 20001 AND 35000 THEN 2019
                WHEN n.bike_num BETWEEN 40001 AND 49999 THEN 2020
                WHEN n.bike_num BETWEEN 50001 AND 69999 THEN 2022
                WHEN n.bike_num BETWEEN 70001 AND 79999 THEN 2024
                WHEN n.bike_num >= 80001 AND n.bike_num <= 99999 THEN 2020
                ELSE CAST(date_part('year', n.first_seen_at) AS INT)
            END AS INT
        ) AS start_year
    FROM numbered n
    LEFT JOIN existing_bike_ids e ON n.bike_id = e.bike_id
    WHERE e.bike_id IS NULL
"""


def _compute_new_bikes(
    silver_table: pa.Table,
    existing_bike_ids: pa.Table,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """silver.rental_history(범위)와 gold.dim_bike의 기존 bike_id 목록만으로 동작하는
    순수 로직 - 신규 등장 자전거의 snapshot_date/first_seen_at/start_year를 계산한다."""
    conn = con or connect()
    conn.register("silver_rental_history", silver_table)
    conn.register("existing_bike_ids", existing_bike_ids)
    return query_arrow(conn, _NEW_BIKES_SQL)


def _ensure_gold_table(catalog):
    """gold.dim_bike가 없으면 만든다. 이미 있으면 스키마/스펙을 건드리지 않고 그대로 쓴다."""
    catalog.create_namespace_if_not_exists("gold")
    try:
        return catalog.load_table(GOLD_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", GOLD_TABLE)
        return catalog.create_table(GOLD_TABLE, schema=GOLD_SCHEMA, partition_spec=GOLD_PARTITION_SPEC)


def _read_silver(catalog, start_str: str, end_str: str) -> pa.Table:
    """rent_date_partition은 identity 파티션이라 이 범위 비교가 그대로 파티션 프루닝이 된다."""
    table = catalog.load_table(SILVER_TABLE)
    return table.scan(
        row_filter=And(
            GreaterThanOrEqual("rent_date_partition", start_str),
            LessThanOrEqual("rent_date_partition", end_str),
        ),
        selected_fields=("bike_id", "rent_dt"),
    ).to_arrow()


def _read_existing_bike_ids(catalog) -> pa.Table:
    return catalog.load_table(GOLD_TABLE).scan(selected_fields=("bike_id",)).to_arrow()


def _validate_dim_bike(dim_bike_table: pa.Table) -> None:
    """common/sql_assert.py(#140)를 재사용 - 이미 쓴 뒤 gold 테이블 전체를 다시 읽어
    검증한다 (dim_bike 전체 누적 행 수 기준이라도 자전거 대수 규모라 부담 없음)."""
    (
        QualityCheck("dim_bike_check")
        .is_complete("first_seen_at")
        .is_complete("start_year")
        .has_uniqueness("bike_id", threshold=0.99)
        .run(dim_bike_table)
        .raise_if_failed(QualityCheckError)
    )


def _process_range(catalog, gold_table, start_date: date, end_date: date) -> int:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    range_label = start_str if start_date == end_date else f"{start_str}~{end_str}"

    silver_arrow = _read_silver(catalog, start_str, end_str)
    if len(silver_arrow) == 0:
        logger.info("%s: Silver에 처리할 데이터 없음", range_label)
        return 0

    existing_bike_ids = _read_existing_bike_ids(catalog)
    new_bikes = _compute_new_bikes(silver_arrow, existing_bike_ids)
    new_count = len(new_bikes)

    if new_count == 0:
        logger.info("%s: 신규 등장 자전거 없음", range_label)
        return 0

    # Spark의 dynamic partition overwrite를 파티션 값별 overwrite로 옮긴 것.
    # 신규 자전거의 snapshot_date(=first_seen_at 날짜)가 여러 날에 걸칠 수 있으므로
    # 값별로 나눠 커밋한다 - 기존 파티션(오늘 신규 등장이 없는 날짜)은 손대지 않는다.
    con = connect()
    con.register("new_bikes", new_bikes)
    partition_values = [
        row[0]
        for row in con.execute(
            f"SELECT DISTINCT strftime({PARTITION_COLUMN}, '%Y-%m-%d') FROM new_bikes ORDER BY 1"
        ).fetchall()
    ]
    for value in partition_values:
        chunk = query_arrow(
            con,
            f"SELECT * FROM new_bikes WHERE strftime({PARTITION_COLUMN}, '%Y-%m-%d') = ?",
            [value],
        ).select(GOLD_COLUMNS)
        overwrite_partition(gold_table, chunk, PARTITION_COLUMN, value)

    # 원래 Spark 잡과 동일한 순서: 쓴 뒤 gold 테이블 전체를 다시 읽어 검증한다.
    # 실패해도 이미 커밋된 뒤이므로 롤백은 아니다 - 기존 동작과 동일.
    full_gold = catalog.load_table(GOLD_TABLE).scan().to_arrow()
    _validate_dim_bike(full_gold)  # 실패 시 QualityCheckError -> 배치 중단

    logger.info(
        "%s: 신규 자전거 %d대 dim_bike 추가 (파티션 %d개)",
        range_label, new_count, len(partition_values),
    )
    return new_count


def run() -> None:
    # raw_bucket에는 워터마크 JSON이 있음 - 이 잡만 단독 실행하는 경우에도 안전하도록 보장
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    gold_table = _ensure_gold_table(catalog)

    # 상한선: Silver가 확정 승격한 날짜
    silver_watermark = read_watermark(watermark_key=SILVER_WATERMARK_KEY)
    # 하한선: Gold 전용 워터마크
    gold_watermark = read_watermark(watermark_key=GOLD_WATERMARK_KEY)

    start_date = gold_watermark + timedelta(days=1)
    end_date = silver_watermark

    max_days = os.getenv("MAX_DAYS_PER_RUN")
    if max_days:
        capped_end = start_date + timedelta(days=int(max_days) - 1)
        if capped_end < end_date:
            logger.info(
                "MAX_DAYS_PER_RUN=%s 적용 - 이번 실행은 %s ~ %s까지만 처리 (원래 끝: %s)",
                max_days, start_date, capped_end, end_date,
            )
            end_date = capped_end

    if start_date > end_date:
        logger.info(
            "처리할 신규 날짜 없음 (Gold 워터마크=%s, Silver 워터마크=%s)",
            gold_watermark, silver_watermark,
        )
        return

    try:
        _process_range(catalog, gold_table, start_date, end_date)
        write_watermark(end_date, watermark_key=GOLD_WATERMARK_KEY)
    except QualityCheckError as e:
        logger.error("%s~%s 처리 실패, 배치 중단: %s", start_date, end_date, e)
        sys.exit(1)


if __name__ == "__main__":
    run()
