"""
Silver - 대여이력 (bronze.rental_history -> silver.rental_history)

### Spark 제거 (#143)
이 잡은 이 파이프라인에서 DuckDB가 볼륨 때문에 실제로 필요한 두 곳 중 하나다
(다른 하나는 초기 적재의 700MB CSV). 중복 제거가 윈도우 함수
`row_number() over (partition by bike_id, rent_dt)`인데 pyarrow에는 윈도우 함수가
없어서 pyarrow만으로는 옮길 수 없다. 읽기/쓰기는 pyiceberg, 변환/중복 제거는 DuckDB,
품질 검증은 common/sql_assert.py(PyDeequ 대체)로 옮긴다.

PyDeequ를 걷어내면서 콜백 서버 종료 처리(stop_spark_session_with_deequ)도 같이
사라졌다 - 그게 있어야 했던 이유(sys.exit 후에도 프로세스가 안 끝남) 자체가 없어진다.

### 청크 안전성: 중복 제거 윈도우는 하루 안에서 닫힌다
`MAX_DAYS_PER_RUN`으로 구간을 잘라 여러 번 돌려도, 하루씩 파티션을 나눠 써도
결과가 달라지지 않는다. 근거는 두 가지다.

  1. 윈도우 파티션 키가 `(bike_id, rent_dt)`다. rent_dt는 타임스탬프 값 자체이므로
     같은 그룹에 들어가는 두 행은 rent_dt가 완전히 동일하다 - 날짜가 다를 수 없다.
  2. 청크를 자르는 축인 `rent_date_partition`은 rent_dt에서 결정론적으로 파생된
     값이다 (ingestion/jobs/initial_load_rental_history.py:_derive_date_partition -
     rent_dt에서 숫자만 뽑아 YYYY-MM-DD로 조립, daily_batch_rental_history.py는
     아예 그 날짜 상수를 그대로 넣는다). 즉 rent_dt가 같으면
     rent_date_partition도 반드시 같다.

두 조건이 겹치므로 한 중복 그룹의 모든 행은 항상 같은 rent_date_partition 안에
들어 있고, 어떤 청크 분해도 그룹을 반으로 가르지 못한다. 날짜 청크 분해와
`MAX_DAYS_PER_RUN` 재실행이 안전한 이유가 이것이고, 이후 재구축 때 이 판단을
다시 할 필요가 없다.

여기에 더해 중복 제거 정렬키를 전순서로 만들어(_DEDUP_SQL 주석 참고) 재실행 결과가
값까지 같아지게 했다 - Spark 시절 정렬키(2개)로는 완전 동률인 그룹에서 어느 행이
남는지가 실행마다 달라졌다.

사용법:
    python -m jobs.transform_silver_rental_history
    MAX_DAYS_PER_RUN=30 python -m jobs.transform_silver_rental_history
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
from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

import config
from common.duckdb_io import query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket  # 워커/로컬 실행 시 대상 버킷이 없으면 만들어주는 안전장치
from common.sql_assert import QualityCheck, QualityCheckError
from common.watermark import read_watermark, write_watermark  # Bronze용 모듈을 키만 바꿔 Silver 전용으로 재사용
from config.watermark_keys import SILVER_RENTAL_HISTORY  # build_dim_bike/set_watermark와 같은 값을 공유

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# S3(혹은 LocalStack)상의 실제 객체 키 경로
SILVER_WATERMARK_KEY = SILVER_RENTAL_HISTORY

# MAX_DAYS_PER_RUN 미지정 시 쓰는 기본 상한 - 평소 daily 운영(구간이 보통 하루)엔 영향 없음.
DEFAULT_MAX_DAYS_PER_RUN = 31

BRONZE_TABLE = "bronze.rental_history"
SILVER_TABLE = "silver.rental_history"
PARTITION_COLUMN = "rent_date_partition"

SILVER_COLUMNS = [
    "bike_id",
    "rent_dt",
    "return_dt",
    "use_distance_m",
    "rent_station_id",
    "return_station_id",
    "rent_date_partition",
    "source_file",
    "ingested_at",
]

# 브론즈 20컬럼 중 실버가 실제로 쓰는 것만 스캔한다 - 31일치(최대 300만 행)를
# 통째로 메모리에 올리므로 안 쓰는 컬럼을 읽지 않는 게 그대로 메모리 절약이 된다.
BRONZE_FIELDS = tuple(SILVER_COLUMNS)

# pyiceberg로 테이블을 새로 만들 때만 쓰는 정의. 기존 테이블의 스키마/파티션 스펙과
# 정확히 같아야 한다(rent_date_partition identity 파티션은 변경 금지 - #143).
SILVER_SCHEMA = Schema(
    NestedField(1, "bike_id", StringType(), required=False),
    NestedField(2, "rent_dt", TimestamptzType(), required=False),
    NestedField(3, "return_dt", TimestamptzType(), required=False),
    NestedField(4, "use_distance_m", DoubleType(), required=False),
    NestedField(5, "rent_station_id", StringType(), required=False),
    NestedField(6, "return_station_id", StringType(), required=False),
    NestedField(7, "rent_date_partition", StringType(), required=False),
    NestedField(8, "source_file", StringType(), required=False),
    NestedField(9, "ingested_at", TimestamptzType(), required=False),
)
SILVER_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=7, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)
SILVER_PROPERTIES = {"write.distribution-mode": "hash"}

# Bronze는 원본 문자열을 그대로 보존하므로 소스(API/CSV 초기 적재)마다 포맷이 다를 수 있음
# (rent_date_partition을 만들 때도 같은 이유로 포맷 편차에 대응함 - initial_load_rental_history.py 참고).
# Spark to_timestamp 패턴 -> DuckDB strptime 포맷으로 1:1 번역. 순서가 곧 우선순위다.
_KNOWN_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",   # yyyy-MM-dd HH:mm:ss
    "%Y%m%d%H%M%S",        # yyyyMMddHHmmss
    "%Y-%m-%d",            # yyyy-MM-dd
    "%Y%m%d",              # yyyyMMdd
]


class SilverValidationError(Exception):
    """품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


def _parse_datetime_sql(column: str) -> str:
    """알려진 포맷을 순서대로 시도해서 처음 매칭되는 값을 채택 (coalesce가 첫 not-null을 고름)."""
    attempts = ", ".join(f"try_strptime({column}, '{fmt}')" for fmt in _KNOWN_DATETIME_FORMATS)
    # Iceberg 컬럼 타입이 timestamptz라 TIMESTAMPTZ로 맞춰준다. 세션 타임존을 UTC로
    # 고정해두므로(_connect) naive -> UTC 해석이 Spark(세션 TZ=UTC)와 동일하다.
    return f"CAST(COALESCE({attempts}) AS TIMESTAMPTZ)"


def _cast_sql(source: str) -> str:
    """Bronze(전부 STRING) -> Silver 타입 캐스팅. 파싱 실패 감지용 원본 컬럼을 함께 남긴다."""
    return f"""
        SELECT
            bike_id,
            rent_dt                                   AS _raw_rent_dt,
            return_dt                                 AS _raw_return_dt,
            {_parse_datetime_sql("rent_dt")}          AS rent_dt,
            {_parse_datetime_sql("return_dt")}        AS return_dt,
            TRY_CAST(use_distance_m AS DOUBLE)        AS use_distance_m,
            rent_station_id,
            return_station_id,
            rent_date_partition,
            source_file,
            ingested_at
        FROM {source}
    """


# 결측 제거 -> 중복 제거를 한 번에.
#   - 결측: bike_id / rent_dt가 null인 행 드롭
#   - 중복: 같은 자전거가 같은 시각에 대여를 시작한 행들끼리 그룹핑해 첫 행만 남김
#
# ORDER BY 앞의 두 키(return_dt, rent_station_id)는 기존 Spark 잡의
# `orderBy(col("return_dt").asc_nulls_last(), col("rent_station_id").asc())`를
# 그대로 옮긴 것이다. null 위치를 일일이 적는 이유: Spark의 `asc()`는 NULLS FIRST가
# 기본인데 DuckDB의 `ASC`는 NULLS LAST가 기본이라, 안 적으면 rent_station_id가 null인
# 행에서 기존 잡과 다른 행이 선택된다. `asc_nulls_last()`는 그대로 NULLS LAST.
#
# 나머지 3개 키는 Spark 잡에 없던 것을 이번에 추가했다(#143). 앞의 두 키만으로는
# 순서가 전순서(total order)가 아니라서, 완전히 동률인 중복 그룹에서 row_number()가
# 어느 행을 1번으로 고르는지가 실행마다 달라진다 - 같은 입력으로 같은 잡을 두 번
# 돌렸을 때 실제로 결과가 갈리는 걸 실측했다(2026-08-22, 109,866행 중 4행). 이건
# Spark 시절에도 있던 성질이고(그쪽도 셔플 순서에 달려 있다), 이대로 두면 재실행
# /백필이 "같은 답"을 준다고 말할 수 없다. 남은 컬럼을 뒤에 붙여 순서를 전순서로
# 만들어 재실행 결과를 고정한다. 동률이 아닌 그룹의 선택 결과는 앞 두 키가 그대로
# 결정하므로 기존 동작과 달라지지 않는다.
_DEDUP_SQL = """
    SELECT bike_id, rent_dt, return_dt, use_distance_m, rent_station_id,
           return_station_id, rent_date_partition, source_file, ingested_at
    FROM {source}
    WHERE bike_id IS NOT NULL AND rent_dt IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY bike_id, rent_dt
        ORDER BY return_dt ASC NULLS LAST, rent_station_id ASC NULLS FIRST,
                 return_station_id ASC NULLS FIRST, use_distance_m ASC NULLS FIRST,
                 source_file ASC NULLS FIRST, ingested_at ASC NULLS FIRST
    ) = 1
"""

# 원본 문자열은 있는데 알려진 포맷으로 하나도 안 파싱된 행 = 알려지지 않은 날짜 포맷.
_PARSE_FAILED_SQL = """
    SELECT count(*) FROM {source}
    WHERE (_raw_rent_dt IS NOT NULL AND trim(_raw_rent_dt) <> '' AND rent_dt IS NULL)
       OR (_raw_return_dt IS NOT NULL AND trim(_raw_return_dt) <> '' AND return_dt IS NULL)
"""


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    # TIMESTAMP -> TIMESTAMPTZ 캐스팅이 세션 타임존을 따르므로 UTC로 못박는다.
    # 컨테이너 TZ에 결과가 흔들리면 Spark 잡과 값이 어긋난다.
    con.execute("SET TimeZone='UTC'")
    return con


def transform(bronze_table: pa.Table, con: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
    """
    브론즈 PyArrow Table을 실버 9컬럼으로 변환한다. 읽기/쓰기를 하지 않는다.

    알려지지 않은 날짜 포맷이 하나라도 있으면 SilverValidationError를 던진다 -
    조용히 드롭하면 그만큼 실버가 소리 없이 비어버린다.
    """
    conn = con or _connect()
    conn.register("bronze_rental_history", bronze_table)
    conn.execute(f"CREATE OR REPLACE TEMP VIEW cast_rental AS {_cast_sql('bronze_rental_history')}")

    parse_failed = conn.execute(_PARSE_FAILED_SQL.format(source="cast_rental")).fetchone()[0]
    if parse_failed:
        raise SilverValidationError(
            f"rent_dt/return_dt 파싱 실패 {parse_failed}행 - 알려지지 않은 날짜 포맷일 가능성"
        )

    return query_arrow(conn, _DEDUP_SQL.format(source="cast_rental")).select(SILVER_COLUMNS)


def validate(silver_table: pa.Table, range_label: str) -> None:
    """
    PyDeequ Check를 sql_assert QualityCheck로 1:1 옮긴 것. 실패 시 SilverValidationError.

    hasUniqueness만 표현이 조금 다르다 - PyDeequ는 `fraction > 0.98`(초과),
    sql_assert는 `ratio >= threshold`(이상)로 판정한다. 정확히 0.98인 경계에서만
    갈리는 차이라 실질 판정은 같다.
    """
    result = (
        QualityCheck(f"silver_rental_history_{range_label}")
        # transform()의 결측 제거가 이미 걸렀어야 할 조건 재확인
        .is_complete("bike_id")
        .is_complete("rent_dt")
        # transform()의 중복 제거가 이미 제거했어야 할 중복의 재확인
        .has_uniqueness(["bike_id", "rent_dt"], threshold=0.98)
        # 이동거리가 음수인 데이터 확인
        .is_non_negative("use_distance_m")
        # 대여일시 이후에 반납일시가 있어야 함
        .satisfies("return_dt IS NULL OR return_dt >= rent_dt", "return_dt는 rent_dt 이후여야 함")
        .run(silver_table)
    )

    for constraint in result.results:
        logger.info(
            "  [%s] %s (위반 %d / 전체 %d)",
            constraint.status, constraint.name, constraint.violation_count, constraint.total_count,
        )

    try:
        result.raise_if_failed()
    except QualityCheckError as e:
        raise SilverValidationError(f"{range_label} Silver 품질 검증 실패: {e}") from e


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


def _read_bronze(catalog, start_str: str, end_str: str) -> pa.Table:
    """rent_date_partition은 identity 파티션이라 이 범위 비교가 그대로 파티션 프루닝이 된다."""
    table = catalog.load_table(BRONZE_TABLE)
    return table.scan(
        row_filter=And(
            GreaterThanOrEqual(PARTITION_COLUMN, start_str),
            LessThanOrEqual(PARTITION_COLUMN, end_str),
        ),
        selected_fields=BRONZE_FIELDS,
    ).to_arrow()


def _process_range(catalog, silver_table, start_date: date, end_date: date) -> int:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    range_label = start_str if start_date == end_date else f"{start_str}~{end_str}"

    bronze_table = _read_bronze(catalog, start_str, end_str)
    row_count = len(bronze_table)

    # 0행이면 Silver로 승격할 데이터가 없으므로 바로 종료
    if row_count == 0:
        logger.info("%s: Bronze에 처리할 데이터 없음", range_label)
        return 0

    silver_arrow = transform(bronze_table)
    silver_count = len(silver_arrow)
    if silver_count < row_count:
        logger.info(
            "%s: bike_id/rent_dt 결측 또는 중복으로 %d행 제외 (Bronze %d행 -> Silver %d행)",
            range_label, row_count - silver_count, row_count, silver_count,
        )

    validate(silver_arrow, range_label)  # 실패 시 SilverValidationError -> 배치 중단

    # Spark의 dynamic partition overwrite를 파티션 값별 overwrite로 옮긴 것.
    # 위 docstring의 "중복 제거 윈도우는 하루 안에서 닫힌다"가 이 분해의 근거다 -
    # 한 중복 그룹이 두 파티션에 걸치는 일이 없으므로 파티션별로 따로 써도
    # 한 번에 쓴 것과 결과가 같다. Bronze에 없는 날짜는 손대지 않는 것도 동일.
    con = _connect()
    con.register("silver_rental_history", silver_arrow)
    partition_values = [
        row[0]
        for row in con.execute(
            f"SELECT DISTINCT {PARTITION_COLUMN} FROM silver_rental_history "
            f"ORDER BY {PARTITION_COLUMN}"
        ).fetchall()
    ]
    for value in partition_values:
        chunk = query_arrow(
            con,
            f"SELECT * FROM silver_rental_history WHERE {PARTITION_COLUMN} = ?",
            [value],
        ).select(SILVER_COLUMNS)
        overwrite_partition(silver_table, chunk, PARTITION_COLUMN, value)

    logger.info("%s: %d행 Silver 승격 완료 (파티션 %d개)", range_label, silver_count, len(partition_values))
    return silver_count


def run() -> None:
    # 백필/daily_batch를 안 거치고 이 잡만 단독 실행하는 경우에도 안전하도록 버킷을 보장
    # (raw_bucket에는 워터마크 JSON이 있음)
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    silver_table = _ensure_silver_table(catalog)

    # 상한선: Bronze가 확정 커밋한 날짜 (rental_history 기본 워터마크 키)
    bronze_watermark = read_watermark()
    # 하한선: Silver 전용 워터마크
    silver_watermark = read_watermark(watermark_key=SILVER_WATERMARK_KEY)

    start_date = silver_watermark + timedelta(days=1)
    end_date = bronze_watermark

    # 미지정이면 DEFAULT_MAX_DAYS_PER_RUN을 대신 써서 항상 상한을 건다
    max_days = os.getenv("MAX_DAYS_PER_RUN") or str(DEFAULT_MAX_DAYS_PER_RUN)
    capped_end = start_date + timedelta(days=int(max_days) - 1)
    if capped_end < end_date:
        logger.info(
            "MAX_DAYS_PER_RUN=%s 적용 - 이번 실행은 %s ~ %s까지만 처리 (원래 끝: %s)",
            max_days, start_date, capped_end, end_date,
        )
        end_date = capped_end

    if start_date > end_date:
        logger.info(
            "처리할 신규 날짜 없음 (Silver 워터마크=%s, Bronze 워터마크=%s)",
            silver_watermark, bronze_watermark,
        )
        return

    try:
        # _process_range 성공 시에만 다음 줄 write_watermark 실행
        _process_range(catalog, silver_table, start_date, end_date)
        write_watermark(end_date, watermark_key=SILVER_WATERMARK_KEY)
    except SilverValidationError as e:
        logger.error("%s~%s 처리 실패, 배치 중단: %s", start_date, end_date, e)
        sys.exit(1)


if __name__ == "__main__":
    run()
