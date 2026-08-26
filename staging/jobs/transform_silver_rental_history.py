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
from datetime import date, datetime, timedelta, timezone

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

import config
from common.duckdb_io import query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import replace_range
from common.s3_utils import ensure_bucket, get_json, put_json  # 워커/로컬 실행 시 대상 버킷이 없으면 만들어주는 안전장치
from common.silver_rental_history_completion import (
    build_completion_marker,
    write_completion_marker,
)
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

# Bronze가 0행이라 transform()을 아예 돌리지 않는 경우에 쓰는 빈 결과의 스키마.
# SILVER_SCHEMA와 컬럼/타입이 1:1로 대응해야 한다.
SILVER_ARROW_SCHEMA = pa.schema([
    pa.field("bike_id", pa.string()),
    pa.field("rent_dt", pa.timestamp("us", tz="UTC")),
    pa.field("return_dt", pa.timestamp("us", tz="UTC")),
    pa.field("use_distance_m", pa.float64()),
    pa.field("rent_station_id", pa.string()),
    pa.field("return_station_id", pa.string()),
    pa.field("rent_date_partition", pa.string()),
    pa.field("source_file", pa.string()),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
])

QUARANTINE_TABLE = "silver.rental_history_quarantine"

# silver.bikeman_action_quarantine과 동일한 convention: 원본 컬럼 + 격리 사유/시각.
# 언파티션 - quarantine 테이블은 볼륨이 작고 조회가 감사 목적이라 파티션이 필요 없다
# (#216 조사 근거를 그대로 따름 - bootstrap_iceberg_tables.py 주석 참고).
QUARANTINE_SCHEMA = Schema(
    NestedField(1, "bike_id", StringType(), required=False),
    NestedField(2, "rent_dt", TimestamptzType(), required=False),
    NestedField(3, "return_dt", TimestamptzType(), required=False),
    NestedField(4, "use_distance_m", DoubleType(), required=False),
    NestedField(5, "rent_station_id", StringType(), required=False),
    NestedField(6, "return_station_id", StringType(), required=False),
    NestedField(7, "rent_date_partition", StringType(), required=False),
    NestedField(8, "source_file", StringType(), required=False),
    NestedField(9, "ingested_at", TimestamptzType(), required=False),
    NestedField(10, "quarantine_reason", StringType(), required=False),
    NestedField(11, "quarantined_at", TimestamptzType(), required=False),
)
QUARANTINE_ARROW_SCHEMA = pa.schema([
    pa.field("bike_id", pa.string()),
    pa.field("rent_dt", pa.timestamp("us", tz="UTC")),
    pa.field("return_dt", pa.timestamp("us", tz="UTC")),
    pa.field("use_distance_m", pa.float64()),
    pa.field("rent_station_id", pa.string()),
    pa.field("return_station_id", pa.string()),
    pa.field("rent_date_partition", pa.string()),
    pa.field("source_file", pa.string()),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    pa.field("quarantine_reason", pa.string()),
    pa.field("quarantined_at", pa.timestamp("us", tz="UTC")),
])

# 이 비율을 넘는 quarantine은 구조적 이상으로 보고 배치 전체를 막는다(#232) -
# 12/185956 ≈ 0.006%처럼 알려진 노이즈 수준은 통과시키되, 대량 위반은 여전히
# 사람이 봐야 하므로 하드 게이트로 남긴다.
DEFAULT_MAX_QUARANTINE_RATIO = 0.01

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

# #137: 06시 일 배치의 publish_bronze_asset이 Asset event에 실어 보낸 정확한 promotion만
# 신뢰한다. S3의 "최신 COMPLETE promotion"을 추측하면 Catchup(metadata 없이 같은 Asset을
# 발행) 실행이 이전 일 배치 promotion을 잘못 집어 당일 파티션을 처리할 수 있다.
SNAPSHOT_FINAL = "FINAL"
SNAPSHOT_PRELIMINARY = "PRELIMINARY"
MODE_NORMAL = "NORMAL"
MODE_DEGRADED = "DEGRADED"

# ingestion/jobs/rental_history_snapshot_policy.py의 PROMOTION_PREFIX와 짝을 이루는
# Silver 쪽 marker 경로. staging 잡은 `-m jobs.X`로 실행되면 cwd(staging/jobs)가
# 먼저 잡혀 `jobs` 패키지가 staging/jobs에 고정되므로, ingestion/jobs 안의 정책 모듈을
# `from jobs...`로 가져올 수 없다 (docs/architecture.md §3 - 디렉터리가 레이어와 1:1이
# 아님). 그래서 key 포맷을 여기서 다시 정의한다.
SILVER_PROMOTION_PREFIX = "_meta/promotion/silver_rental_history/"


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
        # return_dt < rent_dt, use_distance_m < 0는 더 이상 여기서 hard-fail하지
        # 않는다 (#232) - _process_range가 _split_quarantine_violations로 미리
        # quarantine 분리한 뒤 이 validate()에는 이미 걸러진 clean 행만 들어온다.
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
        try:
            return catalog.create_table(
                SILVER_TABLE,
                schema=SILVER_SCHEMA,
                partition_spec=SILVER_PARTITION_SPEC,
                properties=SILVER_PROPERTIES,
            )
        except TableAlreadyExistsError:
            # 동시에 실행된 다른 백필 청크가 먼저 만들었다 - 동일 스키마 상수로
            # 만들어졌으므로 그냥 로드해서 쓴다.
            logger.info("%s 테이블이 다른 청크에서 이미 생성됨 - 로드로 대체", SILVER_TABLE)
            return catalog.load_table(SILVER_TABLE)


def _ensure_quarantine_table(catalog):
    """quarantine 테이블이 없으면 만든다 (silver.bikeman_action_quarantine과 동일 패턴)."""
    catalog.create_namespace_if_not_exists("silver")
    try:
        return catalog.load_table(QUARANTINE_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", QUARANTINE_TABLE)
        try:
            return catalog.create_table(QUARANTINE_TABLE, schema=QUARANTINE_SCHEMA)
        except TableAlreadyExistsError:
            # 동시에 실행된 다른 백필 청크가 먼저 만들었다 - 동일 스키마 상수로
            # 만들어졌으므로 그냥 로드해서 쓴다.
            logger.info("%s 테이블이 다른 청크에서 이미 생성됨 - 로드로 대체", QUARANTINE_TABLE)
            return catalog.load_table(QUARANTINE_TABLE)


# 행 단위 값 이상치 판정식 - 둘 다 알려진 노이즈 수준이면 quarantine 대상이지,
# 구조적 오류(파싱 실패, 결측)와 달리 단 1건으로 배치 전체를 막을 이유가 없다 (#232).
_RETURN_BEFORE_RENT = "(return_dt IS NOT NULL AND return_dt < rent_dt)"
_NEGATIVE_DISTANCE = "(use_distance_m IS NOT NULL AND use_distance_m < 0)"
_QUARANTINE_VIOLATION = f"{_RETURN_BEFORE_RENT} OR {_NEGATIVE_DISTANCE}"
_QUARANTINE_REASON_SQL = f"""
    concat_ws(', ',
        CASE WHEN {_RETURN_BEFORE_RENT} THEN 'return_dt < rent_dt' END,
        CASE WHEN {_NEGATIVE_DISTANCE} THEN 'use_distance_m < 0' END
    )
"""


def _split_quarantine_violations(
    silver_arrow: pa.Table, con: duckdb.DuckDBPyConnection
) -> tuple[pa.Table, pa.Table]:
    """
    return_dt < rent_dt 또는 use_distance_m < 0인 행을 분리한다 (#232). 예전엔
    validate()의 hard-fail 조건이었지만, 알려진 노이즈 수준까지 배치 전체를 막는
    건 과하다 - 이제 이 행들은 quarantine으로 옮기고 나머지는 정상 진행한다.
    비율이 threshold를 넘는 경우의 하드 게이트는 _process_range에서 별도로 건다.
    """
    con.register("split_target", silver_arrow)
    clean = query_arrow(
        con,
        f"SELECT * FROM split_target WHERE NOT ({_QUARANTINE_VIOLATION})",
    ).select(SILVER_COLUMNS)
    violations = query_arrow(
        con,
        f"SELECT *, {_QUARANTINE_REASON_SQL} AS quarantine_reason "
        f"FROM split_target WHERE {_QUARANTINE_VIOLATION}",
    )

    quarantined_at = datetime.now(timezone.utc)
    quarantine = violations.select(SILVER_COLUMNS + ["quarantine_reason"]).append_column(
        "quarantined_at", pa.array([quarantined_at] * len(violations), type=pa.timestamp("us", tz="UTC"))
    )
    return clean, quarantine.cast(QUARANTINE_ARROW_SCHEMA)


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


def _process_range(catalog, silver_table, start_date: date, end_date: date) -> dict:
    """
    선언 구간 [start_date, end_date]를 이번 입력 결과로 완전히 교체하고 처리 요약을 돌려준다.

    ### 읽기·변환·검증과 쓰기를 분리한다
    품질 검증(비율 게이트 + DQ)이 끝나기 전에는 어떤 테이블도 건드리지 않는다. 예전에는
    quarantine append가 validate()보다 먼저였는데, 그러면 DQ 실패로 중단된 실행이
    quarantine 행만 남겨놓고 죽는다.

    ### 왜 날짜별 overwrite가 아니라 범위 1회 교체인가 (#256 후속)
    예전에는 `SELECT DISTINCT rent_date_partition`으로 실제 데이터가 있는 날짜만 뽑아
    날짜마다 overwrite_partition()을 불렀다. 최대 31일 청크면 Iceberg transaction과
    JDBC Catalog commit이 31번 반복돼, 운영 실측에서 고밀도 청크 태스크 시간의 97~99%가
    그 반복 구간이었다. 이제 범위 필터 하나로 한 번만 커밋한다.

    분해가 안전하다는 근거(모듈 docstring의 "중복 제거 윈도우는 하루 안에서 닫힌다")는
    여전히 유효하지만, 이제는 그걸 근거로 쪼갤 이유가 없다 - 한 번에 쓰는 쪽이 더 싸고,
    청크 중간 실패 시 일부 날짜만 새 결과인 중간 상태도 사라진다.

    ### 왜 "데이터가 있는 날짜"가 아니라 "선언 구간"을 지우는가
    이번 결과가 0행인 날짜를 교체 대상에서 빼면 그 날짜의 과거 Silver 행이 그대로 남는다.
    그러면 완료 marker가 "이 구간이 현재 입력 결과로 완전히 교체됐다"는 뜻을 가질 수 없다.
    Bronze가 통째로 0행인 경우도 조기 반환하지 않고 0행 결과로 구간을 비운다.
    """
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    range_label = start_str if start_date == end_date else f"{start_str}~{end_str}"

    bronze_table = _read_bronze(catalog, start_str, end_str)
    row_count = len(bronze_table)

    if row_count == 0:
        # 조기 반환하지 않는다 - 스키마가 맞는 0행 결과로 두 테이블의 선언 구간을 비운다.
        # transform()/validate()는 건너뛴다: 입력이 없으면 파싱 실패도 중복도 있을 수
        # 없고, 0행 테이블에 유일성 비율 같은 DQ를 매기는 건 의미가 없다.
        logger.info("%s: Bronze에 처리할 데이터 없음 - 선언 구간을 0행으로 교체", range_label)
        silver_arrow = SILVER_ARROW_SCHEMA.empty_table()
        quarantine_arrow = QUARANTINE_ARROW_SCHEMA.empty_table()
        dedup_count = 0
    else:
        silver_arrow = transform(bronze_table)
        dedup_count = len(silver_arrow)
        if dedup_count < row_count:
            logger.info(
                "%s: bike_id/rent_dt 결측 또는 중복으로 %d행 제외 (Bronze %d행 -> Silver %d행)",
                range_label, row_count - dedup_count, row_count, dedup_count,
            )

        con = _connect()
        con.register("silver_rental_history_pre_quarantine", silver_arrow)
        silver_arrow, quarantine_arrow = _split_quarantine_violations(silver_arrow, con)

    quarantine_count = len(quarantine_arrow)
    if quarantine_count:
        # 비율 판정과 경고 로그에만 쓰는 분기다. quarantine 쓰기 자체는 아래에서 항상 한다.
        ratio = quarantine_count / dedup_count
        max_ratio = float(os.getenv("MAX_QUARANTINE_RATIO") or DEFAULT_MAX_QUARANTINE_RATIO)
        if ratio > max_ratio:
            raise SilverValidationError(
                f"{range_label}: 행 단위 이상치 비율 {ratio:.4f}가 임계치 {max_ratio}를 "
                f"초과 - 구조적 이상 가능성, quarantine 대신 배치 중단 (위반 {quarantine_count}/{dedup_count}행)"
            )
        logger.warning(
            "%s: 행 단위 이상치 %d행(%.4f%%) quarantine 처리 - silver.rental_history_quarantine 확인 필요",
            range_label, quarantine_count, ratio * 100,
        )

    silver_count = len(silver_arrow)
    if row_count:
        validate(silver_arrow, range_label)  # 실패 시 SilverValidationError -> 배치 중단

    # 쓰기 순서: quarantine -> 본 테이블 -> (호출자가) marker.
    # 두 테이블은 서로 다른 Iceberg 테이블이라 한 트랜잭션으로 묶을 수 없다. quarantine이
    # 먼저 성공하고 본 테이블이 실패하면 짧은 불일치가 생기지만, quarantine은 Gold의
    # 비즈니스 입력이 아닌 감사 테이블이고 marker가 없는 청크는 미완료로 취급되므로 이
    # 창을 허용한다 - 재실행이 같은 구간을 다시 교체해 최종적으로 수렴한다.
    #
    # 이상치가 0건이어도 항상 교체한다. append()로 쌓던 예전 방식은 같은 청크를 재실행할
    # 때마다 동일한 이상치가 다시 추가돼 감사 건수가 부풀었다. 구간 교체로 바꾸면 재실행
    # 결과가 행 수와 내용까지 같아진다.
    replace_range(
        _ensure_quarantine_table(catalog), quarantine_arrow, PARTITION_COLUMN,
        start_str, end_str, catalog=catalog,
    )
    replace_range(silver_table, silver_arrow, PARTITION_COLUMN, start_str, end_str, catalog=catalog)

    logger.info(
        "%s: %d행 Silver 승격 완료 (구간 단일 커밋, quarantine %d행)",
        range_label, silver_count, quarantine_count,
    )
    return {
        "bronze_row_count": row_count,
        "silver_row_count": silver_count,
        "quarantine_row_count": quarantine_count,
    }


def _parse_bool_env(name: str, default: bool = False) -> bool:
    """`true`/`false`만 허용한다 (rental_history_snapshot_policy.parse_bool과 동일 규칙)."""
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if not normalized:
        return default
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise SilverValidationError(f"{name} must be 'true' or 'false': {value!r}")


def _load_bronze_promotion_metadata() -> dict | None:
    """
    silver_rental_history_dag.py가 triggering_asset_events에서 뽑아 넘긴 값을 읽는다.

    수동 트리거처럼 Asset event에 metadata가 없으면 세 값이 모두 빈 문자열로 넘어온다 -
    이 경우 None을 돌려줘 기존 확정 워터마크 구간만 처리하게 한다.
    """
    run_date = (os.getenv("RENTAL_HISTORY_BRONZE_RUN_DATE") or "").strip()
    promotion_id = (os.getenv("RENTAL_HISTORY_BRONZE_PROMOTION_ID") or "").strip()
    promotion_key = (os.getenv("RENTAL_HISTORY_BRONZE_PROMOTION_KEY") or "").strip()
    if not (run_date and promotion_id and promotion_key):
        return None
    return {"run_date": run_date, "promotion_id": promotion_id, "promotion_key": promotion_key}


def _validate_bronze_marker(
    marker, expected_run_date: str, expected_promotion_id: str, promotion_key: str
) -> dict:
    """Asset metadata와 실제 Bronze marker가 어긋나면 안전하게 실패시킨다."""
    if not isinstance(marker, dict):
        raise SilverValidationError(f"Bronze promotion marker 없음: {promotion_key}")
    if marker.get("dataset") != "rental_history":
        raise SilverValidationError(f"dataset 불일치: {marker.get('dataset')!r} ({promotion_key})")
    if marker.get("run_date") != expected_run_date:
        raise SilverValidationError(
            f"run_date 불일치: {marker.get('run_date')!r} != {expected_run_date!r} ({promotion_key})"
        )
    if marker.get("promotion_id") != expected_promotion_id:
        raise SilverValidationError(
            f"promotion_id 불일치: {marker.get('promotion_id')!r} != {expected_promotion_id!r} "
            f"({promotion_key})"
        )
    if marker.get("status") != "COMPLETE":
        raise SilverValidationError(f"status가 COMPLETE가 아님: {marker.get('status')!r} ({promotion_key})")
    return marker


def _find_current_day_entry(marker: dict, run_date_str: str) -> dict | None:
    """Bronze marker의 selected_snapshots 중 당일(run_date) 파티션 항목을 찾는다."""
    for entry in marker.get("selected_snapshots") or []:
        if entry.get("target_date") == run_date_str:
            return entry
    return None


def _derive_mode(bronze_mode: str | None, snapshot_type: str) -> str:
    """당일 파티션의 원천이 PRELIMINARY거나 Bronze marker 자체가 DEGRADED면 계승한다.

    Bronze marker의 mode는 같은 promotion에 포함된 확정 backlog 날짜까지 반영하므로,
    당일 entry만으로는 못 잡는 혼합(backlog PRELIMINARY + 당일 FINAL) 케이스를 놓치지
    않기 위해 marker.mode도 함께 본다.
    """
    if bronze_mode == MODE_DEGRADED or snapshot_type == SNAPSHOT_PRELIMINARY:
        return MODE_DEGRADED
    return MODE_NORMAL


def _silver_promotion_key(run_date_str: str, promotion_id: str) -> str:
    return f"{SILVER_PROMOTION_PREFIX}run_date={run_date_str}/promotion_id={promotion_id}/promotion.json"


def _build_silver_promotion_document(
    *,
    run_date_str: str,
    promotion_id: str,
    source_bronze_promotion_key: str,
    entry: dict,
    bronze_mode: str | None,
    silver_row_count: int,
    confirmed_through: date,
    processed_at: str,
) -> dict:
    snapshot_type = entry["snapshot_type"]
    return {
        "dataset": "rental_history",
        "run_date": run_date_str,
        "promotion_id": promotion_id,
        "source_bronze_promotion_key": source_bronze_promotion_key,
        "mode": _derive_mode(bronze_mode, snapshot_type),
        "source_snapshot_type": snapshot_type,
        "source_observed_at": entry["observed_at"],
        "processed_partition": run_date_str,
        "silver_row_count": silver_row_count,
        "confirmed_through": confirmed_through.strftime("%Y-%m-%d"),
        "status": "COMPLETE",
        "processed_at": processed_at,
    }


def _prepare_current_day_promotion(
    catalog, silver_table, promotion_meta: dict, confirmed_through: date
) -> dict | None:
    """
    Asset이 지정한 정확한 promotion만 검증 후 당일 파티션을 처리하고 Silver COMPLETE
    marker 문서를 구성한다. marker 저장(put_json)은 하지 않는다 - #137이 확정
    워터마크와 당일 promotion 처리를 둘 다 성공해야 확정 워터마크를 전진시키도록
    요구하므로, 워터마크를 쓴 다음에야 marker를 최종적으로 남겨야 한다
    (run()의 `_persist_current_day_promotion_marker` 호출 참고 - marker-last 보장 유지).

    확정 워터마크는 이 함수에서 건드리지 않는다 - 이 파티션은 아직 확정 후보가
    아니라서 다음 실행에서 확정 backlog로 다시 덮어써질 수 있다.

    marker.t0_enabled=false로 처리를 건너뛰면 None을 돌려준다.
    """
    bucket = config.SETTINGS.raw_bucket
    marker = get_json(bucket, promotion_meta["promotion_key"])
    marker = _validate_bronze_marker(
        marker,
        promotion_meta["run_date"],
        promotion_meta["promotion_id"],
        promotion_meta["promotion_key"],
    )

    if marker.get("t0_enabled") is not True:
        logger.info("Bronze marker t0_enabled=false - 당일 파티션 처리 건너뜀")
        return None

    run_date_str = promotion_meta["run_date"]
    entry = _find_current_day_entry(marker, run_date_str)
    if entry is None:
        raise SilverValidationError(
            f"Bronze marker에 당일({run_date_str}) 파티션 항목이 없음: "
            f"{promotion_meta['promotion_key']}"
        )

    run_date_value = date.fromisoformat(run_date_str)
    summary = _process_range(catalog, silver_table, run_date_value, run_date_value)
    silver_row_count = summary["silver_row_count"]

    document = _build_silver_promotion_document(
        run_date_str=run_date_str,
        promotion_id=promotion_meta["promotion_id"],
        source_bronze_promotion_key=promotion_meta["promotion_key"],
        entry=entry,
        bronze_mode=marker.get("mode"),
        silver_row_count=silver_row_count,
        confirmed_through=confirmed_through,
        processed_at=datetime.now(timezone.utc).isoformat(),
    )
    return {
        "bucket": bucket,
        "key": _silver_promotion_key(run_date_str, promotion_meta["promotion_id"]),
        "document": document,
    }


def _persist_current_day_promotion_marker(prepared: dict) -> None:
    """_prepare_current_day_promotion이 구성한 문서를 실제로 S3에 남긴다 (marker-last)."""
    put_json(prepared["bucket"], prepared["key"], prepared["document"])
    document = prepared["document"]
    logger.info(
        "Silver promotion marker 기록 완료: run_date=%s promotion_id=%s mode=%s row_count=%d",
        document["run_date"], document["promotion_id"], document["mode"], document["silver_row_count"],
    )


def _write_backfill_completion_marker(range_start: str, range_end: str, summary: dict) -> None:
    """청크 COMPLETE marker를 남긴다 - 두 Iceberg 테이블 쓰기가 모두 성공한 뒤 마지막에만.

    실패 marker는 만들지 않는다. "marker가 없다 = 미완료"라는 단일 규칙이 planner와
    finalizer 양쪽에서 그대로 성립해야, 어떤 실패 지점에서 죽든 재실행이 수렴한다.

    dag_run_id는 감사용이다 - BashOperator가 넣어주는 AIRFLOW_CTX_DAG_RUN_ID를 쓰고,
    marker 매칭에는 관여하지 않는다(다른 Run이 만든 marker도 재사용해야 하므로).
    """
    marker = build_completion_marker(
        range_start=range_start,
        range_end=range_end,
        bronze_watermark_at_start=(os.getenv("BRONZE_WATERMARK_AT_START") or "").strip() or None,
        bronze_row_count=summary["bronze_row_count"],
        silver_row_count=summary["silver_row_count"],
        quarantine_row_count=summary["quarantine_row_count"],
        dag_run_id=(
            os.getenv("DAG_RUN_ID") or os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or "unknown"
        ),
        processed_at=datetime.now(timezone.utc).isoformat(),
    )
    key = write_completion_marker(config.SETTINGS.raw_bucket, marker)
    logger.info(
        "청크 COMPLETE marker 기록: %s (Bronze %d행 -> Silver %d행, quarantine %d행)",
        key, marker["bronze_row_count"], marker["silver_row_count"], marker["quarantine_row_count"],
    )


def run() -> None:
    # 백필/daily_batch를 안 거치고 이 잡만 단독 실행하는 경우에도 안전하도록 버킷을 보장
    # (raw_bucket에는 워터마크 JSON이 있음)
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    catalog = build_iceberg_catalog()
    silver_table = _ensure_silver_table(catalog)

    # #232: 백필 DAG가 청크를 dynamic task mapping으로 독립 실행할 때 쓰는 경로.
    # 워터마크를 읽지도 쓰지도 않는다 - 병렬로 도는 청크들이 서로 다른 시점의
    # 워터마크를 읽어 겹치거나, 워터마크를 여러 번 잘못된 순서로 쓰는 걸 막기 위함.
    # 워터마크 전진은 airflow/dags/bronze_initial_load_all_sources_dag.py의 마무리
    # 태스크(advance_silver_rental_history_watermark)가 연속 완료 구간까지만 담당한다.
    range_start = os.getenv("BACKFILL_RANGE_START")
    range_end = os.getenv("BACKFILL_RANGE_END")
    if range_start and range_end:
        start_date = datetime.strptime(range_start, "%Y-%m-%d").date()
        end_date = datetime.strptime(range_end, "%Y-%m-%d").date()
        try:
            summary = _process_range(catalog, silver_table, start_date, end_date)
        except SilverValidationError as e:
            logger.error("%s~%s 처리 실패, 배치 중단: %s", start_date, end_date, e)
            sys.exit(1)
        _write_backfill_completion_marker(range_start, range_end, summary)
        return

    # 상한선: Bronze가 확정 커밋한 날짜 (rental_history 기본 워터마크 키)
    bronze_watermark = read_watermark()
    # 하한선: Silver 전용 워터마크
    silver_watermark = read_watermark(watermark_key=SILVER_WATERMARK_KEY)

    start_date = silver_watermark + timedelta(days=1)
    confirmed_end_date = bronze_watermark

    # 미지정이면 DEFAULT_MAX_DAYS_PER_RUN을 대신 써서 항상 상한을 건다
    max_days = os.getenv("MAX_DAYS_PER_RUN") or str(DEFAULT_MAX_DAYS_PER_RUN)
    capped_end = start_date + timedelta(days=int(max_days) - 1)
    if capped_end < confirmed_end_date:
        logger.info(
            "MAX_DAYS_PER_RUN=%s 적용 - 이번 실행은 %s ~ %s까지만 처리 (원래 끝: %s)",
            max_days, start_date, capped_end, confirmed_end_date,
        )
        confirmed_end_date = capped_end

    has_confirmed_range = start_date <= confirmed_end_date

    # #137: T0_ENABLED가 꺼져 있으면 metadata가 있어도 기존 T-1 처리만 유지하고
    # Silver promotion marker를 만들지 않는다.
    t0_enabled = _parse_bool_env("RENTAL_HISTORY_T0_ENABLED")
    promotion_meta = _load_bronze_promotion_metadata() if t0_enabled else None

    # 확정 신규 날짜가 없어도 당일 promotion이 있으면 조기 반환하지 않는다.
    if not has_confirmed_range and promotion_meta is None:
        logger.info(
            "처리할 신규 날짜 없음 (Silver 워터마크=%s, Bronze 워터마크=%s)",
            silver_watermark, bronze_watermark,
        )
        return

    try:
        confirmed_through = silver_watermark
        if has_confirmed_range:
            _process_range(catalog, silver_table, start_date, confirmed_end_date)
            confirmed_through = confirmed_end_date

        # #137: 확정 구간과 당일 promotion 처리가 둘 다 성공해야 확정 워터마크를
        # 전진시킨다 - 당일 처리 실패로 예외가 나면 아래 write_watermark 호출 전에
        # SilverValidationError로 빠져나가 워터마크가 조기 전진하지 않는다.
        prepared_promotion = None
        if promotion_meta is not None:
            prepared_promotion = _prepare_current_day_promotion(
                catalog, silver_table, promotion_meta, confirmed_through
            )

        if has_confirmed_range:
            write_watermark(confirmed_end_date, watermark_key=SILVER_WATERMARK_KEY)

        # marker는 워터마크를 쓴 다음에도 항상 마지막에 남긴다 (marker-last 보장).
        if prepared_promotion is not None:
            _persist_current_day_promotion_marker(prepared_promotion)
    except SilverValidationError as e:
        logger.error("%s~%s 처리 실패, 배치 중단: %s", start_date, confirmed_end_date, e)
        sys.exit(1)


if __name__ == "__main__":
    run()
