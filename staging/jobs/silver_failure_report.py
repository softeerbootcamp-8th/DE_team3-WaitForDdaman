"""
Silver - 공공자전거 고장신고 내역 (bronze.failure_report -> silver.failure_report)

### 확정 스키마 (변경 금지)
    bike_no       STRING
    reg_dttm      TIMESTAMP   (브론즈 STRING -> 캐스팅)
    failure_type  STRING      (trim)

유일키: (bike_no, reg_dttm, failure_type)
그레인: 고장부위 신고 1건 = 1행 (이벤트 단위로 접지 않는다)

### ⚠️ 파티션 컬럼: reg_date_partition (Gold 담당자와의 인터페이스 계약, #143)
파티션이 hidden transform `days(reg_dttm)`에서 **identity 날짜 문자열 컬럼
`reg_date_partition`(yyyy-MM-dd)**으로 바뀌었다. 값은 `date(reg_dttm)`을
문자열로 포맷한 것이다.

  - 컬럼명을 `reg_date_partition`으로 정한 이유: silver.rental_history가 이미
    이벤트 날짜 identity 파티션을 `rent_date_partition`으로 부르고 있어 같은
    규칙을 따른다. Silver 안에서 두 이름 규칙이 갈리지 않게 한다.
  - **이 컬럼은 테이블 스키마의 일부다** (4번째 컬럼). Iceberg의 identity 파티션은
    hidden transform과 달리 원본 컬럼이 스키마에 실제로 존재해야 하기 때문에
    선택지가 없다. 다만 위의 "확정 스키마" 3컬럼은 이름·타입·의미가 전혀 바뀌지
    않았고, 3컬럼만 select하는 다운스트림(risk_model 등)은 영향을 받지 않는다.
  - **브론즈의 동명 컬럼과 의미가 다르다.** bronze.failure_report의
    `reg_date_partition`은 "API를 조회한 적재일"이고, 여기 실버의
    `reg_date_partition`은 "고장 등록일(reg_dttm의 날짜)"이다. 실제로 브론즈
    적재일 파티션 하나 안에 실버 등록일 파티션 여러 개가 섞여 나온다.

왜 바꿨나: pyiceberg는 transform 파티션 쓰기에 제약이 있어서 Spark 없이 이 잡을
돌리려면 identity 파티션이어야 한다. 기존 테이블은 스펙이 다르므로 이 잡이
처음 도는 시점에 한 번 재생성된다(_ensure_silver_table 참고) - 원래부터 매번
브론즈 전체를 재처리하는 잡이라 재생성 직후 한 번 돌리면 복구가 끝난다.

### 단일 태스크로 통합한 이유 (#82) - 유지
원래 check -> transform -> validate -> overwrite -> metrics 5단계로 분할돼 있었으나,
실측 결과 이 데이터 규모(3.9MB, 229개 파일)에서는 분할 안 했을 때보다 3.8배 느렸다
(50.5s vs 13.4s). 태스크당 세션 기동 비용만으로는 설명이 안 되고, 각 단계가 staging
Iceberg 테이블에 썼다가 다시 읽는 구조라 매 단계 S3 I/O 왕복이 추가로 붙는 게
원인이었다. 분할이 주던 이점(단계별 실패 지점 구분)은 로그 메시지로 대체하고,
staging 테이블 없이 한 프로세스 안에서 PyArrow Table을 그대로 넘긴다.
Spark를 걷어낸 뒤에도(#143) 이 판단은 그대로다 - 세션 기동 비용이 사라졌을 뿐
단계별 S3 왕복 비용은 그대로 남기 때문이다.

### Spark 제거 (#143)
타입 캐스팅 + trim + 전체 재처리(341행~수만 행)라 분산 엔진이 필요 없다. 읽기/쓰기는
pyiceberg, 변환은 DuckDB SQL로 옮긴다. DuckDB를 쓰는 이유는 볼륨이 아니라 기존
Spark SQL 표현(regexp_extract / to_date / trim)을 번역 오류 없이 옮기기 위해서다.

### check: 브론즈 부분 적재 방어
단일 DAG(매일 브론즈 전체 재처리 + 전량 덮어쓰기)에서는 이 검증이 실버를 지키는
첫 번째 방어선이다(두 번째는 validate). ingestion/jobs/initial_load_failure_report.py의
브론즈 적재는 파일 단위로 개별 커밋되고 배치 전체를 감싸는 트랜잭션이 없어서, 배치
도중 실패하면 브론즈가 부분 적재 상태로 남을 수 있다. 이 상태로 그대로 재처리하면
전량 덮어쓰기가 멀쩡하던 실버를 부분 데이터로 통째로 교체해버리므로, 브론즈
현재 행수가 직전 실버 행수의 MIN_BRONZE_TO_PREV_SILVER_RATIO 미만이면 중단한다.
직전 실버가 없거나(최초 실행) 0행이면 비교 기준이 없으므로 통과시킨다.

### transform
- reg_dttm: STRING -> TIMESTAMP. 원본 포맷이 'yyyy-MM-dd HH:mm:ss'(19자, 2026-01~06
  API 수집분 실측 100%)와 'yyyyMMdd'(일부 초기 적재 파일, ingestion/jobs/
  initial_load_failure_report.py:_derive_date_partition 주석 참고) 둘 다 나타날 수
  있어 순서대로 시도한다. 원본 시각 표기가 파일마다 제각각(0패딩 유무, 구분자
  -/./없음, 초/시각 유무 자체가 다름)이라 모든 변형에 안전한 timestamp 패턴을 다
  나열하는 대신 날짜 부분만 뽑아 자정(00:00:00) TIMESTAMP로 통일한다. 다운스트림
  (risk_model)이 reg_dttm을 날짜 단위로만 쓰고 있어 시각 정밀도가 필요 없고, bronze가
  원본 문자열을 그대로 보존하므로 나중에 시각까지 필요해지면 거기서 다시 뽑아 쓸 수
  있다.
- failure_type: 뒤쪽 공백 제거(trim) - bronze CSV 파서가 ignoreTrailingWhiteSpace=false라
  '기타 ', '타이어 ' 같은 원본 공백이 그대로 살아있다.
- bike_no: 원본 그대로 (형식 100% 검증된 값이라 추가 정제 불필요).
- reg_date_partition: reg_dttm을 자정으로 통일한 뒤 그 날짜를 문자열로 포맷한 값.

집계/조인 없음 - 그레인은 bronze 1행 = silver 1행 그대로 유지한다.

### validate
- 필수 컬럼(bike_no, reg_dttm, failure_type) null 없음 (실패 시 파이프라인 중단).
  reg_dttm null은 위 STRING->TIMESTAMP 캐스팅이 못 잡은 포맷 편차 의심.
- 유일키(bike_no, reg_dttm, failure_type) 중복은 경고만 하고 통과시킨다 - reg_dttm을
  날짜 단위(자정)로 통일하기로 하면서 같은 날 같은 자전거가 같은 고장유형으로 여러 번
  신고되면 자연스럽게 발생하는 중복이라 더 이상 이례적인 실패 조건이 아니다.

### 쓰기: 확정 구간 증분 + 미확정 tail 재계산 (#288)
예전엔 매번 Bronze 전체를 읽어 overwrite_all()로 통째 교체했다. 비용이 데이터 총량에
비례해 계속 커지므로, Bronze가 이미 하는 확정/미확정 구분을 그대로 따라간다.

  확정 구간 [Silver WM+1, Bronze WM]  -> replace_range, 워터마크 전진
  미확정 tail (> Bronze WM)           -> replace_range, 워터마크 미갱신

미확정 tail은 daily_batch_failure_report가 FAILURE_REPORT_T0_ENABLED로 워터마크를
올리지 않고 적재해둔 당일 파티션이다. 그 끝은 Bronze의 MAX(파티션)을 매니페스트에서
읽어 구한다 - FAILURE_REPORT_BRONZE Asset은 extra를 싣지 않으므로(rental_history와
다른 점) Bronze 상태에서 직접 판단해야 하고, 그래야 catch-up 트리거(#286)처럼
metadata 없는 경로에서도 동작한다.

구간 교체는 "데이터가 있는 날짜"가 아니라 "선언 구간"을 지운다. 0행 결과여도 구간을
비워야 재실행 결과가 행 수와 내용까지 같아진다(common/iceberg_io.py replace_range 참고).

### quarantine: 유도 신고일 != 선언 신고일
Bronze 파티션은 "선언" 신고일(API 조회 대상일, daily_batch_failure_report가 루프
날짜를 그대로 찍는다)이고 Silver 파티션은 "유도" 신고일(date(reg_dttm))이다. 두 값이
어긋나는 행은 Bronze 파티션 D에서 Silver 파티션 D'로 옮겨가는데, 그러면 구간 교체가
정확할 수 없다 - D'가 교체 구간 밖이면 옛 행이 안 지워져 재실행마다 중복이 쌓이고,
교체 구간 안의 D'가 구간 밖 Bronze에서 기여받은 행을 갖고 있으면 삭제 후 복원되지 않는다.
불일치 행을 silver.failure_report_quarantine으로 격리하면 남은 행은 전부 유도 == 선언이라
Silver 파티션 == Bronze 파티션이 성립한다. Silver 파티션의 정의는 바꾸지 않는다(#143 계약).

### 단일 DAG, Asset 기반 스케줄
자세한 이유는 airflow/dags/silver_failure_report_dag.py 참고.

사용법:
    python -m jobs.silver_failure_report
"""
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField, StringType, TimestamptzType

from common.duckdb_io import query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import build_range_filter, replace_range
from common.sql_assert import QualityCheck
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import BRONZE_FAILURE_REPORT, SILVER_FAILURE_REPORT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BRONZE_TABLE = "bronze.failure_report"
SILVER_TABLE = "silver.failure_report"

# ⚠️ Gold 담당자와의 인터페이스 계약 - 위 docstring 참고. 이름을 바꾸면 그쪽 파티션
# 필터가 같이 바뀌어야 한다.
PARTITION_COLUMN = "reg_date_partition"

MIN_BRONZE_TO_PREV_SILVER_RATIO = 0.95  # 잠정치 - 실측 후 조정

REQUIRED_COLUMNS = ("bike_no", "reg_dttm", "failure_type")

SILVER_COLUMNS = ["bike_no", "reg_dttm", "failure_type", PARTITION_COLUMN]

# Bronze의 파티션 컬럼(선언 신고일)도 함께 읽는다 - 유도 신고일(date(reg_dttm))과
# 어긋나는 행을 골라내는 기준이고, quarantine 테이블의 구간 교체 키로도 쓴다.
BRONZE_FIELDS = ("bike_no", "reg_dttm", "failure_type", PARTITION_COLUMN)

# Bronze가 선언한 신고일. transform()이 유도한 PARTITION_COLUMN과 비교해 불일치를 판정한다.
DECLARED_COLUMN = "declared_reg_date"

# 0행 구간을 교체할 때 쓸 빈 테이블의 스키마. SILVER_SCHEMA(Iceberg)와 1:1로 맞춘다.
SILVER_ARROW_SCHEMA = pa.schema([
    pa.field("bike_no", pa.string()),
    pa.field("reg_dttm", pa.timestamp("us", tz="UTC")),
    pa.field("failure_type", pa.string()),
    pa.field(PARTITION_COLUMN, pa.string()),
])

QUARANTINE_TABLE = "silver.failure_report_quarantine"

# silver.rental_history_quarantine과 동일 convention: 원본 컬럼 + 격리 사유/시각.
# 언파티션 - 볼륨이 작고 조회가 감사 목적이다. 구간 교체는 DECLARED_COLUMN 필터로 한다.
QUARANTINE_SCHEMA = Schema(
    NestedField(1, "bike_no", StringType(), required=False),
    NestedField(2, "reg_dttm", TimestamptzType(), required=False),
    NestedField(3, "failure_type", StringType(), required=False),
    NestedField(4, PARTITION_COLUMN, StringType(), required=False, doc="유도 신고일 date(reg_dttm)"),
    NestedField(5, DECLARED_COLUMN, StringType(), required=False, doc="Bronze가 선언한 신고일"),
    NestedField(6, "quarantine_reason", StringType(), required=False),
    NestedField(7, "quarantined_at", TimestamptzType(), required=False),
)
QUARANTINE_ARROW_SCHEMA = pa.schema([
    pa.field("bike_no", pa.string()),
    pa.field("reg_dttm", pa.timestamp("us", tz="UTC")),
    pa.field("failure_type", pa.string()),
    pa.field(PARTITION_COLUMN, pa.string()),
    pa.field(DECLARED_COLUMN, pa.string()),
    pa.field("quarantine_reason", pa.string()),
    pa.field("quarantined_at", pa.timestamp("us", tz="UTC")),
])

# 이 비율을 넘는 불일치는 구조적 이상으로 보고 배치를 막는다 (rental_history와 동일 발상).
DEFAULT_MAX_QUARANTINE_RATIO = 0.01

# 한 실행이 소화할 확정 날짜 수 상한. transform_silver_rental_history와 같은 규칙 -
# 오래 밀린 워터마크가 한 번에 수개월을 처리하지 않게 자른다.
DEFAULT_MAX_DAYS_PER_RUN = 31

SILVER_SCHEMA = Schema(
    NestedField(1, "bike_no", StringType(), required=False, doc="자전거 번호"),
    NestedField(2, "reg_dttm", TimestamptzType(), required=False, doc="고장 등록일시 (날짜 단위 자정으로 통일)"),
    NestedField(3, "failure_type", StringType(), required=False, doc="고장 유형 (trim)"),
    NestedField(
        4, PARTITION_COLUMN, StringType(), required=False,
        doc="고장 등록일(yyyy-MM-dd), identity 파티션 키. 브론즈의 동명 컬럼(적재일)과 의미가 다름",
    ),
)
SILVER_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name=PARTITION_COLUMN)
)
SILVER_PROPERTIES = {"write.distribution-mode": "hash"}

# Spark 표현의 1:1 번역.
#   regexp_extract(reg_dttm, r"(\d{4}[-.]\d{1,2}[-.]\d{1,2})", 1) -> 날짜 부분만 추출
#   regexp_replace(..., r"\.", "-")                               -> 2026.01.02 -> 2026-01-02
#   to_date(date_only, "yyyy-M-d") / to_date(reg_dttm, "yyyyMMdd")-> 두 포맷 순서대로 시도
# DuckDB의 %m/%d도 1~2자리를 모두 받으므로 "yyyy-M-d"와 동일하게 동작한다.
# 추출 실패 시 regexp_extract는 ''를 돌려주고 try_strptime('')은 NULL이라 다음 후보로 넘어간다.
# 정규식의 `\d{4}` 같은 중괄호 때문에 f-string/str.format을 못 쓴다 - 입력 뷰 이름은
# transform()이 항상 같은 이름으로 register하므로 그냥 상수로 박는다.
TRANSFORM_SQL = """
    SELECT
        bike_no,
        reg_dttm_ts                                    AS reg_dttm,
        failure_type,
        strftime(reg_dttm_ts, '%Y-%m-%d')              AS reg_date_partition,
        declared_reg_date
    FROM (
        SELECT
            bike_no,
            reg_date_partition                         AS declared_reg_date,
            CAST(
                COALESCE(
                    try_strptime(
                        replace(regexp_extract(reg_dttm, '(\\d{4}[-.]\\d{1,2}[-.]\\d{1,2})', 1), '.', '-'),
                        '%Y-%m-%d'
                    ),
                    try_strptime(reg_dttm, '%Y%m%d')
                ) AS TIMESTAMPTZ
            )                                          AS reg_dttm_ts,
            trim(failure_type)                         AS failure_type
        FROM bronze_failure_report
    )
"""


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    # TIMESTAMP -> TIMESTAMPTZ 캐스팅과 strftime이 세션 타임존을 따르므로 UTC로 못박는다.
    # 컨테이너 TZ에 결과가 흔들리면 파티션 값이 하루씩 밀 수 있다.
    con.execute("SET TimeZone='UTC'")
    return con


def evaluate_partial_load(
    bronze_count: int,
    prev_silver_count: int,
    threshold: float = MIN_BRONZE_TO_PREV_SILVER_RATIO,
) -> tuple[bool, str]:
    """브론즈가 직전 실버 대비 threshold 미만으로 줄었으면 (True, 사유)를 반환한다."""
    if prev_silver_count == 0:
        return False, "직전 silver 데이터 없음 - 비율 체크 스킵"

    ratio = bronze_count / prev_silver_count
    reason = f"브론즈 {bronze_count}행 / 직전 실버 {prev_silver_count}행 = {ratio:.1%}"
    if ratio < threshold:
        return True, f"{reason} (임계값 {threshold:.0%} 미만 - 브론즈 부분 적재 의심)"
    return False, reason


def transform_with_declared(
    bronze_table: pa.Table, con: duckdb.DuckDBPyConnection | None = None
) -> pa.Table:
    """확정 스키마 + 유도 신고일에 Bronze의 선언 신고일까지 붙여 돌려준다.

    불일치 판정과 quarantine 구간 교체가 선언 신고일을 필요로 하므로, 두 값을 같은
    SQL 한 번에서 함께 뽑는다 - 따로 계산하면 유도 규칙이 두 곳으로 갈린다.
    """
    conn = con or _connect()
    conn.register("bronze_failure_report", bronze_table)
    result = query_arrow(conn, TRANSFORM_SQL)
    return result.select(SILVER_COLUMNS + [DECLARED_COLUMN])


def transform(bronze_table: pa.Table, con: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
    """확정 스키마 + 유도 파티션 컬럼만 남긴다 (Silver 본 테이블 스키마). 읽기/쓰기를 하지 않는다."""
    return transform_with_declared(bronze_table, con).select(SILVER_COLUMNS)


def validate(silver_table: pa.Table) -> list[str]:
    """필수 컬럼 null을 검사하고 에러 목록을 반환한다. 유일키 중복은 경고만 로그로 남긴다."""
    check = QualityCheck("silver_failure_report")
    for column in REQUIRED_COLUMNS:
        check = check.is_complete(column)
    result = check.run(silver_table)

    errors = []
    for constraint in result.failed_constraints:
        col_name = constraint.name[len("isComplete(") : -1]
        suffix = " (캐스팅 실패 의심)" if col_name == "reg_dttm" else ""
        errors.append(f"{col_name} null {constraint.violation_count}행{suffix}")

    con = _connect()
    con.register("silver_failure_report", silver_table)
    dup_count = con.execute(
        f"SELECT count(*) FROM (SELECT {', '.join(REQUIRED_COLUMNS)} FROM silver_failure_report "
        f"GROUP BY ALL HAVING count(*) > 1)"
    ).fetchone()[0]
    if dup_count:
        logger.warning("유일키%s 중복 %d건 (날짜 단위 통일에 따른 예상된 중복 - 통과)", REQUIRED_COLUMNS, dup_count)

    return errors


def _ensure_quarantine_table(catalog):
    """quarantine 테이블이 없으면 만든다 (silver.rental_history_quarantine과 동일 패턴)."""
    try:
        return catalog.load_table(QUARANTINE_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성", QUARANTINE_TABLE)
        return catalog.create_table(QUARANTINE_TABLE, schema=QUARANTINE_SCHEMA)


def _read_bronze_range(catalog, start_str: str, end_str: str) -> pa.Table:
    """Bronze의 선언 신고일이 [start, end]인 행만 읽는다 (전체 스캔 제거)."""
    return (
        catalog.load_table(BRONZE_TABLE)
        .scan(
            row_filter=build_range_filter(PARTITION_COLUMN, start_str, end_str),
            selected_fields=BRONZE_FIELDS,
        )
        .to_arrow()
    )


def bronze_max_partition(catalog) -> Optional[date]:
    """Bronze의 MAX(선언 신고일). 매니페스트만 읽어 데이터 파일은 건드리지 않는다.

    미확정 tail(Bronze 워터마크보다 뒤에 있는 파티션)의 끝을 구하는 데 쓴다.
    rental_history는 Bronze Asset의 extra로 당일 파티션을 지정받지만
    FAILURE_REPORT_BRONZE는 extra를 싣지 않으므로, Bronze 상태에서 직접 읽는다 -
    catch-up 트리거(#286)처럼 metadata 없는 경로에서도 그대로 동작한다.

    reg_dttm이 NULL인 원본 행은 파티션 값이 ""로 떨어지므로 걸러낸다
    (ingestion/jobs/check_watermark_date.py의 _max_partition_value와 동일한 이유).
    """
    try:
        table = catalog.load_table(BRONZE_TABLE)
    except NoSuchTableError:
        return None
    values = [
        row["partition"].get(PARTITION_COLUMN)
        for row in table.inspect.partitions().to_pylist()
    ]
    values = [v for v in values if v]
    if not values:
        return None
    return date.fromisoformat(max(values))


def _split_partition_mismatch(silver_with_declared: pa.Table) -> tuple[pa.Table, pa.Table]:
    """유도 신고일이 선언 신고일과 다른 행을 quarantine으로 분리한다.

    왜 필요한가: Bronze 파티션은 "선언" 신고일(API 조회 대상일)이고 Silver 파티션은
    "유도" 신고일(date(reg_dttm))이다. 두 값이 어긋나는 행은 Bronze 파티션 D에서
    Silver 파티션 D'로 옮겨가는데, 그러면 구간 교체가 정확할 수 없다 - D'가 교체 구간
    밖이면 옛 행이 안 지워져 재실행마다 중복이 쌓이고, 교체 구간 안의 D'가 구간 밖
    Bronze에서 기여받은 행을 갖고 있으면 삭제 후 복원되지 않는다.

    불일치를 격리하면 남은 행은 전부 유도 == 선언이므로 Silver 파티션 == Bronze 파티션이
    성립하고, replace_range가 선언 구간을 정확히 교체한다. Silver 파티션의 정의
    (date(reg_dttm))는 바꾸지 않는다 - Gold 인터페이스 계약(#143) 유지.
    """
    derived = silver_with_declared.column(PARTITION_COLUMN)
    declared = silver_with_declared.column(DECLARED_COLUMN)
    matched = pc.equal(derived, declared)
    # null은 equal()에서 null이 되어 filter가 버린다 - 명시적으로 불일치로 몰아둔다.
    matched = pc.fill_null(matched, False)

    clean = silver_with_declared.filter(matched).select(SILVER_COLUMNS)
    violations = silver_with_declared.filter(pc.invert(matched))
    if len(violations) == 0:
        return clean, QUARANTINE_ARROW_SCHEMA.empty_table()

    quarantined = violations.append_column(
        "quarantine_reason",
        pa.array(["derived_reg_date != declared_reg_date"] * len(violations), pa.string()),
    ).append_column(
        "quarantined_at",
        pa.array([datetime.now(timezone.utc)] * len(violations), pa.timestamp("us", tz="UTC")),
    )
    return clean, quarantined.select(QUARANTINE_ARROW_SCHEMA.names).cast(QUARANTINE_ARROW_SCHEMA)


def _silver_row_count(catalog, start_str: str, end_str: str) -> int:
    """선언 구간에 해당하는 직전 Silver 행 수. 부분 적재 방어선의 비교 기준이다.

    전량 재처리 시절엔 테이블 전체 행 수를 셌지만, 이제 교체 단위가 구간이므로
    비교도 같은 구간으로 좁혀야 한다 - 전체와 비교하면 31일 구간을 처리할 때마다
    비율이 항상 임계치 밑으로 떨어져 매번 배치가 막힌다.
    """
    try:
        table = catalog.load_table(SILVER_TABLE)
    except NoSuchTableError:
        return 0
    scan = table.scan(
        row_filter=build_range_filter(PARTITION_COLUMN, start_str, end_str),
        selected_fields=("bike_no",),
    )
    return len(scan.to_arrow())


def _ensure_silver_table(catalog):
    """
    실버 테이블을 reg_date_partition identity 파티션으로 보장한다.

    기존 테이블은 `days(reg_dttm)` hidden transform 파티션이라 스펙이 다르다. Iceberg는
    파티션 스펙 진화를 지원하지만 옛 스펙으로 쓰인 데이터 파일이 그대로 남아 스펙 두 개가
    공존하게 되고, 컬럼 추가까지 겹치면 상태가 헷갈린다. 이 테이블은 원래부터 매번 브론즈
    전체를 재처리하는 구조라(워터마크 없음) 지우고 다시 만든 직후 한 번 돌리면 완전히
    복구된다 - 그래서 재생성이 가장 단순하고 안전하다.
    """
    catalog.create_namespace_if_not_exists("silver")

    try:
        table = catalog.load_table(SILVER_TABLE)
    except NoSuchTableError:
        logger.info("%s 테이블 신규 생성 (파티션=%s identity)", SILVER_TABLE, PARTITION_COLUMN)
        return catalog.create_table(
            SILVER_TABLE,
            schema=SILVER_SCHEMA,
            partition_spec=SILVER_PARTITION_SPEC,
            properties=SILVER_PROPERTIES,
        )

    current_partition_columns = [field.name for field in table.spec().fields]
    if current_partition_columns == [PARTITION_COLUMN]:
        return table

    # 전량 재처리 시절엔 여기서 drop+create해도 그 실행이 전부 다시 채웠다. 증분으로
    # 바뀐 뒤에는 워터마크 아래 구간을 다시 처리하지 않으므로 drop이 곧 데이터 손실이다.
    # 사람이 워터마크를 되돌려 재적재를 지시해야 한다.
    raise SilverFailureReportError(
        f"{SILVER_TABLE}의 파티션 스펙이 {current_partition_columns}라 "
        f"{PARTITION_COLUMN} identity와 다르다. 증분 처리는 이 테이블을 재생성할 수 없다 "
        f"(워터마크 아래 구간을 다시 채우지 않으므로 drop이 데이터 손실이 된다). "
        f"set_watermark로 Silver 워터마크를 되돌린 뒤 재적재할 것."
    )


class SilverFailureReportError(RuntimeError):
    """구간 처리 실패 - 워터마크를 전진시키지 않고 배치를 중단한다."""


def _process_range(catalog, silver_table, start_date: date, end_date: date) -> dict:
    """선언 구간 [start_date, end_date]를 이번 입력 결과로 완전히 교체한다.

    Bronze 전체를 읽지 않는다. 선언 신고일이 구간 안인 Bronze 행만 읽어 변환하고,
    quarantine과 본 테이블을 각각 구간 교체한다(커밋 2회).

    입력이 0행이어도 조기 반환하지 않는다 - 선언 구간을 0행으로 비워야 "이 구간은
    이번 결과로 완전히 교체됐다"가 성립한다. 실제 데이터가 있는 날짜만 교체하면
    이번에 사라진 날짜의 과거 행이 그대로 남는다(replace_range docstring 참고).
    """
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    label = start_str if start_date == end_date else f"{start_str}~{end_str}"

    bronze_arrow = _read_bronze_range(catalog, start_str, end_str)
    bronze_count = len(bronze_arrow)

    # 파티션 스펙 재생성 전에 직전 실버 행수를 읽어야 한다 - 재생성이 먼저면 0행이 되어
    # 부분 적재 방어선이 통째로 무력화된다.
    prev_silver_count = _silver_row_count(catalog, start_str, end_str)
    is_partial, reason = evaluate_partial_load(bronze_count, prev_silver_count)
    logger.info("%s: %s", label, reason)
    if is_partial:
        raise SilverFailureReportError(f"{label}: {reason}")

    if bronze_count == 0:
        logger.info("%s: Bronze에 처리할 데이터 없음 - 선언 구간을 0행으로 교체", label)
        clean = SILVER_ARROW_SCHEMA.empty_table()
        quarantined = QUARANTINE_ARROW_SCHEMA.empty_table()
    else:
        with_declared = transform_with_declared(bronze_arrow)

        # 캐스팅 실패(reg_dttm null)는 기존과 동일하게 배치를 중단시킨다 - 포맷 편차를
        # 조용히 quarantine으로 흘리지 않는다. validate가 유도 파티션 null도 함께 잡는다.
        errors = validate(with_declared.select(SILVER_COLUMNS))
        if errors:
            for e in errors:
                logger.error("%s 검증 실패: %s", label, e)
            raise SilverFailureReportError(f"{label}: 검증 실패 {errors}")

        clean, quarantined = _split_partition_mismatch(with_declared)

    quarantine_count = len(quarantined)
    if quarantine_count:
        ratio = quarantine_count / bronze_count
        max_ratio = float(os.getenv("MAX_QUARANTINE_RATIO") or DEFAULT_MAX_QUARANTINE_RATIO)
        if ratio > max_ratio:
            raise SilverFailureReportError(
                f"{label}: 신고일 불일치 비율 {ratio:.4f}가 임계치 {max_ratio}를 초과 - "
                f"구조적 이상 가능성으로 배치 중단 (불일치 {quarantine_count}/{bronze_count}행)"
            )
        logger.warning(
            "%s: 신고일 불일치 %d행(%.4f%%) quarantine 처리 - %s 확인 필요",
            label, quarantine_count, ratio * 100, QUARANTINE_TABLE,
        )

    # 쓰기 순서: quarantine -> 본 테이블. 서로 다른 Iceberg 테이블이라 한 트랜잭션으로
    # 묶을 수 없다. quarantine이 먼저 성공하고 본 테이블이 실패하면 짧은 불일치가 생기지만,
    # quarantine은 Gold의 입력이 아닌 감사 테이블이고 워터마크가 전진하지 않으므로
    # 재실행이 같은 구간을 다시 교체해 수렴한다.
    #
    # append가 아니라 구간 교체인 이유: append면 같은 구간을 재실행할 때마다 동일한
    # 불일치 행이 다시 쌓여 감사 건수가 부풀고 멱등성이 깨진다. quarantine의 구간 키는
    # 유도 신고일이 아니라 DECLARED_COLUMN이다 - 불일치 행의 유도 신고일은 정의상
    # 선언 구간 밖일 수 있어서 그걸로는 범위 단정(_assert_rows_within_range)을 통과할 수 없다.
    replace_range(
        _ensure_quarantine_table(catalog), quarantined, DECLARED_COLUMN,
        start_str, end_str, catalog=catalog,
    )
    replace_range(silver_table, clean, PARTITION_COLUMN, start_str, end_str, catalog=catalog)

    logger.info(
        "%s: Silver %d행 교체 완료 (Bronze %d행, quarantine %d행)",
        label, len(clean), bronze_count, quarantine_count,
    )
    return {
        "bronze_row_count": bronze_count,
        "silver_row_count": len(clean),
        "quarantine_row_count": quarantine_count,
    }


def resolve_confirmed_range(
    silver_watermark: date, bronze_watermark: date, max_days: Optional[str] = None
) -> Optional[tuple[date, date]]:
    """확정 구간 [silver WM+1, bronze WM]에 MAX_DAYS_PER_RUN 상한을 적용한다.

    처리할 신규 확정 날짜가 없으면 None.
    """
    start_date = silver_watermark + timedelta(days=1)
    end_date = bronze_watermark
    if start_date > end_date:
        return None

    limit = int(max_days or DEFAULT_MAX_DAYS_PER_RUN)
    capped_end = start_date + timedelta(days=limit - 1)
    if capped_end < end_date:
        logger.info(
            "MAX_DAYS_PER_RUN=%s 적용 - 이번 실행은 %s ~ %s까지만 처리 (원래 끝: %s)",
            limit, start_date, capped_end, end_date,
        )
        end_date = capped_end
    return start_date, end_date


def run() -> None:
    catalog = build_iceberg_catalog()

    # 워터마크를 Bronze 데이터보다 먼저 읽는다 (#286). 증분 구조에서는 이게 순서 문제가
    # 아니라 필연이다 - 두 값이 읽을 범위를 정하기 때문이다.
    bronze_watermark = read_watermark(watermark_key=BRONZE_FAILURE_REPORT)
    silver_watermark = read_watermark(watermark_key=SILVER_FAILURE_REPORT)

    silver_table = _ensure_silver_table(catalog)

    confirmed = resolve_confirmed_range(
        silver_watermark, bronze_watermark, os.getenv("MAX_DAYS_PER_RUN")
    )
    if confirmed is None:
        logger.info(
            "처리할 신규 확정 날짜 없음 (Silver 워터마크=%s, Bronze 워터마크=%s)",
            silver_watermark, bronze_watermark,
        )
    else:
        start_date, end_date = confirmed
        try:
            _process_range(catalog, silver_table, start_date, end_date)
        except SilverFailureReportError as e:
            logger.error("확정 구간 처리 실패, 배치 중단: %s", e)
            sys.exit(1)
        # 확정 구간이 성공한 뒤에만 전진한다. 값은 Bronze 워터마크 복사가 아니라
        # 이번 실행이 실제로 교체한 구간의 끝이다 - 상한(MAX_DAYS_PER_RUN)에 걸려
        # 뒷부분을 남겼을 때 처리하지 않은 날짜를 완료로 선언하지 않기 위함이다.
        write_watermark(end_date, watermark_key=SILVER_FAILURE_REPORT)
        logger.info("Silver 워터마크 전진: %s -> %s", silver_watermark, end_date)

    # 미확정 tail. Bronze가 워터마크를 올리지 않고 적재해둔 구간(FAILURE_REPORT_T0_ENABLED로
    # 적재되는 당일 파티션이 대표 사례)을 매 실행 다시 계산한다. 워터마크는 갱신하지 않는다 -
    # 아직 확정 후보가 아니라 다음 실행에서 확정 구간으로 다시 덮어써진다.
    #
    # 이 처리를 빼면 당일 고장신고가 Silver에서 사라진다. build_bike_features_daily가
    # FAILURE_REPORT_T0_ENABLED를 읽어 당일 고장신고를 위험도 피처에 반영하므로
    # 현재 동작이 깨진다.
    tail_end = bronze_max_partition(catalog)
    if tail_end is not None and tail_end > bronze_watermark:
        tail_start = bronze_watermark + timedelta(days=1)
        logger.info("미확정 tail 처리: %s ~ %s (워터마크 미갱신)", tail_start, tail_end)
        try:
            _process_range(catalog, silver_table, tail_start, tail_end)
        except SilverFailureReportError as e:
            logger.error("미확정 tail 처리 실패, 배치 중단: %s", e)
            sys.exit(1)


if __name__ == "__main__":
    run()
