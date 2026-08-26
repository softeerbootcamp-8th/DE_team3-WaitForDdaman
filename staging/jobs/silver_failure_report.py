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

### overwrite: 파티션별이 아니라 전량 교체
이 잡은 매번 브론즈 전체를 재처리하므로 결과가 곧 테이블 전체다. 등록일 파티션마다
overwrite_partition()을 반복하면 커밋이 파티션 수만큼 쪼개져 느리고, 이번 입력에
더 이상 없는 과거 파티션이 남는다. common/iceberg_io.py의 overwrite_all()로 커밋
1회에 전량 교체한다 - 재실행해도 결과가 같다(멱등).

### 단일 DAG (전체 재처리), Asset 기반 스케줄
자세한 이유는 airflow/dags/silver_failure_report_dag.py 참고.

사용법:
    python -m jobs.silver_failure_report
"""
import logging
import sys

import duckdb
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField, StringType, TimestamptzType

from common.duckdb_io import query_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import overwrite_all
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

BRONZE_FIELDS = ("bike_no", "reg_dttm", "failure_type")

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
        strftime(reg_dttm_ts, '%Y-%m-%d')              AS reg_date_partition
    FROM (
        SELECT
            bike_no,
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


def transform(bronze_table: pa.Table, con: duckdb.DuckDBPyConnection | None = None) -> pa.Table:
    """bronze.failure_report 전체를 확정 스키마 + 파티션 컬럼으로 변환한다. 읽기/쓰기를 하지 않는다."""
    conn = con or _connect()
    conn.register("bronze_failure_report", bronze_table)
    result = query_arrow(conn, TRANSFORM_SQL)
    return result.select(SILVER_COLUMNS)


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


def _silver_row_count(catalog) -> int:
    try:
        table = catalog.load_table(SILVER_TABLE)
    except NoSuchTableError:
        return 0
    return len(table.scan(selected_fields=("bike_no",)).to_arrow())


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

    logger.warning(
        "%s의 파티션 스펙이 %s라 %s identity로 재생성한다 (#143). 이 실행이 전량 재적재하므로 "
        "데이터 손실은 없다.",
        SILVER_TABLE, current_partition_columns, PARTITION_COLUMN,
    )
    catalog.drop_table(SILVER_TABLE)
    return catalog.create_table(
        SILVER_TABLE,
        schema=SILVER_SCHEMA,
        partition_spec=SILVER_PARTITION_SPEC,
        properties=SILVER_PROPERTIES,
    )


def run() -> None:
    catalog = build_iceberg_catalog()

    # Bronze 테이블을 읽기 "전에" Bronze 워터마크를 고정한다 (#286).
    # 이 값은 성공 후 Silver 워터마크에 그대로 기록되므로, 아래 read보다 늦게 읽으면
    # 그 사이 다른 writer(bronze_catchup_all_sources의 advance_failure_report_watermark,
    # 06:00 일배치, 수동 set_watermark)가 Bronze 워터마크를 전진시켰을 때 Silver가
    # 자기 테이블에 없는 날짜까지 "처리 완료"로 선언한다. 과소 보고는 다음 실행이
    # 흡수하므로 안전하지만(전량 재구축), 과대 보고는 하류 check_silver_watermark를
    # 통과시켜 Gold가 없는 데이터로 돌게 한다 - 부등호 방향을 여기서 고정한다.
    # initial_load_dag의 planner가 bronze_watermark_at_start를 시작 시점에 고정하는
    # 것과 같은 이유다.
    bronze_watermark = read_watermark(watermark_key=BRONZE_FAILURE_REPORT)

    bronze_table = catalog.load_table(BRONZE_TABLE).scan(selected_fields=BRONZE_FIELDS).to_arrow()
    bronze_count = len(bronze_table)
    logger.info("bronze.failure_report: %d행", bronze_count)

    if bronze_count == 0:
        logger.error("브론즈에 데이터가 없음 - 브론즈 적재 완료 여부 확인 필요")
        sys.exit(1)

    # 파티션 스펙 재생성 전에 직전 실버 행수를 읽어야 한다 - 재생성이 먼저면 0행이 되어
    # 부분 적재 방어선이 통째로 무력화된다.
    prev_silver_count = _silver_row_count(catalog)
    is_partial, reason = evaluate_partial_load(bronze_count, prev_silver_count)
    logger.info(reason)
    if is_partial:
        logger.error("파이프라인 중단: %s", reason)
        sys.exit(1)

    silver_arrow = transform(bronze_table)

    errors = validate(silver_arrow)
    if errors:
        for e in errors:
            logger.error("검증 실패: %s", e)
        sys.exit(1)

    silver_table = _ensure_silver_table(catalog)
    row_count = len(silver_arrow)
    overwrite_all(silver_table, silver_arrow)

    # overwrite가 끝난 뒤에만 쓴다 - 부분 실패에서 워터마크를 전진시키지 않는다는
    # 계약(common/watermark.py)은 그대로다. 다만 쓰는 값은 여기서 다시 읽지 않고
    # Bronze를 읽기 전에 고정해둔 값이다(위 주석 참고).
    write_watermark(bronze_watermark, watermark_key=SILVER_FAILURE_REPORT)

    logger.info("전량 덮어쓰기 완료: %d행 (Silver 워터마크=%s)", row_count, bronze_watermark)


if __name__ == "__main__":
    run()
