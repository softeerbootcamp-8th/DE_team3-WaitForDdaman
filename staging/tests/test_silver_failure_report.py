"""
silver.failure_report 변환/검증 로직 테스트 (#143)

Spark를 걷어내고 DuckDB + pyiceberg로 옮기면서 확인해야 할 것:
  1. reg_dttm STRING -> TIMESTAMP 캐스팅이 기존 Spark 표현과 같은 포맷들을 받는가
  2. failure_type trim
  3. 새 파티션 컬럼 reg_date_partition = date(reg_dttm), identity 파티션 스펙
  4. 확정 3컬럼(bike_no, reg_dttm, failure_type)의 이름/타입이 안 바뀌었는가
"""
import os
from datetime import date, datetime, timezone

import pyarrow as pa
import pytest

from jobs.silver_failure_report import (
    DECLARED_COLUMN,
    DEFAULT_MAX_DAYS_PER_RUN,
    PARTITION_COLUMN,
    QUARANTINE_ARROW_SCHEMA,
    REQUIRED_COLUMNS,
    SILVER_COLUMNS,
    SILVER_PARTITION_SPEC,
    SILVER_SCHEMA,
    resolve_confirmed_range,
    transform,
    validate,
)


def mod_default_max_days() -> int:
    return DEFAULT_MAX_DAYS_PER_RUN

# Bronze는 API 요청일(PARTITION_COLUMN)도 갖는다 - transform이 요청일을 함께 뽑아
# quarantine 구간 교체 키로 쓴다(#288, #304).
BRONZE_COLUMNS = ["bike_no", "reg_dttm", "failure_type", PARTITION_COLUMN]

DEFAULT_ROW = {
    "bike_no": "SPB-30242",
    "reg_dttm": "2026-08-21 09:12:34",
    "failure_type": "페달",
    PARTITION_COLUMN: "2026-08-21",
}


def bronze_table(*overrides) -> pa.Table:
    """overrides에 선언 신고일이 없으면 reg_dttm에서 유도해 채운다 (일치 케이스가 기본)."""
    rows = []
    for over in overrides or [{}]:
        row = dict(DEFAULT_ROW)
        row.update(over)
        if PARTITION_COLUMN not in over and "reg_dttm" in over:
            row[PARTITION_COLUMN] = _derive_declared(over["reg_dttm"])
        rows.append(row)
    return pa.table(
        {col: pa.array([r[col] for r in rows], type=pa.string()) for col in BRONZE_COLUMNS}
    )


def _derive_declared(raw_reg_dttm: str) -> str | None:
    """테스트 픽스처용 - transform과 같은 날짜를 뽑아 선언 신고일 기본값으로 쓴다."""
    digits = "".join(ch for ch in raw_reg_dttm if ch.isdigit())
    if len(digits) < 8:
        return None
    return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"


def one(table: pa.Table) -> dict:
    return table.to_pylist()[0]


def utc(y, m, d) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


# ---------------------------------------------------------------- reg_dttm 캐스팅


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2026-08-21 09:12:34", utc(2026, 8, 21)),   # API 수집분 (19자) - 실측 100%
        ("2026-08-21", utc(2026, 8, 21)),            # 날짜만
        ("2026-8-1 09:12:34", utc(2026, 8, 1)),      # 0패딩 없음
        ("2026.08.21 09:12:34", utc(2026, 8, 21)),   # 구분자가 점
        ("20260821", utc(2026, 8, 21)),              # 초기 적재 파일 (yyyyMMdd)
    ],
)
def test_reg_dttm_formats_parse_to_midnight(raw, expected):
    """
    시각 정밀도는 버리고 날짜 단위 자정으로 통일한다 - 원본 시각 표기가 파일마다
    제각각이라 모든 변형에 안전한 timestamp 패턴을 나열하는 대신 날짜만 뽑는다.
    """
    row = one(transform(bronze_table({"reg_dttm": raw})))
    assert row["reg_dttm"] == expected


def test_unparseable_reg_dttm_becomes_null():
    row = one(transform(bronze_table({"reg_dttm": "등록일시없음"})))
    assert row["reg_dttm"] is None


def test_reg_dttm_is_timestamptz():
    """Iceberg 컬럼 타입이 timestamptz라 tz-aware여야 pyiceberg가 그대로 쓴다."""
    assert transform(bronze_table()).schema.field("reg_dttm").type == pa.timestamp("us", tz="UTC")


# ---------------------------------------------------------------- failure_type


@pytest.mark.parametrize("raw,expected", [("기타 ", "기타"), ("타이어 ", "타이어"), (" 체인 ", "체인")])
def test_failure_type_is_trimmed(raw, expected):
    """bronze CSV 파서가 ignoreTrailingWhiteSpace=false라 원본 공백이 그대로 살아있다."""
    assert one(transform(bronze_table({"failure_type": raw})))["failure_type"] == expected


def test_bike_no_is_untouched():
    assert one(transform(bronze_table({"bike_no": "SPB-99999"})))["bike_no"] == "SPB-99999"


# ---------------------------------------------------------------- reg_date_partition


def test_reg_date_partition_is_date_of_reg_dttm():
    row = one(transform(bronze_table({"reg_dttm": "2026-08-22 23:59:59"})))
    assert row[PARTITION_COLUMN] == "2026-08-22"


def test_reg_date_partition_splits_one_bronze_load_date_into_many():
    """
    브론즈의 동명 컬럼(적재일)과 의미가 다르다는 걸 고정한다 - 같은 적재분 안에
    등록일이 여러 날이면 실버 파티션도 여러 개가 된다.
    """
    table = transform(
        bronze_table({"reg_dttm": "2026-08-21 23:00:00"}, {"reg_dttm": "2026-08-22 01:00:00"})
    )
    assert [r[PARTITION_COLUMN] for r in table.to_pylist()] == ["2026-08-21", "2026-08-22"]


def test_partition_spec_is_identity_on_reg_date_partition():
    """pyiceberg가 transform 파티션을 못 써서 identity로 바꿨다 - 스펙 자체를 고정한다."""
    fields = SILVER_PARTITION_SPEC.fields
    assert len(fields) == 1
    assert fields[0].name == PARTITION_COLUMN
    assert fields[0].transform.__class__.__name__ == "IdentityTransform"
    # identity 파티션은 원본 컬럼이 스키마에 실제로 있어야 한다
    assert SILVER_SCHEMA.find_field(fields[0].source_id).name == PARTITION_COLUMN


def test_confirmed_schema_columns_unchanged():
    """확정 3컬럼은 이름/타입/순서 모두 변경 금지. reg_date_partition만 뒤에 추가된다."""
    assert [f.name for f in SILVER_SCHEMA.fields] == list(REQUIRED_COLUMNS) + [PARTITION_COLUMN]
    assert SILVER_COLUMNS == list(REQUIRED_COLUMNS) + [PARTITION_COLUMN]
    assert transform(bronze_table()).column_names == SILVER_COLUMNS


# ---------------------------------------------------------------- validate


def test_validate_passes_on_clean_rows():
    assert validate(transform(bronze_table())) == []


def test_validate_reports_null_reg_dttm_as_cast_failure():
    errors = validate(transform(bronze_table({"reg_dttm": "포맷깨짐"})))
    assert len(errors) == 1
    assert "reg_dttm" in errors[0] and "캐스팅 실패 의심" in errors[0]


def test_validate_reports_null_bike_no():
    errors = validate(transform(bronze_table({"bike_no": None})))
    assert any(e.startswith("bike_no null") for e in errors)


def test_validate_allows_duplicate_unique_key():
    """
    reg_dttm을 날짜 단위로 통일하면서 같은 날 같은 자전거의 같은 고장유형 중복은
    자연스럽게 발생한다 - 경고만 하고 통과시킨다.
    """
    duplicated = bronze_table(
        {"reg_dttm": "2026-08-21 01:00:00"}, {"reg_dttm": "2026-08-21 20:00:00"}
    )
    assert validate(transform(duplicated)) == []


# ---------------------------------------------------------------------------
# 확정 구간 계산 (#288)
# ---------------------------------------------------------------------------


def test_confirmed_range_starts_after_silver_watermark_and_ends_at_bronze():
    assert resolve_confirmed_range(date(2026, 8, 20), date(2026, 8, 24)) == (
        date(2026, 8, 21),
        date(2026, 8, 24),
    )


def test_confirmed_range_is_none_when_silver_caught_up():
    """Silver가 Bronze를 따라잡았으면 처리할 확정 날짜가 없다."""
    assert resolve_confirmed_range(date(2026, 8, 24), date(2026, 8, 24)) is None
    # Silver가 Bronze보다 앞서는 이상 상태에서도 구간을 만들지 않는다.
    assert resolve_confirmed_range(date(2026, 8, 25), date(2026, 8, 24)) is None


def test_confirmed_range_is_capped_by_max_days_per_run():
    """오래 밀린 워터마크가 한 번에 수개월을 처리하지 않게 자른다."""
    assert resolve_confirmed_range(date(2021, 1, 31), date(2026, 8, 24), max_days="3") == (
        date(2021, 2, 1),
        date(2021, 2, 3),
    )


def test_confirmed_range_default_cap_is_applied_when_env_is_empty():
    """DAG params가 빈 문자열을 넘기므로 기본 상한으로 폴백해야 한다."""
    start, end = resolve_confirmed_range(date(2021, 1, 31), date(2026, 8, 24), max_days="")
    assert (end - start).days + 1 == mod_default_max_days()


# ---------------------------------------------------------------------------
# 요청일 != 신고일 (#304)
#
# #288은 유도 신고일이 Bronze 파티션(선언 신고일)과 다른 행을 전부 quarantine으로
# 격리했다. API가 요청일 기준 최대 31일치를 돌려준다는 게 확인되면서(#304) 그 전제가
# 무너졌다 - 불일치는 이상이 아니라 이 원천의 정상 동작이다. 이제 Silver는 실제
# 신고일 기준 sliding window로 처리하고, quarantine은 cutoff보다 미래인 행만 받는다.
# window/중복 제거/구간 교체 자체는 test_silver_failure_report_sliding_window.py 참고.
# ---------------------------------------------------------------------------


def _with_declared(*overrides) -> pa.Table:
    from jobs.silver_failure_report import transform_with_declared

    return transform_with_declared(bronze_table(*overrides))


def test_declared_date_prefers_the_explicit_requested_date_column():
    """신규 수집분은 requested_date를 싣는다 - 파티션 값보다 그쪽이 우선이다."""
    from jobs.silver_failure_report import REQUESTED_DATE_COLUMN, transform_with_declared

    bronze = pa.table({
        "bike_no": pa.array(["SPB-1"], type=pa.string()),
        "reg_dttm": pa.array(["2026-08-21 09:00:00"], type=pa.string()),
        "failure_type": pa.array(["페달"], type=pa.string()),
        PARTITION_COLUMN: pa.array(["2026-07-25"], type=pa.string()),
        REQUESTED_DATE_COLUMN: pa.array(["2026-07-26"], type=pa.string()),
    })
    row = transform_with_declared(bronze).to_pylist()[0]

    assert row[DECLARED_COLUMN] == "2026-07-26"
    assert row[PARTITION_COLUMN] == "2026-08-21"   # 유도 신고일은 그대로


def test_declared_date_falls_back_to_the_partition_for_backfilled_rows():
    """파일 백필분에는 requested_date가 없다(NULL) - 파티션 값으로 폴백한다."""
    row = _with_declared(
        {"reg_dttm": "2026-08-21 09:00:00", PARTITION_COLUMN: "2026-08-21"}
    ).to_pylist()[0]

    assert row[DECLARED_COLUMN] == "2026-08-21"


def test_report_date_different_from_the_request_date_is_no_longer_quarantined():
    """31일 응답에서는 이게 정상이다 - 격리하면 데이터 대부분이 본 테이블에서 사라진다."""
    from jobs.silver_failure_report import _split_future_rows

    table = _with_declared(
        {"reg_dttm": "2026-08-21 09:00:00", PARTITION_COLUMN: "2026-07-25"},
        {"reg_dttm": "2026-09-10 09:00:00", PARTITION_COLUMN: "2026-07-25"},
    )
    kept, quarantined = _split_future_rows(table, date(2026, 9, 30))

    assert len(kept) == 2
    assert len(quarantined) == 0


def test_quarantine_schema_is_fixed():
    """감사 테이블 스키마를 고정한다 - 원본 컬럼 + 요청일 + 사유/시각."""
    from jobs.silver_failure_report import _split_future_rows

    _kept, quarantined = _split_future_rows(
        _with_declared({"reg_dttm": "2026-09-10 09:00:00", PARTITION_COLUMN: "2026-07-25"}),
        date(2026, 8, 26),
    )
    assert quarantined.schema == QUARANTINE_ARROW_SCHEMA


# ---------------------------------------------------------------------------
# Bronze MAX(파티션) - 미확정 tail의 끝 (#288)
# ---------------------------------------------------------------------------


class _FakePartitions:
    def __init__(self, values):
        self._values = values

    def to_pylist(self):
        return [{"partition": {PARTITION_COLUMN: v}} for v in self._values]


class _FakeInspect:
    def __init__(self, values):
        self.partitions = lambda: _FakePartitions(values)


class _FakeBronze:
    def __init__(self, values):
        self.inspect = _FakeInspect(values)


class _FakeCatalog:
    def __init__(self, table):
        self._table = table

    def load_table(self, _identifier):
        if self._table is None:
            from pyiceberg.exceptions import NoSuchTableError

            raise NoSuchTableError("no table")
        return self._table


@pytest.mark.parametrize(
    "values,expected",
    [
        (["2026-08-24", "2026-08-26", "2026-08-25"], date(2026, 8, 26)),
        ([], None),
        # reg_dttm이 NULL인 원본 행은 파티션 값이 ""로 떨어진다 - MAX를 오염시키면 안 된다.
        (["", "2026-08-24"], date(2026, 8, 24)),
        ([""], None),
        ([None, "2026-08-24"], date(2026, 8, 24)),
    ],
)
def test_bronze_max_partition(values, expected):
    from jobs.silver_failure_report import bronze_max_partition

    assert bronze_max_partition(_FakeCatalog(_FakeBronze(values))) == expected


def test_bronze_max_partition_is_none_when_table_missing():
    from jobs.silver_failure_report import bronze_max_partition

    assert bronze_max_partition(_FakeCatalog(None)) is None
