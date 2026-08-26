"""
silver.failure_report 변환/검증 로직 테스트 (#143)

Spark를 걷어내고 DuckDB + pyiceberg로 옮기면서 확인해야 할 것:
  1. reg_dttm STRING -> TIMESTAMP 캐스팅이 기존 Spark 표현과 같은 포맷들을 받는가
  2. failure_type trim
  3. 새 파티션 컬럼 reg_date_partition = date(reg_dttm), identity 파티션 스펙
  4. 확정 3컬럼(bike_no, reg_dttm, failure_type)의 이름/타입이 안 바뀌었는가
"""
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from jobs.silver_failure_report import (
    PARTITION_COLUMN,
    REQUIRED_COLUMNS,
    SILVER_COLUMNS,
    SILVER_PARTITION_SPEC,
    SILVER_SCHEMA,
    evaluate_partial_load,
    transform,
    validate,
)

BRONZE_COLUMNS = ["bike_no", "reg_dttm", "failure_type"]

DEFAULT_ROW = {
    "bike_no": "SPB-30242",
    "reg_dttm": "2026-08-21 09:12:34",
    "failure_type": "페달",
}


def bronze_table(*overrides) -> pa.Table:
    rows = []
    for over in overrides or [{}]:
        row = dict(DEFAULT_ROW)
        row.update(over)
        rows.append(row)
    return pa.table(
        {col: pa.array([r[col] for r in rows], type=pa.string()) for col in BRONZE_COLUMNS}
    )


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


# ---------------------------------------------------------------- 부분 적재 방어


@pytest.mark.parametrize(
    "bronze,prev_silver,expected_stop",
    [
        (1000, 0, False),     # 최초 실행 - 비교 기준 없음
        (1000, 1000, False),  # 동일
        (960, 1000, False),   # 96% - 임계값 이상
        (940, 1000, True),    # 94% - 브론즈 부분 적재 의심
    ],
)
def test_evaluate_partial_load(bronze, prev_silver, expected_stop):
    stop, _ = evaluate_partial_load(bronze, prev_silver)
    assert stop is expected_stop


# ---------------------------------------------------------------------------
# run()의 워터마크 읽기 순서 (#286)
# ---------------------------------------------------------------------------


class _FakeCatalog:
    """load_table(BRONZE)만 의미 있게 동작하는 최소 catalog 스텁.

    Bronze 테이블을 읽는 순간 Bronze 워터마크를 전진시킨다 - 실행 중에 catch-up의
    advance_failure_report_watermark나 06:00 일배치가 워터마크를 올리는 상황을
    재현하는 장치다. 이게 없으면 run()이 워터마크를 언제 읽는지 테스트가 구분하지
    못한다(호출이 한 번뿐이면 어느 시점에 읽어도 같은 값이 나온다).
    """

    def __init__(self, bronze: pa.Table, calls: list, state: dict):
        self._bronze = bronze
        self._calls = calls
        self._state = state

    def load_table(self, identifier):
        self._calls.append(f"load_table:{identifier}")
        if identifier == ADVANCING_ON_BRONZE_READ[0]:
            self._state["watermark"] = ADVANCING_ON_BRONZE_READ[1]
        return _FakeTable(self._bronze)


class _FakeTable:
    def __init__(self, table: pa.Table):
        self._table = table

    def scan(self, **_kwargs):
        return self

    def to_arrow(self) -> pa.Table:
        return self._table


WATERMARK_BEFORE_BRONZE_READ = "2026-08-20"
# (Bronze를 읽는 순간 워터마크가 이 값으로 전진한다, 전진 후 값)
ADVANCING_ON_BRONZE_READ = ("bronze.failure_report", "2026-08-24")


@pytest.fixture
def run_harness(monkeypatch):
    """run()을 Spark/S3 없이 돌리고 호출 순서를 기록한다.

    Bronze 테이블을 읽는 순간 Bronze 워터마크가 전진하므로, run()이 그 전에 읽었는지
    후에 읽었는지가 기록된 값으로 드러난다.
    """
    from jobs import silver_failure_report as mod

    calls: list[str] = []
    written: list[tuple] = []
    state = {"watermark": WATERMARK_BEFORE_BRONZE_READ}

    def fake_read_watermark(watermark_key=None, **_kwargs):
        value = state["watermark"]
        calls.append(f"read_watermark:{value}")
        return value

    def fake_write_watermark(value, watermark_key=None, **_kwargs):
        calls.append(f"write_watermark:{value}")
        written.append((value, watermark_key))

    def fake_overwrite_all(_table, _rows):
        calls.append("overwrite_all")

    monkeypatch.setattr(
        mod, "build_iceberg_catalog", lambda: _FakeCatalog(bronze_table(), calls, state)
    )
    monkeypatch.setattr(mod, "read_watermark", fake_read_watermark)
    monkeypatch.setattr(mod, "write_watermark", fake_write_watermark)
    monkeypatch.setattr(mod, "overwrite_all", fake_overwrite_all)
    monkeypatch.setattr(mod, "_silver_row_count", lambda _catalog: 1)
    monkeypatch.setattr(mod, "_ensure_silver_table", lambda _catalog: object())
    return mod, calls, written


def test_silver_watermark_uses_value_read_before_bronze(run_harness):
    """Silver에 기록되는 값은 Bronze를 읽은 시점의 워터마크여야 한다.

    overwrite 이후에 다시 읽으면, 그 사이 catch-up이나 일배치가 Bronze 워터마크를
    전진시켰을 때 Silver가 자기 테이블에 없는 날짜까지 완료로 선언한다.
    """
    mod, _calls, written = run_harness

    mod.run()

    # Bronze 읽기 전 값이어야 한다. 전진 후 값(2026-08-24)이면 Silver 테이블에 없는
    # 날짜까지 완료로 선언하는 과대 보고다.
    assert written == [(WATERMARK_BEFORE_BRONZE_READ, mod.SILVER_FAILURE_REPORT)]


def test_watermark_is_read_before_bronze_and_written_after_overwrite(run_harness):
    """순서 자체를 고정한다 - 읽기는 Bronze 앞, 쓰기는 overwrite 뒤."""
    mod, calls, _written = run_harness

    mod.run()

    read_idx = calls.index(f"read_watermark:{WATERMARK_BEFORE_BRONZE_READ}")
    bronze_idx = calls.index(f"load_table:{mod.BRONZE_TABLE}")
    overwrite_idx = calls.index("overwrite_all")
    write_idx = calls.index(f"write_watermark:{WATERMARK_BEFORE_BRONZE_READ}")

    assert read_idx < bronze_idx, calls
    assert overwrite_idx < write_idx, calls


def test_watermark_not_written_when_overwrite_fails(run_harness, monkeypatch):
    """부분 실패에서 워터마크를 전진시키지 않는다는 계약은 그대로 유지된다."""
    mod, _calls, written = run_harness

    def boom(_table, _rows):
        raise RuntimeError("commit conflict")

    monkeypatch.setattr(mod, "overwrite_all", boom)

    with pytest.raises(RuntimeError):
        mod.run()

    assert written == []
