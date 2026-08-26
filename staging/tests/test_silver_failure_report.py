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
    _split_partition_mismatch,
    resolve_confirmed_range,
    transform,
    validate,
)


def mod_default_max_days() -> int:
    return DEFAULT_MAX_DAYS_PER_RUN

# Bronze는 선언 신고일(PARTITION_COLUMN)도 갖는다 - transform이 유도 신고일과 비교할
# 기준으로 통과시킨다(#288).
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
# 신고일 불일치 quarantine (#288)
# ---------------------------------------------------------------------------


def _with_declared(*overrides) -> pa.Table:
    from jobs.silver_failure_report import transform_with_declared

    return transform_with_declared(bronze_table(*overrides))


def test_matching_rows_stay_in_main_table():
    clean, quarantined = _split_partition_mismatch(_with_declared({}))

    assert len(clean) == 1
    assert len(quarantined) == 0
    assert clean.column_names == SILVER_COLUMNS


def test_mismatched_row_goes_to_quarantine():
    """선언 신고일과 유도 신고일이 다른 행은 본 테이블에 남지 않는다.

    이 행을 남기면 Bronze 파티션 D에서 Silver 파티션 D'로 옮겨가 구간 교체가
    부정확해진다(구간 밖이면 중복 누적, 구간 안이면 삭제 후 미복원).
    """
    table = _with_declared(
        {"reg_dttm": "2026-08-21 09:00:00", PARTITION_COLUMN: "2026-08-21"},   # 일치
        {"reg_dttm": "2026-08-19 23:00:00", PARTITION_COLUMN: "2026-08-21"},   # 불일치
    )

    clean, quarantined = _split_partition_mismatch(table)

    assert [r[PARTITION_COLUMN] for r in clean.to_pylist()] == ["2026-08-21"]
    assert len(quarantined) == 1
    row = quarantined.to_pylist()[0]
    assert row[PARTITION_COLUMN] == "2026-08-19"      # 유도
    assert row[DECLARED_COLUMN] == "2026-08-21"       # 선언 - quarantine 구간 교체 키
    assert row["quarantine_reason"] == "derived_reg_date != declared_reg_date"
    assert row["quarantined_at"] is not None


def test_quarantine_schema_is_fixed():
    """감사 테이블 스키마를 고정한다 - 원본 컬럼 + 선언 신고일 + 사유/시각."""
    _clean, quarantined = _split_partition_mismatch(
        _with_declared({"reg_dttm": "2026-08-19 23:00:00", PARTITION_COLUMN: "2026-08-21"})
    )
    assert quarantined.schema == QUARANTINE_ARROW_SCHEMA


def test_unparseable_reg_dttm_is_treated_as_mismatch_not_silently_kept():
    """유도 신고일이 null이면 어느 파티션에 속하는지 알 수 없어 본 테이블에 못 넣는다."""
    table = _with_declared({"reg_dttm": "깨진값", PARTITION_COLUMN: "2026-08-21"})

    clean, quarantined = _split_partition_mismatch(table)

    assert len(clean) == 0
    assert len(quarantined) == 1


# ---------------------------------------------------------------------------
# run() 오케스트레이션 (#288)
# ---------------------------------------------------------------------------


@pytest.fixture
def run_harness(monkeypatch):
    """run()을 S3/Iceberg 없이 돌리고 구간 처리·워터마크 호출을 기록한다."""
    from jobs import silver_failure_report as mod

    processed: list[tuple] = []
    written: list[tuple] = []
    state = {
        "bronze_wm": date(2026, 8, 24),
        "silver_wm": date(2026, 8, 20),
        "bronze_max": date(2026, 8, 24),
        "fail_on": None,
    }

    def fake_read_watermark(watermark_key=None, **_kwargs):
        if watermark_key == mod.BRONZE_FAILURE_REPORT:
            return state["bronze_wm"]
        if watermark_key == mod.SILVER_FAILURE_REPORT:
            return state["silver_wm"]
        raise AssertionError(f"예상하지 못한 워터마크 키: {watermark_key}")

    def fake_process_range(_catalog, _silver, start_date, end_date):
        processed.append((start_date, end_date))
        if state["fail_on"] == (start_date, end_date):
            raise mod.SilverFailureReportError("의도된 실패")
        return {"bronze_row_count": 1, "silver_row_count": 1, "quarantine_row_count": 0}

    monkeypatch.setattr(mod, "build_iceberg_catalog", lambda: object())
    monkeypatch.setattr(mod, "_ensure_silver_table", lambda _catalog: object())
    monkeypatch.setattr(mod, "read_watermark", fake_read_watermark)
    monkeypatch.setattr(
        mod, "write_watermark",
        lambda value, watermark_key=None, **_k: written.append((value, watermark_key)),
    )
    monkeypatch.setattr(mod, "_process_range", fake_process_range)
    monkeypatch.setattr(mod, "bronze_max_partition", lambda _catalog: state["bronze_max"])
    monkeypatch.delenv("MAX_DAYS_PER_RUN", raising=False)
    return mod, state, processed, written


def test_run_processes_confirmed_range_and_advances_watermark(run_harness):
    mod, _state, processed, written = run_harness

    mod.run()

    assert processed == [(date(2026, 8, 21), date(2026, 8, 24))]
    assert written == [(date(2026, 8, 24), mod.SILVER_FAILURE_REPORT)]


def test_run_advances_to_processed_end_not_bronze_watermark(run_harness):
    """상한에 걸려 뒷부분을 남겼으면 처리한 구간 끝까지만 전진해야 한다.

    Bronze 워터마크를 그대로 복사하면 처리하지 않은 날짜를 완료로 선언한다 - 증분에서는
    그 구간이 다시 처리되지 않으므로 영구 공백이 된다.
    """
    mod, _state, processed, written = run_harness
    os.environ["MAX_DAYS_PER_RUN"] = "2"
    try:
        mod.run()
    finally:
        del os.environ["MAX_DAYS_PER_RUN"]

    assert processed[0] == (date(2026, 8, 21), date(2026, 8, 22))
    assert written == [(date(2026, 8, 22), mod.SILVER_FAILURE_REPORT)]


def test_run_processes_unconfirmed_tail_without_advancing_watermark(run_harness):
    """Bronze 워터마크보다 뒤에 있는 파티션(당일 T0)은 처리하되 워터마크는 그대로."""
    mod, state, processed, written = run_harness
    state["bronze_max"] = date(2026, 8, 26)   # Bronze WM(8/24)보다 뒤 = 미확정

    mod.run()

    assert processed == [
        (date(2026, 8, 21), date(2026, 8, 24)),   # 확정
        (date(2026, 8, 25), date(2026, 8, 26)),   # 미확정 tail
    ]
    # tail 처리가 워터마크를 올리지 않는다 - 확정 구간 끝 한 번만 기록된다.
    assert written == [(date(2026, 8, 24), mod.SILVER_FAILURE_REPORT)]


def test_run_skips_tail_when_bronze_has_nothing_unconfirmed(run_harness):
    mod, state, processed, _written = run_harness
    state["bronze_max"] = date(2026, 8, 24)   # Bronze WM과 같음

    mod.run()

    assert processed == [(date(2026, 8, 21), date(2026, 8, 24))]


def test_run_processes_tail_even_when_no_new_confirmed_dates(run_harness):
    """확정 구간이 없어도 당일 파티션은 매 실행 다시 계산해야 한다."""
    mod, state, processed, written = run_harness
    state["silver_wm"] = date(2026, 8, 24)    # 확정 구간 없음
    state["bronze_max"] = date(2026, 8, 26)

    mod.run()

    assert processed == [(date(2026, 8, 25), date(2026, 8, 26))]
    assert written == []


def test_run_does_not_advance_watermark_when_confirmed_range_fails(run_harness):
    mod, state, processed, written = run_harness
    state["fail_on"] = (date(2026, 8, 21), date(2026, 8, 24))

    with pytest.raises(SystemExit) as exc:
        mod.run()

    assert exc.value.code == 1
    assert written == []
    assert processed == [(date(2026, 8, 21), date(2026, 8, 24))]


def test_run_keeps_confirmed_watermark_when_tail_fails(run_harness):
    """tail 실패는 확정 구간의 성과를 되돌리지 않는다 - 워터마크는 이미 전진해 있다."""
    mod, state, _processed, written = run_harness
    state["bronze_max"] = date(2026, 8, 26)
    state["fail_on"] = (date(2026, 8, 25), date(2026, 8, 26))

    with pytest.raises(SystemExit) as exc:
        mod.run()

    assert exc.value.code == 1
    assert written == [(date(2026, 8, 24), mod.SILVER_FAILURE_REPORT)]


# ---------------------------------------------------------------------------
# _process_range 쓰기 규약 (#288)
# ---------------------------------------------------------------------------


@pytest.fixture
def range_harness(monkeypatch):
    """_process_range를 Iceberg 없이 돌리고 replace_range 호출을 기록한다."""
    from jobs import silver_failure_report as mod

    writes: list[dict] = []
    state = {"bronze": bronze_table(), "prev_silver": 1}

    monkeypatch.setattr(mod, "_read_bronze_range", lambda _c, _s, _e: state["bronze"])
    monkeypatch.setattr(mod, "_silver_row_count", lambda _c, _s, _e: state["prev_silver"])
    monkeypatch.setattr(mod, "_ensure_quarantine_table", lambda _c: "QUARANTINE")
    monkeypatch.setattr(
        mod, "replace_range",
        lambda table, rows, column, start, end, catalog=None: writes.append(
            {"table": table, "rows": rows, "column": column, "start": start, "end": end}
        ),
    )
    monkeypatch.delenv("MAX_QUARANTINE_RATIO", raising=False)
    return mod, state, writes


def test_process_range_writes_quarantine_before_main_table(range_harness):
    mod, _state, writes = range_harness

    mod._process_range(object(), "SILVER", date(2026, 8, 21), date(2026, 8, 21))

    assert [w["table"] for w in writes] == ["QUARANTINE", "SILVER"]


def test_process_range_keys_quarantine_by_declared_date(range_harness):
    """quarantine 구간 키는 유도 신고일이 아니라 선언 신고일이어야 한다.

    불일치 행의 유도 신고일은 정의상 선언 구간 밖일 수 있어서, 그걸 키로 쓰면
    replace_range의 범위 단정을 통과하지 못한다.
    """
    mod, _state, writes = range_harness

    mod._process_range(object(), "SILVER", date(2026, 8, 21), date(2026, 8, 21))

    quarantine_write, main_write = writes
    assert quarantine_write["column"] == DECLARED_COLUMN
    assert main_write["column"] == PARTITION_COLUMN


def test_process_range_blanks_declared_range_when_bronze_is_empty(range_harness):
    """0행이어도 선언 구간을 비워야 재실행 결과가 같아진다."""
    mod, state, writes = range_harness
    state["bronze"] = bronze_table().slice(0, 0)
    state["prev_silver"] = 0

    mod._process_range(object(), "SILVER", date(2026, 8, 21), date(2026, 8, 22))

    assert len(writes) == 2
    for w in writes:
        assert len(w["rows"]) == 0
        assert (w["start"], w["end"]) == ("2026-08-21", "2026-08-22")


def test_process_range_stops_batch_when_mismatch_ratio_exceeds_threshold(range_harness):
    """불일치가 임계치를 넘으면 quarantine이 아니라 배치 중단 - 구조적 이상 신호다."""
    mod, state, _writes = range_harness
    state["bronze"] = bronze_table(
        {"reg_dttm": "2026-08-19 23:00:00", PARTITION_COLUMN: "2026-08-21"}
    )
    state["prev_silver"] = 0

    with pytest.raises(mod.SilverFailureReportError, match="불일치 비율"):
        mod._process_range(object(), "SILVER", date(2026, 8, 21), date(2026, 8, 21))


def test_process_range_allows_quarantine_ratio_up_to_fifty_percent(range_harness):
    """E2E 운영에서는 절반 이하의 날짜 불일치를 quarantine으로 보존한다."""
    mod, state, writes = range_harness
    state["bronze"] = bronze_table(
        {"reg_dttm": "2026-08-21 09:12:34", PARTITION_COLUMN: "2026-08-21"},
        {"reg_dttm": "2026-08-19 23:00:00", PARTITION_COLUMN: "2026-08-21"},
    )

    mod._process_range(object(), "SILVER", date(2026, 8, 21), date(2026, 8, 21))

    assert [w["table"] for w in writes] == ["QUARANTINE", "SILVER"]
    assert len(writes[0]["rows"]) == 1


def test_process_range_allows_incremental_bronze_load(range_harness):
    """일자별 증분 Bronze는 직전 Silver보다 적어도 해당 구간을 교체한다."""
    mod, state, writes = range_harness
    state["prev_silver"] = 100   # Bronze 1행 / 직전 Silver 100행

    mod._process_range(object(), "SILVER", date(2026, 8, 21), date(2026, 8, 21))

    assert [w["table"] for w in writes] == ["QUARANTINE", "SILVER"]


def test_process_range_allows_small_bronze_range(range_harness):
    """작은 일자별 Bronze 범위도 현재 입력으로 교체한다."""
    mod, state, writes = range_harness
    state["prev_silver"] = 100   # Bronze 1행 / 직전 Silver 100행

    mod._process_range(object(), "SILVER", date(2026, 8, 21), date(2026, 8, 21))

    assert [w["table"] for w in writes] == ["QUARANTINE", "SILVER"]


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
