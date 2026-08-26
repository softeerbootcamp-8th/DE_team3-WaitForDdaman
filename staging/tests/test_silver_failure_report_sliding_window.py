"""
silver.failure_report 실제 신고일 기준 sliding window 재처리 테스트 (#304)

고장신고 API는 요청일 하루치가 아니라 요청일 기준 최대 31일치를 돌려준다. Bronze
파티션(=요청일)과 실제 신고일(date(reg_dttm))이 대량으로 어긋나므로, Silver는

  1. 실제 신고일 기준 최근 31일 sliding window를 매 실행 재처리하고,
  2. 그 구간을 파티션 교체하며,
  3. cutoff보다 미래의 신고일 행은 본 테이블에 넣지 않고,
  4. (bike_no, reg_dttm, failure_type) 중복을 deterministic하게 제거한다.

중복 제거가 선택이 아니라 필수인 이유: 같은 신고 1건이 최대 31개의 서로 다른
요청일 파티션에 함께 실려 온다.
"""
from datetime import date, datetime, timezone

import pyarrow as pa
import pytest

import jobs.silver_failure_report as mod
from jobs.silver_failure_report import (
    BRONZE_LOOKBACK_DAYS,
    DECLARED_COLUMN,
    PARTITION_COLUMN,
    REQUESTED_DATE_COLUMN,
    SILVER_COLUMNS,
    SLIDING_WINDOW_DAYS,
    dedupe,
    resolve_backlog_range,
    resolve_cutoff_date,
    resolve_sliding_window,
    transform_with_declared,
)

BRONZE_COLUMNS = [
    "bike_no", "reg_dttm", "failure_type", PARTITION_COLUMN, REQUESTED_DATE_COLUMN,
]


def bronze_rows(*rows: dict) -> pa.Table:
    """Bronze 모양의 Arrow Table. requested_date를 안 주면 파티션 값과 같게 채운다."""
    filled = []
    for row in rows:
        row = dict(row)
        row.setdefault(REQUESTED_DATE_COLUMN, row.get(PARTITION_COLUMN))
        filled.append(row)
    return pa.table(
        {c: pa.array([r.get(c) for r in filled], type=pa.string()) for c in BRONZE_COLUMNS}
    )


def report(bike_no: str, reg_dttm: str, requested: str, failure_type: str = "체인") -> dict:
    """요청일 `requested` 응답에 섞여 온 신고 1건."""
    return {
        "bike_no": bike_no,
        "reg_dttm": reg_dttm,
        "failure_type": failure_type,
        PARTITION_COLUMN: requested,
        REQUESTED_DATE_COLUMN: requested,
    }


def d(value: str) -> date:
    return date.fromisoformat(value)


# ------------------------------------------------------- window / cutoff 계산


def test_sliding_window_is_31_days_ending_at_the_cutoff_date():
    assert resolve_sliding_window(d("2026-08-26")) == (d("2026-07-27"), d("2026-08-26"))
    start, end = resolve_sliding_window(d("2026-08-26"))
    assert (end - start).days + 1 == SLIDING_WINDOW_DAYS


def test_cutoff_date_comes_from_the_latest_bronze_request_date():
    """`datetime.now()`가 아니라 Bronze가 실제로 요청한 최신 날짜를 기준으로 삼는다."""
    assert resolve_cutoff_date(d("2026-08-26"), d("2026-08-24")) == d("2026-08-26")
    # Bronze 파티션을 못 읽으면 워터마크로 폴백한다.
    assert resolve_cutoff_date(None, d("2026-08-24")) == d("2026-08-24")
    # 워터마크가 더 앞서면 그쪽을 쓴다.
    assert resolve_cutoff_date(d("2026-08-20"), d("2026-08-24")) == d("2026-08-24")


def test_cutoff_date_can_be_pinned_for_reruns():
    """재실행 재현성을 위해 논리 기준일을 외부에서 못박을 수 있어야 한다."""
    assert resolve_cutoff_date(
        d("2026-08-26"), d("2026-08-24"), env_value="2026-08-10"
    ) == d("2026-08-10")


def test_backlog_range_stops_where_the_sliding_window_starts():
    """window가 매 실행 다시 계산하는 구간을 backlog가 또 처리하면 안 된다."""
    assert resolve_backlog_range(
        silver_watermark=d("2026-06-30"),
        bronze_watermark=d("2026-08-25"),
        window_start=d("2026-07-27"),
    ) == (d("2026-07-01"), d("2026-07-26"))


def test_backlog_range_is_none_once_silver_reaches_the_window():
    assert resolve_backlog_range(
        silver_watermark=d("2026-07-26"),
        bronze_watermark=d("2026-08-25"),
        window_start=d("2026-07-27"),
    ) is None


def test_backlog_range_never_passes_the_bronze_watermark():
    assert resolve_backlog_range(
        silver_watermark=d("2026-06-30"),
        bronze_watermark=d("2026-07-05"),
        window_start=d("2026-07-27"),
    ) == (d("2026-07-01"), d("2026-07-05"))


def test_backlog_range_is_capped_by_max_days_per_run():
    assert resolve_backlog_range(
        silver_watermark=d("2021-01-31"),
        bronze_watermark=d("2026-08-25"),
        window_start=d("2026-07-27"),
        max_days="3",
    ) == (d("2021-02-01"), d("2021-02-03"))


# ---------------------------------------------------------------- 중복 제거


def _silver(*rows: dict) -> pa.Table:
    return transform_with_declared(bronze_rows(*rows)).select(SILVER_COLUMNS)


def test_the_same_report_arriving_under_many_request_dates_collapses_to_one_row():
    """31일 응답이 겹치면 같은 신고가 요청일 수만큼 중복된다 - 1행으로 접어야 한다."""
    same = [
        report("SPB-1", "2026-08-25 09:00:00", requested=f"2026-08-{day:02d}")
        for day in range(1, 26)
    ]
    deduped = dedupe(_silver(*same))

    assert len(deduped) == 1
    assert deduped.to_pylist()[0][PARTITION_COLUMN] == "2026-08-25"


def test_dedupe_keeps_rows_that_differ_in_the_unique_key():
    rows = [
        report("SPB-1", "2026-08-25 09:00:00", "2026-08-25", failure_type="체인"),
        report("SPB-1", "2026-08-25 09:00:00", "2026-08-25", failure_type="페달"),
        report("SPB-2", "2026-08-25 09:00:00", "2026-08-25", failure_type="체인"),
    ]
    assert len(dedupe(_silver(*rows))) == 3


def test_dedupe_output_is_deterministic_regardless_of_input_order():
    """재실행이 같은 바이트를 쓰려면 행 순서까지 입력 순서에 흔들리면 안 된다."""
    rows = [
        report("SPB-2", "2026-08-25 09:00:00", "2026-08-25", failure_type="페달"),
        report("SPB-1", "2026-08-26 09:00:00", "2026-08-26"),
        report("SPB-1", "2026-08-25 09:00:00", "2026-08-25"),
    ]
    forward = dedupe(_silver(*rows)).to_pylist()
    backward = dedupe(_silver(*reversed(rows))).to_pylist()

    assert forward == backward


def test_dedupe_keeps_the_silver_schema():
    assert dedupe(_silver(report("SPB-1", "2026-08-25 09:00:00", "2026-08-25"))).column_names == (
        SILVER_COLUMNS
    )


# ----------------------------------------------------- cutoff 미래 행 제외


def test_rows_after_the_cutoff_are_split_out():
    table = transform_with_declared(
        bronze_rows(
            report("SPB-1", "2026-08-26 09:00:00", "2026-08-25"),
            report("SPB-2", "2026-09-20 09:00:00", "2026-08-25"),   # cutoff 이후
        )
    )
    kept, future = mod._split_future_rows(table, d("2026-08-26"))

    assert [r["bike_no"] for r in kept.to_pylist()] == ["SPB-1"]
    assert len(future) == 1
    row = future.to_pylist()[0]
    assert row[PARTITION_COLUMN] == "2026-09-20"
    assert row[DECLARED_COLUMN] == "2026-08-25"
    assert row["quarantine_reason"] == "reg_date > collection_cutoff"


def test_rows_on_the_cutoff_date_are_kept():
    table = transform_with_declared(
        bronze_rows(report("SPB-1", "2026-08-26 23:59:59", "2026-08-25"))
    )
    kept, future = mod._split_future_rows(table, d("2026-08-26"))

    assert len(kept) == 1
    assert len(future) == 0


# ------------------------------------------------------------- _process_range


@pytest.fixture
def range_harness(monkeypatch):
    """_process_range를 Iceberg 없이 돌리고 읽은 범위와 쓴 내용을 기록한다."""
    reads: list[tuple[str, str]] = []
    writes: list[dict] = []
    state = {"bronze": bronze_rows()}

    def fake_read(_catalog, start, end):
        reads.append((start, end))
        return state["bronze"]

    monkeypatch.setattr(mod, "_read_bronze_range", fake_read)
    monkeypatch.setattr(mod, "_ensure_quarantine_table", lambda _c: "QUARANTINE")
    monkeypatch.setattr(
        mod, "replace_range",
        lambda table, rows, column, start, end, catalog=None: writes.append(
            {"table": table, "rows": rows, "column": column, "start": start, "end": end}
        ),
    )
    monkeypatch.delenv("MAX_QUARANTINE_RATIO", raising=False)
    return state, reads, writes


def run_range(start: str, end: str, cutoff: str) -> None:
    mod._process_range(object(), "SILVER", d(start), d(end), d(cutoff))


def test_bronze_read_looks_back_far_enough_to_find_late_request_dates(range_harness):
    """신고일 D는 요청일 [D-30, D] 응답 어디에나 실려 올 수 있다 - 그만큼 뒤로 읽어야 한다."""
    _state, reads, _writes = range_harness

    run_range("2026-07-27", "2026-08-26", cutoff="2026-08-26")

    assert reads == [("2026-06-27", "2026-08-26")]
    assert BRONZE_LOOKBACK_DAYS == SLIDING_WINDOW_DAYS - 1


def test_only_rows_whose_report_date_is_inside_the_window_are_written(range_harness):
    state, _reads, writes = range_harness
    state["bronze"] = bronze_rows(
        report("SPB-in", "2026-08-25 09:00:00", "2026-07-01"),    # 창 안 (오래된 요청일)
        report("SPB-out", "2026-07-10 09:00:00", "2026-07-01"),   # 창 밖 (과거 신고일)
    )

    run_range("2026-07-27", "2026-08-26", cutoff="2026-08-26")

    main = writes[-1]
    assert [r["bike_no"] for r in main["rows"].to_pylist()] == ["SPB-in"]
    assert (main["start"], main["end"]) == ("2026-07-27", "2026-08-26")
    assert main["column"] == PARTITION_COLUMN


def test_written_partitions_are_report_dates_not_request_dates(range_harness):
    state, _reads, writes = range_harness
    state["bronze"] = bronze_rows(
        report("SPB-1", "2026-08-25 09:00:00", "2026-08-01"),
        report("SPB-2", "2026-08-26 09:00:00", "2026-08-01"),
    )

    run_range("2026-07-27", "2026-08-26", cutoff="2026-08-26")

    assert sorted(r[PARTITION_COLUMN] for r in writes[-1]["rows"].to_pylist()) == [
        "2026-08-25", "2026-08-26",
    ]


def test_duplicates_across_request_dates_are_removed_before_writing(range_harness):
    state, _reads, writes = range_harness
    state["bronze"] = bronze_rows(
        *[
            report("SPB-1", "2026-08-25 09:00:00", requested=f"2026-08-{day:02d}")
            for day in range(1, 26)
        ]
    )

    run_range("2026-07-27", "2026-08-26", cutoff="2026-08-26")

    assert len(writes[-1]["rows"]) == 1


def test_future_rows_go_to_quarantine_keyed_by_request_date(range_harness):
    state, _reads, writes = range_harness
    state["bronze"] = bronze_rows(
        report("SPB-1", "2026-08-26 09:00:00", "2026-08-25"),
        report("SPB-2", "2026-09-20 09:00:00", "2026-08-25"),
    )

    run_range("2026-07-27", "2026-08-26", cutoff="2026-08-26")

    quarantine, main = writes
    assert quarantine["table"] == "QUARANTINE"
    assert quarantine["column"] == DECLARED_COLUMN
    assert (quarantine["start"], quarantine["end"]) == ("2026-06-27", "2026-08-26")
    assert [r["bike_no"] for r in quarantine["rows"].to_pylist()] == ["SPB-2"]
    assert [r["bike_no"] for r in main["rows"].to_pylist()] == ["SPB-1"]


def test_empty_window_still_blanks_the_declared_range(range_harness):
    _state, _reads, writes = range_harness

    run_range("2026-07-27", "2026-08-26", cutoff="2026-08-26")

    assert [w["table"] for w in writes] == ["QUARANTINE", "SILVER"]
    assert all(len(w["rows"]) == 0 for w in writes)


def test_legacy_bronze_rows_without_requested_date_still_work(range_harness):
    """초기 적재분에는 requested_date가 없다 - 파티션 값으로 폴백해야 한다."""
    state, _reads, writes = range_harness
    state["bronze"] = pa.table(
        {
            "bike_no": pa.array(["SPB-1"], type=pa.string()),
            "reg_dttm": pa.array(["2026-08-25 09:00:00"], type=pa.string()),
            "failure_type": pa.array(["체인"], type=pa.string()),
            PARTITION_COLUMN: pa.array(["2026-08-25"], type=pa.string()),
        }
    )

    run_range("2026-07-27", "2026-08-26", cutoff="2026-08-26")

    assert len(writes[-1]["rows"]) == 1


# ---------------------------------------------------------------- run() 흐름


@pytest.fixture
def run_harness(monkeypatch):
    processed: list[tuple] = []
    written: list[tuple] = []
    state = {
        "bronze_wm": date(2026, 8, 25),
        "silver_wm": date(2026, 6, 30),
        "bronze_max": date(2026, 8, 26),
    }

    def fake_read_watermark(watermark_key=None, **_kwargs):
        if watermark_key == mod.BRONZE_FAILURE_REPORT:
            return state["bronze_wm"]
        return state["silver_wm"]

    monkeypatch.setattr(mod, "build_iceberg_catalog", lambda: object())
    monkeypatch.setattr(mod, "_ensure_silver_table", lambda _c: object())
    monkeypatch.setattr(mod, "read_watermark", fake_read_watermark)
    monkeypatch.setattr(
        mod, "write_watermark",
        lambda value, watermark_key=None, **_k: written.append((value, watermark_key)),
    )
    monkeypatch.setattr(
        mod, "_process_range",
        lambda _c, _s, start, end, cutoff: processed.append((start, end, cutoff)) or {},
    )
    monkeypatch.setattr(mod, "bronze_max_partition", lambda _c: state["bronze_max"])
    monkeypatch.delenv("MAX_DAYS_PER_RUN", raising=False)
    monkeypatch.delenv("COLLECTION_CUTOFF_DATE", raising=False)
    return state, processed, written


def test_run_processes_backlog_then_the_sliding_window(run_harness):
    _state, processed, written = run_harness

    mod.run()

    assert processed == [
        (d("2026-07-01"), d("2026-07-26"), d("2026-08-26")),   # backlog
        (d("2026-07-27"), d("2026-08-26"), d("2026-08-26")),   # sliding window
    ]
    # 워터마크는 backlog 끝까지만 전진한다 - window는 매 실행 다시 계산된다.
    assert written == [(d("2026-07-26"), mod.SILVER_FAILURE_REPORT)]


def test_run_reprocesses_the_window_even_with_no_backlog(run_harness):
    state, processed, written = run_harness
    state["silver_wm"] = date(2026, 7, 26)

    mod.run()

    assert processed == [(d("2026-07-27"), d("2026-08-26"), d("2026-08-26"))]
    assert written == []


def test_run_does_not_advance_watermark_when_backlog_fails(run_harness, monkeypatch):
    _state, _processed, written = run_harness

    def boom(*_args, **_kwargs):
        raise mod.SilverFailureReportError("의도된 실패")

    monkeypatch.setattr(mod, "_process_range", boom)

    with pytest.raises(SystemExit) as exc:
        mod.run()

    assert exc.value.code == 1
    assert written == []


def test_rerunning_the_same_state_replays_the_same_ranges(run_harness):
    """재실행 회귀: 상태가 같으면 두 번째 실행도 같은 구간을 같은 순서로 교체한다."""
    state, processed, _written = run_harness
    state["silver_wm"] = date(2026, 7, 26)

    mod.run()
    first = list(processed)
    processed.clear()
    mod.run()

    assert processed == first


# ------------------------------- 실제 Iceberg 카탈로그 위에서의 재실행 멱등성
#
# 여기서만 진짜 카탈로그(sqlite SqlCatalog + 로컬 warehouse)를 쓴다. "31일 응답을 두 번
# 처리해도 같은 결과"는 스냅샷을 다시 읽어야만 확인할 수 있다.


@pytest.fixture
def iceberg_env(tmp_path):
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "silver_failure_report_window_test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
    silver_table = mod._ensure_silver_table(catalog)
    quarantine_table = mod._ensure_quarantine_table(catalog)
    return catalog, silver_table, quarantine_table


def thirty_one_day_bronze() -> pa.Table:
    """요청일 8/01~8/26 각각이 자기 이후 31일치를 함께 돌려준 Bronze 적재 결과."""
    rows = []
    for requested_day in range(1, 27):
        requested = f"2026-08-{requested_day:02d}"
        for report_day in range(requested_day, 27):
            rows.append(
                report("SPB-%02d" % report_day, f"2026-08-{report_day:02d} 09:00:00", requested)
            )
    return bronze_rows(*rows)


def test_31_day_response_lands_on_report_date_partitions_and_reruns_identically(
    monkeypatch, iceberg_env
):
    catalog, silver_table, _quarantine = iceberg_env
    monkeypatch.setattr(mod, "_read_bronze_range", lambda _c, _s, _e: thirty_one_day_bronze())
    monkeypatch.delenv("MAX_QUARANTINE_RATIO", raising=False)

    def once():
        mod._process_range(
            catalog, silver_table, d("2026-07-27"), d("2026-08-26"), d("2026-08-26")
        )
        return sorted(
            silver_table.refresh().scan().to_arrow().to_pylist(),
            key=lambda r: (r["bike_no"], r[PARTITION_COLUMN]),
        )

    first = once()
    second = once()

    # 26개 신고일 파티션 - 요청일(8/01) 파티션 하나로 뭉치지 않는다.
    assert sorted({r[PARTITION_COLUMN] for r in first}) == [
        f"2026-08-{day:02d}" for day in range(1, 27)
    ]
    assert len(first) == 26            # 중복 제거 후 신고 26건
    assert second == first             # 재실행 멱등
    assert all(
        r["reg_dttm"] == datetime.fromisoformat(f"{r[PARTITION_COLUMN]}T00:00:00+00:00")
        for r in first
    )


def test_window_slide_rerun_neither_duplicates_nor_drops_rows(monkeypatch, iceberg_env):
    """창이 하루 미끄러진 다음 실행도 같은 신고를 중복시키거나 잃지 않는다."""
    catalog, silver_table, _q = iceberg_env
    monkeypatch.setattr(mod, "_read_bronze_range", lambda _c, _s, _e: thirty_one_day_bronze())

    mod._process_range(catalog, silver_table, d("2026-07-27"), d("2026-08-26"), d("2026-08-26"))
    mod._process_range(catalog, silver_table, d("2026-07-28"), d("2026-08-27"), d("2026-08-27"))

    rows = silver_table.refresh().scan().to_arrow().to_pylist()
    assert len(rows) == 26
    assert sorted({r[PARTITION_COLUMN] for r in rows}) == [
        f"2026-08-{day:02d}" for day in range(1, 27)
    ]
