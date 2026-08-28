"""
고장신고 API의 31일 윈도우 응답 회귀 테스트 (#304)

서울시 `tbCycleFailureReport`는 요청일 하루치가 아니라 요청일 기준 최대 31일치를
돌려준다(실측 2026-08-26: `20260825` 요청이 8/25·8/26을 함께 반환). 이 파일은
Raw/Bronze가 그 응답을 **자르지 않고 원문 그대로 보존**하는지, 그리고 "요청일"과
"실제 신고일"이 서로 다른 이름으로 남는지를 고정한다.

  - Raw   : 응답 전체 보존 + target_date/observed_at/snapshot_type 표준 메타데이터
            (기존 reg_dt= 경로/키도 함께 유지)
  - Bronze: 원본 3컬럼 + requested_date/observed_at 수집 메타데이터 보존,
            파티션은 요청일이며 실제 신고일과 의미가 섞이지 않는다
"""
from datetime import date, datetime, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pyarrow as pa

from jobs.collect_failure_report_raw import collect_one_day
from jobs.daily_batch_failure_report import _build_arrow_table, _process_one_day

KST = ZoneInfo("Asia/Seoul")

REQUESTED = date(2026, 8, 25)
REQUESTED_STR = "2026-08-25"
CUTOFF = datetime(2026, 8, 26, 5, 0, 0, tzinfo=KST)


def api_rows(*reg_dttms: str) -> list[dict]:
    """API 응답 모양(대문자 별칭 + 페이징 메타 필드 포함)의 row 목록."""
    return [
        {
            "BIKENO": f"SPB-{i:05d}",
            "REGDTTM": reg_dttm,
            "MLANGCOMCDNAME": "체인",
            "RNUM": str(i + 1),
            "START_INDEX": "1",
            "END_INDEX": "1000",
        }
        for i, reg_dttm in enumerate(reg_dttms)
    ]


def thirty_one_day_response() -> list[dict]:
    """요청일(8/25)부터 31일치 신고일이 섞인 응답."""
    return api_rows(
        *["2026-08-25 09:00:00"]
        + ["2026-08-26 10:00:00"]
        + [f"2026-09-{d:02d} 11:00:00" for d in range(1, 25)]
    )


# --------------------------------------------------------------------- Raw


def _collect(rows) -> dict:
    saved: dict[str, dict] = {}
    with patch(
        "jobs.collect_failure_report_raw.fetch_failure_reports_by_date", return_value=rows
    ), patch(
        "jobs.collect_failure_report_raw.put_json",
        side_effect=lambda bucket, key, data: saved.__setitem__(key, data),
    ):
        collect_one_day(REQUESTED, CUTOFF, "PRELIMINARY", "test-bucket")
    return saved


def test_raw_payload_keeps_every_row_of_a_31_day_response():
    """요청일 기준으로 잘라내면 원본이 유실된다 - 26행 전부 남아야 한다."""
    rows = thirty_one_day_response()
    saved = _collect(rows)

    payload = next(v for k, v in saved.items() if k.endswith("payload.json"))
    assert payload["row_count"] == len(rows)
    assert len(payload["rows"]) == len(rows)
    assert {r["REGDTTM"][:10] for r in payload["rows"]} == {
        r["REGDTTM"][:10] for r in rows
    }


def test_raw_payload_carries_standard_snapshot_metadata():
    saved = _collect(thirty_one_day_response())
    payload = next(v for k, v in saved.items() if k.endswith("payload.json"))

    assert payload["target_date"] == REQUESTED_STR
    assert payload["requested_date"] == REQUESTED_STR
    assert payload["observed_at"] == CUTOFF.isoformat()
    assert payload["snapshot_type"] == "PRELIMINARY"
    # 기존 경로/키 호환 - 이미 이 키를 읽는 소비자가 있다.
    assert payload["reg_dt"] == REQUESTED_STR


def test_raw_legacy_and_standard_paths_hold_the_same_payload():
    saved = _collect(thirty_one_day_response())

    standard = next(
        v for k, v in saved.items()
        if k.endswith("payload.json") and "target_date=" in k
    )
    legacy = next(
        v for k, v in saved.items()
        if k.endswith("payload.json") and "reg_dt=" in k
    )
    assert standard == legacy


def test_raw_manifest_records_the_report_dates_the_response_spans():
    """계층 간 행 수를 그대로 비교하지 않도록, 응답이 걸친 신고일 수를 매니페스트에 남긴다."""
    saved = _collect(thirty_one_day_response())
    manifest = next(v for k, v in saved.items() if k.endswith("manifest.json"))

    assert manifest["requested_date"] == REQUESTED_STR
    assert manifest["reg_date_span"] == ["2026-08-25", "2026-09-24"]
    assert manifest["distinct_reg_date_count"] == 26


def test_raw_preserves_rows_that_arrive_across_api_pages():
    """페이지네이션 경계를 넘어온 행도 한 payload에 모두 담긴다."""
    pages = [api_rows("2026-08-25 09:00:00"), api_rows("2026-09-20 09:00:00")]

    def paged(_target_date):
        for page in pages:
            yield from page

    with patch(
        "jobs.collect_failure_report_raw.fetch_failure_reports_by_date", side_effect=paged
    ), patch("jobs.collect_failure_report_raw.put_json", side_effect=lambda *a: None) as put:
        count = collect_one_day(REQUESTED, CUTOFF, "PRELIMINARY", "test-bucket")

    assert count == 2
    payload = next(
        call.args[2] for call in put.call_args_list if call.args[1].endswith("payload.json")
    )
    assert len(payload["rows"]) == 2


# ------------------------------------------------------------------ Bronze


def test_bronze_rows_keep_request_metadata():
    table = _build_arrow_table(
        [{"bikeNo": "SPB-1", "regDttm": "2026-09-20 10:00:00", "mlangComCdName": "체인"}],
        REQUESTED_STR,
        observed_at=CUTOFF,
    )
    row = table.to_pylist()[0]

    assert row["requested_date"] == REQUESTED_STR
    assert row["observed_at"] == CUTOFF.astimezone(timezone.utc)
    assert table.schema.field("observed_at").type == pa.timestamp("us", tz="UTC")


def test_bronze_partition_is_the_requested_date_not_the_report_date():
    """파티션은 '요청일'이다 - 실제 신고일(reg_dttm)과 의미가 섞이면 안 된다."""
    table = _build_arrow_table(
        [{"bikeNo": "SPB-1", "regDttm": "2026-09-20 10:00:00", "mlangComCdName": "체인"}],
        REQUESTED_STR,
        observed_at=CUTOFF,
    )
    row = table.to_pylist()[0]

    assert row["reg_date_partition"] == REQUESTED_STR
    assert row["requested_date"] == REQUESTED_STR
    assert row["reg_dttm"] == "2026-09-20 10:00:00"   # 원문 보존


def test_bronze_keeps_every_row_of_a_31_day_response():
    """Bronze도 요청일로 잘라내지 않는다 - 31일 응답 26행이 전부 적재된다."""
    rows = thirty_one_day_response()
    captured = {}

    with patch(
        "jobs.daily_batch_failure_report.fetch_failure_reports_by_date", return_value=rows
    ), patch("jobs.daily_batch_failure_report.ensure_bucket"), patch(
        "jobs.daily_batch_failure_report.put_json",
        side_effect=lambda bucket, key, data: captured.setdefault("raw", data),
    ), patch(
        "jobs.daily_batch_failure_report._ensure_bronze_columns"
    ), patch(
        "jobs.daily_batch_failure_report.overwrite_partition",
        side_effect=lambda *args, **kwargs: captured.setdefault("arrow", args[1]),
    ):
        count = _process_one_day(REQUESTED, cutoff=CUTOFF)

    assert count == len(rows)
    assert len(captured["arrow"]) == len(rows)
    # 요청일 파티션 하나에 여러 신고일이 함께 들어간다.
    assert set(captured["arrow"].column("reg_date_partition").to_pylist()) == {REQUESTED_STR}
    assert len(set(captured["arrow"].column("reg_dttm").to_pylist())) == len(rows)


def test_bronze_raw_payload_carries_request_metadata():
    captured = {}

    with patch(
        "jobs.daily_batch_failure_report.fetch_failure_reports_by_date",
        return_value=api_rows("2026-08-25 09:00:00"),
    ), patch("jobs.daily_batch_failure_report.ensure_bucket"), patch(
        "jobs.daily_batch_failure_report.put_json",
        side_effect=lambda bucket, key, data: captured.setdefault("raw", data),
    ), patch(
        "jobs.daily_batch_failure_report._ensure_bronze_columns"
    ), patch("jobs.daily_batch_failure_report.overwrite_partition"):
        _process_one_day(REQUESTED, cutoff=CUTOFF)

    assert captured["raw"]["requested_date"] == REQUESTED_STR
    assert captured["raw"]["observed_at"] == CUTOFF.isoformat()
    assert captured["raw"]["reg_dt"] == REQUESTED_STR   # 기존 키 호환
