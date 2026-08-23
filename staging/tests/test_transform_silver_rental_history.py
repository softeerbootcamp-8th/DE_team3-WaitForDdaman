"""
silver.rental_history 변환/중복 제거 로직 테스트 (#143)

이 잡은 이 파이프라인에서 DuckDB가 볼륨 때문에 실제로 필요한 두 곳 중 하나다
(중복 제거가 윈도우 함수라 pyarrow만으로는 못 옮긴다). 그래서 다른 Silver 잡보다
검증할 게 많다.

  1. rent_dt/return_dt의 알려진 포맷 4종이 Spark to_timestamp와 같게 파싱되는가
  2. 알려지지 않은 포맷은 조용히 드롭하지 않고 배치를 멈추는가
  3. 중복 제거 tie-break 순서(return_dt asc nulls last, rent_station_id asc nulls first)가
     Spark와 같은가 - DuckDB의 기본 NULL 정렬이 Spark와 반대라 여기서 어긋나기 쉽다
  4. 중복 제거 윈도우가 하루 안에서 닫히는가 (날짜 청크 분해 안전성의 근거)
"""
import os
from datetime import date, datetime, timezone
from unittest import mock

import pyarrow as pa
import pytest

import jobs.transform_silver_rental_history as tsr
from jobs.transform_silver_rental_history import (
    PARTITION_COLUMN,
    SILVER_COLUMNS,
    SILVER_PARTITION_SPEC,
    SILVER_PROMOTION_PREFIX,
    SILVER_SCHEMA,
    SilverValidationError,
    _build_silver_promotion_document,
    _derive_mode,
    _find_current_day_entry,
    _load_bronze_promotion_metadata,
    _parse_bool_env,
    _silver_promotion_key,
    _validate_bronze_marker,
    run,
    transform,
    validate,
)

STRING_COLUMNS = [
    "bike_id", "rent_dt", "return_dt", "use_distance_m",
    "rent_station_id", "return_station_id", "rent_date_partition", "source_file",
]

DEFAULT_ROW = {
    "bike_id": "SPB-30036",
    "rent_dt": "2026-08-21 18:12:03",
    "return_dt": "2026-08-21 18:39:10",
    "use_distance_m": "664.90",
    "rent_station_id": "ST-2697",
    "return_station_id": "ST-1840",
    "rent_date_partition": "2026-08-21",
    "source_file": "api:2026-08-21",
}

INGESTED_AT = datetime(2026, 8, 22, 10, 6, 53, tzinfo=timezone.utc)


def bronze_table(*overrides) -> pa.Table:
    """브론즈 모양(전부 STRING + ingested_at timestamptz)의 PyArrow Table을 만든다."""
    rows = []
    for over in overrides or [{}]:
        row = dict(DEFAULT_ROW)
        row.update(over)
        rows.append(row)
    columns = {
        col: pa.array([r[col] for r in rows], type=pa.string()) for col in STRING_COLUMNS
    }
    columns["ingested_at"] = pa.array([INGESTED_AT] * len(rows), type=pa.timestamp("us", tz="UTC"))
    return pa.table(columns)


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


# ---------------------------------------------------------------- 날짜 파싱


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2026-08-21 18:12:03", utc(2026, 8, 21, 18, 12, 3)),  # yyyy-MM-dd HH:mm:ss
        ("20260821181203", utc(2026, 8, 21, 18, 12, 3)),        # yyyyMMddHHmmss
        ("2026-08-21", utc(2026, 8, 21)),                       # yyyy-MM-dd
        ("20260821", utc(2026, 8, 21)),                         # yyyyMMdd
    ],
)
def test_known_datetime_formats_parse(raw, expected):
    row = transform(bronze_table({"rent_dt": raw, "return_dt": raw})).to_pylist()[0]
    assert row["rent_dt"] == expected
    assert row["return_dt"] == expected


def test_unknown_datetime_format_stops_the_batch():
    """조용히 드롭하면 그만큼 실버가 소리 없이 비어버린다 - 배치를 멈춘다."""
    with pytest.raises(SilverValidationError, match="파싱 실패"):
        transform(bronze_table({"rent_dt": "2026년 8월 21일"}))


def test_empty_return_dt_is_not_a_parse_failure():
    """원본이 빈 문자열이면 미반납이지 포맷 오류가 아니다."""
    row = transform(bronze_table({"return_dt": ""})).to_pylist()[0]
    assert row["return_dt"] is None


def test_timestamps_are_timestamptz():
    schema = transform(bronze_table()).schema
    assert schema.field("rent_dt").type == pa.timestamp("us", tz="UTC")
    assert schema.field("ingested_at").type == pa.timestamp("us", tz="UTC")


def test_use_distance_m_becomes_double():
    row = transform(bronze_table({"use_distance_m": "664.90"})).to_pylist()[0]
    assert row["use_distance_m"] == pytest.approx(664.90)


def test_unparseable_distance_becomes_null_not_a_failure():
    """Spark의 cast()와 동일하게 실패 시 null (파싱 실패 감지 대상은 날짜뿐)."""
    row = transform(bronze_table({"use_distance_m": "거리없음"})).to_pylist()[0]
    assert row["use_distance_m"] is None


# ---------------------------------------------------------------- 결측 / 중복 제거


def test_missing_bike_id_or_rent_dt_is_dropped():
    table = transform(
        bronze_table(
            {"bike_id": "SPB-1"},
            {"bike_id": None},
            {"bike_id": "SPB-3", "rent_dt": ""},
        )
    )
    assert [r["bike_id"] for r in table.to_pylist()] == ["SPB-1"]


def test_same_bike_and_rent_dt_is_deduped():
    table = transform(
        bronze_table(
            {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00", "return_dt": "2026-08-21 11:00:00"},
            {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00", "return_dt": "2026-08-21 12:00:00"},
        )
    )
    assert table.num_rows == 1
    # return_dt asc -> 이른 쪽이 남는다
    assert table.to_pylist()[0]["return_dt"] == utc(2026, 8, 21, 11, 0, 0)


def test_different_rent_dt_is_not_deduped():
    table = transform(
        bronze_table(
            {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00"},
            {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:01"},
        )
    )
    assert table.num_rows == 2


def test_null_return_dt_loses_tie_break():
    """Spark의 asc_nulls_last()와 동일하게 미반납 행은 뒤로 밀린다."""
    table = transform(
        bronze_table(
            {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00", "return_dt": ""},
            {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00", "return_dt": "2026-08-21 11:00:00"},
        )
    )
    assert table.num_rows == 1
    assert table.to_pylist()[0]["return_dt"] == utc(2026, 8, 21, 11, 0, 0)


def test_null_rent_station_id_wins_tie_break():
    """
    Spark의 `asc()`는 NULLS FIRST가 기본이고 DuckDB의 `ASC`는 NULLS LAST가 기본이라,
    명시하지 않으면 여기서 결과가 갈린다. Spark 쪽(NULLS FIRST)에 맞춘 걸 고정한다.
    """
    table = transform(
        bronze_table(
            {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00", "rent_station_id": "ST-1"},
            {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00", "rent_station_id": None},
        )
    )
    assert table.num_rows == 1
    assert table.to_pylist()[0]["rent_station_id"] is None


def test_dedup_is_deterministic_when_sort_keys_tie():
    """
    Spark 시절 정렬키(return_dt, rent_station_id)만으로는 완전 동률인 그룹에서
    어느 행이 남는지가 실행마다 달라졌다(실측). 남은 컬럼을 뒤에 붙여 전순서로
    만들었으므로, 입력 행 순서를 뒤집어도 같은 행이 남아야 한다.
    """
    row_a = {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00",
             "return_dt": "2026-08-21 11:00:00", "rent_station_id": "ST-1",
             "return_station_id": "ST-9", "use_distance_m": "2170.70"}
    row_b = {**row_a, "return_station_id": "ST-2", "use_distance_m": "2689.23"}

    forward = transform(bronze_table(row_a, row_b)).to_pylist()
    reversed_ = transform(bronze_table(row_b, row_a)).to_pylist()

    assert len(forward) == len(reversed_) == 1
    assert forward[0] == reversed_[0]
    # 추가된 tie-break 첫 키가 return_station_id라 사전순으로 앞선 ST-2가 남는다
    assert forward[0]["return_station_id"] == "ST-2"


def test_dedup_window_closes_within_one_day():
    """
    중복 제거 윈도우가 `(bike_id, rent_dt)`라 한 그룹의 모든 행은 rent_dt가 완전히
    같고, rent_date_partition은 rent_dt에서 파생되므로 같은 파티션에 들어간다.
    -> 날짜 청크로 잘라 처리해도(MAX_DAYS_PER_RUN) 그룹이 반으로 갈리지 않는다.

    청크를 나눠 두 번 돌린 결과와 한 번에 돌린 결과가 같은지로 확인한다.
    """
    day1 = [
        {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00", "return_dt": "2026-08-21 11:00:00",
         "rent_date_partition": "2026-08-21"},
        {"bike_id": "SPB-1", "rent_dt": "2026-08-21 10:00:00", "return_dt": "2026-08-21 12:00:00",
         "rent_date_partition": "2026-08-21"},
    ]
    day2 = [
        {"bike_id": "SPB-1", "rent_dt": "2026-08-22 10:00:00", "return_dt": "2026-08-22 11:00:00",
         "rent_date_partition": "2026-08-22"},
    ]

    at_once = transform(bronze_table(*(day1 + day2))).to_pylist()
    chunked = transform(bronze_table(*day1)).to_pylist() + transform(bronze_table(*day2)).to_pylist()

    key = lambda rows: sorted((r["bike_id"], r["rent_dt"], r["return_dt"]) for r in rows)
    assert key(at_once) == key(chunked)
    assert len(at_once) == 2


# ---------------------------------------------------------------- 출력 스키마 / 검증


def test_output_columns_and_partition_spec_unchanged():
    assert transform(bronze_table()).column_names == SILVER_COLUMNS
    assert [f.name for f in SILVER_SCHEMA.fields] == SILVER_COLUMNS
    fields = SILVER_PARTITION_SPEC.fields
    assert len(fields) == 1 and fields[0].name == PARTITION_COLUMN
    assert fields[0].transform.__class__.__name__ == "IdentityTransform"


def test_validate_passes_on_clean_rows():
    validate(transform(bronze_table()), "2026-08-21")  # 예외가 없으면 통과


def test_validate_rejects_negative_distance():
    with pytest.raises(SilverValidationError, match="isNonNegative"):
        validate(transform(bronze_table({"use_distance_m": "-1.0"})), "2026-08-21")


def test_validate_rejects_return_before_rent():
    with pytest.raises(SilverValidationError, match="satisfies"):
        validate(
            transform(bronze_table({"rent_dt": "2026-08-21 12:00:00", "return_dt": "2026-08-21 11:00:00"})),
            "2026-08-21",
        )


# ---------------------------------------------------------------- #137 당일 promotion 처리
#
# 06시 일 배치의 publish_bronze_asset이 Asset event에 실어 보낸 정확한 run_date/
# promotion_id/promotion_key만 신뢰해야 한다 (S3의 "최신 COMPLETE promotion"을
# 추측하면 Catchup이 이전 일 배치 promotion을 잘못 집어 처리할 수 있다 - #137 배경).


def _env(**overrides):
    base = {
        "RENTAL_HISTORY_BRONZE_RUN_DATE": "",
        "RENTAL_HISTORY_BRONZE_PROMOTION_ID": "",
        "RENTAL_HISTORY_BRONZE_PROMOTION_KEY": "",
    }
    base.update(overrides)
    return mock.patch.dict(os.environ, base, clear=False)


def test_parse_bool_env_accepts_true_false_only():
    with mock.patch.dict(os.environ, {"FLAG": "true"}):
        assert _parse_bool_env("FLAG") is True
    with mock.patch.dict(os.environ, {"FLAG": "false"}):
        assert _parse_bool_env("FLAG") is False
    with mock.patch.dict(os.environ, {}, clear=True):
        assert _parse_bool_env("FLAG") is False
        assert _parse_bool_env("FLAG", default=True) is True


def test_parse_bool_env_rejects_unknown_values():
    with mock.patch.dict(os.environ, {"FLAG": "yes"}):
        with pytest.raises(SilverValidationError):
            _parse_bool_env("FLAG")


def test_load_bronze_promotion_metadata_requires_all_three_fields():
    """Catchup/수동 트리거처럼 Asset event에 metadata가 없으면 셋 다 빈 문자열이고,
    확정 구간만 처리하도록 None을 돌려줘야 한다."""
    with _env():
        assert _load_bronze_promotion_metadata() is None

    with _env(RENTAL_HISTORY_BRONZE_RUN_DATE="2026-08-22"):
        assert _load_bronze_promotion_metadata() is None  # promotion_id/key 없음


def test_load_bronze_promotion_metadata_parses_when_present():
    with _env(
        RENTAL_HISTORY_BRONZE_RUN_DATE="2026-08-22",
        RENTAL_HISTORY_BRONZE_PROMOTION_ID="20260822T060000+0900",
        RENTAL_HISTORY_BRONZE_PROMOTION_KEY="_meta/promotion/bronze_rental_history/x/promotion.json",
    ):
        metadata = _load_bronze_promotion_metadata()
    assert metadata == {
        "run_date": "2026-08-22",
        "promotion_id": "20260822T060000+0900",
        "promotion_key": "_meta/promotion/bronze_rental_history/x/promotion.json",
    }


VALID_MARKER = {
    "dataset": "rental_history",
    "run_date": "2026-08-22",
    "promotion_id": "20260822T060000+0900",
    "status": "COMPLETE",
    "t0_enabled": True,
    "selected_snapshots": [
        {"target_date": "2026-08-21", "snapshot_type": "FINAL", "observed_at": "2026-08-22T06:00:00+09:00"},
        {"target_date": "2026-08-22", "snapshot_type": "FINAL", "observed_at": "2026-08-22T06:00:00+09:00"},
    ],
}


def test_validate_bronze_marker_passes_on_matching_complete_marker():
    marker = _validate_bronze_marker(
        dict(VALID_MARKER), "2026-08-22", "20260822T060000+0900", "some/key.json"
    )
    assert marker["status"] == "COMPLETE"


@pytest.mark.parametrize(
    "overrides",
    [
        {"dataset": "failure_report"},
        {"run_date": "2026-08-21"},
        {"promotion_id": "20260822T070000+0900"},
        {"status": "IN_PROGRESS"},
    ],
)
def test_validate_bronze_marker_rejects_mismatch(overrides):
    marker = {**VALID_MARKER, **overrides}
    with pytest.raises(SilverValidationError):
        _validate_bronze_marker(marker, "2026-08-22", "20260822T060000+0900", "some/key.json")


def test_validate_bronze_marker_rejects_missing_marker():
    with pytest.raises(SilverValidationError):
        _validate_bronze_marker(None, "2026-08-22", "20260822T060000+0900", "some/key.json")


def test_find_current_day_entry_matches_target_date():
    entry = _find_current_day_entry(VALID_MARKER, "2026-08-22")
    assert entry["target_date"] == "2026-08-22"
    assert entry["snapshot_type"] == "FINAL"


def test_find_current_day_entry_returns_none_when_absent():
    marker = {**VALID_MARKER, "selected_snapshots": VALID_MARKER["selected_snapshots"][:1]}
    assert _find_current_day_entry(marker, "2026-08-22") is None


def test_derive_mode_final_is_normal_preliminary_is_degraded():
    assert _derive_mode(None, "FINAL") == "NORMAL"
    assert _derive_mode("NORMAL", "PRELIMINARY") == "DEGRADED"


def test_derive_mode_inherits_degraded_bronze_mode_even_when_today_is_final():
    """backlog 날짜가 PRELIMINARY라 Bronze marker.mode가 DEGRADED면, 당일 entry가
    FINAL이어도 Silver mode는 DEGRADED를 계승해야 한다."""
    assert _derive_mode("DEGRADED", "FINAL") == "DEGRADED"


def test_silver_promotion_key_matches_documented_layout():
    key = _silver_promotion_key("2026-08-22", "20260822T060000+0900")
    assert key == (
        f"{SILVER_PROMOTION_PREFIX}run_date=2026-08-22/"
        "promotion_id=20260822T060000+0900/promotion.json"
    )


def test_build_silver_promotion_document_final_is_normal_mode():
    document = _build_silver_promotion_document(
        run_date_str="2026-08-22",
        promotion_id="20260822T060000+0900",
        source_bronze_promotion_key="_meta/promotion/bronze_rental_history/x/promotion.json",
        entry={"target_date": "2026-08-22", "snapshot_type": "FINAL", "observed_at": "2026-08-22T06:00:00+09:00"},
        bronze_mode="NORMAL",
        silver_row_count=12345,
        confirmed_through=date(2026, 8, 21),
        processed_at="2026-08-22T06:08:00+00:00",
    )
    assert document == {
        "dataset": "rental_history",
        "run_date": "2026-08-22",
        "promotion_id": "20260822T060000+0900",
        "source_bronze_promotion_key": "_meta/promotion/bronze_rental_history/x/promotion.json",
        "mode": "NORMAL",
        "source_snapshot_type": "FINAL",
        "source_observed_at": "2026-08-22T06:00:00+09:00",
        "processed_partition": "2026-08-22",
        "silver_row_count": 12345,
        "confirmed_through": "2026-08-21",
        "status": "COMPLETE",
        "processed_at": "2026-08-22T06:08:00+00:00",
    }


def test_build_silver_promotion_document_preliminary_is_degraded_mode():
    document = _build_silver_promotion_document(
        run_date_str="2026-08-22",
        promotion_id="20260822T060000+0900",
        source_bronze_promotion_key="_meta/promotion/bronze_rental_history/x/promotion.json",
        entry={"target_date": "2026-08-22", "snapshot_type": "PRELIMINARY", "observed_at": "2026-08-22T05:00:00+09:00"},
        bronze_mode="DEGRADED",
        silver_row_count=10,
        confirmed_through=date(2026, 8, 21),
        processed_at="2026-08-22T06:08:00+00:00",
    )
    assert document["mode"] == "DEGRADED"
    assert document["source_snapshot_type"] == "PRELIMINARY"


# --------------------------------------------------------- run() 처리 순서 (#137)
#
# 확정 구간(_process_range)과 당일 promotion 처리(_prepare_current_day_promotion)가
# 둘 다 성공해야 확정 워터마크(write_watermark)를 전진시켜야 한다. 당일 처리가
# 실패했는데 워터마크가 먼저 전진하면, 다음 실행이 실패한 당일 파티션을 확정
# backlog로 다시 확인할 기회를 잃는다. marker 저장(_persist_current_day_promotion_marker)은
# 워터마크를 쓴 뒤에도 항상 전체 처리의 마지막 단계여야 한다(marker-last 보장).


def _run_with_mocks(monkeypatch, *, current_day_raises=False, promotion_meta=None):
    """run()을 실제 S3/Iceberg 없이 호출하고 호출 순서를 기록한다."""
    order = []
    silver_watermark = date(2026, 8, 19)
    bronze_watermark = date(2026, 8, 21)

    def fake_read_watermark(default_start=None, watermark_key=None):
        if watermark_key == tsr.SILVER_WATERMARK_KEY:
            return silver_watermark
        return bronze_watermark

    def fake_process_range(catalog, silver_table, start, end):
        order.append(("process_range", start, end))
        return 10

    def fake_prepare(catalog, silver_table, meta, confirmed_through):
        order.append(("prepare_current_day", confirmed_through))
        if current_day_raises:
            raise SilverValidationError("당일 파티션 처리 실패")
        return {"bucket": "b", "key": "k", "document": {
            "run_date": "2026-08-22", "promotion_id": "p", "mode": "NORMAL", "silver_row_count": 5,
        }}

    def fake_write_watermark(processed_date, watermark_key=None, extra=None):
        order.append(("write_watermark", processed_date))

    def fake_persist(prepared):
        order.append(("persist_marker", prepared["key"]))

    monkeypatch.setattr(tsr, "ensure_bucket", lambda bucket: None)
    monkeypatch.setattr(tsr, "build_iceberg_catalog", lambda: object())
    monkeypatch.setattr(tsr, "_ensure_silver_table", lambda catalog: object())
    monkeypatch.setattr(tsr, "read_watermark", fake_read_watermark)
    monkeypatch.setattr(tsr, "write_watermark", fake_write_watermark)
    monkeypatch.setattr(tsr, "_process_range", fake_process_range)
    monkeypatch.setattr(tsr, "_parse_bool_env", lambda name: promotion_meta is not None)
    monkeypatch.setattr(tsr, "_load_bronze_promotion_metadata", lambda: promotion_meta)
    monkeypatch.setattr(tsr, "_prepare_current_day_promotion", fake_prepare)
    monkeypatch.setattr(tsr, "_persist_current_day_promotion_marker", fake_persist)

    return order


def test_run_writes_watermark_only_after_current_day_promotion_succeeds(monkeypatch):
    """성공 시 순서: 확정 구간 -> 당일 promotion -> 확정 워터마크 -> marker 저장."""
    order = _run_with_mocks(
        monkeypatch,
        promotion_meta={"run_date": "2026-08-22", "promotion_id": "p", "promotion_key": "k"},
    )

    run()

    assert [step[0] for step in order] == [
        "process_range", "prepare_current_day", "write_watermark", "persist_marker",
    ]
    # 확정 워터마크는 confirmed_end_date(bronze_watermark)로 전진해야 한다
    assert order[2][1] == date(2026, 8, 21)


def test_run_does_not_advance_watermark_when_current_day_promotion_fails(monkeypatch, capsys):
    """
    #137 회귀: 당일 promotion 처리가 실패하면, 확정 구간이 이미 성공했더라도
    확정 워터마크를 전진시키면 안 된다 (marker도 당연히 남기지 않는다).
    """
    order = _run_with_mocks(
        monkeypatch,
        current_day_raises=True,
        promotion_meta={"run_date": "2026-08-22", "promotion_id": "p", "promotion_key": "k"},
    )

    with pytest.raises(SystemExit):
        run()

    steps = [step[0] for step in order]
    assert steps == ["process_range", "prepare_current_day"]
    assert "write_watermark" not in steps
    assert "persist_marker" not in steps


def test_run_skips_current_day_and_still_advances_watermark_without_promotion_meta(monkeypatch):
    """당일 promotion metadata가 없으면(Catchup/수동 트리거) 확정 워터마크만 전진한다."""
    order = _run_with_mocks(monkeypatch, promotion_meta=None)

    run()

    assert [step[0] for step in order] == ["process_range", "write_watermark"]


def test_build_silver_promotion_document_inherits_degraded_bronze_mode_on_mixed_promotion():
    """확정 backlog가 PRELIMINARY라 Bronze marker.mode=DEGRADED인 채로 당일 entry가
    FINAL이면, 당일 entry만 보고 NORMAL로 기록하지 않고 DEGRADED를 계승해야 한다."""
    document = _build_silver_promotion_document(
        run_date_str="2026-08-22",
        promotion_id="20260822T060000+0900",
        source_bronze_promotion_key="_meta/promotion/bronze_rental_history/x/promotion.json",
        entry={"target_date": "2026-08-22", "snapshot_type": "FINAL", "observed_at": "2026-08-22T06:00:00+09:00"},
        bronze_mode="DEGRADED",
        silver_row_count=5,
        confirmed_through=date(2026, 8, 21),
        processed_at="2026-08-22T06:08:00+00:00",
    )
    assert document["mode"] == "DEGRADED"
    assert document["source_snapshot_type"] == "FINAL"
