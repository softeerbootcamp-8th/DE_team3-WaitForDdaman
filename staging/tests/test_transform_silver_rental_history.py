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
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from jobs.transform_silver_rental_history import (
    PARTITION_COLUMN,
    SILVER_COLUMNS,
    SILVER_PARTITION_SPEC,
    SILVER_SCHEMA,
    SilverValidationError,
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
