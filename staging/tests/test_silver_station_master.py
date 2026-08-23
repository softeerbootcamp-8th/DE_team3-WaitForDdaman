"""
silver.station_master 정제 로직 테스트

브론즈는 원본 보존을 위해 전부 STRING이다. 실버는 타입 캐스팅 / region 파생 /
문자열 정규화 세 가지만 하고, 골드가 쓰지 않는 컬럼은 떨어뜨린다.

#143에서 Spark를 걷어내고 DuckDB로 옮겼으므로 SparkSession 대신 PyArrow Table을
직접 만들어 넣는다 - Iceberg/S3도 필요 없고, 세션 기동이 없어 훨씬 빠르다.
"""
from datetime import date

import pyarrow as pa
import pytest

from jobs.silver_station_master import (
    REGION_BY_DISTRICT,
    SILVER_COLUMNS,
    UnknownDistrictError,
    normalize,
)

# bronze.station_master 중 실버가 읽는 컬럼 (전부 STRING, daily_batch_station_master.py 참고)
BRONZE_COLUMNS = [
    "snapshot_date", "station_id", "station_name", "district",
    "latitude", "longitude", "hold_num", "source_file",
]

DEFAULT_ROW = {
    "snapshot_date": "2026-08-14",
    "station_id": "ST-10",
    "station_name": "서교동 사거리",
    "district": "마포구",
    "latitude": "37.55274582",
    "longitude": "126.91861725",
    "hold_num": "12",
    "source_file": "api:2026-08-14",
}


def bronze_table(*overrides) -> pa.Table:
    """브론즈 모양의 PyArrow Table을 만든다. 인자마다 한 행이 된다."""
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


# ---------------------------------------------------------------- 타입 캐스팅


def test_coordinates_become_double():
    row = one(normalize(bronze_table()))
    assert row["latitude"] == pytest.approx(37.55274582)
    assert row["longitude"] == pytest.approx(126.91861725)
    assert isinstance(row["latitude"], float)


def test_hold_num_becomes_int():
    row = one(normalize(bronze_table({"hold_num": "12"})))
    assert row["hold_num"] == 12
    assert isinstance(row["hold_num"], int)


def test_snapshot_date_becomes_date():
    row = one(normalize(bronze_table({"snapshot_date": "2026-08-14"})))
    assert row["snapshot_date"] == date(2026, 8, 14)


def test_empty_hold_num_becomes_null_not_zero():
    """
    실측(2026-08-14): hold_num이 없는 대여소가 15곳 있다.
    0으로 채우면 target_bike_cnt = 0이 되어 "자전거를 다 빼라"는 뜻이 된다.
    """
    row = one(normalize(bronze_table({"hold_num": ""})))
    assert row["hold_num"] is None


def test_unparseable_coordinate_becomes_null():
    """
    Spark의 cast()는 실패 시 null이었다. DuckDB의 CAST는 예외를 던지므로
    TRY_CAST로 옮겼고, 그 동작이 유지되는지 확인한다.
    """
    row = one(normalize(bronze_table({"latitude": "위도없음"})))
    assert row["latitude"] is None


# ---------------------------------------------------------------- region 파생


def test_region_map_covers_25_districts():
    """서울 자치구는 25개로 고정이다."""
    assert len(REGION_BY_DISTRICT) == 25
    assert set(REGION_BY_DISTRICT.values()) == {"강남", "강북"}


@pytest.mark.parametrize(
    "district,expected",
    [
        ("마포구", "강북"),
        ("종로구", "강북"),
        ("노원구", "강북"),
        ("강남구", "강남"),
        ("송파구", "강남"),
        ("강서구", "강남"),
    ],
)
def test_region_derived_from_district(district, expected):
    row = one(normalize(bronze_table({"district": district})))
    assert row["region"] == expected


def test_all_25_districts_map_without_error():
    """CASE 식이 25개 자치구를 하나도 빠뜨리지 않았는지 한 번에 확인한다."""
    table = normalize(bronze_table(*[{"district": d} for d in REGION_BY_DISTRICT]))
    assert table.num_rows == 25
    assert all(r["region"] in ("강남", "강북") for r in table.to_pylist())


def test_unknown_district_raises():
    """
    원천에 없던 자치구가 나오면 조용히 null로 두지 않고 실패시킨다.
    서울시가 자치구를 신설하는 일은 없으므로, 나오면 원천 이상이다.
    """
    with pytest.raises(UnknownDistrictError, match="분당구"):
        normalize(bronze_table({"district": "분당구"}))


def test_null_district_does_not_raise():
    """자치구 자체가 null이면 매핑 실패가 아니라 원천 결측이므로 통과시킨다."""
    row = one(normalize(bronze_table({"district": None})))
    assert row["region"] is None


# ---------------------------------------------------------------- 문자열 정규화


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("  망원역 1번출구 앞  ", "망원역 1번출구 앞"),
        ("망원역  1번출구", "망원역 1번출구"),
        ("강남구 개포동\n(개포동역)", "강남구 개포동 (개포동역)"),
    ],
)
def test_station_name_normalized(raw, expected):
    row = one(normalize(bronze_table({"station_name": raw})))
    assert row["station_name"] == expected


# ---------------------------------------------------------------- 출력 스키마


def test_output_has_exactly_eight_columns():
    table = normalize(bronze_table())
    assert table.column_names == SILVER_COLUMNS
    assert len(SILVER_COLUMNS) == 8


@pytest.mark.parametrize("dropped", ["station_no", "station_id_name", "address1", "address2", "source_file"])
def test_unused_columns_dropped(dropped):
    """골드·프론트·백엔드에서 쓰지 않는 컬럼은 실버에 넣지 않는다."""
    assert dropped not in normalize(bronze_table()).column_names


def test_output_arrow_types_match_iceberg_schema():
    """pyiceberg가 그대로 쓸 수 있는 타입이어야 한다 (date32 / double / int32 / string)."""
    schema = normalize(bronze_table()).schema
    assert schema.field("snapshot_date").type == pa.date32()
    assert schema.field("latitude").type == pa.float64()
    assert schema.field("hold_num").type == pa.int32()
    assert schema.field("station_id").type == pa.string()


def test_zero_coordinate_row_is_kept():
    """
    실측(2026-08-14): 위경도가 0.00000000인 대여소가 3곳 있다(ST-1090 등).
    원천 오류를 실버가 감추지 않는다. 세 곳 모두 실시간 API 응답이 없어
    골드의 운영 대여소 필터에서 어차피 제외된다.
    """
    table = normalize(bronze_table({"latitude": "0.00000000", "longitude": "0.00000000"}))
    assert table.num_rows == 1
    assert one(table)["latitude"] == 0.0


def test_multiple_rows_are_all_processed():
    table = normalize(
        bronze_table(
            {"station_id": "ST-10", "district": "마포구"},
            {"station_id": "ST-100", "district": "강남구"},
        )
    )
    regions = {r["station_id"]: r["region"] for r in table.to_pylist()}
    assert regions == {"ST-10": "강북", "ST-100": "강남"}
