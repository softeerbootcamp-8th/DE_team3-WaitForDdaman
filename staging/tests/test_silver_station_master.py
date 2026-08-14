"""
silver.station_master 정제 로직 테스트

브론즈는 원본 보존을 위해 전부 STRING이다. 실버는 타입 캐스팅 / region 파생 /
문자열 정규화 세 가지만 하고, 골드가 쓰지 않는 컬럼은 떨어뜨린다.

Iceberg/S3가 필요 없으므로 카탈로그 설정 없는 최소 SparkSession을 직접 만든다.
"""
import pytest
from pyspark.sql import types as T

from jobs.silver_station_master import (
    REGION_BY_DISTRICT,
    SILVER_COLUMNS,
    UnknownDistrictError,
    normalize,
)

# 브론즈 테이블의 컬럼 구성 (전부 STRING, ingested_at만 timestamp)
BRONZE_SCHEMA = T.StructType([
    T.StructField(name, T.StringType())
    for name in [
        "station_no", "station_id", "station_name", "station_id_name",
        "district", "hold_num", "address1", "address2",
        "latitude", "longitude", "snapshot_date", "source_file",
    ]
])

DEFAULT_ROW = {
    "station_no": "108",
    "station_id": "ST-10",
    "station_name": "서교동 사거리",
    "station_id_name": "108. 서교동 사거리",
    "district": "마포구",
    "hold_num": "12",
    "address1": "서울특별시 마포구 양화로 93",
    "address2": "427",
    "latitude": "37.55274582",
    "longitude": "126.91861725",
    "snapshot_date": "2026-08-14",
    "source_file": "api:2026-08-14",
}


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-silver-station-master")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def bronze_df(spark, *overrides):
    """브론즈 모양의 DataFrame을 만든다. 인자마다 한 행이 된다."""
    rows = []
    for over in overrides or [{}]:
        row = dict(DEFAULT_ROW)
        row.update(over)
        rows.append(tuple(row[f.name] for f in BRONZE_SCHEMA.fields))
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def one(df):
    return df.collect()[0]


# ---------------------------------------------------------------- 타입 캐스팅


def test_coordinates_become_double(spark):
    row = one(normalize(bronze_df(spark)))
    assert row["latitude"] == pytest.approx(37.55274582)
    assert row["longitude"] == pytest.approx(126.91861725)
    assert isinstance(row["latitude"], float)


def test_hold_num_becomes_int(spark):
    row = one(normalize(bronze_df(spark, {"hold_num": "12"})))
    assert row["hold_num"] == 12
    assert isinstance(row["hold_num"], int)


def test_snapshot_date_becomes_date(spark):
    from datetime import date

    row = one(normalize(bronze_df(spark, {"snapshot_date": "2026-08-14"})))
    assert row["snapshot_date"] == date(2026, 8, 14)


def test_empty_hold_num_becomes_null_not_zero(spark):
    """
    실측(2026-08-14): hold_num이 없는 대여소가 15곳 있다.
    0으로 채우면 target_bike_cnt = 0이 되어 "자전거를 다 빼라"는 뜻이 된다.
    """
    row = one(normalize(bronze_df(spark, {"hold_num": ""})))
    assert row["hold_num"] is None


def test_unparseable_coordinate_becomes_null(spark):
    row = one(normalize(bronze_df(spark, {"latitude": "위도없음"})))
    assert row["latitude"] is None


# ---------------------------------------------------------------- region 파생


def test_region_map_covers_25_districts(spark):
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
def test_region_derived_from_district(spark, district, expected):
    row = one(normalize(bronze_df(spark, {"district": district})))
    assert row["region"] == expected


def test_unknown_district_raises(spark):
    """
    원천에 없던 자치구가 나오면 조용히 null로 두지 않고 실패시킨다.
    서울시가 자치구를 신설하는 일은 없으므로, 나오면 원천 이상이다.
    """
    with pytest.raises(UnknownDistrictError, match="분당구"):
        normalize(bronze_df(spark, {"district": "분당구"})).collect()


# ---------------------------------------------------------------- 문자열 정규화


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("  망원역 1번출구 앞  ", "망원역 1번출구 앞"),
        ("망원역  1번출구", "망원역 1번출구"),
        ("강남구 개포동\n(개포동역)", "강남구 개포동 (개포동역)"),
    ],
)
def test_station_name_normalized(spark, raw, expected):
    row = one(normalize(bronze_df(spark, {"station_name": raw})))
    assert row["station_name"] == expected


# ---------------------------------------------------------------- 출력 스키마


def test_output_has_exactly_eight_columns(spark):
    df = normalize(bronze_df(spark))
    assert df.columns == SILVER_COLUMNS
    assert len(SILVER_COLUMNS) == 8


@pytest.mark.parametrize(
    "dropped", ["station_no", "station_id_name", "address1", "address2", "source_file"]
)
def test_unused_columns_dropped(spark, dropped):
    """골드·프론트·백엔드에서 쓰지 않는 컬럼은 실버에 넣지 않는다."""
    assert dropped not in normalize(bronze_df(spark)).columns


def test_zero_coordinate_row_is_kept(spark):
    """
    실측(2026-08-14): 위경도가 0.00000000인 대여소가 3곳 있다(ST-1090 등).
    원천 오류를 실버가 감추지 않는다. 세 곳 모두 실시간 API 응답이 없어
    골드의 운영 대여소 필터에서 어차피 제외된다.
    """
    df = normalize(bronze_df(spark, {"latitude": "0.00000000", "longitude": "0.00000000"}))
    assert df.count() == 1
    assert one(df)["latitude"] == 0.0


def test_multiple_rows_are_all_processed(spark):
    df = normalize(
        bronze_df(
            spark,
            {"station_id": "ST-10", "district": "마포구"},
            {"station_id": "ST-100", "district": "강남구"},
        )
    )
    regions = {r["station_id"]: r["region"] for r in df.collect()}
    assert regions == {"ST-10": "강북", "ST-100": "강남"}
