import pytest

from schema.station_master_schema import (
    SchemaValidationError,
    is_station_master_file,
    normalize_station_no,
    validate_and_report,
)

# tbCycleStationInfo 실측 응답 필드 (2026-08-11 확인)
API_COLUMNS = [
    "STA_LOC", "RENT_ID", "RENT_NO", "RENT_NM", "RENT_ID_NM",
    "HOLD_NUM", "STA_ADD1", "STA_ADD2", "STA_LAT", "STA_LONG",
]

# xlsx 파일 (skiprows=5 후 수동 지정하는 컬럼명)
FILE_COLUMNS = [
    "대여소번호", "대여소명", "자치구", "상세주소", "위도", "경도",
    "설치시기", "LCD거치대수", "QR거치대수", "운영방식",
]

RENTAL_HISTORY_COLUMNS = ["자전거번호", "대여일시", "이용시간(분)", "반납일시"]


def test_api_format_passes():
    result = validate_and_report(API_COLUMNS)
    assert result["unknown_columns"] == []
    # API엔 설치시기/LCD·QR거치대수/운영방식이 없다
    assert "install_date" in result["missing_optional"]


def test_file_format_passes():
    result = validate_and_report(FILE_COLUMNS)
    assert result["unknown_columns"] == []
    # 파일엔 station_id(ST-xxx)/RENT_ID_NM/address2가 없다
    assert "station_id" in result["missing_optional"]


@pytest.mark.parametrize("missing_col", ["RENT_NO", "RENT_NM", "STA_LAT", "STA_LONG"])
def test_missing_required_column_raises_api(missing_col):
    broken = [c for c in API_COLUMNS if c != missing_col]
    with pytest.raises(SchemaValidationError):
        validate_and_report(broken)


@pytest.mark.parametrize("missing_col", ["대여소번호", "대여소명", "위도", "경도"])
def test_missing_required_column_raises_file(missing_col):
    broken = [c for c in FILE_COLUMNS if c != missing_col]
    with pytest.raises(SchemaValidationError):
        validate_and_report(broken)


def test_is_station_master_file_accepts_both_formats():
    assert is_station_master_file(API_COLUMNS) is True
    assert is_station_master_file(FILE_COLUMNS) is True


def test_is_station_master_file_rejects_other_dataset():
    assert is_station_master_file(RENTAL_HISTORY_COLUMNS) is False


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("00108", "108"),  # API의 5자리 zero-padding
        ("108", "108"),  # 파일은 padding 없음
        ("01022", "1022"),
        ("0", "0"),  # 전부 0이어도 빈 문자열이 되면 안 됨
        ("000", "0"),
        ("ST-99", "ST-99"),  # 숫자가 아니면 원본 유지
        ("  00042  ", "42"),  # 공백 정리도 함께
    ],
)
def test_normalize_station_no(raw, expected):
    """
    실측 확인: API는 zero-padding('00108'), 파일은 padding 없음('108').
    정규화 없이 비교하면 같은 대여소가 전부 다른 것으로 잡힌다.
    """
    assert normalize_station_no(raw) == expected


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("공공자전거 대여소 정보(26.6월 기준).xlsx", "2026-06-30"),
        ("공공자전거 대여소 정보(2026.6월 기준).xlsx", "2026-06-30"),
        ("공공자전거 대여소 정보(25.12월 기준).xlsx", "2025-12-31"),  # 연도 넘김
        ("공공자전거 대여소 정보(26.2월 기준).xlsx", "2026-02-28"),  # 2월 말일
        ("station_2026-06-30.csv", "2026-06-30"),
    ],
)
def test_parse_snapshot_date_from_filename(filename, expected):
    """파일 안에 기준일 컬럼이 없어서 파일명이 유일한 근거다 (source_data 실측)."""
    from jobs.backfill_station_master import parse_snapshot_date_from_filename

    assert parse_snapshot_date_from_filename(filename) == expected


def test_parse_snapshot_date_fails_loudly_when_unparseable():
    """잘못된 날짜로 조용히 적재하는 대신 명확히 실패해야 한다."""
    from jobs.backfill_station_master import parse_snapshot_date_from_filename

    with pytest.raises(ValueError, match="SNAPSHOT_DATE"):
        parse_snapshot_date_from_filename("대여소정보_최신.xlsx")
