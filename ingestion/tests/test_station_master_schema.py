import pytest

from schemas.station_master_schema import (
    OPTIONAL_STANDARD_COLUMNS,
    REQUIRED_STANDARD_COLUMNS,
    SchemaValidationError,
    collect_response_fields,
    is_station_master_response,
    normalize_station_no,
    validate_and_report,
)

# tbCycleStationInfo 실측 응답 필드 (2026-08-14 확인)
API_COLUMNS = [
    "STA_LOC", "RENT_ID", "RENT_NO", "RENT_NM", "RENT_ID_NM",
    "HOLD_NUM", "STA_ADD1", "STA_ADD2", "STA_LAT", "STA_LONG",
]

RENTAL_HISTORY_COLUMNS = ["자전거번호", "대여일시", "이용시간(분)", "반납일시"]


def test_api_format_passes():
    result = validate_and_report(API_COLUMNS)
    assert result["unknown_columns"] == []
    assert result["missing_optional"] == []
    assert result["column_count"] == 10


@pytest.mark.parametrize(
    "missing_col",
    ["RENT_NO", "RENT_ID", "RENT_NM", "STA_LOC", "HOLD_NUM", "STA_LAT", "STA_LONG"],
)
def test_missing_required_column_raises(missing_col):
    """
    골드가 실제로 쓰는 컬럼이 사라지면 즉시 실패해야 한다.
    서울시가 하반기 API 현행화를 예고했으므로 응답 구조가 바뀔 수 있다.
    """
    broken = [c for c in API_COLUMNS if c != missing_col]
    with pytest.raises(SchemaValidationError):
        validate_and_report(broken)


@pytest.mark.parametrize("missing_col", ["RENT_ID_NM", "STA_ADD1", "STA_ADD2"])
def test_missing_optional_column_only_warns(missing_col):
    """다운스트림에서 안 쓰는 보조 정보는 없어도 진행한다."""
    partial = [c for c in API_COLUMNS if c != missing_col]
    result = validate_and_report(partial)
    assert len(result["missing_optional"]) == 1


def test_unknown_column_is_reported_not_fatal():
    """API 현행화로 신규 필드가 생겨도 파이프라인은 멈추지 않되 로그로 알린다."""
    result = validate_and_report(API_COLUMNS + ["NEW_FIELD"])
    assert result["unknown_columns"] == ["NEW_FIELD"]


def test_required_and_optional_do_not_overlap():
    assert set(REQUIRED_STANDARD_COLUMNS).isdisjoint(OPTIONAL_STANDARD_COLUMNS)


def test_is_station_master_response():
    assert is_station_master_response(API_COLUMNS) is True
    assert is_station_master_response(RENTAL_HISTORY_COLUMNS) is False


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("00108", "108"),  # API의 5자리 zero-padding
        ("108", "108"),  # 다른 원천은 padding 없음
        ("01022", "1022"),
        ("0", "0"),  # 전부 0이어도 빈 문자열이 되면 안 됨
        ("000", "0"),
        ("ST-99", "ST-99"),  # 숫자가 아니면 원본 유지
        ("  00042  ", "42"),  # 공백 정리도 함께
    ],
)
def test_normalize_station_no(raw, expected):
    """
    실측 확인: API는 zero-padding('00108'), 실시간 API·대여이력은 padding 없음('108').
    정규화 없이 조인하면 같은 대여소가 전부 다른 것으로 잡힌다.
    """
    assert normalize_station_no(raw) == expected


# ------------------------------------------------------------ 응답 필드 수집
#
# 이 API는 행마다 필드 구성이 다르다 (2026-08-14 실측):
#   3,227행은 필드 10개, 13행은 HOLD_NUM 키가 아예 없어 9개.
# 첫 행만 보고 스키마를 판단하면, HOLD_NUM 없는 행이 응답의 첫 번째로 오는 날
# 필수 컬럼 누락으로 잡 전체가 실패한다. 응답 순서는 보장되지 않는다.


def test_collect_response_fields_merges_all_rows():
    rows = [
        {"RENT_ID": "ST-10", "RENT_NM": "가"},  # HOLD_NUM 없음
        {"RENT_ID": "ST-11", "RENT_NM": "나", "HOLD_NUM": "12"},
    ]
    assert collect_response_fields(rows) == ["HOLD_NUM", "RENT_ID", "RENT_NM"]


def test_collect_response_fields_handles_empty():
    assert collect_response_fields([]) == []


def test_validation_passes_when_only_first_row_lacks_hold_num():
    """실측 13건에 해당하는 상황 - 일부 대여소만 HOLD_NUM이 없다."""
    rows = [
        {c: "x" for c in API_COLUMNS if c != "HOLD_NUM"},
        {c: "x" for c in API_COLUMNS},
    ]
    result = validate_and_report(collect_response_fields(rows))
    assert result["column_count"] == len(API_COLUMNS)


def test_validation_fails_when_no_row_has_hold_num():
    """원천이 필드를 완전히 없앤 경우는 여전히 실패해야 한다."""
    rows = [{c: "x" for c in API_COLUMNS if c != "HOLD_NUM"} for _ in range(3)]
    with pytest.raises(SchemaValidationError, match="hold_num"):
        validate_and_report(collect_response_fields(rows))
