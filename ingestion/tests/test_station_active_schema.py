import pytest

from schema.station_active_schema import (
    REQUIRED_STANDARD_COLUMNS,
    SchemaValidationError,
    collect_response_fields,
    is_station_active_response,
    validate_and_report,
)

# bikeList(rentBikeStatus) 실측 응답 필드 (2026-08-16 확인, 전수 2,735건 페이징)
API_COLUMNS = [
    "stationId", "stationName", "rackTotCnt", "parkingBikeTotCnt",
    "shared", "stationLatitude", "stationLongitude",
]

# station_master(tbCycleStationInfo) 응답 필드 - 다른 서비스가 섞였을 때 판별용
STATION_MASTER_COLUMNS = ["STA_LOC", "RENT_ID", "RENT_NO", "RENT_NM", "RENT_ID_NM", "HOLD_NUM"]


def test_api_format_passes():
    result = validate_and_report(API_COLUMNS)
    assert result["unknown_columns"] == []
    assert result["column_count"] == 7


@pytest.mark.parametrize("missing_col", API_COLUMNS)
def test_missing_required_column_raises(missing_col):
    """
    실측(2026-08-16, 전수 2,735건)상 7개 필드가 전부 빠짐없이 존재했다. 서울시가
    API를 현행화하면 필드가 빠질 수 있으므로 하나라도 없으면 즉시 실패해야 한다.
    """
    broken = [c for c in API_COLUMNS if c != missing_col]
    with pytest.raises(SchemaValidationError):
        validate_and_report(broken)


def test_unknown_column_is_reported_not_fatal():
    """API 현행화로 신규 필드가 생겨도 파이프라인은 멈추지 않되 로그로 알린다."""
    result = validate_and_report(API_COLUMNS + ["NEW_FIELD"])
    assert result["unknown_columns"] == ["NEW_FIELD"]


def test_required_columns_cover_all_mapped_fields():
    assert set(REQUIRED_STANDARD_COLUMNS) == {
        "station_id", "station_name", "rack_tot_cnt",
        "parking_bike_tot_cnt", "shared", "latitude", "longitude",
    }


def test_is_station_active_response():
    assert is_station_active_response(API_COLUMNS) is True
    assert is_station_active_response(STATION_MASTER_COLUMNS) is False


def test_collect_response_fields_merges_all_rows():
    rows = [
        {"stationId": "ST-4", "stationName": "가"},
        {"stationId": "ST-5", "stationName": "나", "shared": "10"},
    ]
    assert collect_response_fields(rows) == ["shared", "stationId", "stationName"]


def test_collect_response_fields_handles_empty():
    assert collect_response_fields([]) == []


def test_shared_value_over_100_does_not_fail_validation():
    """
    실측(2026-08-16): shared=120 같은 100 초과 값이 실제로 관측된다 (거치대 수 대비
    자전거가 더 많이 반납된 경우). 상한 있는 퍼센트가 아니므로 검증 단계에서
    값 범위를 제한하면 안 되고, 컬럼 존재 여부만 검증한다.
    """
    result = validate_and_report(API_COLUMNS)
    assert result["column_count"] == len(API_COLUMNS)
