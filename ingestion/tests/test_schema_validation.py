import pytest

from schemas.rental_history_schema import (
    SchemaValidationError,
    is_rental_history_file,
    validate_and_report,
)

# API(tbCycleRentData) 응답 - 영문 코드, 2025년 기준 17개(BIKE_SE_CD 포함)
API_COLUMNS_2025 = [
    "BIKE_ID", "RENT_DT", "RENT_ID", "RENT_NM", "RENT_HOLD", "RTN_DT", "RTN_ID", "RTN_NM",
    "RTN_HOLD", "USE_MIN", "USE_DST", "USR_CLS_CD", "SEX_CD", "BIRTH_YEAR",
    "RENT_STATION_ID", "RETURN_STATION_ID", "BIKE_SE_CD",
]
# 2026년 API 응답 - BIKE_SE_CD 없이 16개 (실제 사례)
API_COLUMNS_2026 = [c for c in API_COLUMNS_2025 if c != "BIKE_SE_CD"]

# 대량 다운로드 CSV(백필) - 한글 헤더, 2026-08-11 실제 파일로 확인된 16개
FILE_COLUMNS = [
    "자전거번호", "대여일시", "대여 대여소번호", "대여 대여소명", "대여거치대", "반납일시",
    "반납대여소번호", "반납대여소명", "반납거치대", "이용시간(분)", "이용거리(M)", "생년",
    "성별", "이용자종류", "대여대여소ID", "반납대여소ID",
]

# 다른 팀원의 데이터셋 (고장신고) - 대여이력과 컬럼이 거의 안 겹쳐야 함
BREAKDOWN_REPORT_COLUMNS = ["자전거번호", "등록일시", "고장구분"]


def test_api_format_2025_17_columns_passes():
    result = validate_and_report(API_COLUMNS_2025)
    assert result["missing_optional"] == []
    assert result["column_count"] == 17


def test_api_format_2026_16_columns_passes_with_optional_missing():
    """실제 사례: 2026년 API 응답은 BIKE_SE_CD 없이 16컬럼 - 경고만 남기고 통과해야 한다."""
    result = validate_and_report(API_COLUMNS_2026)
    assert "bike_se_cd" in result["missing_optional"]


def test_file_format_korean_headers_passes():
    """실제 사례: 백필 CSV는 한글 헤더를 쓴다 - API 영문 코드와 별개로 통과해야 한다."""
    result = validate_and_report(FILE_COLUMNS)
    assert "bike_se_cd" in result["missing_optional"]  # 이 파일엔 자전거구분 컬럼이 없음
    assert result["unknown_columns"] == []


@pytest.mark.parametrize("required_col", ["BIKE_ID", "RENT_DT", "RTN_DT", "USE_DST"])
def test_missing_required_column_raises_in_api_format(required_col):
    broken = [c for c in API_COLUMNS_2025 if c != required_col]
    with pytest.raises(SchemaValidationError):
        validate_and_report(broken)


@pytest.mark.parametrize("required_col", ["대여일시", "반납일시", "이용시간(분)"])
def test_missing_required_column_raises_in_file_format(required_col):
    broken = [c for c in FILE_COLUMNS if c != required_col]
    with pytest.raises(SchemaValidationError):
        validate_and_report(broken)


def test_unknown_new_column_is_warned_not_failed():
    """스키마에 새 컬럼이 통보 없이 추가돼도 파이프라인이 죽으면 안 된다 (경고만)."""
    columns_with_new_field = API_COLUMNS_2025 + ["NEW_UNKNOWN_FIELD"]
    result = validate_and_report(columns_with_new_field)  # 예외 없이 통과해야 함
    assert "NEW_UNKNOWN_FIELD" in result["unknown_columns"]


def test_is_rental_history_file_accepts_both_formats():
    assert is_rental_history_file(API_COLUMNS_2025) is True
    assert is_rental_history_file(FILE_COLUMNS) is True


def test_is_rental_history_file_rejects_other_dataset():
    """입력 폴더에 다른 팀원 데이터셋(고장신고 등)이 섞여 있을 때 대여이력으로 오인하면 안 된다."""
    assert is_rental_history_file(BREAKDOWN_REPORT_COLUMNS) is False