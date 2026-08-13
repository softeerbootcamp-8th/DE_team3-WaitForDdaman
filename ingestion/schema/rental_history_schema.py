"""
대여이력(OA-15182) 스키마 정의 및 검증

이 데이터셋은 실제로 두 가지 다른 헤더 표기 방식으로 들어온다 (2026-08-11 실측 확인):

    1) API(tbCycleRentData) 응답  - 영문 코드   예) BIKE_ID, RENT_DT, RENT_ID ...
    2) 대량 다운로드 CSV(백필)    - 한글 헤더   예) 자전거번호, 대여일시, 대여 대여소번호 ...

같은 데이터를 가리키는 두 표기법이므로, 컬럼 "이름" 기준으로 표준 컬럼(bike_id, rent_dt, ...)
하나로 합친다. 필수값 검증도 소스 표기법이 아니라 "표준 컬럼이 매핑됐는가"를 기준으로 하므로
API/파일 어느 쪽이 들어와도 동일한 검증 로직이 적용된다.

배경: 2025년 API 응답은 17개 컬럼(BIKE_SE_CD 포함), 2026년은 16개 컬럼으로 통보 없이
변경된 사례가 실측으로 확인됨(source_data 참고). 컬럼 "위치"가 아니라 "이름" 기준으로
매핑하고, 필수 컬럼이 없으면 파이프라인을 즉시 실패시킨다.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# 소스 컬럼명(API 영문 코드 / 백필 파일 한글 헤더 둘 다) -> Bronze 표준 컬럼명
# Bronze는 "원본 그대로" 원칙 -> 값은 전부 문자열 유지, 타입 캐스팅은 Silver 계층 책임.
COLUMN_ALIAS_MAP = {
    # ---- API(tbCycleRentData) 영문 코드 ----
    "BIKE_ID": "bike_id",
    "RENT_DT": "rent_dt",
    "RENT_ID": "rent_station_no",
    "RENT_NM": "rent_station_name",
    "RENT_HOLD": "rent_hold",
    "RTN_DT": "return_dt",
    "RTN_ID": "return_station_no",
    "RTN_NM": "return_station_name",
    "RTN_HOLD": "return_hold",
    "USE_MIN": "use_min",
    "USE_DST": "use_distance_m",
    "USR_CLS_CD": "user_class_cd",
    "SEX_CD": "sex_cd",
    "BIRTH_YEAR": "birth_year",
    "RENT_STATION_ID": "rent_station_id",
    "RETURN_STATION_ID": "return_station_id",
    "BIKE_SE_CD": "bike_se_cd",  # 2025년 일부 파일/응답에만 존재

    # ---- 대량 다운로드 CSV(백필) 한글 헤더 (2026-08-11 실제 파일로 확인) ----
    "자전거번호": "bike_id",
    "대여일시": "rent_dt",
    "대여 대여소번호": "rent_station_no",
    "대여 대여소명": "rent_station_name",
    "대여거치대": "rent_hold",
    "반납일시": "return_dt",
    "반납대여소번호": "return_station_no",
    "반납대여소명": "return_station_name",
    "반납거치대": "return_hold",
    "이용시간(분)": "use_min",
    "이용거리(M)": "use_distance_m",
    "이용자종류": "user_class_cd",
    "성별": "sex_cd",
    "생년": "birth_year",
    "대여대여소ID": "rent_station_id",
    "반납대여소ID": "return_station_id",
    "자전거구분": "bike_se_cd",  # 파일에 따라 없을 수 있음 (API의 BIKE_SE_CD와 동일한 성격)
}

# 표준(output) 컬럼 기준 필수값 - API/파일 어느 소스든 이 표준 컬럼들이 매핑되어야 한다.
REQUIRED_STANDARD_COLUMNS = [
    "bike_id",
    "rent_dt",
    "rent_station_no",
    "return_dt",
    "return_station_no",
    "use_min",
    "use_distance_m",
]

# 있으면 매핑, 없으면 null로 채우고 경고만 (파일에 따라 없을 수 있음이 실측으로 확인됨)
OPTIONAL_STANDARD_COLUMNS = ["bike_se_cd"]

ALL_STANDARD_COLUMNS = sorted(set(COLUMN_ALIAS_MAP.values()))

BRONZE_COLUMNS = ALL_STANDARD_COLUMNS + [
    "rent_date_partition",  # YYYY-MM-DD, 파티션 키
    "source_file",  # 원본 파일명/출처 (lineage 추적)
    "ingested_at",  # 적재 시각
]

# 표준 컬럼 -> 그 표준 컬럼에 매핑되는 모든 소스 별칭 목록 (역 인덱스)
_STANDARD_TO_ALIASES: dict[str, list[str]] = {}
for _src, _dst in COLUMN_ALIAS_MAP.items():
    _STANDARD_TO_ALIASES.setdefault(_dst, []).append(_src)


class SchemaValidationError(Exception):
    """필수 컬럼 누락 등 - 이 예외는 파이프라인을 즉시 실패시켜야 한다."""


def is_rental_history_file(actual_columns: list[str]) -> bool:
    """
    실제 컬럼 목록이 대여이력 스키마와 조금이라도 겹치는지 빠르게 판별한다.
    입력 디렉토리에 다른 데이터셋(고장신고 등) 파일이 섞여 들어왔을 때,
    스키마 검증 실패(에러)로 잡기 전에 "애초에 이 데이터셋이 아님"을 조용히 걸러내는 용도.
    """
    known_source_columns = set(COLUMN_ALIAS_MAP.keys())
    return len(set(actual_columns) & known_source_columns) >= 3


def validate_and_report(actual_columns: list[str]) -> dict:
    """
    실제 파일/응답의 컬럼 목록을 검증한다. API 영문 코드든 파일 한글 헤더든
    표준 컬럼으로 매핑된 결과를 기준으로 검증하므로 소스 포맷을 가리지 않는다.

    - 필수 표준 컬럼 누락    -> SchemaValidationError (안전하게 실패)
    - 선택 표준 컬럼 누락    -> 경고 로그만 (null로 채워서 계속 진행)
    - 알 수 없는 신규 컬럼   -> 경고 로그 (스키마 변경 가능성을 알리되 드롭 후 계속 진행)
    """
    matched_standard = {COLUMN_ALIAS_MAP[c] for c in actual_columns if c in COLUMN_ALIAS_MAP}

    missing_required = [s for s in REQUIRED_STANDARD_COLUMNS if s not in matched_standard]
    if missing_required:
        raise SchemaValidationError(
            f"필수 컬럼 누락(표준명 기준): {missing_required} (실제 컬럼: {actual_columns})"
        )

    missing_optional = [s for s in OPTIONAL_STANDARD_COLUMNS if s not in matched_standard]
    if missing_optional:
        logger.warning("선택 컬럼 누락 (null로 채워서 진행): %s", missing_optional)

    known_source_columns = set(COLUMN_ALIAS_MAP.keys())
    unknown_columns = [c for c in actual_columns if c not in known_source_columns]
    if unknown_columns:
        logger.warning(
            "알 수 없는 신규 컬럼 발견 (드롭됨, 스키마 변경 가능성 점검 필요): %s",
            unknown_columns,
        )

    return {
        "missing_optional": missing_optional,
        "unknown_columns": unknown_columns,
        "column_count": len(actual_columns),
    }


def build_select_exprs(actual_columns: list[str]):
    """
    표준 컬럼별로 실제 존재하는 소스 별칭(API 영문 또는 파일 한글)을 찾아 select한다.
    둘 다 없으면 null 리터럴로 채운다. 컬럼 "위치"가 아니라 "이름" 기준이라
    컬럼 순서·개수·표기법 변경에 영향받지 않는다.

    PySpark Column 표현식 리스트를 반환한다 (Spark 세션이 필요해 함수 내부에서 import).
    """
    from pyspark.sql import functions as F

    actual_set = set(actual_columns)
    exprs = []
    for dst in ALL_STANDARD_COLUMNS:
        found_src = next((src for src in _STANDARD_TO_ALIASES[dst] if src in actual_set), None)
        if found_src:
            exprs.append(F.col(found_src).cast("string").alias(dst))
        else:
            exprs.append(F.lit(None).cast("string").alias(dst))
    return exprs