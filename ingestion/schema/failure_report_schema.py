"""
공공자전거 고장신고 내역(OA-15644) 스키마 정의 및 검증

두 가지 소스 포맷:
    1) API(tbCycleFailureReport) 응답 - bikeNo, regDttm, mlangComCdName
    2) 대량 다운로드 CSV(백필) 한글 헤더 - 자전거번호, 등록일시, 고장구분

실측(2026-08-11, 대여이력 백필 로그): 같은 컬럼이 파일마다 "고장구분" 또는 "구분"으로
다르게 표기됨(source_data에서도 "2017년 4월 이후 열 이름 변경" 사례가 언급됨) - 둘 다 별칭에 포함.

동일 (bike_no, reg_dttm) 조합에 고장유형별로 여러 행이 존재하는 게 정상이다
(예: SPB-82091이 같은 시각에 "체인"/"페달" 두 행으로 따로 신고됨).
Bronze는 원본 그대로 보존하고, 이벤트 단위 통합은 Silver 계층 책임.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

COLUMN_ALIAS_MAP = {
    # ---- API(tbCycleFailureReport) 응답 ----
    "BIKENO": "bike_no",
    "REGDTTM": "reg_dttm",
    "MLANGCOMCDNAME": "failure_type",
    "bikeNo": "bike_no",
    "regDttm": "reg_dttm",
    "mlangComCdName": "failure_type",

    # ---- 대량 다운로드 CSV(백필) 한글 헤더 ----
    "자전거번호": "bike_no",
    "등록일시": "reg_dttm",
    "고장구분": "failure_type",
    "구분": "failure_type",  # 일부 파일은 "고장구분" 대신 "구분"으로만 표기됨 (실측 확인)
}

# 컬럼이 3개뿐이라 전부 필수 - 선택 컬럼 없음
REQUIRED_STANDARD_COLUMNS = ["bike_no", "reg_dttm", "failure_type"]
OPTIONAL_STANDARD_COLUMNS: list[str] = []

ALL_STANDARD_COLUMNS = sorted(set(COLUMN_ALIAS_MAP.values()))

BRONZE_COLUMNS = ALL_STANDARD_COLUMNS + [
    "reg_date_partition",  # YYYY-MM-DD, 파티션 키
    "source_file",
    "ingested_at",
]

_STANDARD_TO_ALIASES: dict[str, list[str]] = {}
for _src, _dst in COLUMN_ALIAS_MAP.items():
    _STANDARD_TO_ALIASES.setdefault(_dst, []).append(_src)


class SchemaValidationError(Exception):
    """필수 컬럼 누락 등 - 이 예외는 파이프라인을 즉시 실패시켜야 한다."""


def is_failure_report_file(actual_columns: list[str]) -> bool:
    """
    입력 폴더에 다른 데이터셋 파일이 섞여 있을 때, 스키마 검증 실패(에러)로 잡기 전에
    "애초에 이 데이터셋이 아님"을 조용히 걸러내는 용도. 컬럼이 3개뿐이라 2개 이상
    겹치면 이 데이터셋으로 판단한다.
    """
    known_source_columns = set(COLUMN_ALIAS_MAP.keys())
    return len(set(actual_columns) & known_source_columns) >= 2


def validate_and_report(actual_columns: list[str]) -> dict:
    matched_standard = {COLUMN_ALIAS_MAP[c] for c in actual_columns if c in COLUMN_ALIAS_MAP}

    missing_required = [s for s in REQUIRED_STANDARD_COLUMNS if s not in matched_standard]
    if missing_required:
        raise SchemaValidationError(
            f"필수 컬럼 누락(표준명 기준): {missing_required} (실제 컬럼: {actual_columns})"
        )

    known_source_columns = set(COLUMN_ALIAS_MAP.keys())
    unknown_columns = [c for c in actual_columns if c not in known_source_columns]
    if unknown_columns:
        logger.warning(
            "알 수 없는 신규 컬럼 발견 (드롭됨, 스키마 변경 가능성 점검 필요): %s", unknown_columns
        )

    return {
        "missing_optional": [],
        "unknown_columns": unknown_columns,
        "column_count": len(actual_columns),
    }


def build_select_exprs(actual_columns: list[str]):
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
