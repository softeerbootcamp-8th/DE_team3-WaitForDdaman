"""
공공자전거 대여소 정보(OA-13252) 스키마 정의 및 검증

두 소스의 성격이 앞선 두 데이터셋과 결정적으로 다르다:
    - 이벤트 로그가 아니라 마스터(참조) 데이터 → 증분 개념 없음, 매번 전체 스냅샷
    - 그래서 파티션 키가 "발생일"이 아니라 "스냅샷 기준일(snapshot_date)"이다

1) API(tbCycleStationInfo) - 2026-08-11 실측 확인된 필드:
   STA_LOC, RENT_ID, RENT_NO, RENT_NM, RENT_ID_NM, HOLD_NUM, STA_ADD1, STA_ADD2, STA_LAT, STA_LONG
   ⚠️ RENT_NO가 5자리 zero-padding으로 온다('00108') - 파일은 padding 없음('108')

2) 파일(xlsx) - source_data 실측: 병합 헤더 1~5행, 6행부터 데이터, 10개 컬럼.
   헤더 자동 인식이 불가능해서 skiprows=5 + 컬럼 순서 수동 지정이 필요하다
   (compare_station_master.py의 FILE_COLUMN_NAMES와 동일한 순서).

Bronze는 원본 그대로 보존(전부 STRING)하고, SCD Type 2 이력화는 Silver 계층 책임.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

COLUMN_ALIAS_MAP = {
    # ---- API(tbCycleStationInfo) 실측 필드 ----
    "RENT_NO": "station_no",
    "RENT_ID": "station_id",
    "RENT_NM": "station_name",
    "RENT_ID_NM": "station_id_name",
    "STA_LOC": "district",
    "HOLD_NUM": "hold_num",
    "STA_ADD1": "address1",
    "STA_ADD2": "address2",
    "STA_LAT": "latitude",
    "STA_LONG": "longitude",

    # ---- 파일(xlsx) 한글 헤더 (skiprows=5 후 수동 지정하는 컬럼명) ----
    "대여소번호": "station_no",
    "대여소명": "station_name",
    "자치구": "district",
    "상세주소": "address1",
    "위도": "latitude",
    "경도": "longitude",
    "설치시기": "install_date",
    "LCD거치대수": "lcd_hold_num",
    "QR거치대수": "qr_hold_num",
    "운영방식": "operation_type",
}

# 표준 컬럼 기준 필수값 - 조인 키(station_no)와 공간 분석에 필수인 위경도
REQUIRED_STANDARD_COLUMNS = ["station_no", "station_name", "latitude", "longitude"]

# 소스에 따라 있을 수도 없을 수도 있는 것들
# (API엔 설치시기/LCD·QR거치대수가 없고, 파일엔 station_id/RENT_ID_NM/address2가 없음)
OPTIONAL_STANDARD_COLUMNS = [
    "station_id",
    "station_id_name",
    "district",
    "hold_num",
    "address2",
    "install_date",
    "lcd_hold_num",
    "qr_hold_num",
    "operation_type",
]

ALL_STANDARD_COLUMNS = sorted(set(COLUMN_ALIAS_MAP.values()))

BRONZE_COLUMNS = ALL_STANDARD_COLUMNS + [
    "snapshot_date",  # YYYY-MM-DD, 파티션 키 (이벤트 발생일이 아니라 "언제 찍은 스냅샷인지")
    "source_file",
    "ingested_at",
]

_STANDARD_TO_ALIASES: dict[str, list[str]] = {}
for _src, _dst in COLUMN_ALIAS_MAP.items():
    _STANDARD_TO_ALIASES.setdefault(_dst, []).append(_src)


class SchemaValidationError(Exception):
    """필수 컬럼 누락 등 - 이 예외는 파이프라인을 즉시 실패시켜야 한다."""


def normalize_station_no(raw_id: str) -> str:
    """
    실측 확인(2026-08-11): API는 대여소번호를 5자리 zero-padding으로 준다('00108').
    파일은 padding 없이 준다('108'). 같은 대여소인데 문자열이 달라 조인이 전부 어긋나므로
    Bronze 적재 시점에 정규화해서 두 소스가 같은 키를 갖게 만든다.
    """
    stripped = str(raw_id).strip()
    return str(int(stripped)) if stripped.isdigit() else stripped


def is_station_master_file(actual_columns: list[str]) -> bool:
    """입력에 다른 데이터셋이 섞여 들어왔을 때 조용히 걸러내기 위한 판별."""
    known = set(COLUMN_ALIAS_MAP.keys())
    return len(set(actual_columns) & known) >= 3


def validate_and_report(actual_columns: list[str]) -> dict:
    matched_standard = {COLUMN_ALIAS_MAP[c] for c in actual_columns if c in COLUMN_ALIAS_MAP}

    missing_required = [s for s in REQUIRED_STANDARD_COLUMNS if s not in matched_standard]
    if missing_required:
        raise SchemaValidationError(
            f"필수 컬럼 누락(표준명 기준): {missing_required} (실제 컬럼: {actual_columns})"
        )

    missing_optional = [s for s in OPTIONAL_STANDARD_COLUMNS if s not in matched_standard]
    if missing_optional:
        logger.info("이 소스에 없는 선택 컬럼 (null로 채워서 진행): %s", missing_optional)

    known = set(COLUMN_ALIAS_MAP.keys())
    unknown_columns = [c for c in actual_columns if c not in known]
    if unknown_columns:
        logger.warning(
            "알 수 없는 신규 컬럼 발견 (드롭됨, 스키마 변경 가능성 점검 필요): %s", unknown_columns
        )

    return {
        "missing_optional": missing_optional,
        "unknown_columns": unknown_columns,
        "column_count": len(actual_columns),
    }


def build_select_exprs(actual_columns: list[str]):
    """
    표준 컬럼별로 실제 존재하는 소스 별칭을 찾아 select하고, 없으면 null로 채운다.
    station_no는 zero-padding 정규화를 SQL 표현식으로 적용해 두 소스의 키를 일치시킨다.
    """
    from pyspark.sql import functions as F

    actual_set = set(actual_columns)
    exprs = []
    for dst in ALL_STANDARD_COLUMNS:
        found_src = next((src for src in _STANDARD_TO_ALIASES[dst] if src in actual_set), None)
        if not found_src:
            exprs.append(F.lit(None).cast("string").alias(dst))
            continue

        col = F.col(f"`{found_src}`").cast("string")
        if dst == "station_no":
            # 숫자로만 이루어진 값이면 앞자리 0 제거 ('00108' -> '108'), 그 외는 원본 유지.
            # regexp_replace만 쓰면 '000' 같은 값이 빈 문자열이 되어버리므로,
            # 제거 결과가 비면 '0'으로 되돌린다 (파이썬 normalize_station_no와 동일한 결과).
            trimmed = F.trim(col)
            stripped = F.regexp_replace(trimmed, "^0+", "")
            col = F.when(
                trimmed.rlike("^[0-9]+$"),
                F.when(stripped == F.lit(""), F.lit("0")).otherwise(stripped),
            ).otherwise(trimmed)
        exprs.append(col.alias(dst))
    return exprs
