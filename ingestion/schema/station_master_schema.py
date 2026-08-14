"""
대여소 정보(OA-13252 / tbCycleStationInfo) 스키마 정의 및 검증

이 원천의 성격이 대여이력·고장신고와 결정적으로 다르다:
    - 이벤트 로그가 아니라 마스터(참조) 데이터 -> 증분 개념 없음, 매번 전체 스냅샷
    - API가 날짜 파라미터를 받지 않아 과거 소급 조회가 불가능 -> 워터마크 없음
    - 그래서 파티션 키가 "발생일"이 아니라 "스냅샷 기준일(snapshot_date)"이다

원천 선택 근거 (2026-08-13 실측 + 서울시 공식 답변):
    같은 대여소를 다루는 원천이 4개 있으나 이것만 쓴다.
    - bikeStationMaster : 필드 5개뿐. 대여소명/자치구/거치대수가 없어 사용 불가.
                          좌표 결측도 77/3,426건으로 26배 나쁘다.
    - 반기 파일(xlsx)   : station_id가 없어 골드 조인 불가. 거치대수도 API와 8.1% 불일치.
                          파일에만 있는 컬럼(설치시기/LCD·QR거치대수/운영방식)은
                          골드·프론트·백엔드 어디에서도 쓰지 않는다.
    - bikeList(실시간)  : 자치구·주소가 없다. 운영 여부 판정용으로 별도 사용.
    - tbCycleStationInfo: 골드가 필요한 컬럼을 모두 제공하는 유일한 원천. <- 이것

API 응답 필드 (2026-08-14 실측 확인):
    STA_LOC, RENT_ID, RENT_NO, RENT_NM, RENT_ID_NM,
    HOLD_NUM, STA_ADD1, STA_ADD2, STA_LAT, STA_LONG
    (+ START_INDEX/END_INDEX/RNUM 은 페이징 메타라 적재 전에 제거)

Bronze는 원본 그대로 보존(전부 STRING)하고, 타입 캐스팅·정규화·SCD Type 2 이력화는
Silver 계층 책임이다.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# API 응답 필드 -> Bronze 표준 컬럼
COLUMN_ALIAS_MAP = {
    "RENT_NO": "station_no",       # '00108' (5자리 zero-padding)
    "RENT_ID": "station_id",       # 'ST-10'  <- 골드 전체의 조인 키
    "RENT_NM": "station_name",     # '서교동 사거리'
    "RENT_ID_NM": "station_id_name",  # '108. 서교동 사거리'
    "STA_LOC": "district",         # '마포구'
    "HOLD_NUM": "hold_num",        # '12'
    "STA_ADD1": "address1",
    "STA_ADD2": "address2",
    "STA_LAT": "latitude",
    "STA_LONG": "longitude",
}

# 하나라도 없으면 즉시 실패시킨다.
# 골드가 실제로 쓰는 컬럼들이라, 원천에서 사라지면 조용히 null로 채우는 대신
# 파이프라인을 세워서 사람이 알아차리게 해야 한다.
# (서울시가 하반기 API 현행화를 예고했으므로 응답 구조 변경 가능성이 있다)
REQUIRED_STANDARD_COLUMNS = [
    "station_no",
    "station_id",
    "station_name",
    "district",
    "hold_num",
    "latitude",
    "longitude",
]

# 없으면 null로 채우고 경고만 남긴다 (다운스트림에서 쓰지 않는 보조 정보)
OPTIONAL_STANDARD_COLUMNS = [
    "station_id_name",      # 필수인지 확인 필요함.
    "address1",
    "address2",
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
    실측 확인(2026-08-14): API는 대여소번호를 5자리 zero-padding으로 준다('00108').
    반면 대여이력·실시간 API 등 다른 원천은 padding 없이 준다('108').
    같은 대여소인데 문자열이 달라 조인이 전부 어긋나므로 적재 시점에 정규화한다.
    """
    stripped = str(raw_id).strip()
    return str(int(stripped)) if stripped.isdigit() else stripped


def is_station_master_response(actual_columns: list[str]) -> bool:
    """다른 서비스의 응답이 섞여 들어왔을 때 조용히 걸러내기 위한 판별."""
    return len(set(actual_columns) & set(COLUMN_ALIAS_MAP)) >= 3


def validate_and_report(actual_columns: list[str]) -> dict:
    """
    - 필수 컬럼 누락  -> SchemaValidationError (즉시 실패)
    - 선택 컬럼 누락  -> 경고 로그 후 null로 채워서 진행
    - 알 수 없는 컬럼 -> 경고 로그 (원천 스키마 변경 신호일 수 있음)
    """
    matched = {COLUMN_ALIAS_MAP[c] for c in actual_columns if c in COLUMN_ALIAS_MAP}

    missing_required = [s for s in REQUIRED_STANDARD_COLUMNS if s not in matched]
    if missing_required:
        raise SchemaValidationError(
            f"필수 컬럼 누락(표준명 기준): {missing_required} (실제 응답 필드: {actual_columns})"
        )

    missing_optional = [s for s in OPTIONAL_STANDARD_COLUMNS if s not in matched]
    if missing_optional:
        logger.warning("응답에 없는 선택 컬럼 (null로 채워서 진행): %s", missing_optional)

    unknown_columns = [c for c in actual_columns if c not in COLUMN_ALIAS_MAP]
    if unknown_columns:
        logger.warning(
            "알 수 없는 신규 필드 발견 (드롭됨, API 현행화로 인한 스키마 변경 가능성 점검 필요): %s",
            unknown_columns,
        )

    return {
        "missing_optional": missing_optional,
        "unknown_columns": unknown_columns,
        "column_count": len(actual_columns),
    }


def build_select_exprs(actual_columns: list[str]):
    """
    표준 컬럼별로 응답 필드를 찾아 select하고, 없으면 null로 채운다.
    station_no는 zero-padding 정규화를 SQL 표현식으로 적용한다.

    PySpark Column 표현식 리스트를 반환한다 (Spark 세션이 필요해 함수 내부에서 import).
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
