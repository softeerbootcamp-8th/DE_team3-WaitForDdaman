"""
실시간 대여정보(OA-15493 / bikeList) 스키마 정의 및 검증

이 원천의 성격이 station_master(tbCycleStationInfo)와 다르다:
    - station_master는 자치구·주소 등 마스터 속성을 준다. bikeList를 쓰는 목적은
      재고 수치 자체가 아니라 "지금 실제로 운영 중인 대여소가 어디인지" 판별하는 것
      (2026-08-16 사용자 확인). 거치대 수/주차된 자전거 수/거치율은 그 부산물로 같이
      온다 (station_master로 못 쓰는 이유는 station_master_schema.py 참고)
    - 날짜 파라미터를 받지 않아 과거 소급 조회가 불가능 -> 워터마크 없음,
      파티션 키가 "발생일"이 아니라 "스냅샷 기준일(snapshot_date)"

API 응답 필드 (2026-08-16 실측 확인, 전체 2,735건 페이징 전수 조회):
    stationId, stationName, rackTotCnt, parkingBikeTotCnt, shared,
    stationLatitude, stationLongitude
    7개 필드가 2,735건 전부에 빠짐없이 존재한다 (station_master의 HOLD_NUM처럼
    행마다 빠지는 필드가 없다) - 그래서 optional 컬럼 개념이 필요 없다.

    stationId는 이미 'ST-4' 포맷으로 온다. station_master의 RENT_ID와 동일한
    골드 조인 키 포맷이라 station_no 같은 zero-padding 정규화가 필요 없다.

    shared(거치율)는 100을 넘는 값도 관측된다 (예: '120') - 거치대 수 대비 자전거가
    더 많이 반납된 경우로 보이며, 상한 있는 퍼센트가 아니라 단순 비율 계산값이다.

Bronze는 원본 그대로 보존(전부 STRING)하고, 타입 캐스팅·정규화는 Silver 계층 책임이다.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# API 응답 필드 -> Bronze 표준 컬럼
COLUMN_ALIAS_MAP = {
    "stationId": "station_id",              # 'ST-4', 골드 조인 키
    "stationName": "station_name",          # '102. 망원역 1번출구 앞'
    "rackTotCnt": "rack_tot_cnt",           # 거치대 수
    "parkingBikeTotCnt": "parking_bike_tot_cnt",  # 주차된 자전거 수
    "shared": "shared",                      # 거치율 (100 초과 가능)
    "stationLatitude": "latitude",
    "stationLongitude": "longitude",
}

# 실측 결과 7개 필드가 전량 존재해 전부 필수로 둔다. 그래도 서울시가 API를
# 현행화하면 필드가 빠질 수 있으므로, 하나라도 없으면 즉시 실패시킨다.
REQUIRED_STANDARD_COLUMNS = [
    "station_id",
    "station_name",
    "rack_tot_cnt",
    "parking_bike_tot_cnt",
    "shared",
    "latitude",
    "longitude",
]

# station_master_schema.py와 인터페이스를 맞추기 위해 남겨두되, 현재는 비어있다
# (실측상 선택적으로 빠지는 필드가 없다).
OPTIONAL_STANDARD_COLUMNS: list[str] = []

ALL_STANDARD_COLUMNS = sorted(set(COLUMN_ALIAS_MAP.values()))

BRONZE_COLUMNS = ALL_STANDARD_COLUMNS + [
    "snapshot_date",  # YYYY-MM-DD, 파티션 키
    "source_file",
    "ingested_at",
]

_STANDARD_TO_ALIASES: dict[str, list[str]] = {}
for _src, _dst in COLUMN_ALIAS_MAP.items():
    _STANDARD_TO_ALIASES.setdefault(_dst, []).append(_src)


class SchemaValidationError(Exception):
    """필수 컬럼 누락 등 - 이 예외는 파이프라인을 즉시 실패시켜야 한다."""


def collect_response_fields(rows: list[dict]) -> list[str]:
    """
    응답 전체 행의 키 합집합을 돌려준다. station_master_schema.py와 동일한 이유로
    첫 행만 보지 않는다 - 향후 API 현행화로 행별 필드 구성이 달라져도 안전하게
    합집합으로 판단하기 위함.
    """
    fields: set[str] = set()
    for row in rows:
        fields.update(row)
    return sorted(fields)


def is_station_active_response(actual_columns: list[str]) -> bool:
    """다른 서비스의 응답이 섞여 들어왔을 때 조용히 걸러내기 위한 판별."""
    return len(set(actual_columns) & set(COLUMN_ALIAS_MAP)) >= 3


def validate_and_report(actual_columns: list[str]) -> dict:
    """
    - 필수 컬럼 누락  -> SchemaValidationError (즉시 실패)
    - 알 수 없는 컬럼 -> 경고 로그 (원천 스키마 변경 신호일 수 있음)
    """
    matched = {COLUMN_ALIAS_MAP[c] for c in actual_columns if c in COLUMN_ALIAS_MAP}

    missing_required = [s for s in REQUIRED_STANDARD_COLUMNS if s not in matched]
    if missing_required:
        raise SchemaValidationError(
            f"필수 컬럼 누락(표준명 기준): {missing_required} (실제 응답 필드: {actual_columns})"
        )

    unknown_columns = [c for c in actual_columns if c not in COLUMN_ALIAS_MAP]
    if unknown_columns:
        logger.warning(
            "알 수 없는 신규 필드 발견 (드롭됨, API 현행화로 인한 스키마 변경 가능성 점검 필요): %s",
            unknown_columns,
        )

    return {
        "missing_optional": [],
        "unknown_columns": unknown_columns,
        "column_count": len(actual_columns),
    }


def build_select_exprs(actual_columns: list[str]):
    """
    표준 컬럼별로 응답 필드를 찾아 select하고, 없으면 null로 채운다.
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
        exprs.append(F.col(f"`{found_src}`").cast("string").alias(dst))
    return exprs
