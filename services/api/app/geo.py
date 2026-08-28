"""위경도 -> 지도 SVG 좌표 투영.

옛 오프라인 파이프라인(services/api/scripts/build_snapshot.py, 3f026f0 커밋 당시 버전)이
쓰던 것과 같은 방식 — 등장방형도법(equirectangular)에 위도 보정(cos(lat))을 더해서,
외부 지도 라이브러리 없이 서울 자치구 경계 geojson만으로 그린다. x/y 스케일을 하나로
공유해야 폴리곤이 찌그러지지 않는다.

아래 상수(LAT_MIN..SCALE/HEIGHT)는 seoul_districts.geojson의 전체 폴리곤 + 실제 대여소
위경도(mart_station_daily 샘플) 바운딩 박스로 한 번 계산해 고정한 값이다. dim_district
시드 스크립트(scripts/seed_dim_district.py)가 반드시 이 값과 같은 투영을 써야 대여소
점이 소속 구 폴리곤 안에 놓인다.
"""
from __future__ import annotations

LAT_MIN, LAT_MAX = 37.4283, 37.7013
LNG_MIN, LNG_MAX = 126.7646, 127.1831
_COS_LAT_MID = 0.792664  # cos(radians((LAT_MIN + LAT_MAX) / 2))

MAP_PAD = 24.0
_INNER_W = 880.0 - 2 * MAP_PAD
_LNG_SPAN_ADJ = (LNG_MAX - LNG_MIN) * _COS_LAT_MID
SCALE = _INNER_W / _LNG_SPAN_ADJ

VIEW_BOX_W = 880.0
VIEW_BOX_H = (LAT_MAX - LAT_MIN) * SCALE + 2 * MAP_PAD


def project(lat: float, lng: float) -> tuple[float, float]:
    x = (lng - LNG_MIN) * _COS_LAT_MID * SCALE + MAP_PAD
    y = (LAT_MAX - lat) * SCALE + MAP_PAD
    return x, y
