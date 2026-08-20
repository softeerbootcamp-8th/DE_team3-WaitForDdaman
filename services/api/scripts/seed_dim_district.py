"""serving.dim_district 임시 시드 스크립트.

원래 이 데이터(서울 25개 구 경계 SVG path/cx/cy/viewBox)는 팀원이 DB에 직접 넣어뒀던
값인데, 그 원본이 사라져서 더는 복구할 수 없다. 실제 구 경계(폴리곤) 대신, 구청 기준
대략적인 위경도를 viewBox 좌표로 선형 변환해서 그 위치에 작은 정사각형만 놓은
플레이스홀더로 채운다 — 모양은 가짜지만 배치는 실제 지리적 위치에 가깝다.

DISTRICT_LATLON은 정확한 경계 데이터가 아니라 각 구청 근방의 대략적인 위경도다
(공개적으로 알려진 값 기준 근사치). 실제 구 경계가 필요해지면 서울 열린데이터광장 등
공개 GeoJSON으로 이 스크립트 전체를 교체해야 한다.

같은 (min/max 위경도 -> viewBox) 변환식을 station의 x/y에도 쓰면 대여소 점이 소속 구
사각형 안쪽에 놓이게 맞출 수 있다 (services/api/app/state.py의 get_map_data 참고).

사용법:
    DATABASE_URL=postgresql+psycopg2://airflow:airflow@localhost:5433/airflow \
    SNAPSHOT_DATE=2026-08-18 python3 seed_dim_district.py
"""
from __future__ import annotations

import os
from datetime import date

import psycopg2

# (lat, lon) - 구청 근방 대략적인 위경도 근사치. 실제 경계 데이터 아님.
DISTRICT_LATLON: dict[str, tuple[float, float]] = {
    "종로구": (37.5735, 126.9788),
    "중구": (37.5641, 126.9979),
    "용산구": (37.5326, 126.9900),
    "성동구": (37.5633, 127.0371),
    "광진구": (37.5384, 127.0822),
    "동대문구": (37.5744, 127.0396),
    "중랑구": (37.6063, 127.0925),
    "성북구": (37.5894, 127.0167),
    "강북구": (37.6396, 127.0257),
    "도봉구": (37.6688, 127.0471),
    "노원구": (37.6542, 127.0568),
    "은평구": (37.6027, 126.9291),
    "서대문구": (37.5791, 126.9368),
    "마포구": (37.5663, 126.9019),
    "양천구": (37.5169, 126.8664),
    "강서구": (37.5509, 126.8495),
    "구로구": (37.4954, 126.8874),
    "금천구": (37.4519, 126.9020),
    "영등포구": (37.5264, 126.8963),
    "동작구": (37.5124, 126.9393),
    "관악구": (37.4784, 126.9516),
    "서초구": (37.4837, 127.0324),
    "강남구": (37.5172, 127.0473),
    "송파구": (37.5145, 127.1058),
    "강동구": (37.5301, 127.1238),
}

# 위경도 -> viewBox 변환 기준 범위 (서울 전체를 넉넉히 덮는 값 + 여백).
# station x/y도 같은 상수를 써야 구 사각형 안에 점이 놓인다.
LAT_MIN, LAT_MAX = 37.42, 37.70
LON_MIN, LON_MAX = 126.76, 127.19
VIEW_BOX_W = 800.0
VIEW_BOX_H = 800.0
MARKER_SIZE = 44.0


def latlon_to_xy(lat: float, lon: float) -> tuple[float, float]:
    x = (lon - LON_MIN) / (LON_MAX - LON_MIN) * VIEW_BOX_W
    y = (LAT_MAX - lat) / (LAT_MAX - LAT_MIN) * VIEW_BOX_H  # 위도는 위로 갈수록 커지므로 뒤집는다
    return x, y


def _marker_path(cx: float, cy: float) -> str:
    half = MARKER_SIZE / 2
    x0, y0 = cx - half, cy - half
    return f"M{x0},{y0} h{MARKER_SIZE} v{MARKER_SIZE} h-{MARKER_SIZE} Z"


def main() -> None:
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow"
    )
    dsn = database_url.replace("postgresql+psycopg2://", "postgresql://")
    snapshot_date = os.environ.get("SNAPSHOT_DATE") or date.today().isoformat()

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS serving")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS serving.dim_district (
                    snapshot_date  DATE NOT NULL,
                    name           TEXT NOT NULL,
                    path           TEXT NOT NULL,
                    cx             DOUBLE PRECISION NOT NULL,
                    cy             DOUBLE PRECISION NOT NULL,
                    view_box_w     DOUBLE PRECISION NOT NULL,
                    view_box_h     DOUBLE PRECISION NOT NULL,
                    PRIMARY KEY (name, snapshot_date)
                )
                """
            )
            cur.execute("DELETE FROM serving.dim_district WHERE snapshot_date = %s", (snapshot_date,))
            for name, (lat, lon) in DISTRICT_LATLON.items():
                cx, cy = latlon_to_xy(lat, lon)
                path = _marker_path(cx, cy)
                cur.execute(
                    """
                    INSERT INTO serving.dim_district
                        (snapshot_date, name, path, cx, cy, view_box_w, view_box_h)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (snapshot_date, name, path, cx, cy, VIEW_BOX_W, VIEW_BOX_H),
                )
        conn.commit()
    finally:
        conn.close()

    print(f"serving.dim_district: {snapshot_date} 파티션에 {len(DISTRICT_LATLON)}개 구 임시 시드 완료")


if __name__ == "__main__":
    main()
