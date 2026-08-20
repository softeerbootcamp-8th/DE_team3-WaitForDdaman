"""serving.dim_district 임시 시드 스크립트.

원래 이 데이터(서울 25개 구 경계 SVG path/cx/cy/viewBox)는 팀원이 DB에 직접 넣어뒀던
값인데, 그 원본이 사라져서 더는 복구할 수 없다. 실제 구 경계 대신 5x5 격자에 자리만
잡아준 플레이스홀더로 채워서 /api/map이 최소한 200을 반환하게 만드는 용도다.

실제 구 경계가 필요해지면 서울 열린데이터광장 등 공개 GeoJSON을 받아 위경도를
viewBox 좌표로 변환하는 스크립트로 교체해야 한다 — 이 스크립트는 그 전까지의 임시값이다.

사용법:
    DATABASE_URL=postgresql+psycopg2://airflow:airflow@localhost:5433/airflow \
    SNAPSHOT_DATE=2026-08-19 python3 seed_dim_district.py
"""
from __future__ import annotations

import os
from datetime import date

import psycopg2

GU_NAMES = [
    "종로구", "중구", "용산구", "성동구", "광진구",
    "동대문구", "중랑구", "성북구", "강북구", "도봉구",
    "노원구", "은평구", "서대문구", "마포구", "양천구",
    "강서구", "구로구", "금천구", "영등포구", "동작구",
    "관악구", "서초구", "강남구", "송파구", "강동구",
]

GRID_COLS = 5
CELL = 100
GAP = 10
VIEW_BOX_W = GRID_COLS * CELL
VIEW_BOX_H = ((len(GU_NAMES) + GRID_COLS - 1) // GRID_COLS) * CELL


def _cell_path(col: int, row: int) -> tuple[str, float, float]:
    x0 = col * CELL + GAP / 2
    y0 = row * CELL + GAP / 2
    size = CELL - GAP
    path = f"M{x0},{y0} h{size} v{size} h-{size} Z"
    return path, x0 + size / 2, y0 + size / 2


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
            for i, name in enumerate(GU_NAMES):
                col, row = i % GRID_COLS, i // GRID_COLS
                path, cx, cy = _cell_path(col, row)
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

    print(f"serving.dim_district: {snapshot_date} 파티션에 {len(GU_NAMES)}개 구 임시 시드 완료")


if __name__ == "__main__":
    main()
