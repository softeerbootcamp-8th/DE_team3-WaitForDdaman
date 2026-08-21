"""serving.dim_district 시드 스크립트 — 서울 공공 GeoJSON + 원래 투영 방식으로 재현.

원래 이 데이터(구 경계 SVG path/cx/cy/viewBox)는 팀원이 DB에 직접 넣어뒀던 값인데
원본이 사라져 복구 불가였다. 경계 데이터는 공개 행정구역 경계(KOSTAT/서울 열린데이터광장,
southkorea/seoul-maps 저장소가 정리해 배포)로 대체하고, 위경도 -> SVG 좌표 투영은 옛
오프라인 파이프라인(build_snapshot.py, 3f026f0 커밋 당시 버전)이 쓰던 등장방형도법 +
위도 보정 방식을 그대로 재현한다 (services/api/app/geo.py).

station의 x/y도 반드시 같은 geo.project()를 써야 대여소 점이 소속 구 폴리곤 안에 놓인다
(services/api/app/state.py 참고).

사용법 (레포 루트를 PYTHONPATH에 둬야 app.geo를 import할 수 있다):
    cd services/api
    PYTHONPATH=../.. DATABASE_URL=postgresql+psycopg2://airflow:airflow@localhost:5433/airflow \
    SNAPSHOT_DATE=2026-08-18 python3 scripts/seed_dim_district.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # repo root -> services.api.app import 가능하게

from services.api.app.geo import VIEW_BOX_H, VIEW_BOX_W, project  # noqa: E402

GEOJSON_PATH = Path(__file__).parent / "geo_source" / "seoul_districts.geojson"


def _ring_to_path(ring: list[list[float]]) -> tuple[str, list[tuple[float, float]]]:
    points = [project(lat, lng) for lng, lat in ring]
    d = "M" + " L".join(f"{x:.2f},{y:.2f}" for x, y in points) + " Z"
    return d, points


def _largest_ring(geometry: dict) -> list[list[float]]:
    """Polygon은 exterior ring(첫 번째)을 그대로, MultiPolygon은 가장 큰 파트를 쓴다
    (서울 자치구는 섬이 없어 다 Polygon이지만, 방어적으로 처리)."""
    if geometry["type"] == "Polygon":
        return geometry["coordinates"][0]
    parts = [poly[0] for poly in geometry["coordinates"]]
    return max(parts, key=len)


def build_rows() -> list[tuple[str, str, float, float]]:
    geojson = json.loads(GEOJSON_PATH.read_text(encoding="utf-8"))
    rows = []
    for feature in geojson["features"]:
        name = feature["properties"]["SIG_KOR_NM"]
        ring = _largest_ring(feature["geometry"])
        path, points = _ring_to_path(ring)
        # 원래 파이프라인과 동일하게 라벨 위치는 폴리곤 꼭짓점의 단순 평균(정확한 무게중심은 아님)
        cx = sum(p[0] for p in points) / len(points)
        cy = sum(p[1] for p in points) / len(points)
        rows.append((name, path, cx, cy))
    return rows


def main() -> None:
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow"
    )
    dsn = database_url.replace("postgresql+psycopg2://", "postgresql://")
    snapshot_date = os.environ.get("SNAPSHOT_DATE") or date.today().isoformat()

    rows = build_rows()

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
            for name, path, cx, cy in rows:
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

    print(f"serving.dim_district: {snapshot_date} 파티션에 {len(rows)}개 구 실제 경계 시드 완료")


if __name__ == "__main__":
    main()
