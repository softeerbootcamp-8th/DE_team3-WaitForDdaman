"""오늘자 콘솔 데이터 조회 계층.

Airflow 파이프라인이 매일 dim_district / station_daily / bike_risk_daily를 UPSERT해두고,
여기서는 그중 최신 snapshot_date만 읽어 API 응답 모양으로 옮긴다. 상태를 메모리에 들고
바꾸던 예전 방식(OperationState의 mutable 리스트)은 없다 — 매 요청이 그 시점의 DB를 그대로 본다.
"""
from __future__ import annotations

from sqlalchemy import text

from services.api.app.db import engine

# 정비소 하루 처리 capacity 기본값. 실제 배차 여력은 운영자가 프론트에서 구/지역별로
# 조정하는 값이라 DB에는 저장하지 않는다 — 이 값은 그 조정의 초기 기준값일 뿐이다.
DEFAULT_CAPACITY = 700

NO_ACTION = "조치없음"


def _latest_snapshot_date():
    with engine.connect() as conn:
        return conn.execute(text("SELECT MAX(snapshot_date) FROM station_daily")).scalar()


def get_meta() -> dict:
    snapshot_date = _latest_snapshot_date()
    return {
        "generatedAt": snapshot_date.isoformat() if snapshot_date else "",
        "capacity": {"max": DEFAULT_CAPACITY},
    }


def get_map_data() -> dict:
    snapshot_date = _latest_snapshot_date()
    with engine.connect() as conn:
        districts = conn.execute(
            text("SELECT name, path, cx, cy, view_box_w, view_box_h FROM dim_district WHERE snapshot_date = :d"),
            {"d": snapshot_date},
        ).mappings().all()
        stations = conn.execute(
            text(
                """
                SELECT station_id, station_name, region, gu, x, y, hold_num,
                       bike_count, risk_count, healthy_ratio, urgency
                FROM station_daily
                WHERE snapshot_date = :d
                """
            ),
            {"d": snapshot_date},
        ).mappings().all()

    # view_box_w/h는 지도 전체 크기라 모든 구 행에 동일하게 중복 저장되어 있다 — 한 행에서만 읽는다.
    view_box = [districts[0]["view_box_w"], districts[0]["view_box_h"]] if districts else [0, 0]
    return {
        "viewBox": view_box,
        "districts": [{"name": d["name"], "path": d["path"], "cx": d["cx"], "cy": d["cy"]} for d in districts],
        "stations": [
            {
                "id": s["station_id"],
                "name": s["station_name"],
                "gu": s["gu"],
                "region": s["region"],
                "x": s["x"],
                "y": s["y"],
                "holdNum": s["hold_num"],
                "bikeCount": s["bike_count"],
                "riskCount": s["risk_count"],
                "healthyRatio": s["healthy_ratio"],
                "urgency": s["urgency"],
            }
            for s in stations
        ],
    }


def get_bikes() -> tuple[list[dict], list[dict]]:
    """수거 후보 Pool(조치없음이 아닌 자전거)을 파이프라인이 정한 action 기준으로
    source(대여중단) / dest(수거) 두 묶음으로 나눠 돌려준다.
    프론트는 이 둘을 다시 합쳐(`pool`) capacity 슬라이더로 직접 재분류하므로,
    여기서의 source/dest 나눔은 파이프라인이 정한 초기값 역할만 한다.
    """
    snapshot_date = _latest_snapshot_date()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT b.bike_id, b.station_name, b.gu, b.region, b.healthy_ratio,
                       b.risk_grade, b.risk_score, b.dist_km, b.dur_h, b.aging,
                       b.fail_history, b.reason, b.action, s.urgency AS station_urgency
                FROM bike_risk_daily b
                LEFT JOIN station_daily s
                  ON s.station_id = b.station_id AND s.snapshot_date = b.snapshot_date
                WHERE b.snapshot_date = :d AND b.action != :no_action
                ORDER BY b.risk_score DESC
                """
            ),
            {"d": snapshot_date, "no_action": NO_ACTION},
        ).mappings().all()

    def to_bike(r) -> dict:
        return {
            "id": r["bike_id"],
            "station": r["station_name"],
            "gu": r["gu"],
            "region": r["region"],
            "stationUrgency": r["station_urgency"] or "정보없음",
            "healthyRatio": r["healthy_ratio"],
            "tier": r["risk_grade"],
            "score": r["risk_score"],
            "reason": r["reason"],
            "distKm": r["dist_km"],
            "durH": r["dur_h"],
            "aging": r["aging"],
            "history": r["fail_history"] or [],
        }

    dest = [to_bike(r) for r in rows if r["action"] == "수거"]
    source = [to_bike(r) for r in rows if r["action"] == "대여중단"]
    return source, dest


class OperationState:
    @property
    def meta(self) -> dict:
        return get_meta()

    @property
    def map_data(self) -> dict:
        return get_map_data()

    def bikes(self) -> tuple[list[dict], list[dict]]:
        return get_bikes()


state = OperationState()
