"""오늘자 콘솔 데이터 조회 계층.

파이프라인의 gold_to_serving_sync가 매일 serving.station_daily / serving.bike_risk_daily를
파티션 교체(UPSERT 아님)해두고, 여기서는 그중 최신 snapshot_date만 읽어 API 응답 모양으로
옮긴다. serving.dim_district는 시드 데이터인데 아직 어디서도 만들어지지 않아 /api/map은
당분간 계속 실패한다 (#102 범위 밖, 별도 처리 필요).

상태를 메모리에 들고 바꾸던 예전 방식(OperationState의 mutable 리스트)은 없다 — 매 요청이
그 시점의 DB를 그대로 본다.
"""
from __future__ import annotations

from sqlalchemy import text

from services.api.app.db import engine
from services.api.app.geo import project as _latlon_to_xy

# 정비소 하루 처리 capacity 기본값. 실제 배차 여력은 운영자가 프론트에서 구/지역별로
# 조정하는 값이라 DB에는 저장하지 않는다 — 이 값은 그 조정의 초기 기준값일 뿐이다.
DEFAULT_CAPACITY = 700

# 수거/대여중단 승격은 파이프라인이 아니라 프론트+백엔드가 capacity 기준으로 정한다
# (gold.mart_bike_risk_daily에는 action 컬럼 자체가 없다, #104) — 그래서 여기서는
# action으로 걸러내지 않고 전부 source로 돌려준다.
REASON_PLACEHOLDER = "정보없음"  # 아직 파이프라인이 안 만듦, #102 후속으로 원천 확인 예정
DUR_H_PLACEHOLDER = 0.0


def _latest_snapshot_date():
    with engine.connect() as conn:
        return conn.execute(text("SELECT MAX(snapshot_date) FROM serving.station_daily")).scalar()


def get_meta() -> dict:
    snapshot_date = _latest_snapshot_date()
    return {
        "generatedAt": snapshot_date.isoformat() if snapshot_date else "",
        "capacity": {"max": DEFAULT_CAPACITY},
    }


def get_map_data() -> dict:
    # NOTE: dim_district는 원본이 사라져 복구 불가 — 구청 근방 대략적 위경도를 아래와
    # 같은 방식으로 viewBox에 투영한 정사각형 플레이스홀더로 대체했다
    # (services/api/scripts/seed_dim_district.py). station의 x/y도 같은 변환식(위경도
    # 기준 범위 동일)을 써서, 점이 소속 구 사각형 안쪽에 오도록 맞췄다. 둘 다 실제 경계/
    # 위치가 아니라 임시값이다.
    snapshot_date = _latest_snapshot_date()
    with engine.connect() as conn:
        districts = conn.execute(
            text("SELECT name, path, cx, cy, view_box_w, view_box_h FROM serving.dim_district WHERE snapshot_date = :d"),
            {"d": snapshot_date},
        ).mappings().all()
        stations = conn.execute(
            text(
                """
                SELECT station_id, station_name, region, district AS gu,
                       longitude, latitude, hold_num,
                       bike_cnt AS bike_count, risk_cnt AS risk_count, healthy_ratio, urgency
                FROM serving.station_daily
                WHERE snapshot_date = :d
                """
            ),
            {"d": snapshot_date},
        ).mappings().all()

    # view_box_w/h는 지도 전체 크기라 모든 구 행에 동일하게 중복 저장되어 있다 — 한 행에서만 읽는다.
    view_box = [districts[0]["view_box_w"], districts[0]["view_box_h"]] if districts else [0, 0]

    def _xy(s) -> tuple[float, float]:
        return _latlon_to_xy(s["latitude"], s["longitude"])

    return {
        "viewBox": view_box,
        "districts": [{"name": d["name"], "path": d["path"], "cx": d["cx"], "cy": d["cy"]} for d in districts],
        "stations": [
            {
                "id": s["station_id"],
                "name": s["station_name"],
                "gu": s["gu"],
                "region": s["region"],
                "x": _xy(s)[0],
                "y": _xy(s)[1],
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
    """위험 자전거 후보를 전부 source로 돌려준다 (dest는 항상 빈 배열).

    gold.mart_bike_risk_daily에 action 컬럼이 없어(#104) 파이프라인이 정한 수거/대여중단
    구분 자체가 없다 — 그 승격은 프론트+백엔드가 capacity 기준으로 한다(useClassifiedPool).
    """
    snapshot_date = _latest_snapshot_date()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT b.bike_id, b.station_name, b.district AS gu, b.region, b.healthy_ratio,
                       b.risk_grade, b.risk_score, b.dist_km, b.aging,
                       b.fail_history, s.urgency AS station_urgency
                FROM serving.bike_risk_daily b
                LEFT JOIN serving.station_daily s
                  ON s.station_id = b.station_id AND s.snapshot_date = b.snapshot_date
                WHERE b.snapshot_date = :d AND b.region IS NOT NULL
                ORDER BY b.risk_score DESC
                """
            ),
            {"d": snapshot_date},
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
            "reason": REASON_PLACEHOLDER,
            "distKm": r["dist_km"],
            "durH": DUR_H_PLACEHOLDER,
            "aging": r["aging"],
            "history": r["fail_history"] or [],
        }

    source = [to_bike(r) for r in rows]
    dest: list[dict] = []
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
