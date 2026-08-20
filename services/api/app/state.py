"""오늘자 콘솔 데이터 조회 계층.

파이프라인의 gold_to_serving_sync가 매일 serving.station_daily / serving.bike_risk_daily를
파티션 교체(UPSERT 아님)해두고, 여기서는 그중 최신 snapshot_date만 읽어 API 응답 모양으로
옮긴다. serving.dim_district는 파이프라인 산출물이 아니라 시드 데이터라
services/api/scripts/seed_dim_district.py로 별도 채워야 한다.

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

# 조치 대상이 아닌 등급. action 컬럼이 있던 시절엔 `action != '조치없음'`으로 걸렀는데
# #95에서 그 컬럼이 사라진 뒤 조건을 안 넣어서, 정상 자전거까지 수거후보 Pool에 섞여
# "총 대여중단 대수"가 34,586대(그중 32,922대가 Normal)로 잡히는 문제가 있었다.
NO_ACTION_GRADE = "Normal"

COLLECT_ACTION = "수거"
# 이 앱에는 인증이 없어 운영자를 특정할 수 없다 — 실제 신원은 인증 도입 시 채운다.
ACTIONED_BY = "console"


def _latest_snapshot_date():
    with engine.connect() as conn:
        return conn.execute(text("SELECT MAX(snapshot_date) FROM serving.station_daily")).scalar()


def get_meta() -> dict:
    snapshot_date = _latest_snapshot_date()
    return {
        "snapshot_date": snapshot_date.isoformat() if snapshot_date else "",
        "capacity": {"max": DEFAULT_CAPACITY},
    }


def get_map_data() -> dict:
    # dim_district의 실제 구 경계 원본은 유실됐다 — 공개 GeoJSON(services/api/scripts/
    # seed_dim_district.py 참고)으로 대체해서 미리 투영해둔 path/cx/cy를 그대로 쓴다.
    # station의 x/y는 위경도를 geo.project()로 요청마다 투영한다 — dim_district를 만들 때
    # 쓴 것과 같은 투영식이라 점이 소속 구 폴리곤 안에 놓인다.
    snapshot_date = _latest_snapshot_date()
    with engine.connect() as conn:
        districts = conn.execute(
            text("SELECT name, path, cx, cy, view_box_w, view_box_h FROM serving.dim_district WHERE snapshot_date = :d"),
            {"d": snapshot_date},
        ).mappings().all()
        stations = conn.execute(
            text(
                """
                SELECT station_id, station_name, region, district,
                       longitude, latitude, hold_num, bike_cnt, risk_cnt, healthy_ratio, urgency
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
        "view_box": view_box,
        "districts": [{"name": d["name"], "path": d["path"], "cx": d["cx"], "cy": d["cy"]} for d in districts],
        "stations": [
            {
                "station_id": s["station_id"],
                "station_name": s["station_name"],
                "district": s["district"],
                "region": s["region"],
                "x": _xy(s)[0],
                "y": _xy(s)[1],
                "hold_num": s["hold_num"],
                "bike_cnt": s["bike_cnt"],
                "risk_cnt": s["risk_cnt"],
                "healthy_ratio": s["healthy_ratio"],
                "urgency": s["urgency"],
            }
            for s in stations
        ],
    }


def _to_bike(r) -> dict:
    """조회 행을 API의 Bike 모양으로. 수거 후보 Pool과 확정 목록이 같은 모양을 쓰므로
    프론트가 두 화면에서 BikeTable/DetailPanel을 그대로 재사용한다."""
    return {
        "bike_id": r["bike_id"],
        "station_name": r["station_name"],
        "district": r["district"],
        "region": r["region"],
        "station_urgency": r["station_urgency"] or "정보없음",
        "healthy_ratio": r["healthy_ratio"],
        "risk_grade": r["risk_grade"],
        "risk_score": r["risk_score"],
        "dist_km": r["dist_km"],
        "aging": r["aging"],
        "fail_history": r["fail_history"] or [],
    }


def get_bikes() -> tuple[list[dict], list[dict]]:
    """수거 후보 Pool을 전부 source로 돌려준다 (dest는 항상 빈 배열).

    gold.mart_bike_risk_daily에 action 컬럼이 없어(#104) 파이프라인이 정한 수거/대여중단
    구분 자체가 없다 — 그 승격은 프론트+백엔드가 capacity 기준으로 한다(useClassifiedPool).

    Normal 등급은 조치 대상이 아니라 Pool에서 뺀다(NO_ACTION_GRADE 주석 참고).
    """
    snapshot_date = _latest_snapshot_date()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT b.bike_id, b.station_name, b.district, b.region, b.healthy_ratio,
                       b.risk_grade, b.risk_score, b.dist_km, b.aging,
                       b.fail_history, s.urgency AS station_urgency
                FROM serving.bike_risk_daily b
                LEFT JOIN serving.station_daily s
                  ON s.station_id = b.station_id AND s.snapshot_date = b.snapshot_date
                WHERE b.snapshot_date = :d AND b.region IS NOT NULL
                  AND b.risk_grade <> :no_action_grade
                ORDER BY b.risk_score DESC
                """
            ),
            {"d": snapshot_date, "no_action_grade": NO_ACTION_GRADE},
        ).mappings().all()

    source = [_to_bike(r) for r in rows]
    dest: list[dict] = []
    return source, dest


def ensure_action_log_table() -> None:
    """app.action_log 준비 (멱등).

    이 테이블은 develop의 sql/bike_man/bikeman_seed_init.sql에 "app 스키마 placeholder"로
    선언만 돼 있고 아무도 읽고 쓰지 않는다. 확정 기록에 쓰려면 어느 날짜 확정인지 구분할
    snapshot_date와, 나중에 이벤트 생성기가 필요로 하는 station_id가 있어야 해서 여기서
    덧붙인다 — ADD COLUMN IF NOT EXISTS라 seed SQL 실행 순서와 무관하게 안전하다.
    """
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS app"))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app.action_log (
                    id            SERIAL PRIMARY KEY,
                    bike_id       VARCHAR(20) NOT NULL,
                    action_taken  VARCHAR(50),
                    actioned_by   VARCHAR(50),
                    actioned_at   TIMESTAMP DEFAULT now()
                )
                """
            )
        )
        conn.execute(text("ALTER TABLE app.action_log ADD COLUMN IF NOT EXISTS snapshot_date DATE"))
        conn.execute(text("ALTER TABLE app.action_log ADD COLUMN IF NOT EXISTS station_id VARCHAR(20)"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS action_log_snapshot_idx "
                "ON app.action_log (snapshot_date, bike_id)"
            )
        )


def get_latest_confirmation() -> dict:
    """오늘 스냅샷의 마지막 확정 상태. 새로고침해도 확정 여부가 보이도록 프론트가 이걸 읽는다.

    append-only라 같은 날 여러 배치가 쌓일 수 있으므로 최신 배치(actioned_at 최대값)만 센다.
    어제 확정이 남아있어도 오늘 스냅샷 기준으로만 보므로 오늘 것으로 오인되지 않는다.
    """
    ensure_action_log_table()
    snapshot_date = _latest_snapshot_date()
    if snapshot_date is None:
        return {"snapshot_date": "", "confirmed": 0, "actioned_at": None}

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT count(*) AS confirmed, max(actioned_at) AS actioned_at
                FROM app.action_log
                WHERE snapshot_date = :d AND action_taken = :action
                  AND actioned_at = (
                      SELECT max(actioned_at) FROM app.action_log
                      WHERE snapshot_date = :d AND action_taken = :action
                  )
                """
            ),
            {"d": snapshot_date, "action": COLLECT_ACTION},
        ).mappings().first()

    actioned_at = row["actioned_at"] if row else None
    return {
        "snapshot_date": snapshot_date.isoformat(),
        "confirmed": row["confirmed"] if row else 0,
        "actioned_at": actioned_at.isoformat() if actioned_at else None,
    }


def get_confirmed_bikes() -> dict:
    """가장 최근 확정 배치에 담긴 자전거 목록. 확정 내역 조회 화면이 쓴다.

    app.action_log에는 bike_id/station_id만 있으므로 화면에 필요한 위험도·대여소 정보는
    serving.bike_risk_daily / station_daily에서 조인해 채운다.
    """
    summary = get_latest_confirmation()
    if summary["confirmed"] == 0:
        return {**summary, "bikes": []}

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT b.bike_id, b.station_name, b.district, b.region, b.healthy_ratio,
                       b.risk_grade, b.risk_score, b.dist_km, b.aging,
                       b.fail_history, s.urgency AS station_urgency
                FROM app.action_log a
                JOIN serving.bike_risk_daily b
                  ON b.bike_id = a.bike_id AND b.snapshot_date = a.snapshot_date
                LEFT JOIN serving.station_daily s
                  ON s.station_id = b.station_id AND s.snapshot_date = b.snapshot_date
                WHERE a.snapshot_date = :d AND a.action_taken = :action
                  AND a.actioned_at = (
                      SELECT max(actioned_at) FROM app.action_log
                      WHERE snapshot_date = :d AND action_taken = :action
                  )
                ORDER BY b.risk_score DESC
                """
            ),
            {"d": summary["snapshot_date"], "action": COLLECT_ACTION},
        ).mappings().all()

    return {**summary, "bikes": [_to_bike(r) for r in rows]}


def confirm_collection(bike_ids: list[str]) -> dict:
    """운영자가 확정한 수거 목록을 app.action_log에 기록한다.

    snapshot_date/station_id는 클라이언트를 믿지 않고 서버가 serving.bike_risk_daily에서
    가져온다 — INSERT ... SELECT 한 문장이라 조회와 삽입 사이에 스냅샷이 바뀔 틈이 없고,
    그 스냅샷에 없는 bike_id는 자연히 무시된다.

    append-only 로그다(테이블의 SERIAL + actioned_at DEFAULT now() 설계 그대로). 같은 날
    capacity를 바꿔 다시 확정하면 이전 배치를 지우지 않고 새 배치가 쌓인다.

    ### 읽는 쪽 주의: bike_id별 최신이 아니라 "최신 배치" 기준으로 읽어야 한다
    확정을 700대 -> 100대로 줄이고 다시 누르면, 빠진 600대는 이전 배치의 '수거' 행을 그대로
    갖고 있고 그걸 취소하는 행은 없다. 그래서 bike_id별 최신 행을 집는 DISTINCT ON 방식은
    600대를 여전히 확정된 것으로 잘못 읽는다(실측: 100대가 나와야 하는데 700대가 나옴).

    한 번의 확정은 INSERT 한 문장이고 now()는 트랜잭션 내에서 고정이므로, 한 배치의 모든
    행은 actioned_at이 정확히 같다. 따라서 최신 배치만 집으면 된다:

        SELECT bike_id, station_id
        FROM app.action_log
        WHERE snapshot_date = %(d)s AND action_taken = '수거'
          AND actioned_at = (
              SELECT max(actioned_at) FROM app.action_log
              WHERE snapshot_date = %(d)s AND action_taken = '수거'
          )
    """
    ensure_action_log_table()
    snapshot_date = _latest_snapshot_date()
    if snapshot_date is None or not bike_ids:
        return get_latest_confirmation()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO app.action_log
                    (snapshot_date, bike_id, station_id, action_taken, actioned_by)
                SELECT snapshot_date, bike_id, station_id, :action, :actioned_by
                FROM serving.bike_risk_daily
                WHERE snapshot_date = :d AND bike_id = ANY(:bike_ids)
                """
            ),
            {
                "d": snapshot_date,
                "bike_ids": list(bike_ids),
                "action": COLLECT_ACTION,
                "actioned_by": ACTIONED_BY,
            },
        )

    # 방금 넣은 배치를 그대로 다시 읽어 돌려준다 — GET /api/actions/confirm과 항상 같은 값을
    # 보게 되므로, 확정 직후 화면과 새로고침 후 화면이 어긋나지 않는다.
    return get_latest_confirmation()


class OperationState:
    @property
    def meta(self) -> dict:
        return get_meta()

    @property
    def map_data(self) -> dict:
        return get_map_data()

    def bikes(self) -> tuple[list[dict], list[dict]]:
        return get_bikes()

    def confirm_collection(self, bike_ids: list[str]) -> dict:
        return confirm_collection(bike_ids)

    @property
    def latest_confirmation(self) -> dict:
        return get_latest_confirmation()

    @property
    def confirmed_bikes(self) -> dict:
        return get_confirmed_bikes()


state = OperationState()
