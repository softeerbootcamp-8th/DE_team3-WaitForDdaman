"""
bikeman.fact_worker_event 조회/적재 DB 레이어.

이 모듈은 airflow를 import하지 않는다 - psycopg2 스타일 connection 객체(conn)를
인자로 받기만 한다(어디서 얻어왔는지는 모른다). 실제로는
generate_collect_events.py/deploy_returned_bikes.py가 PostgresHook(bikeman_postgres)
.get_conn()으로 얻은 연결을 넘겨준다.

이 파일은 실제 DB 연결이 필요해 pytest 유닛테스트 대상이 아니다(pipeline/serving_sync가
동일한 이유로 test_serving_db.py를 만들었다가 제거한 것과 같은 판단) - 대신
E2E_VERIFICATION.md의 라이브 스모크 테스트로 검증한다.
"""
import psycopg2.extras

SCHEMA = "bikeman"
TABLE = "fact_worker_event"
BATCH_SIZE = 500
COLLECT_LIMIT_DEFAULT = 500

EVENT_COLUMNS = ["event_id", "event_type", "bike_id", "station_id", "worker_id", "occurred_at", "received_at"]

_FETCH_COLLECT_TARGETS_SQL = """
    SELECT bike_id, station_id
    FROM serving.bike_risk_daily
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM serving.bike_risk_daily WHERE snapshot_date <= %(target_date)s
    )
    ORDER BY risk_score DESC NULLS LAST, bike_id ASC
    LIMIT %(limit)s
"""

_FETCH_DEPLOY_TARGETS_SQL = """
    WITH latest AS (
        SELECT DISTINCT ON (bike_id) bike_id, event_type, station_id, occurred_at
        FROM bikeman.fact_worker_event
        WHERE occurred_at < %(target_date)s::date
        ORDER BY bike_id, occurred_at DESC, (event_type = 'COLLECT') DESC
    )
    SELECT bike_id, station_id FROM latest
    WHERE event_type = 'COLLECT' AND occurred_at::date = %(target_date)s::date - INTERVAL '1 day'
"""
# WHERE occurred_at < target_date (최종 리뷰에서 발견): 이 절이 없으면 "이 자전거의
# 가장 최근 이벤트"가 target_date 이후에 쌓인 어떤 행(예: 재백필로 나중에 채워진 미래
# 날짜, 혹은 단순히 이미 처리된 다른 날짜)에도 잡힐 수 있어, 이미 지난 날짜를 재실행하면
# "어제 COLLECT"가 더 이상 최신이 아니게 되어 DEPLOY 대상이 매번 조용히 0건이 된다
# (실측: 2026-07-25 재실행 시 이 절 없이 0건 -> 절 추가 후 700건). Task 9에서 발견한
# 두 버그(태스크 순서 강제, 동률 타이브레이커)는 이 근본 원인의 증상에 대한 우회였다 -
# 이 날짜 경계가 생기면서 이번 실행 자신의 COLLECT 행이 이 CTE에 아예 들어올 수 없으므로
# deploy_returned_bikes >> generate_collect_events 순서 의존성은 이제 방어적 안전장치일
# 뿐 필수는 아니다(그래도 유지 - 굳이 제거할 이유가 없음).
#
# ORDER BY의 세 번째 키(동률 타이브레이커): event_builder가 COLLECT/DEPLOY 모두
# occurred_at을 target_date 09:00로 고정 부여하므로(event_builder.py 참고), 같은
# 자전거가 어제 COLLECT되고 오늘 다시 "수거" 목록에 올라 오늘 DEPLOY+COLLECT가 동시에
# 발생하면(위 날짜 경계 안에서, 즉 target_date 이전의 다른 날짜들 사이에서) 둘의
# occurred_at이 정확히 같아질 수 있다. DISTINCT ON은 동률일 때 어느 행이 남을지
# 보장하지 않으므로(Task 9 E2E 백필에서 실측: 이 타이브레이커 없이는 둘째 날 이후
# DEPLOY 건수가 700이 아니라 매일 376~512 사이로 들쭉날쭉했다) "그 날 COLLECT 목록에
# 다시 올랐다"는 사실이 "그 날 재배치됐다"보다 더 최신 상태를 나타낸다고 보고 동률에서
# COLLECT가 이기도록 강제한다.


def fetch_collect_targets(conn, target_date: str, limit: int = COLLECT_LIMIT_DEFAULT) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_FETCH_COLLECT_TARGETS_SQL, {"target_date": target_date, "limit": limit})
        return [dict(r) for r in cur.fetchall()]


def fetch_deploy_targets(conn, target_date: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_FETCH_DEPLOY_TARGETS_SQL, {"target_date": target_date})
        return [dict(r) for r in cur.fetchall()]


def insert_events(conn, events: list[dict]) -> int:
    """event_id(PK) 충돌 시 스킵(ON CONFLICT DO NOTHING) - 반환값은 실제로 새로 삽입된
    행 수(충돌로 스킵된 행 제외). 동일 target_date로 재실행하면 0을 반환해야 정상."""
    if not events:
        return 0

    rows = [tuple(e[c] for c in EVENT_COLUMNS) for e in events]
    query = (
        f"INSERT INTO {SCHEMA}.{TABLE} ({', '.join(EVENT_COLUMNS)}) VALUES %s "
        "ON CONFLICT (event_id) DO NOTHING"
    )

    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH_SIZE):
            psycopg2.extras.execute_values(cur, query, rows[i : i + BATCH_SIZE], page_size=BATCH_SIZE)
            inserted += cur.rowcount
    conn.commit()
    return inserted
