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

EVENT_COLUMNS = ["event_id", "event_type", "bike_id", "station_id", "worker_id", "occurred_at", "received_at"]

_FETCH_COLLECT_TARGETS_SQL = """
    SELECT bike_id, station_id
    FROM serving.bike_risk_daily
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) FROM serving.bike_risk_daily WHERE snapshot_date <= %(target_date)s
    )
    AND action = '수거'
"""

_FETCH_DEPLOY_TARGETS_SQL = """
    WITH latest AS (
        SELECT DISTINCT ON (bike_id) bike_id, event_type, station_id, occurred_at
        FROM bikeman.fact_worker_event
        ORDER BY bike_id, occurred_at DESC, (event_type = 'COLLECT') DESC
    )
    SELECT bike_id, station_id FROM latest
    WHERE event_type = 'COLLECT' AND occurred_at::date = %(target_date)s::date - INTERVAL '1 day'
"""
# ORDER BY의 세 번째 키: event_builder가 COLLECT/DEPLOY 모두 occurred_at을 target_date
# 09:00로 고정 부여하므로(event_builder.py 참고), 같은 자전거가 어제 COLLECT되고 오늘
# 다시 "수거" 목록에 올라 오늘 DEPLOY+COLLECT가 동시에 발생하면 둘의 occurred_at이
# 정확히 같아진다. DISTINCT ON은 동률일 때 어느 행이 남을지 보장하지 않으므로(Task 9
# E2E 백필에서 실측: 이 타이브레이커 없이는 둘째 날 이후 DEPLOY 건수가 700이 아니라
# 매일 376~512 사이로 들쭉날쭉했다) "오늘 COLLECT 목록에 다시 올랐다"는 사실이 "오늘
# 재배치됐다"보다 더 최신 상태를 나타낸다고 보고 동률에서 COLLECT가 이기도록 강제한다.


def fetch_collect_targets(conn, target_date: str) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_FETCH_COLLECT_TARGETS_SQL, {"target_date": target_date})
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
