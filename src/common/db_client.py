"""
따맨/bikeman OLTP Postgres 접속 클라이언트.

다른 원천이 공공 API(common.api_client)에서 오는 것과 달리, 이 데이터는
우리 자신의 서비스(bikeman 스키마)에서 직접 SQL로 읽어온다.

Airflow의 PostgresHook을 쓰지 않고 psycopg2를 직접 쓰는 이유: 이 프로젝트의 다른
jobs/*.py가 전부 `python -m jobs.X`로 Airflow 없이도 단독 실행 가능하다는 컨벤션을
따르고 있어서(각 파일 상단 "사용법" 참고), 이 잡도 Airflow 런타임에 의존하지 않게
하기 위함이다.

일 배치로 확정되면서 조회 기준을 received_at range가 아니라 occurred_at 날짜로
바꿨다 (common/api_client.py의 fetch_rent_history_by_date와 동일한 패턴).

필요한 .env 변수 (docker-compose의 airflow_reader 롤과 반드시 일치해야 함):
    BIKEMAN_DB_HOST=postgres
    BIKEMAN_DB_PORT=5432
    BIKEMAN_DB_NAME=<POSTGRES_DB와 동일 - 실행 로그 기준 현재 값은 hamzzi>
    BIKEMAN_DB_USER=airflow_reader
    BIKEMAN_DB_PASSWORD=airflow_reader_pw

airflow_reader는 bikeman 스키마에 SELECT 권한만 있는 롤이다. INSERT/UPDATE 권한이
없어서, 이 잡의 버그로 원본 OLTP 데이터가 오염될 위험이 구조적으로 차단되어 있다.

⚠️ occurred_at 컬럼에 인덱스가 아직 없다. 지금(500건 규모)은 문제없지만, 데이터가
   쌓이면 아래 인덱스를 추가하는 게 좋다 (idempotent하게 IF NOT EXISTS로):
       CREATE INDEX IF NOT EXISTS idx_fact_worker_event_occurred_at
           ON bikeman.fact_worker_event (occurred_at);
"""
import logging
import os
from datetime import date, datetime, timedelta

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class BikemanDbError(Exception):
    """bikeman DB 연결/쿼리 실패. 호출부에서 배치를 안전하게 중단시키는 용도."""


def _connect():
    try:
        return psycopg2.connect(
            host=os.environ["BIKEMAN_DB_HOST"],
            port=os.environ.get("BIKEMAN_DB_PORT", "5432"),
            dbname=os.environ["BIKEMAN_DB_NAME"],
            user=os.environ["BIKEMAN_DB_USER"],
            password=os.environ["BIKEMAN_DB_PASSWORD"],
            connect_timeout=10,
        )
    except KeyError as e:
        raise BikemanDbError(
            f"필수 환경변수 누락: {e}. .env에 BIKEMAN_DB_* 를 설정하세요."
        ) from e
    except psycopg2.OperationalError as e:
        raise BikemanDbError(f"bikeman DB 연결 실패: {e}") from e


def fetch_events_by_date(target_date: date) -> list[dict]:
    """
    occurred_at이 target_date(하루)에 속하는 행을 전부 가져온다.
    received_at 기준이 아니라 occurred_at 기준이라, 이 함수를 여러 번(재처리 lookback
    포함) 호출해도 항상 그날 발생한 이벤트 전체 - 즉 늦게 도착(late arrival)한 이벤트도
    받은 시점과 무관하게 자동으로 포함된다.
    """
    range_start = datetime.combine(target_date, datetime.min.time())
    range_end = range_start + timedelta(days=1)

    query = """
        SELECT event_id, event_type, bike_id, station_id, worker_id, occurred_at, received_at
        FROM bikeman.fact_worker_event
        WHERE occurred_at >= %s AND occurred_at < %s
        ORDER BY occurred_at ASC
    """
    try:
        conn = _connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (range_start, range_end))
                rows = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except psycopg2.Error as e:
        raise BikemanDbError(f"bikeman 이벤트 조회 실패 ({target_date}): {e}") from e

    logger.info("%s: bikeman 이벤트 %d건 조회", target_date, len(rows))
    return rows
