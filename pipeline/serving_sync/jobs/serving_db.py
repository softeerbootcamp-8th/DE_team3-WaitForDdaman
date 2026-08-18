"""
서빙 Postgres(station_daily/bike_risk_daily) 접속 + UPSERT/count/DDL 유틸.

db_client.py(bikeman)와 동일한 이유로 psycopg2 직접 연결 - 이 job도 `python -m jobs.X`로
Airflow 없이 단독 실행 가능해야 한다.

필요한 .env 변수 (docker-compose의 postgres 롤과 반드시 일치해야 함):
    SERVING_DB_HOST=postgres
    SERVING_DB_PORT=5432
    SERVING_DB_NAME=<루트 .env의 POSTGRES_DB와 동일>
    SERVING_DB_USER=<루트 .env의 POSTGRES_USER와 동일>
    SERVING_DB_PASSWORD=<루트 .env의 POSTGRES_PASSWORD와 동일>
"""
import logging
import os

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


class ServingDbError(Exception):
    """서빙 DB 연결/쿼리 실패. 호출부에서 배치를 안전하게 중단시키는 용도."""


def connect():
    try:
        return psycopg2.connect(
            host=os.environ["SERVING_DB_HOST"],
            port=os.environ.get("SERVING_DB_PORT", "5432"),
            dbname=os.environ["SERVING_DB_NAME"],
            user=os.environ["SERVING_DB_USER"],
            password=os.environ["SERVING_DB_PASSWORD"],
            connect_timeout=10,
        )
    except KeyError as e:
        raise ServingDbError(f"필수 환경변수 누락: {e}. .env에 SERVING_DB_*를 설정하세요.") from e
    except psycopg2.OperationalError as e:
        raise ServingDbError(f"서빙 DB 연결 실패: {e}") from e


def _build_upsert_query(table: str, columns: list[str], conflict_keys: list[str]) -> str:
    update_cols = [c for c in columns if c not in conflict_keys]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_keys)}) DO UPDATE SET {set_clause}"
    )


def upsert_rows(table: str, columns: list[str], conflict_keys: list[str], rows: list[tuple]) -> int:
    """rows를 BATCH_SIZE 단위로 나눠 INSERT ... ON CONFLICT DO UPDATE. 반환값: 적재한 행 수."""
    if not rows:
        return 0

    query = _build_upsert_query(table, columns, conflict_keys)
    conn = connect()
    try:
        with conn.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                psycopg2.extras.execute_values(cur, query, batch, page_size=BATCH_SIZE)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise ServingDbError(f"{table} UPSERT 실패: {e}") from e
    finally:
        conn.close()

    logger.info("%s: %d행 UPSERT 완료", table, len(rows))
    return len(rows)


def count_rows(table: str, snapshot_date: str) -> int:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE snapshot_date = %s", (snapshot_date,))
            return cur.fetchone()[0]
    finally:
        conn.close()


def ensure_serving_tables() -> None:
    """station_daily / bike_risk_daily 테이블이 없으면 생성한다.

    dim_district는 시드 데이터라 이 잡의 책임 밖이라 여기서 만들지 않는다.
    """
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS station_daily (
                    snapshot_date  DATE NOT NULL,
                    station_id     TEXT NOT NULL,
                    station_name   TEXT NOT NULL,
                    region         TEXT NOT NULL,
                    district       TEXT NOT NULL,
                    x              DOUBLE PRECISION,
                    y              DOUBLE PRECISION,
                    hold_num       INT NOT NULL,
                    bike_count     INT NOT NULL,
                    risk_count     INT NOT NULL,
                    healthy_ratio  DOUBLE PRECISION NOT NULL,
                    urgency        TEXT NOT NULL,
                    PRIMARY KEY (station_id, snapshot_date)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bike_risk_daily (
                    snapshot_date  DATE NOT NULL,
                    bike_id        TEXT NOT NULL,
                    station_id     TEXT,
                    station_name   TEXT,
                    region         TEXT,
                    district       TEXT,
                    healthy_ratio  DOUBLE PRECISION NOT NULL,
                    risk_grade     TEXT NOT NULL,
                    risk_score     DOUBLE PRECISION NOT NULL,
                    dist_km        DOUBLE PRECISION,
                    start_year     INT,
                    aging          INT,
                    fail_history   TEXT[],
                    action         TEXT NOT NULL,
                    PRIMARY KEY (bike_id, snapshot_date)
                )
                """
            )
        conn.commit()
    finally:
        conn.close()
    logger.info("station_daily / bike_risk_daily 테이블 준비 완료")
