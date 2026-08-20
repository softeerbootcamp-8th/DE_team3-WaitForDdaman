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
SCHEMA = "serving"


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


def replace_partition(table: str, columns: list[str], snapshot_date: str, rows: list[tuple]) -> int:
    """해당 snapshot_date 파티션을 삭제 후 재삽입 (같은 트랜잭션) - Iceberg의
    overwritePartitions와 동일한 의미. mart가 줄어드는 경우(대여소 비활성화 등)에도
    postgres에 오래된 행이 남지 않아 verify_serving_sync의 row count 비교가 항상 유효하다."""
    qualified = f"{SCHEMA}.{table}"
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {qualified} WHERE snapshot_date = %s", (snapshot_date,))
            if rows:
                query = f"INSERT INTO {qualified} ({', '.join(columns)}) VALUES %s"
                for i in range(0, len(rows), BATCH_SIZE):
                    psycopg2.extras.execute_values(cur, query, rows[i : i + BATCH_SIZE], page_size=BATCH_SIZE)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise ServingDbError(f"{qualified} 파티션 교체 실패: {e}") from e
    finally:
        conn.close()

    logger.info("%s: %s 파티션 교체 완료 (%d행)", qualified, snapshot_date, len(rows))
    return len(rows)


def count_rows(table: str, snapshot_date: str) -> int:
    qualified = f"{SCHEMA}.{table}"
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {qualified} WHERE snapshot_date = %s", (snapshot_date,))
            return cur.fetchone()[0]
    except psycopg2.Error as e:
        raise ServingDbError(f"{qualified} 행 개수 조회 실패: {e}") from e
    finally:
        conn.close()


def ensure_serving_tables() -> None:
    """serving.station_daily / serving.bike_risk_daily 테이블(스키마 포함)이 없으면 생성한다.

    dim_district는 시드 데이터라 이 잡의 책임 밖이라 여기서 만들지 않는다.
    """
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.station_daily (
                    snapshot_date  DATE NOT NULL,
                    station_id     TEXT NOT NULL,
                    station_name   TEXT NOT NULL,
                    region         TEXT NOT NULL,
                    district       TEXT NOT NULL,
                    latitude       DOUBLE PRECISION,
                    longitude      DOUBLE PRECISION,
                    hold_num       INT NOT NULL,
                    bike_cnt       INT NOT NULL,
                    risk_cnt       INT NOT NULL,
                    healthy_ratio  DOUBLE PRECISION NOT NULL,
                    urgency        TEXT NOT NULL,
                    PRIMARY KEY (station_id, snapshot_date)
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.bike_risk_daily (
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
                    PRIMARY KEY (bike_id, snapshot_date)
                )
                """
            )
            cur.execute(f"ALTER TABLE {SCHEMA}.bike_risk_daily DROP COLUMN IF EXISTS action")
        conn.commit()
    except psycopg2.Error as e:
        raise ServingDbError(f"serving 테이블 생성 실패: {e}") from e
    finally:
        conn.close()
    logger.info("%s.station_daily / %s.bike_risk_daily 테이블 준비 완료", SCHEMA, SCHEMA)
