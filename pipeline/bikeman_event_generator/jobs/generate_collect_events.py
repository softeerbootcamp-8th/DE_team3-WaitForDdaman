"""
serving.bike_risk_daily의 최신 snapshot_date(<= target_date)에서 risk_score 상위 N대에
대해 COLLECT 이벤트를 생성한다. N은 BIKEMAN_COLLECT_LIMIT로 조정할 수 있고 기본값은
500이다.

### Lambda 전환 (#186)
기존엔 Airflow Connection(bikeman_postgres)의 PostgresHook으로 연결했으나, Lambda는
Airflow 컨텍스트가 없어 이 방식을 못 쓴다. serving_db.py/db_client.py와 동일한
컨벤션(psycopg2 + 환경변수)으로 되돌린다 - 이 파일도 `python -m jobs.X`로 Airflow
없이 단독 실행 가능해야 한다는 저장소 전체 컨벤션에 다시 맞춘 것.

BIKEMAN_WRITER_DB_* 접두사를 쓴다 - ingestion/common/db_client.py가 이미 BIKEMAN_DB_*를
airflow_reader(읽기 전용) 역할로 쓰고 있어서, 이 잡이 쓰는 bikeman_writer(쓰기 가능)
자격증명과 이름이 겹치면 안 된다.

사용법 (Lambda에서 호출됨, 단독 실행 시):
    BIKEMAN_WRITER_DB_HOST=... BIKEMAN_WRITER_DB_NAME=... BIKEMAN_WRITER_DB_USER=... \
    BIKEMAN_WRITER_DB_PASSWORD=... python -c "import generate_collect_events; generate_collect_events.run('2026-07-01')"
"""
import logging
import os
import random

import bikeman_db
import event_builder
from bikeman_connection import connect as _connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run(target_date: str) -> int:
    collect_limit = int(os.getenv("BIKEMAN_COLLECT_LIMIT", str(bikeman_db.COLLECT_LIMIT_DEFAULT)))
    conn = _connect()
    try:
        targets = bikeman_db.fetch_collect_targets(conn, target_date, limit=collect_limit)
        events = [
            event_builder.build_collect_event(
                t["bike_id"], t["station_id"], target_date, random.choice(event_builder.WORKER_POOL)
            )
            for t in targets
        ]
        written = bikeman_db.insert_events(conn, events)
    finally:
        conn.close()

    logger.info(
        "%s: risk_score 상위 %d대 중 COLLECT 대상 %d건, 신규 삽입 %d건",
        target_date,
        collect_limit,
        len(targets),
        written,
    )
    return written
