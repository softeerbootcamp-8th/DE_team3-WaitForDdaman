"""
전날(target_date - 1일) COLLECT되고 아직 미배치인 자전거를 원래 station_id로 DEPLOY한다.
"미배치"는 별도 플래그가 아니라 "그 자전거의 가장 최근 이벤트가 COLLECT"라는 사실 자체로
정의된다 - 이미 DEPLOY됐다면 가장 최근 이벤트는 DEPLOY이므로 자동으로 제외된다
(bikeman_db.fetch_deploy_targets의 WITH latest ... 쿼리 참고).

### Lambda 전환 (#186)
generate_collect_events.py와 동일한 이유 - PostgresHook 대신 psycopg2 + 환경변수
(BIKEMAN_WRITER_DB_*)로 연결한다.

사용법 (Lambda에서 호출됨, 단독 실행 시):
    BIKEMAN_WRITER_DB_HOST=... BIKEMAN_WRITER_DB_NAME=... BIKEMAN_WRITER_DB_USER=... \
    BIKEMAN_WRITER_DB_PASSWORD=... python -c "import deploy_returned_bikes; deploy_returned_bikes.run('2026-07-01')"
"""
import logging
import os
import random

import psycopg2

import bikeman_db
import event_builder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _connect():
    return psycopg2.connect(
        host=os.environ["BIKEMAN_WRITER_DB_HOST"],
        port=os.environ.get("BIKEMAN_WRITER_DB_PORT", "5432"),
        dbname=os.environ["BIKEMAN_WRITER_DB_NAME"],
        user=os.environ["BIKEMAN_WRITER_DB_USER"],
        password=os.environ["BIKEMAN_WRITER_DB_PASSWORD"],
        connect_timeout=10,
    )


def run(target_date: str) -> int:
    conn = _connect()
    try:
        targets = bikeman_db.fetch_deploy_targets(conn, target_date)
        events = [
            event_builder.build_deploy_event(
                t["bike_id"], t["station_id"], target_date, random.choice(event_builder.WORKER_POOL)
            )
            for t in targets
        ]
        written = bikeman_db.insert_events(conn, events)
    finally:
        conn.close()

    logger.info("%s: DEPLOY 대상(전날 COLLECT & 미배치) %d건 중 %d건 신규 삽입", target_date, len(targets), written)
    return written