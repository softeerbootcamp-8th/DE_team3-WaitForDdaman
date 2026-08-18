"""
전날(target_date - 1일) COLLECT되고 아직 미배치인 자전거를 원래 station_id로 DEPLOY한다.
"미배치"는 별도 플래그가 아니라 "그 자전거의 가장 최근 이벤트가 COLLECT"라는 사실 자체로
정의된다 - 이미 DEPLOY됐다면 가장 최근 이벤트는 DEPLOY이므로 자동으로 제외된다
(bikeman_db.fetch_deploy_targets의 WITH latest ... 쿼리 참고).

사용법 (Airflow PythonOperator에서 호출됨, 단독 실행 시):
    python -c "import deploy_returned_bikes; deploy_returned_bikes.run('2026-07-01')"
"""
import logging
import random

from airflow.providers.postgres.hooks.postgres import PostgresHook

import bikeman_db
import event_builder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONN_ID = "bikeman_postgres"


def run(target_date: str) -> int:
    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
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
