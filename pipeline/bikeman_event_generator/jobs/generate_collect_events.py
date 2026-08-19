"""
serving.bike_risk_daily의 최신 snapshot_date(<= target_date)에서 risk_score 상위 N대에
대해 COLLECT 이벤트를 생성한다. N은 BIKEMAN_COLLECT_LIMIT로 조정할 수 있고 기본값은
500이다.

사용법 (Airflow PythonOperator에서 호출됨, 단독 실행 시):
    python -c "import generate_collect_events; generate_collect_events.run('2026-07-01')"
"""
import logging
import os
import random

from airflow.providers.postgres.hooks.postgres import PostgresHook

import bikeman_db
import event_builder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONN_ID = "bikeman_postgres"


def run(target_date: str) -> int:
    collect_limit = int(os.getenv("BIKEMAN_COLLECT_LIMIT", str(bikeman_db.COLLECT_LIMIT_DEFAULT)))
    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
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
