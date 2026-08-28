"""Postgres 연결. dim_district / station_daily / bike_risk_daily 3개 테이블을
Airflow 파이프라인이 매일 UPSERT하고, API는 여기서 최신 snapshot_date 기준으로 읽기만 한다.
"""
from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def _database_url() -> str:
    """DATABASE_URL은 필수다.

    예전에는 로컬 docker-compose를 가정한 기본값
    ("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")으로 폴백했는데,
    prod에는 그 호스트(postgres 컨테이너)가 아예 없다. 그래서 DATABASE_URL이
    빠지면 존재하지 않는 host에 조용히 붙으려 하다가, 설정 누락이 아니라 요청
    처리 중 DB 연결 실패처럼 보이는 형태로만 드러났다. 기동 시점에 바로
    터뜨려서 원인이 분명하게 보이도록 한다.

    비밀번호는 URL에 넣지 않고 PGPASSWORD로 넘긴다(docker-compose.prod.yml 참고) -
    @ / : 가 든 비밀번호가 URL에서 조용히 오파싱되는 것을 피하기 위함.
    """
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL 환경변수가 설정되지 않았습니다. "
            "docker-compose.prod.yml의 api 서비스가 DOMAIN_DB_* 값으로 이 값을 조립합니다 - "
            ".env에 DOMAIN_DB_HOST/PORT/NAME/USER가 채워져 있는지 확인하세요."
        )
    return url


engine: Engine = create_engine(_database_url(), pool_pre_ping=True)
