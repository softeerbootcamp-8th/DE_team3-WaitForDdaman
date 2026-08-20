"""Postgres 연결. dim_district / station_daily / bike_risk_daily 3개 테이블을
Airflow 파이프라인이 매일 UPSERT하고, API는 여기서 최신 snapshot_date 기준으로 읽기만 한다.
"""
from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

DEFAULT_DATABASE_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"


def _database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


engine: Engine = create_engine(_database_url(), pool_pre_ping=True)
