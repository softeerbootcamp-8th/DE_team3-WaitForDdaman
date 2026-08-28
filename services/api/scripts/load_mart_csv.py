"""마트 CSV(export)를 serving 스키마로 적재하는 개발용 로더.

실 파이프라인(gold_to_serving_sync)이 없는 로컬 dev 환경에서, 팀원이 뽑아둔
mart_station_daily_*.csv / mart_bike_risk_daily_*.csv를 그대로 serving.station_daily /
serving.bike_risk_daily에 파티션 교체(delete+insert) 방식으로 넣는다.

사용법:
    DATABASE_URL=postgresql+psycopg2://airflow:airflow@localhost:5433/airflow \
    python3 load_mart_csv.py station data/mart_station_daily_20260818.csv
    DATABASE_URL=postgresql+psycopg2://airflow:airflow@localhost:5433/airflow \
    python3 load_mart_csv.py bikes data/mart_bike_risk_daily_20260818.csv
"""
from __future__ import annotations

import ast
import csv
import os
import sys

import psycopg2
import psycopg2.extras

STATION_COLUMNS = [
    "snapshot_date", "station_id", "station_name", "region", "district",
    "latitude", "longitude", "hold_num", "bike_cnt", "risk_cnt",
    "healthy_ratio", "urgency",
]

BIKE_COLUMNS = [
    "snapshot_date", "bike_id", "station_id", "station_name", "region", "district",
    "healthy_ratio", "risk_score", "risk_grade", "dist_km", "start_year", "aging",
    "fail_history",
]

BATCH_SIZE = 1000


def _connect():
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow"
    )
    return psycopg2.connect(database_url.replace("postgresql+psycopg2://", "postgresql://"))


def _rows(csv_path: str, columns: list[str]):
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            values = []
            for col in columns:
                v = row[col]
                if col == "fail_history":
                    v = ast.literal_eval(v) if v else []
                elif v == "":
                    v = None  # 대여소 미배정 자전거 등 - 빈 문자열이 아니라 NULL로
                values.append(v)
            yield tuple(values)


def load(kind: str, csv_path: str) -> None:
    table, columns = {
        "station": ("serving.station_daily", STATION_COLUMNS),
        "bikes": ("serving.bike_risk_daily", BIKE_COLUMNS),
    }[kind]

    rows = list(_rows(csv_path, columns))
    snapshot_dates = {r[0] for r in rows}

    conn = _connect()
    try:
        with conn.cursor() as cur:
            for d in snapshot_dates:
                cur.execute(f"DELETE FROM {table} WHERE snapshot_date = %s", (d,))
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
            for i in range(0, len(rows), BATCH_SIZE):
                psycopg2.extras.execute_values(cur, query, rows[i : i + BATCH_SIZE], page_size=BATCH_SIZE)
        conn.commit()
    finally:
        conn.close()

    print(f"{table}: {len(rows)}행 적재 완료 ({', '.join(sorted(snapshot_dates))})")


if __name__ == "__main__":
    load(sys.argv[1], sys.argv[2])
