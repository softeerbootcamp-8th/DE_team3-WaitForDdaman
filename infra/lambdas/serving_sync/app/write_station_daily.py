"""write_station_daily의 Lambda 진입점 - write_bike_risk_daily.py와 동일 패턴 (#172)."""
import os
from typing import Any

from ._secrets import load_serving_db_secret

load_serving_db_secret()

from write_station_daily import run  # noqa: E402 (자격증명을 채운 뒤에 import해야 함)


def handler(event: dict, context: Any) -> dict:
    event = event or {}
    if event.get("snapshot_date"):
        os.environ["SNAPSHOT_DATE"] = event["snapshot_date"]

    run()
    return {"statusCode": 200}
