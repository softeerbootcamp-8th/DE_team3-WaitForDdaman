"""
write_bike_risk_daily의 Lambda 진입점 (#172).

핸들러는 얇게 유지한다 - 실제 로직은 pipeline/serving_sync/jobs/write_bike_risk_daily.py의
run()에 그대로 있고(로컬 `python -m jobs.write_bike_risk_daily` 경로와 완전히 같은
코드), 여기서는 (1) Secrets Manager에서 DB 자격증명을 채우고 (2) event의
snapshot_date를 SNAPSHOT_DATE 환경변수로 옮겨준 뒤 그 run()을 그대로 부른다.

Terraform의 image_config.command로 "app.write_bike_risk_daily.handler"를 가리키면
이 handler가 진입점이 된다 - 이미지는 write_station_daily/verify_serving_sync와
공유하고 함수(Lambda 리소스)만 셋으로 나뉜다.
"""
import os
from typing import Any

from ._secrets import load_serving_db_secret

load_serving_db_secret()

from write_bike_risk_daily import run  # noqa: E402 (자격증명을 채운 뒤에 import해야 함)


def handler(event: dict, context: Any) -> dict:
    event = event or {}
    if event.get("snapshot_date"):
        os.environ["SNAPSHOT_DATE"] = event["snapshot_date"]

    run()
    return {"statusCode": 200}
