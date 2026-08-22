"""
verify_serving_sync의 Lambda 진입점 (#172).

CLI 버전의 run()은 실패 시 sys.exit(1)로 종료 코드를 신호하는데(BashOperator 시절
관례), Lambda 런타임은 SystemExit을 함수 에러로 깔끔하게 못 다룬다 - 그래서 run()을
그대로 부르지 않고 verify_counts()를 직접 호출해서 ServingSyncVerificationError가
평범한 예외로 올라가게 한다. Lambda가 예외를 던지면 호출부(LambdaInvokeFunctionOperator)가
그걸 Airflow 태스크 실패로 본다.

write_*와 달리 env var 폴백이 없다 - 이 Lambda는 bike_risk_daily/station_daily
검증에 공용으로 쓰이므로, event로 매번 명시적으로 받는 편이 SNAPSHOT_DATE 하나만
받던 write_* 핸들러보다 더 정확하다.
"""
from typing import Any

from ._secrets import load_serving_db_secret

load_serving_db_secret()

from common.iceberg_catalog import build_iceberg_catalog  # noqa: E402
from verify_serving_sync import verify_counts  # noqa: E402


def handler(event: dict, context: Any) -> dict:
    iceberg_table = event["iceberg_table"]
    postgres_table = event["postgres_table"]
    snapshot_date = event["snapshot_date"]

    catalog = build_iceberg_catalog()
    verify_counts(catalog, iceberg_table, postgres_table, snapshot_date)
    return {"statusCode": 200}
