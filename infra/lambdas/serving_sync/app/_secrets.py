"""
Secrets Manager에서 서빙 DB / Iceberg jdbc 카탈로그 자격증명을 읽어 os.environ에
채운다 (#172).

serving_db.py는 SERVING_DB_HOST/PORT/NAME/USER/PASSWORD를, common/iceberg_catalog.py는
(config.SETTINGS를 통해) ICEBERG_JDBC_CATALOG_USER/PASSWORD를 os.environ에서 직접
읽도록 짜여 있다. 이 파일은 그 관례를 그대로 두면서(#172 완료 조건: serving_db.py
변경 없음) Lambda 실행 환경에서만 그 값들의 출처를 Secrets Manager로 바꾼다.

콜드 스타트 시(핸들러 모듈이 import될 때) 1회만 호출한다 - 웜 인스턴스가 재사용될
때마다 Secrets Manager를 다시 부르지 않기 위함. SERVING_DB_SECRET_ARN이 없으면
(로컬 실행 등) 이미 환경변수가 채워져 있다고 보고 조용히 넘어간다.
"""
import json
import os

import boto3

_SECRET_ENV_KEYS = (
    "SERVING_DB_HOST",
    "SERVING_DB_PORT",
    "SERVING_DB_NAME",
    "SERVING_DB_USER",
    "SERVING_DB_PASSWORD",
    "ICEBERG_JDBC_CATALOG_USER",
    "ICEBERG_JDBC_CATALOG_PASSWORD",
)


def load_serving_db_secret() -> None:
    secret_arn = os.environ.get("SERVING_DB_SECRET_ARN")
    if not secret_arn:
        return

    client = boto3.client("secretsmanager")
    secret = json.loads(client.get_secret_value(SecretId=secret_arn)["SecretString"])
    for key in _SECRET_ENV_KEYS:
        if key in secret:
            os.environ[key] = str(secret[key])
