"""
Secrets Manager에서 bikeman_writer DB 자격증명을 읽어 os.environ에 채운다 (#186).

generate_collect_events.py/deploy_returned_bikes.py는 BIKEMAN_WRITER_DB_HOST/PORT/
NAME/USER/PASSWORD를 os.environ에서 직접 읽도록 짜여 있다(#186 완료 조건: 두 파일
쿼리 로직 변경 없음). 이 파일은 그 관례를 그대로 두면서, Lambda 실행 환경에서만
그 값들의 출처를 Secrets Manager로 바꾼다 - serving_sync의 _secrets.py(#172)와
동일한 패턴.

콜드 스타트 시(핸들러 모듈이 import될 때) 1회만 호출한다 - 웜 인스턴스가 재사용될
때마다 Secrets Manager를 다시 부르지 않기 위함. BIKEMAN_DB_SECRET_ARN이 없으면
(로컬 실행 등) 이미 환경변수가 채워져 있다고 보고 조용히 넘어간다.
"""
import json
import os

import boto3

_SECRET_ENV_KEYS = (
    "BIKEMAN_WRITER_DB_HOST",
    "BIKEMAN_WRITER_DB_PORT",
    "BIKEMAN_WRITER_DB_NAME",
    "BIKEMAN_WRITER_DB_USER",
    "BIKEMAN_WRITER_DB_PASSWORD",
)


def load_bikeman_db_secret() -> None:
    secret_arn = os.environ.get("BIKEMAN_DB_SECRET_ARN")
    if not secret_arn:
        return

    client = boto3.client("secretsmanager")
    secret = json.loads(client.get_secret_value(SecretId=secret_arn)["SecretString"])
    for key in _SECRET_ENV_KEYS:
        if key in secret:
            os.environ[key] = str(secret[key])