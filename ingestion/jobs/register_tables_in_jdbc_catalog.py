"""
Hadoop Catalog -> JDBC Catalog 등록 (1회성 마이그레이션)

왜 필요한가: 카탈로그는 "테이블 이름 -> 최신 metadata.json 위치" 포인터만 관리한다.
데이터/메타데이터 파일 자체(Parquet, manifest, snapshot avro)는 이미 warehouse(S3)에
그대로 있으므로, 새 JDBC 카탈로그로 옮긴다는 건 그 파일들을 다시 쓰는 게 아니라
포인터만 새로 등록(register_table)하면 된다.

Hadoop Catalog는 각 테이블의 metadata/version-hint.text에 현재 버전 번호(N)를 담고
있고, 그 버전의 metadata 파일은 metadata/v{N}.metadata.json이라는 규칙을 따른다
(실측 확인, 2026-08-22). 이 잡은:
  1. warehouse 하위에서 모든 version-hint.text를 찾아 (db, table) 목록을 만들고
  2. 각각의 현재 metadata.json 경로를 조립한 뒤
  3. JDBC 카탈로그로 접속한 Spark 세션 하나로 CALL <catalog>.system.register_table(...)
     을 실행한다.

이미 등록된 테이블을 다시 돌리면(재실행) Iceberg가 "already exists" 류 에러를 던진다 -
이 잡은 그 경우를 실패로 취급하지 않고 스킵으로 로그만 남긴다 (멱등하게 재실행 가능).

⚠️ ICEBERG_CATALOG_TYPE=jdbc를 강제로 세팅하고 실행한다 - .env의 기본값(hadoop)을
안 건드리고, 이 스크립트 실행 범위에서만 JDBC 카탈로그로 접속하기 위함이다. 실제로
모든 잡을 JDBC로 전환하는 건 이 스크립트가 전부 등록을 마친 뒤 .env의
ICEBERG_CATALOG_TYPE을 jdbc로 바꾸는 별도 스텝이다.

사용법:
    python -m jobs.register_tables_in_jdbc_catalog
"""
import logging
import os
import re

# spark_session.py/config가 import되기 전에 강제해야 이 프로세스 안에서 jdbc 분기를 탄다.
os.environ["ICEBERG_CATALOG_TYPE"] = "jdbc"

import config
from common.s3_utils import get_s3_client
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

VERSION_HINT_SUFFIX = "/metadata/version-hint.text"
# warehouse/<db>/<table>/metadata/version-hint.text 형태만 대상으로 한다
# (그 이상 깊이의 경로는 이 카탈로그 레이아웃에 없음).
TABLE_PATH_RE = re.compile(r"^(?P<prefix>.+)/(?P<db>[^/]+)/(?P<table>[^/]+)/metadata/version-hint\.text$")


def _discover_tables(bucket: str, warehouse_prefix: str) -> list[tuple[str, str, str]]:
    """warehouse 하위 모든 version-hint.text를 찾아 (db, table, metadata_location) 목록을 만든다."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    results = []
    for page in paginator.paginate(Bucket=bucket, Prefix=warehouse_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(VERSION_HINT_SUFFIX):
                continue
            m = TABLE_PATH_RE.match(key)
            if not m:
                logger.warning("경로 패턴이 예상과 다름 - 스킵: %s", key)
                continue
            db, table = m.group("db"), m.group("table")

            version_str = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8").strip()
            metadata_key = f"{m.group('prefix')}/{db}/{table}/metadata/v{version_str}.metadata.json"
            metadata_location = f"s3a://{bucket}/{metadata_key}"
            results.append((db, table, metadata_location))
    return results


def run() -> None:
    settings = config.SETTINGS
    catalog = settings.iceberg_catalog_name

    # iceberg_warehouse_path는 "s3a://<bucket>/warehouse" 형태 - 버킷과 prefix로 분리.
    warehouse_path = settings.iceberg_warehouse_path
    without_scheme = warehouse_path.split("://", 1)[1]
    bucket, warehouse_prefix = without_scheme.split("/", 1)

    tables = _discover_tables(bucket, warehouse_prefix)
    logger.info("발견된 테이블 %d개: %s", len(tables), [f"{db}.{t}" for db, t, _ in tables])

    spark = build_spark_session("register-tables-in-jdbc-catalog")
    try:
        registered, skipped, failed = [], [], []
        for db, table, metadata_location in tables:
            identifier = f"{db}.{table}"
            try:
                spark.sql(
                    f"CALL {catalog}.system.register_table("
                    f"table => '{identifier}', metadata_file => '{metadata_location}')"
                )
                registered.append(identifier)
                logger.info("등록 완료: %s -> %s", identifier, metadata_location)
            except Exception as e:  # noqa: BLE001 - Iceberg/Spark 예외 타입이 버전마다 달라 광범위하게 잡음
                message = str(e)
                if "already exists" in message.lower():
                    skipped.append(identifier)
                    logger.info("이미 등록돼 있음 - 스킵: %s", identifier)
                else:
                    failed.append((identifier, message))
                    logger.error("등록 실패: %s (%s)", identifier, message)
    finally:
        spark.stop()

    logger.info(
        "마이그레이션 종료 - 등록 %d개, 스킵 %d개, 실패 %d개",
        len(registered), len(skipped), len(failed),
    )
    if failed:
        for identifier, message in failed:
            logger.error("  실패: %s - %s", identifier, message)
        raise SystemExit(1)


if __name__ == "__main__":
    run()
