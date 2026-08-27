"""
DQ 어써션 실행 (#217, Bronze 확장 시 제네릭화)

config/dq/<DQ_ASSERTIONS_CONFIG>.yaml의 지표를 계산해서
_meta/dq/pending/{source_name}/{execution_date}.json에 저장한다 - Iceberg 적재는
log_dq_check_result.py가 별도 태스크로 담당한다(어써션 계산과 적재를 한 태스크로
합치지 않는다는 원칙, #217).

source_name/target_table/partition_column을 전부 YAML에서 읽으므로 이 모듈 자체는
어느 소스/계층에도 묶여있지 않다 - 새 소스가 생기면 YAML만 추가하면 된다.

같은 execution_date로 재실행하면 pending 파일을 그대로 덮어쓰므로 멱등하다. 이 잡은
sql_assert.py의 validate()처럼 배치를 막는 하드 게이트가 아니다 - FAIL이 있어도
배치를 중단하지 않고 기록만 남긴다(추이 판단은 해석 에이전트의 역할).

사용법:
    EXECUTION_DATE=2026-08-24 DQ_ASSERTIONS_CONFIG=/opt/airflow/pylib/config/dq/rental_history.yaml \
        python -m jobs.run_dq_assertions
"""
import dataclasses
import logging
import os
from datetime import date

from pyiceberg.expressions import EqualTo

import config
from common.dq_assertions import load_config, run_checks
from common.iceberg_catalog import build_iceberg_catalog
from common.s3_utils import ensure_bucket, put_json

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def pending_result_key(source_name: str, execution_date: str) -> str:
    return f"_meta/dq/pending/{source_name}/{execution_date}.json"


def run(execution_date_str: str | None = None) -> dict:
    execution_date_str = execution_date_str or os.environ["EXECUTION_DATE"]
    date.fromisoformat(execution_date_str)  # 형식 검증 - 이후 파티션 필터/S3 키에 그대로 씀
    config_path = os.environ["DQ_ASSERTIONS_CONFIG"]

    source_name, target_table, partition_column, checks = load_config(config_path)

    catalog = build_iceberg_catalog()
    table = catalog.load_table(target_table)
    arrow_table = table.scan(
        row_filter=EqualTo(partition_column, execution_date_str),
    ).to_arrow()

    results = run_checks(checks, arrow_table)

    payload = {
        "source_name": source_name,
        "target_table": target_table,
        "execution_date": execution_date_str,
        "row_count": len(arrow_table),
        "results": [dataclasses.asdict(r) for r in results],
    }

    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)
    put_json(bucket, pending_result_key(source_name, execution_date_str), payload)

    fail_count = sum(1 for r in results if r.pass_fail == "FAIL")
    error_count = sum(1 for r in results if r.pass_fail == "ERROR")
    logger.info(
        "%s DQ 어써션 완료 (%s, %d행): %d개 체크, FAIL %d건, ERROR %d건(스키마 불일치 의심)",
        source_name, execution_date_str, len(arrow_table), len(results), fail_count, error_count,
    )
    return payload


if __name__ == "__main__":
    run()
