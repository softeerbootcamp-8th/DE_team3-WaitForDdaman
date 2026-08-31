"""
DQ 어써션 결과 히스토리 적재 (#217)

run_dq_assertions_*.py가 남긴 pending JSON을 읽어 dq.check_result_history Iceberg
테이블에 append한다. source_name을 파라미터로 받으므로 다음 소스가 추가돼도 이 잡
자체는 바뀌지 않는다.

사용법:
    EXECUTION_DATE=2026-08-24 DQ_SOURCE_NAME=rental_history python -m jobs.log_dq_check_result
"""
import logging
import os

import config
from common.dq_assertions import CheckResult
from common.dq_result_store import append_results, results_to_arrow
from common.iceberg_catalog import build_iceberg_catalog
from common.s3_utils import get_json
from operations.run_dq_assertions import pending_result_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run(source_name: str | None = None, execution_date_str: str | None = None) -> int:
    source_name = source_name or os.environ.get("DQ_SOURCE_NAME", "rental_history")
    execution_date_str = execution_date_str or os.environ["EXECUTION_DATE"]

    bucket = config.SETTINGS.raw_bucket
    key = pending_result_key(source_name, execution_date_str)
    payload = get_json(bucket, key)
    if not payload:
        raise RuntimeError(
            f"pending DQ 결과 없음: s3://{bucket}/{key} (run_dq_assertions_*를 먼저 실행해야 함)"
        )

    results = [CheckResult(**r) for r in payload["results"]]

    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "local")
    dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID", "dq_rental_history")
    task_id = os.environ.get("AIRFLOW_CTX_TASK_ID", "log_dq_check_result")

    arrow_table = results_to_arrow(
        results,
        run_id=run_id,
        dag_id=dag_id,
        task_id=task_id,
        source_name=source_name,
        execution_date=execution_date_str,
    )

    catalog = build_iceberg_catalog()
    append_results(catalog, arrow_table)

    logger.info(
        "dq.check_result_history에 %d건 적재 완료 (%s, %s)",
        len(results), source_name, execution_date_str,
    )
    return len(results)


if __name__ == "__main__":
    run()
