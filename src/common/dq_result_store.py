"""
dq.check_result_history Iceberg 히스토리 테이블 (#217).

매 배치 실행마다 SQL 어써션 결과를 append-only로 적재한다. silver.dq_check_result
(dq_utils.py, PyDeequ/bikeman_action용)와 스키마가 달라 그 테이블/경로는 건드리지 않고
새 네임스페이스(dq)로 분리했다.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

from common.dq_assertions import CheckResult
from common.iceberg_io import append as iceberg_append

RESULT_TABLE = "dq.check_result_history"

RESULT_SCHEMA = Schema(
    NestedField(1, "run_id", StringType(), required=False),
    NestedField(2, "dag_id", StringType(), required=False),
    NestedField(3, "task_id", StringType(), required=False),
    NestedField(4, "source_name", StringType(), required=False),
    NestedField(5, "check_name", StringType(), required=False),
    NestedField(6, "target_column", StringType(), required=False),
    NestedField(7, "metric_value", DoubleType(), required=False),
    NestedField(8, "threshold", DoubleType(), required=False),
    NestedField(9, "pass_fail", StringType(), required=False),
    NestedField(10, "executed_at", TimestamptzType(), required=False),
    NestedField(11, "execution_date", StringType(), required=False),
)

RESULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=11, field_id=1000, transform=IdentityTransform(), name="execution_date")
)
RESULT_PROPERTIES = {"write.distribution-mode": "hash"}


def ensure_result_table(catalog: Catalog):
    """테이블이 없으면 만든다. 이미 있으면 스키마/스펙을 건드리지 않고 그대로 쓴다."""
    catalog.create_namespace_if_not_exists("dq")
    try:
        return catalog.load_table(RESULT_TABLE)
    except NoSuchTableError:
        return catalog.create_table(
            RESULT_TABLE,
            schema=RESULT_SCHEMA,
            partition_spec=RESULT_PARTITION_SPEC,
            properties=RESULT_PROPERTIES,
        )


def results_to_arrow(
    results: Iterable[CheckResult],
    run_id: str,
    dag_id: str,
    task_id: str,
    source_name: str,
    execution_date: str,
) -> pa.Table:
    now = datetime.now(timezone.utc)
    rows = [
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "source_name": source_name,
            "check_name": r.check_name,
            "target_column": r.target_column,
            "metric_value": r.metric_value,
            "threshold": r.threshold,
            "pass_fail": r.pass_fail,
            "executed_at": now,
            "execution_date": execution_date,
        }
        for r in results
    ]
    return pa.Table.from_pylist(rows, schema=RESULT_SCHEMA.as_arrow())


def append_results(catalog: Catalog, arrow_table: pa.Table) -> None:
    ensure_result_table(catalog)
    iceberg_append(RESULT_TABLE, arrow_table, catalog=catalog)
