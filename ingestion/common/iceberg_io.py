"""
PyIceberg 입출력 공통 모듈 (Spark 세션 불필요)

Spark의 `writeTo(...).overwritePartitions()`와 `append()`를 대체하여,
PyArrow Table을 Iceberg 테이블에 파티션 단위 덮어쓰기(overwrite) 또는 추가(append)합니다.
"""
from __future__ import annotations

import logging
from typing import Optional, Union

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table

from common.iceberg_catalog import build_iceberg_catalog

logger = logging.getLogger(__name__)


def _resolve_table(table_identifier_or_table: Union[str, Table], catalog: Optional[Catalog] = None) -> Table:
    """테이블 식별자 문자열 또는 Table 객체를 받아 PyIceberg Table 객체를 반환합니다."""
    if isinstance(table_identifier_or_table, Table):
        return table_identifier_or_table

    cat = catalog or build_iceberg_catalog()
    return cat.load_table(table_identifier_or_table)


def overwrite_partition(
    table_identifier_or_table: Union[str, Table],
    arrow_table: pa.Table,
    partition_col: str,
    partition_val: str,
    catalog: Optional[Catalog] = None,
) -> None:
    """
    지정된 파티션(partition_col = partition_val)을 원자적으로 덮어씁니다.
    
    Args:
        table_identifier_or_table: 'bronze.station_active' 식별자 또는 Table 객체
        arrow_table: 적재할 PyArrow Table
        partition_col: 파티션 컬럼명 (예: 'snapshot_date', 'rent_date_partition')
        partition_val: 파티션 값 (예: '2026-08-22')
        catalog: 선택적 PyIceberg Catalog 인스턴스
    """
    table = _resolve_table(table_identifier_or_table, catalog)
    overwrite_filter = EqualTo(partition_col, str(partition_val))

    logger.info(
        "Iceberg 파티션 덮어쓰기 시작: table=%s, filter=(%s=%s), row_count=%d",
        table.name(),
        partition_col,
        partition_val,
        len(arrow_table),
    )
    table.overwrite(arrow_table, overwrite_filter=overwrite_filter)
    logger.info("Iceberg 파티션 덮어쓰기 완료: table=%s", table.name())


def append(
    table_identifier_or_table: Union[str, Table],
    arrow_table: pa.Table,
    catalog: Optional[Catalog] = None,
) -> None:
    """
    PyArrow Table 데이터를 Iceberg 테이블에 추가(append)합니다.
    
    Args:
        table_identifier_or_table: 'bronze.quarantine' 식별자 또는 Table 객체
        arrow_table: 추가할 PyArrow Table
        catalog: 선택적 PyIceberg Catalog 인스턴스
    """
    table = _resolve_table(table_identifier_or_table, catalog)

    logger.info(
        "Iceberg append 시작: table=%s, row_count=%d",
        table.name(),
        len(arrow_table),
    )
    table.append(arrow_table)
    logger.info("Iceberg append 완료: table=%s", table.name())
