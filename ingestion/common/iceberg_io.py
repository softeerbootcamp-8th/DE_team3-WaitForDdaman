"""
PyIceberg 입출력 공통 모듈 (Spark 세션 불필요)

Spark의 `writeTo(...).overwritePartitions()`와 `append()`를 대체하여,
PyArrow Table을 Iceberg 테이블에 파티션 단위 덮어쓰기(overwrite) 또는 추가(append)합니다.
"""
from __future__ import annotations

import functools
import logging
from typing import Optional, Union

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import BooleanExpression, EqualTo, Or
from pyiceberg.table import Table

from common.iceberg_catalog import build_iceberg_catalog

logger = logging.getLogger(__name__)


def _resolve_table(table_identifier_or_table: Union[str, Table], catalog: Optional[Catalog] = None) -> Table:
    """테이블 식별자 문자열 또는 Table 객체를 받아 PyIceberg Table 객체를 반환합니다."""
    if not isinstance(table_identifier_or_table, str):
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


def build_partition_filter(partition_col: str, partition_vals: list) -> BooleanExpression:
    """
    파티션 값 하나 또는 여럿에 대한 EqualTo/OR(EqualTo) 필터를 만듭니다.

    여러 값이 들어오면 `OR(EqualTo(col, v1), EqualTo(col, v2), ...)`로 묶어 반환합니다 -
    이 필터 하나로 여러 날짜 파티션을 한 번의 overwrite() 호출(=단일 snapshot commit)에서
    동시에 교체할 수 있습니다.
    """
    values = [str(v) for v in partition_vals]
    if not values:
        raise ValueError("partition_vals가 비어 있음")
    exprs = [EqualTo(partition_col, v) for v in values]
    return functools.reduce(Or, exprs) if len(exprs) > 1 else exprs[0]


def overwrite_partitions(
    table_identifier_or_table: Union[str, Table],
    arrow_table: pa.Table,
    partition_col: str,
    partition_vals: list,
    catalog: Optional[Catalog] = None,
) -> None:
    """
    여러 파티션 값(partition_col IN partition_vals)을 하나의 Iceberg snapshot commit으로
    원자적으로 덮어씁니다.

    overwrite_partition()과 달리 파티션 값을 여러 개 받아 단일 commit으로 묶는다 - 날짜별로
    overwrite_partition()을 반복 호출하면 파티션 수만큼 커밋이 쪼개져, 일부 날짜만 먼저
    반영된 중간 상태가 노출될 수 있다 (promote_rental_history_raw의 FINAL/PRELIMINARY
    다중 파티션 승격이 이 문제를 피해야 함).

    Args:
        table_identifier_or_table: 테이블 식별자 또는 Table 객체
        arrow_table: 적재할 PyArrow Table (여러 파티션 값의 행이 섞여 있어도 됨)
        partition_col: 파티션 컬럼명 (예: 'rent_date_partition')
        partition_vals: 덮어쓸 파티션 값 목록 (예: ['2026-08-20', '2026-08-21'])
        catalog: 선택적 PyIceberg Catalog 인스턴스
    """
    table = _resolve_table(table_identifier_or_table, catalog)
    overwrite_filter = build_partition_filter(partition_col, partition_vals)

    logger.info(
        "Iceberg 다중 파티션 덮어쓰기 시작: table=%s, filter=(%s in %s), row_count=%d",
        table.name(),
        partition_col,
        partition_vals,
        len(arrow_table),
    )
    table.overwrite(arrow_table, overwrite_filter=overwrite_filter)
    logger.info("Iceberg 다중 파티션 덮어쓰기 완료: table=%s", table.name())


def overwrite_all(
    table_identifier_or_table: Union[str, Table],
    arrow_table: pa.Table,
    catalog: Optional[Catalog] = None,
) -> None:
    """
    테이블 전체를 원자적으로 교체합니다 (파티션 단위가 아닌 전량 덮어쓰기).

    매번 상류 전체를 재처리하는 잡(silver.failure_report 등)을 위한 함수입니다.
    그런 잡에서 overwrite_partition()을 날짜마다 반복 호출하면 파티션 수만큼
    커밋이 쪼개져 느리고, 이번 입력에 더 이상 나타나지 않는 과거 파티션이 삭제되지
    않고 남는 문제도 있습니다. 전량 교체는 커밋 1회로 끝나고 그 잔여 파티션까지
    정리됩니다.

    증분 적재 잡에는 쓰면 안 됩니다 - 이번 구간 밖의 데이터까지 전부 지웁니다.

    Args:
        table_identifier_or_table: 'silver.failure_report' 식별자 또는 Table 객체
        arrow_table: 테이블 전체를 대체할 PyArrow Table
        catalog: 선택적 PyIceberg Catalog 인스턴스
    """
    table = _resolve_table(table_identifier_or_table, catalog)

    logger.info(
        "Iceberg 전량 덮어쓰기 시작: table=%s, row_count=%d",
        table.name(),
        len(arrow_table),
    )
    table.overwrite(arrow_table)
    logger.info("Iceberg 전량 덮어쓰기 완료: table=%s", table.name())


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