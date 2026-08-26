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
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Or,
)
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


def build_range_filter(column: str, start_value: str, end_value: str) -> BooleanExpression:
    """
    `column >= start_value AND column <= end_value` 범위식을 만듭니다.

    연속된 날짜 구간에는 build_partition_filter()의 OR(EqualTo, ...) 나열보다 이 범위식이
    맞다 - 값 목록이 아니라 "선언한 구간 전체"를 뜻하므로, 이번 입력에 데이터가 하나도
    없는 날짜까지 필터에 포함된다(그게 범위 교체의 핵심이다).
    """
    if start_value > end_value:
        raise ValueError(f"start_value가 end_value보다 큼: {start_value!r} > {end_value!r}")
    return And(GreaterThanOrEqual(column, start_value), LessThanOrEqual(column, end_value))


def _assert_rows_within_range(
    arrow_table: pa.Table, column: str, start_value: str, end_value: str
) -> None:
    """입력 행이 전부 선언 범위 안에 있는지 확인한다 (범위 밖 행은 조용히 통과시키면 안 된다).

    overwrite()는 overwrite_filter가 지우는 범위와 무관하게 입력 행을 그대로 쓴다 -
    범위 밖 행이 섞여 있으면 "이 구간을 이번 결과로 완전히 교체했다"는 marker의 의미가
    깨진 채로 커밋이 성공해버린다. 그래서 쓰기 전에 막는다.
    """
    if column not in arrow_table.column_names:
        raise ValueError(f"입력 Arrow 테이블에 {column!r} 컬럼이 없음: {arrow_table.column_names}")

    values = arrow_table.column(column)
    if values.null_count:
        raise ValueError(
            f"{column!r}이 null인 행 {values.null_count}개 - 어느 범위에 속하는지 판단할 수 없음"
        )

    bounds = pc.min_max(values).as_py()
    if bounds["min"] < start_value or bounds["max"] > end_value:
        raise ValueError(
            f"선언 범위 밖의 행이 포함됨: {column} 실제 범위=[{bounds['min']}, {bounds['max']}], "
            f"선언 범위=[{start_value}, {end_value}]"
        )


def replace_range(
    table_identifier_or_table: Union[str, Table],
    rows: pa.Table,
    column: str,
    start_value: str,
    end_value: str,
    catalog: Optional[Catalog] = None,
) -> None:
    """
    `column`이 [start_value, end_value] 구간인 기존 행을 전부 지우고 `rows`로 교체합니다
    (Iceberg snapshot 1개).

    overwrite_partitions()와의 차이는 "무엇을 지우는가"다. overwrite_partitions()는 이번
    입력에 실제로 존재하는 파티션 값만 지운다 - 선언 구간에 포함됐지만 이번 결과가 0행인
    날짜는 손대지 않으므로 그 날짜의 과거 행이 그대로 남는다. replace_range()는 입력이
    아니라 호출자가 선언한 구간을 지운다 - 그래서 "이 구간은 이번 입력 결과로 완전히
    교체됐다"는 완료 marker의 의미를 성립시킬 수 있다.

    rows가 0행이면 overwrite() 대신 delete()를 쓴다. 빈 Arrow 테이블을 overwrite()에
    넘기는 방식도 PyIceberg 0.11.1에서 동작하지만, delete()는 입력 스키마를 요구하지
    않아 0행 처리 의도가 코드에 그대로 드러나고 스키마 불일치 가능성도 없다.

    Args:
        table_identifier_or_table: 'silver.rental_history' 식별자 또는 Table 객체
        rows: 이 구간을 대체할 PyArrow Table (0행 가능)
        column: 범위 비교 대상 컬럼명 (예: 'rent_date_partition')
        start_value: 구간 시작값(포함)
        end_value: 구간 끝값(포함)
        catalog: 선택적 PyIceberg Catalog 인스턴스
    """
    range_filter = build_range_filter(column, start_value, end_value)
    table = _resolve_table(table_identifier_or_table, catalog)

    if len(rows) == 0:
        logger.info(
            "Iceberg 범위 삭제 시작(입력 0행): table=%s, filter=(%s <= %s <= %s)",
            table.name(), start_value, column, end_value,
        )
        table.delete(delete_filter=range_filter)
        logger.info("Iceberg 범위 삭제 완료: table=%s", table.name())
        return

    _assert_rows_within_range(rows, column, start_value, end_value)

    logger.info(
        "Iceberg 범위 교체 시작: table=%s, filter=(%s <= %s <= %s), row_count=%d",
        table.name(), start_value, column, end_value, len(rows),
    )
    table.overwrite(rows, overwrite_filter=range_filter)
    logger.info("Iceberg 범위 교체 완료: table=%s", table.name())


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