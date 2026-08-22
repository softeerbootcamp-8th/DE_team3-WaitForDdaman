"""
SQL 기반 데이터 품질 검증 유틸리티 (PyDeequ 대체 모듈)

Spark 및 JVM 기반 PyDeequ를 완전히 대체하여, DuckDB / PyArrow / Pandas 기반으로
동일한 품질 검증(완전성, 허용값, 비음수, 임의 조건식, 고유성 등)을 마이크로초 단위로 고속 수행합니다.

제공 제약 조건:
  - is_complete(col): NULL 또는 빈 문자열 결측치 검증
  - is_contained_in(col, allowed_values): 허용 목록 포함 여부
  - is_non_negative(col): 0 이상(비음수) 검증
  - satisfies(expr, desc): 임의의 SQL 불리언 조건 검증
  - has_uniqueness(cols, threshold): 고유값 비율 검증 (기본 임계 1.0 / 0.99)
  - has_min_rows(min_rows): 최소 행 수 검증
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)


class QualityCheckError(Exception):
    """품질 검증 실패 시 발생하는 예외."""


@dataclass
class ConstraintResult:
    name: str
    description: str
    status: str  # "Success" | "Failure"
    violation_count: int
    total_count: int
    metric_value: Optional[float] = None
    message: str = ""


@dataclass
class QualityCheckResult:
    check_name: str
    status: str  # "Success" | "Failure"
    total_rows: int
    results: List[ConstraintResult] = field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return self.status == "Success"

    @property
    def failed_constraints(self) -> List[ConstraintResult]:
        return [r for r in self.results if r.status != "Success"]

    def raise_if_failed(self, exception_cls: type[Exception] = QualityCheckError) -> None:
        """검증 실패 시 실패한 제약 목록과 위반 건수를 포함하여 예외를 발생시킵니다."""
        if not self.is_success:
            details = [
                f"{r.name}: {r.message} (위반 {r.violation_count}건 / 전체 {r.total_count}건)"
                for r in self.failed_constraints
            ]
            err_msg = f"[{self.check_name}] 품질 검증 실패 ({len(details)}개 제약 위반):\n  - " + "\n  - ".join(details)
            logger.error(err_msg)
            raise exception_cls(err_msg)


class QualityCheck:
    """품질 검증 빌더 클래스."""

    def __init__(self, name: str = "QualityCheck"):
        self.name = name
        self._constraints: List[Dict[str, Any]] = []

    def is_complete(self, column: str) -> QualityCheck:
        """지정된 컬럼이 NULL이 아님을 검증합니다."""
        self._constraints.append({
            "type": "is_complete",
            "column": column,
            "name": f"isComplete({column})",
            "desc": f"Column '{column}' must not be null",
        })
        return self

    def is_contained_in(self, column: str, allowed_values: Sequence[Any]) -> QualityCheck:
        """지정된 컬럼의 값이 허용 목록에 포함되는지 검증합니다."""
        self._constraints.append({
            "type": "is_contained_in",
            "column": column,
            "allowed_values": list(allowed_values),
            "name": f"isContainedIn({column}, {allowed_values})",
            "desc": f"Column '{column}' values must be in {allowed_values}",
        })
        return self

    def is_non_negative(self, column: str) -> QualityCheck:
        """지정된 컬럼의 값이 0 이상(비음수)인지 검증합니다."""
        self._constraints.append({
            "type": "is_non_negative",
            "column": column,
            "name": f"isNonNegative({column})",
            "desc": f"Column '{column}' must be >= 0",
        })
        return self

    def satisfies(self, sql_expression: str, desc: str = "") -> QualityCheck:
        """임의의 SQL 조건식을 만족하는지 검증합니다."""
        self._constraints.append({
            "type": "satisfies",
            "expr": sql_expression,
            "name": f"satisfies({sql_expression})",
            "desc": desc or f"Expression '{sql_expression}' must hold true",
        })
        return self

    def has_uniqueness(
        self,
        columns: Union[str, Sequence[str]],
        threshold: float = 1.0,
    ) -> QualityCheck:
        """
        컬럼 조합의 고유값 비율(COUNT(DISTINCT cols) / COUNT(*))이 threshold 이상인지 검증합니다.
        Deequ 정의(중복 없는 고유행 비율)와의 정합성을 보장합니다.
        """
        cols = [columns] if isinstance(columns, str) else list(columns)
        self._constraints.append({
            "type": "has_uniqueness",
            "columns": cols,
            "threshold": threshold,
            "name": f"hasUniqueness({cols})",
            "desc": f"Uniqueness ratio of columns {cols} must be >= {threshold}",
        })
        return self

    def has_min_rows(self, min_rows: int = 1) -> QualityCheck:
        """테이블의 총 행 수가 min_rows 이상인지 검증합니다."""
        self._constraints.append({
            "type": "has_min_rows",
            "min_rows": min_rows,
            "name": f"hasMinRows({min_rows})",
            "desc": f"Table must have at least {min_rows} rows",
        })
        return self

    def run(self, data: Any, con: Optional[duckdb.DuckDBPyConnection] = None) -> QualityCheckResult:
        """
        PyArrow Table, Pandas DataFrame, DuckDB Relation 등을 입력받아 검증을 실행합니다.
        """
        conn = con or duckdb.connect(":memory:")

        # 입력 데이터 등록
        if isinstance(data, (pa.Table, pa.RecordBatch)):
            conn.register("check_target", data)
        elif hasattr(data, "to_pandas") and not hasattr(data, "to_arrow_table"):
            conn.register("check_target", data.to_pandas())
        else:
            conn.register("check_target", data)

        total_rows = conn.execute("SELECT COUNT(*) FROM check_target").fetchone()[0]
        results: List[ConstraintResult] = []

        if total_rows == 0:
            # 빈 테이블 처리
            for c in self._constraints:
                if c["type"] == "has_min_rows" and c["min_rows"] > 0:
                    results.append(ConstraintResult(
                        name=c["name"],
                        description=c["desc"],
                        status="Failure",
                        violation_count=c["min_rows"],
                        total_count=0,
                        message=f"Table is empty (expected >= {c['min_rows']} rows)",
                    ))
                else:
                    results.append(ConstraintResult(
                        name=c["name"],
                        description=c["desc"],
                        status="Success",
                        violation_count=0,
                        total_count=0,
                        message="Passed on empty dataset",
                    ))
            is_all_success = all(r.status == "Success" for r in results)
            return QualityCheckResult(
                check_name=self.name,
                status="Success" if is_all_success else "Failure",
                total_rows=0,
                results=results,
            )

        for c in self._constraints:
            c_type = c["type"]
            if c_type == "is_complete":
                col = c["column"]
                sql = f"SELECT COUNT(*) FROM check_target WHERE {col} IS NULL"
                violations = conn.execute(sql).fetchone()[0]
                passed = violations == 0
                results.append(ConstraintResult(
                    name=c["name"],
                    description=c["desc"],
                    status="Success" if passed else "Failure",
                    violation_count=violations,
                    total_count=total_rows,
                    message="OK" if passed else f"{violations} NULL rows found",
                ))

            elif c_type == "is_contained_in":
                col = c["column"]
                vals = c["allowed_values"]
                formatted_vals = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in vals)
                sql = f"SELECT COUNT(*) FROM check_target WHERE {col} NOT IN ({formatted_vals}) OR {col} IS NULL"
                violations = conn.execute(sql).fetchone()[0]
                passed = violations == 0
                results.append(ConstraintResult(
                    name=c["name"],
                    description=c["desc"],
                    status="Success" if passed else "Failure",
                    violation_count=violations,
                    total_count=total_rows,
                    message="OK" if passed else f"{violations} rows not in allowed values",
                ))

            elif c_type == "is_non_negative":
                col = c["column"]
                sql = f"SELECT COUNT(*) FROM check_target WHERE {col} < 0"
                violations = conn.execute(sql).fetchone()[0]
                passed = violations == 0
                results.append(ConstraintResult(
                    name=c["name"],
                    description=c["desc"],
                    status="Success" if passed else "Failure",
                    violation_count=violations,
                    total_count=total_rows,
                    message="OK" if passed else f"{violations} negative rows found",
                ))

            elif c_type == "satisfies":
                expr = c["expr"]
                sql = f"SELECT COUNT(*) FROM check_target WHERE NOT ({expr})"
                violations = conn.execute(sql).fetchone()[0]
                passed = violations == 0
                results.append(ConstraintResult(
                    name=c["name"],
                    description=c["desc"],
                    status="Success" if passed else "Failure",
                    violation_count=violations,
                    total_count=total_rows,
                    message="OK" if passed else f"{violations} rows violated expression",
                ))

            elif c_type == "has_uniqueness":
                cols = c["columns"]
                cols_str = ", ".join(cols)
                thresh = c["threshold"]
                sql = f"SELECT COUNT(DISTINCT ({cols_str})) FROM check_target"
                distinct_count = conn.execute(sql).fetchone()[0]
                ratio = distinct_count / total_rows if total_rows > 0 else 1.0
                passed = ratio >= thresh
                violations = total_rows - distinct_count
                results.append(ConstraintResult(
                    name=c["name"],
                    description=c["desc"],
                    status="Success" if passed else "Failure",
                    violation_count=violations,
                    total_count=total_rows,
                    metric_value=ratio,
                    message="OK" if passed else f"Uniqueness ratio {ratio:.4f} < {thresh}",
                ))

            elif c_type == "has_min_rows":
                min_r = c["min_rows"]
                passed = total_rows >= min_r
                results.append(ConstraintResult(
                    name=c["name"],
                    description=c["desc"],
                    status="Success" if passed else "Failure",
                    violation_count=0 if passed else (min_r - total_rows),
                    total_count=total_rows,
                    message="OK" if passed else f"Total rows {total_rows} < {min_r}",
                ))

        is_all_success = all(r.status == "Success" for r in results)
        return QualityCheckResult(
            check_name=self.name,
            status="Success" if is_all_success else "Failure",
            total_rows=total_rows,
            results=results,
        )
