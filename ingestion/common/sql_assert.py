"""
SQL 기반 데이터 품질 검증 유틸리티 (PyDeequ 대체 모듈)

Spark 및 JVM 기반 PyDeequ를 완전히 대체하여, DuckDB / PyArrow / Pandas 기반으로
동일한 품질 검증(완전성, 허용값, 비음수, 임의 조건식, 고유성 등)을 마이크로초 단위로 고속 수행합니다.

제공 제약 조건 (Deequ의 null 처리 의미를 그대로 따른다 - #146 Gold 잡 7개가
이 어서션의 판정을 옛 PyDeequ와 병행 비교해 확인했다):
  - is_complete(col): NULL 결측치 검증
  - is_contained_in(col, allowed_values): 허용 목록 포함 여부 (null은 통과 -
    Deequ 원문: "asserts that every *non-null* value ... is contained")
  - is_non_negative(col): 0 이상(비음수) 검증 (null은 통과)
  - satisfies(expr, desc): 임의의 SQL 불리언 조건 검증 (조건이 NULL로 평가되는
    행은 위반으로 센다 - Deequ가 내부적으로 CASE WHEN <조건> THEN 1 ELSE 0 END로
    컴파일하는 것과 동일)
  - has_uniqueness(cols, threshold): 고유값 비율 검증 (기본 임계 1.0 / 0.99) -
    "컬럼 조합이 정확히 1번만 등장하는 행의 비율"(Deequ 원래 정의)이지
    COUNT(DISTINCT)/COUNT(*)(distinctness)가 아니다. 중복이 2개 이상인 키가
    늘어날수록 두 정의가 갈린다(#140/#146) - PyDeequ 결과와의 회귀 비교를
    위해 distinctness가 아니라 Deequ 원래 정의를 그대로 재현한다.
  - has_min_rows(min_rows): 최소 행 수 검증
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

import duckdb

from common.duckdb_io import connect
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
        """지정된 컬럼의 non-null 값이 허용 목록에 포함되는지 검증합니다 (null은 통과)."""
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
        """임의의 SQL 조건식을 만족하는지 검증합니다 (조건이 NULL로 평가되는
        행도 위반으로 처리 - Deequ와 동일)."""
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
        컬럼 조합이 "정확히 1번만 등장하는 행"의 비율이 threshold 이상인지 검증합니다.
        (Deequ hasUniqueness 원래 정의 - COUNT(DISTINCT)/COUNT(*)인 distinctness와는
        다르다. 모듈 docstring 참고.)
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
        conn = con or connect()

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
                # null은 통과(Deequ isContainedIn 의미) - non-null인데 목록에 없는 값만 위반
                sql = f"SELECT COUNT(*) FROM check_target WHERE {col} IS NOT NULL AND {col} NOT IN ({formatted_vals})"
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
                # 조건이 NULL로 평가되는 행(예: 컬럼 자체가 null)도 위반으로 센다 -
                # Deequ가 CASE WHEN <조건> THEN 1 ELSE 0 END로 컴파일하는 것과 동일한
                # 의미. 단순히 WHERE NOT (expr)만 쓰면 NULL은 WHERE에서 자동 제외돼
                # 위반이 아닌 것처럼 빠지므로 COALESCE로 명시적으로 위반 처리한다.
                sql = f"SELECT COUNT(*) FROM check_target WHERE NOT (COALESCE(({expr}), FALSE))"
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
                # Deequ 원래 정의: 컬럼 조합 값이 "정확히 1번만" 등장하는 행의 비율.
                # COUNT(DISTINCT)/COUNT(*)(distinctness)와 달리 중복 그룹의 행은
                # 분자에서 전부 빠진다 (모듈 docstring 참고).
                sql = f"""
                    WITH grp AS (
                        SELECT COUNT(*) AS cnt FROM check_target GROUP BY {cols_str}
                    )
                    SELECT COALESCE(SUM(CASE WHEN cnt = 1 THEN cnt ELSE 0 END), 0) FROM grp
                """
                unique_row_count = conn.execute(sql).fetchone()[0]
                ratio = unique_row_count / total_rows if total_rows > 0 else 1.0
                passed = ratio >= thresh
                violations = total_rows - unique_row_count
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
