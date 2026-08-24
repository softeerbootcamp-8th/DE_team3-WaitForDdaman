"""
YAML로 선언한 DQ 어써션을 DuckDB로 실행하는 공통 모듈 (#217).

common/sql_assert.py(QualityCheck)는 "위반 1건=배치 실패"를 위한 하드 게이트라 목적이
다르다 - 여기서는 지표값(비율)을 그대로 계산해서 남기고, threshold가 있으면 PASS/FAIL,
없으면(이미 알려진 이슈를 추이만 관찰) MONITOR로 분류한다. 판정 자체가 목적이 아니라
"수치를 히스토리에 남겨서 해석 에이전트가 추세를 보게" 하는 게 목적이라 별도 모듈로 뺐다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

import duckdb
import yaml

logger = logging.getLogger(__name__)

_COMPARISONS = {
    "lte": lambda value, threshold: value <= threshold,
    "gte": lambda value, threshold: value >= threshold,
    "eq": lambda value, threshold: value == threshold,
}


@dataclass(frozen=True)
class CheckDefinition:
    check_name: str
    target_column: str
    severity: str
    comparison: Optional[str]
    threshold: Optional[float]
    metric_sql: str
    description: str = ""


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    target_column: str
    metric_value: Optional[float]
    threshold: Optional[float]
    pass_fail: str  # "PASS" | "FAIL" | "MONITOR" | "ERROR"(스키마 불일치 등으로 계산 자체가 실패)
    severity: str
    description: str = ""
    error: Optional[str] = None


def load_config(path: Union[str, Path]) -> tuple[str, str, list[CheckDefinition]]:
    """어써션 YAML을 읽어 (source_name, target_table, 체크 목록)을 반환한다."""
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    checks = [
        CheckDefinition(
            check_name=c["check_name"],
            target_column=c["target_column"],
            severity=c["severity"],
            comparison=c.get("comparison"),
            threshold=c.get("threshold"),
            metric_sql=c["metric_sql"],
            description=c.get("description", ""),
        )
        for c in raw["checks"]
    ]
    return raw["source_name"], raw["target_table"], checks


def _judge(value: float, comparison: Optional[str], threshold: Optional[float]) -> str:
    if threshold is None or not comparison:
        return "MONITOR"
    compare_fn = _COMPARISONS.get(comparison)
    if compare_fn is None:
        raise ValueError(f"알 수 없는 comparison: {comparison!r}")
    return "PASS" if compare_fn(value, threshold) else "FAIL"


def run_checks(
    checks: list[CheckDefinition],
    data: Any,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> list[CheckResult]:
    """각 체크의 metric_sql을 등록된 데이터에 대해 실행하고 판정 결과를 반환한다.

    metric_sql은 반드시 단일 숫자(비율 등)를 돌려주는 SELECT여야 하고, 대상 테이블은
    `{table}` 자리표시자로 참조한다.
    """
    conn = con or duckdb.connect(":memory:")
    conn.register("check_target", data)

    total_rows = conn.execute("SELECT COUNT(*) FROM check_target").fetchone()[0]
    available_columns = {row[0] for row in conn.execute("DESCRIBE check_target").fetchall()}

    results: list[CheckResult] = []
    for check in checks:
        # target_column이 아예 없으면 SQL을 실행해보지도 않고 바로 스키마 불일치로 처리한다 -
        # 실제 원인(컬럼 삭제/이름 변경)을 metric_sql의 다른 표현식이 던지는 DuckDB 원본
        # 에러 메시지보다 먼저, 더 명확하게 알려줄 수 있다.
        if check.target_column and check.target_column not in available_columns:
            error_msg = (
                f"target_column '{check.target_column}'이 테이블에 없음 "
                f"(존재하는 컬럼: {sorted(available_columns)}) - 스키마 변경 의심"
            )
            logger.error("[%s] %s: %s", check.severity, check.check_name, error_msg)
            results.append(CheckResult(
                check_name=check.check_name, target_column=check.target_column, metric_value=None,
                threshold=check.threshold, pass_fail="ERROR", severity=check.severity,
                description=check.description, error=error_msg,
            ))
            continue

        if total_rows == 0:
            # 빈 배치 - 지표를 계산할 대상이 없으므로 0으로 기록하고 MONITOR로 남긴다.
            # (has_min_rows류의 "배치 자체가 비어있다"는 판단은 sql_assert.py의 하드 게이트가 이미 담당)
            value = 0.0
        else:
            sql = check.metric_sql.format(table="check_target")
            try:
                fetched = conn.execute(sql).fetchone()[0]
            except duckdb.Error as exc:
                # target_column 외에 metric_sql이 참조하는 다른 컬럼(예: 조인 조건에 쓰인
                # 컬럼)이 없어졌을 때도 여기서 잡힌다 - 이 체크 하나만 ERROR로 남기고
                # 나머지 체크는 계속 실행한다(한 컬럼 사라졌다고 전체 배치가 죽으면 안 됨).
                error_msg = f"metric_sql 실행 실패 - 스키마 불일치 의심: {exc}"
                logger.error("[%s] %s: %s", check.severity, check.check_name, error_msg)
                results.append(CheckResult(
                    check_name=check.check_name, target_column=check.target_column, metric_value=None,
                    threshold=check.threshold, pass_fail="ERROR", severity=check.severity,
                    description=check.description, error=error_msg,
                ))
                continue
            value = float(fetched) if fetched is not None else 0.0

        pass_fail = _judge(value, check.comparison, check.threshold)
        results.append(
            CheckResult(
                check_name=check.check_name,
                target_column=check.target_column,
                metric_value=value,
                threshold=check.threshold,
                pass_fail=pass_fail,
                severity=check.severity,
                description=check.description,
            )
        )
        logger.info(
            "[%s] %s = %.6f (threshold=%s) -> %s",
            check.severity, check.check_name, value, check.threshold, pass_fail,
        )
    return results
