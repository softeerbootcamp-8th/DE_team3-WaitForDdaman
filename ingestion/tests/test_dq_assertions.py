"""
common/dq_assertions.py 단위 테스트 (#217)
"""
import textwrap

import pyarrow as pa
import pytest

from common.dq_assertions import CheckDefinition, load_config, run_checks


def _check(**overrides) -> CheckDefinition:
    base = dict(
        check_name="sample_rate",
        target_column="sex_cd",
        severity="warn",
        comparison="lte",
        threshold=0.5,
        metric_sql="SELECT CAST(COUNT(*) FILTER (WHERE {table}.sex_cd IS NULL) AS DOUBLE) / COUNT(*) FROM {table}",
    )
    base.update(overrides)
    return CheckDefinition(**base)


def test_run_checks_pass_when_metric_within_threshold():
    table = pa.table({"sex_cd": ["M", "F", "M", None]})
    results = run_checks([_check(threshold=0.5)], table)

    assert len(results) == 1
    assert results[0].metric_value == pytest.approx(0.25)
    assert results[0].pass_fail == "PASS"


def test_run_checks_fail_when_metric_exceeds_threshold():
    table = pa.table({"sex_cd": ["M", None, None, None]})
    results = run_checks([_check(threshold=0.5)], table)

    assert results[0].metric_value == pytest.approx(0.75)
    assert results[0].pass_fail == "FAIL"


def test_run_checks_monitor_when_no_threshold():
    """threshold가 없으면(이미 알려진 이슈를 추이만 기록) 값과 무관하게 MONITOR."""
    table = pa.table({"sex_cd": [None, None, None, None]})
    results = run_checks([_check(comparison=None, threshold=None)], table)

    assert results[0].metric_value == pytest.approx(1.0)
    assert results[0].pass_fail == "MONITOR"


def test_run_checks_empty_table_reports_zero_and_monitor():
    table = pa.table({"sex_cd": pa.array([], type=pa.string())})
    results = run_checks([_check(threshold=0.5)], table)

    assert results[0].metric_value == 0.0
    assert results[0].pass_fail == "PASS"


def test_gte_and_eq_comparisons():
    table = pa.table({"sex_cd": ["M", "M", "M", "M"]})  # 결측률 0.0
    gte_result = run_checks([_check(comparison="gte", threshold=0.0)], table)[0]
    eq_result = run_checks([_check(comparison="eq", threshold=0.0)], table)[0]

    assert gte_result.pass_fail == "PASS"
    assert eq_result.pass_fail == "PASS"


def test_unknown_comparison_raises():
    table = pa.table({"sex_cd": ["M"]})
    with pytest.raises(ValueError):
        run_checks([_check(comparison="not_a_real_comparison", threshold=0.1)], table)


def test_run_checks_reports_error_when_target_column_missing():
    """target_column 자체가 테이블에 없으면(스키마 변경) SQL을 실행해보지도 않고
    ERROR로 즉시 판정해야 한다 - DuckDB의 원본 에러보다 명확한 메시지를 남기기 위해."""
    table = pa.table({"other_column": ["M", "F"]})
    results = run_checks([_check(target_column="sex_cd")], table)

    assert results[0].pass_fail == "ERROR"
    assert results[0].metric_value is None
    assert "sex_cd" in results[0].error


def test_run_checks_reports_error_when_metric_sql_references_missing_column():
    """target_column은 있지만 metric_sql이 참조하는 다른 컬럼이 없어졌을 때도
    ERROR로 격리하고, 그 체크만 실패시켜야 한다(전체 배치를 죽이면 안 됨)."""
    table = pa.table({"sex_cd": ["M", "F"]})
    bad_check = _check(
        target_column="sex_cd",
        metric_sql="SELECT CAST(COUNT(*) FILTER (WHERE nonexistent_col IS NULL) AS DOUBLE) / COUNT(*) FROM {table}",
    )
    good_check = _check(check_name="other_check")

    results = run_checks([bad_check, good_check], table)

    assert results[0].pass_fail == "ERROR"
    assert results[0].metric_value is None
    assert "스키마 불일치" in results[0].error
    # 옆의 체크는 영향받지 않고 정상 실행되어야 한다
    assert results[1].pass_fail in ("PASS", "FAIL", "MONITOR")


def test_load_config_parses_yaml(tmp_path):
    config_path = tmp_path / "rental_history.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            source_name: rental_history
            target_table: silver.rental_history
            checks:
              - check_name: sex_cd_null_rate
                target_column: sex_cd
                severity: monitor
                comparison: null
                threshold: null
                description: "결측률"
                metric_sql: |
                  SELECT CAST(COUNT(*) FILTER (WHERE sex_cd IS NULL) AS DOUBLE) / COUNT(*)
                  FROM {table}
              - check_name: birth_year_implausible_rate
                target_column: birth_year
                severity: warn
                comparison: lte
                threshold: 0.05
                metric_sql: "SELECT 0.0 FROM {table}"
            """
        ),
        encoding="utf-8",
    )

    source_name, target_table, checks = load_config(config_path)

    assert source_name == "rental_history"
    assert target_table == "silver.rental_history"
    assert len(checks) == 2
    assert checks[0].check_name == "sex_cd_null_rate"
    assert checks[0].severity == "monitor"
    assert checks[0].threshold is None
    assert checks[1].threshold == 0.05
