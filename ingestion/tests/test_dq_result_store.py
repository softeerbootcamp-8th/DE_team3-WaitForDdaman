"""
common/dq_result_store.py 단위 테스트 (#217)
"""
from unittest.mock import MagicMock

from pyiceberg.exceptions import NoSuchTableError

from common.dq_assertions import CheckResult
from common.dq_result_store import append_results, ensure_result_table, results_to_arrow


def _result(**overrides) -> CheckResult:
    base = dict(
        check_name="sex_cd_null_rate",
        target_column="sex_cd",
        metric_value=0.24,
        threshold=None,
        pass_fail="MONITOR",
        severity="monitor",
        description="결측률",
    )
    base.update(overrides)
    return CheckResult(**base)


def test_results_to_arrow_shapes_rows():
    arrow_table = results_to_arrow(
        [_result(), _result(check_name="birth_year_implausible_rate", threshold=0.05, pass_fail="PASS")],
        run_id="run-1",
        dag_id="dq_rental_history",
        task_id="log_dq_check_result",
        source_name="rental_history",
        execution_date="2026-08-24",
    )

    assert arrow_table.num_rows == 2
    rows = arrow_table.to_pylist()
    assert rows[0]["source_name"] == "rental_history"
    assert rows[0]["execution_date"] == "2026-08-24"
    assert rows[0]["check_name"] == "sex_cd_null_rate"
    assert rows[0]["threshold"] is None
    assert rows[1]["threshold"] == 0.05
    assert rows[0]["executed_at"] is not None


def test_ensure_result_table_creates_when_missing():
    mock_catalog = MagicMock()
    mock_catalog.load_table.side_effect = NoSuchTableError

    ensure_result_table(mock_catalog)

    mock_catalog.create_namespace_if_not_exists.assert_called_once_with("dq")
    mock_catalog.create_table.assert_called_once()
    args, kwargs = mock_catalog.create_table.call_args
    assert args[0] == "dq.check_result_history"


def test_ensure_result_table_reuses_when_exists():
    mock_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    result = ensure_result_table(mock_catalog)

    assert result is mock_table
    mock_catalog.create_table.assert_not_called()


def test_append_results_calls_iceberg_append():
    mock_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    arrow_table = results_to_arrow(
        [_result()],
        run_id="run-1",
        dag_id="dq_rental_history",
        task_id="log_dq_check_result",
        source_name="rental_history",
        execution_date="2026-08-24",
    )

    append_results(mock_catalog, arrow_table)

    mock_table.append.assert_called_once_with(arrow_table)
