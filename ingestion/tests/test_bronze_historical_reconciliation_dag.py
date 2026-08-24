"""Bronze 자동 Reconciliation DAG 구조 테스트 (#195)."""

import sys
from pathlib import Path

import pytest

DAG_ID = "bronze_historical_reconciliation"
DAG_FILE = "bronze_historical_reconciliation_dag.py"


def _dag_folder() -> str:
    repository_path = Path(__file__).resolve().parents[2] / "airflow" / "dags"
    if (repository_path / DAG_FILE).exists():
        return str(repository_path)
    return "/opt/airflow/dags"


@pytest.fixture(scope="module")
def dag():
    from airflow.dag_processing.dagbag import DagBag

    folder = _dag_folder()
    if folder not in sys.path:
        sys.path.insert(0, folder)
    dag_bag = DagBag(folder)
    mine = {
        path: error
        for path, error in dag_bag.import_errors.items()
        if Path(path).name == DAG_FILE
    }
    assert mine == {}, mine
    assert DAG_ID in dag_bag.dags
    return dag_bag.dags[DAG_ID]


def test_reconciliation_runs_at_0030_without_airflow_catchup(dag):
    assert str(dag.schedule) == "30 0 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.max_active_tasks == 5


def test_reconciliation_has_separate_mapped_flows_for_both_sources(dag):
    assert {
        "check_rental_history_gap",
        "check_failure_report_gap",
        "prepare_rental_history_date",
        "promote_rental_history_date",
        "catchup_failure_report_date",
        "advance_rental_history_watermark",
        "advance_failure_report_watermark",
    }.issubset(dag.task_ids)

    prepare = dag.get_task("prepare_rental_history_date")
    promote = dag.get_task("promote_rental_history_date")
    failure = dag.get_task("catchup_failure_report_date")
    assert prepare.pool == "seoul_api"
    assert failure.pool == "seoul_api"
    assert prepare.max_active_tis_per_dag == 3
    assert failure.max_active_tis_per_dag == 1
    assert dag.get_task("advance_rental_history_watermark").trigger_rule == "all_done"
    assert dag.get_task("advance_failure_report_watermark").trigger_rule == "all_done"
    assert "assign_rental_history_api_keys" in dag.get_task(
        "advance_rental_history_watermark"
    ).upstream_task_ids
    assert "assign_failure_report_api_key" in dag.get_task(
        "advance_failure_report_watermark"
    ).upstream_task_ids


def test_promote_rental_history_date_serializes_bronze_commit(dag):
    """prepare는 API 키별로 병렬 실행되지만 promote는 전용 풀에서 slot 1개로 직렬화되어야 한다."""
    prepare = dag.get_task("prepare_rental_history_date")
    promote = dag.get_task("promote_rental_history_date")

    assert promote.pool == "bronze_rental_history_commit"
    assert promote.pool != prepare.pool
    assert promote.max_active_tis_per_dag == 1
    assert promote.trigger_rule == "all_done"
    assert "prepare_rental_history_date" in promote.upstream_task_ids
    assert "promote_rental_history_date" in dag.get_task(
        "advance_rental_history_watermark"
    ).upstream_task_ids
