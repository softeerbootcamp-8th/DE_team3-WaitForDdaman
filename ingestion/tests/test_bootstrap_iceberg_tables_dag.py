"""bootstrap_iceberg_tables 수동 DAG 구조 테스트 (Issue #216)."""

import sys
from pathlib import Path

import pytest

DAG_ID = "bootstrap_iceberg_tables"
DAG_FILE = "bootstrap_iceberg_tables_dag.py"


def _dag_folder() -> str:
    repository_path = Path(__file__).resolve().parents[2] / "airflow" / "dags"
    if repository_path.exists():
        return str(repository_path)
    return "/opt/airflow/dags"


@pytest.fixture(scope="module")
def dag_bag():
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
    return dag_bag


def test_bootstrap_dag_is_registered(dag_bag):
    assert DAG_ID in dag_bag.dags


def test_bootstrap_dag_is_manual_only(dag_bag):
    dag = dag_bag.dags[DAG_ID]
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_bootstrap_dag_only_creates_new_tables(dag_bag):
    dag = dag_bag.dags[DAG_ID]

    assert set(dag.task_ids) == {"create_bronze_tables"}
    bootstrap_task = dag.get_task("create_bronze_tables")

    assert "jobs.bootstrap_iceberg_tables" in bootstrap_task.bash_command
    assert bootstrap_task.trigger_rule == "all_success"
