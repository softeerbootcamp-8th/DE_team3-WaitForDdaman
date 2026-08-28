"""대여이력 빈 날짜 수동 승인 DAG 구조 테스트."""

import sys
from pathlib import Path

import pytest


DAG_ID = "confirm_rental_history_empty"
DAG_FILE = "confirm_rental_history_empty_dag.py"


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


def test_empty_confirmation_dag_is_registered(dag_bag):
    assert DAG_ID in dag_bag.dags


def test_empty_confirmation_dag_is_manual_only(dag_bag):
    dag = dag_bag.dags[DAG_ID]
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_empty_confirmation_dag_passes_auditable_inputs(dag_bag):
    dag = dag_bag.dags[DAG_ID]
    task = dag.get_task("confirm_empty_date")

    assert task.op_kwargs == {
        "target_date_str": "{{ params.target_date }}",
        "confirmed_by": "{{ params.confirmed_by }}",
        "reason": "{{ params.reason }}",
    }
