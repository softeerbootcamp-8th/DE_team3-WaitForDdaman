"""통합 Raw 예비 수집 DAG (raw_preliminary_all_sources) 회귀 테스트."""

import ast
import sys
from pathlib import Path

import pytest


def _assert_imported(dag_bag, *file_names) -> None:
    mine = {
        path: err
        for path, err in dag_bag.import_errors.items()
        if Path(path).name in file_names
    }
    assert mine == {}, mine


def _dag_folder() -> Path:
    repository_path = Path(__file__).resolve().parents[2] / "airflow" / "dags"
    if repository_path.exists():
        return repository_path
    return Path("/opt/airflow/dags")


def _dag_file() -> Path:
    return _dag_folder() / "raw_preliminary_all_sources_dag.py"


@pytest.fixture(scope="module")
def preliminary_dag():
    from airflow.dag_processing.dagbag import DagBag

    folder = str(_dag_folder())
    if folder not in sys.path:
        sys.path.insert(0, folder)
    dag_bag = DagBag(folder)
    _assert_imported(dag_bag, "raw_preliminary_all_sources_dag.py")
    return dag_bag.dags["raw_preliminary_all_sources"]


def test_raw_preliminary_all_sources_dag_structure(preliminary_dag):
    assert preliminary_dag.dag_id == "raw_preliminary_all_sources"
    task_ids = set(preliminary_dag.task_dict.keys())
    assert "collect_rental_history_preliminary_raw" in task_ids
    assert "collect_failure_report_preliminary_raw" in task_ids

    task_rental = preliminary_dag.get_task("collect_rental_history_preliminary_raw")
    task_failure = preliminary_dag.get_task("collect_failure_report_preliminary_raw")

    assert task_rental.outlets == []
    assert task_failure.outlets == []
    assert "collect_rental_history_raw" in task_rental.bash_command
    assert "collect_failure_report_raw" in task_failure.bash_command
    assert task_rental.env["SNAPSHOT_TYPE"] == "PRELIMINARY"
    assert task_failure.env["SNAPSHOT_TYPE"] == "PRELIMINARY"
