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


def test_bootstrap_dag_registers_existing_tables_before_creating_new_ones(dag_bag):
    dag = dag_bag.dags[DAG_ID]

    register_task = dag.get_task("register_existing_hadoop_tables")
    bootstrap_task = dag.get_task("bootstrap_new_tables")

    assert register_task.downstream_task_ids == {"bootstrap_new_tables"}
    assert "jobs.register_tables_in_jdbc_catalog" in register_task.bash_command
    assert "jobs.bootstrap_iceberg_tables" in bootstrap_task.bash_command


def test_bootstrap_new_tables_runs_even_if_registration_step_is_skipped_or_fails(dag_bag):
    """신규 환경(등록할 Hadoop metadata가 없는 환경)에서도 새 테이블 생성이 막히면 안 된다."""
    dag = dag_bag.dags[DAG_ID]
    bootstrap_task = dag.get_task("bootstrap_new_tables")

    assert bootstrap_task.trigger_rule == "all_done"
