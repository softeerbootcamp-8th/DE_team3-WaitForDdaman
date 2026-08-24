"""대여이력 날짜 단위 Backfill DAG 구조 테스트 (#195)."""

import sys
from pathlib import Path

import pytest

DAG_ID = "bronze_rental_history_backfill"
DAG_FILE = "bronze_rental_history_backfill_dag.py"


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


def test_backfill_dag_requires_explicit_backfill(dag):
    assert str(dag.schedule) == "@daily"
    assert dag.catchup is False
    assert dag.max_active_runs == 2
    assert dag.max_active_tasks == 2


def test_backfill_dag_is_one_date_collect_select_promote_marker_chain(dag):
    assert set(dag.task_ids) == {
        "collect_final_raw_for_date",
        "select_final_raw_for_date",
        "promote_date_to_bronze",
        "write_completion_marker",
    }
    collect = dag.get_task("collect_final_raw_for_date")
    select = dag.get_task("select_final_raw_for_date")
    promote = dag.get_task("promote_date_to_bronze")
    marker = dag.get_task("write_completion_marker")

    assert "jobs.collect_rental_history_raw" in collect.bash_command
    assert "jobs.select_rental_history_snapshot" in select.bash_command
    assert "jobs.promote_rental_history_raw" in promote.bash_command
    assert "jobs.write_rental_history_completion_marker" in marker.bash_command
    assert collect.env["BACKFILL_TARGET_DATE"] == "{{ ds }}"
    assert collect.env["COLLECTION_CUTOFF_AT"] == "{{ ds }}T23:59:59+09:00"
    assert select.trigger_rule == "all_done"
    assert marker.trigger_rule == "all_done"
    assert collect.task_id in select.upstream_task_ids
    assert select.task_id in promote.upstream_task_ids
    assert promote.task_id in marker.upstream_task_ids
