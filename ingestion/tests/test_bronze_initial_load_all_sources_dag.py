"""bronze_initial_load_all_sources DAG 구조 테스트 (#232 - dynamic task mapping 재구성)."""

import sys
from pathlib import Path

import pytest

DAG_ID = "bronze_initial_load_all_sources"
DAG_FILE = "bronze_initial_load_all_sources_dag.py"


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


@pytest.fixture(scope="module")
def dag(dag_bag):
    return dag_bag.dags[DAG_ID]


def test_dag_is_registered(dag_bag):
    assert DAG_ID in dag_bag.dags


def test_backfill_range_tasks_exist(dag):
    task_ids = set(dag.task_ids)
    assert "compute_rental_history_backfill_ranges" in task_ids
    assert "parse_rental_history_backfill_ranges" in task_ids
    assert "load_silver_rental_history_chunk" in task_ids
    assert "max_rental_history_backfill_range_end" in task_ids
    assert "finalize_rental_history_backfill_watermark" in task_ids
    # 기존 bash for-loop 단일 태스크는 더 이상 존재하지 않는다
    assert "load_silver_rental_history" not in task_ids


def test_load_silver_rental_history_chunk_is_mapped(dag):
    chunk_task = dag.get_task("load_silver_rental_history_chunk")
    assert chunk_task.is_mapped


def test_finalize_watermark_depends_on_chunk_via_max_end(dag):
    finalize = dag.get_task("finalize_rental_history_backfill_watermark")
    max_end = dag.get_task("max_rental_history_backfill_range_end")
    chunk = dag.get_task("load_silver_rental_history_chunk")
    assert max_end.task_id in finalize.upstream_task_ids
    assert chunk.task_id in max_end.upstream_task_ids
