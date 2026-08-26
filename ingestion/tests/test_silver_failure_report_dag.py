"""silver_failure_report DAG 구조 테스트 (#288)."""

import sys
from pathlib import Path

import pytest

DAG_ID = "silver_failure_report"
DAG_FILE = "silver_failure_report_dag.py"


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


def test_triggered_by_bronze_asset_not_a_fixed_schedule(dag):
    assert [asset.name for asset in dag.timetable.asset_condition.objects] == [
        "failure_report_bronze"
    ]


def test_single_active_run_serializes_range_replacement(dag):
    """확정 구간 증분(#288)은 같은 구간에 두 실행이 동시에 replace_range를 시도하면 안 된다."""
    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_task_receives_max_days_per_run_from_params(dag):
    """오래 밀린 워터마크를 수동 실행에서 한 번에 소화할 수 있어야 한다."""
    task = dag.get_task("silver_failure_report")

    assert "MAX_DAYS_PER_RUN='{{ params.max_days_per_run }}'" in task.bash_command
    # 빈 값이 기본 - 잡이 DEFAULT_MAX_DAYS_PER_RUN으로 폴백한다.
    assert str(dag.params["max_days_per_run"]) == ""


def test_task_uses_silver_pool(dag):
    assert dag.get_task("silver_failure_report").pool == "silver_process"
