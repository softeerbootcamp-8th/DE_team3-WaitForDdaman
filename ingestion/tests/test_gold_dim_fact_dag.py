"""gold_dim_fact의 bike_location T0 기준일 전달 회귀 테스트 (#138)."""

import sys
from pathlib import Path

import pytest


DAG_ID = "gold_dim_fact"
DAG_FILE = "gold_dim_fact_dag.py"


@pytest.fixture(scope="module")
def dag():
    from airflow.dag_processing.dagbag import DagBag

    folder = Path(__file__).resolve().parents[2] / "airflow" / "dags"
    if str(folder) not in sys.path:
        sys.path.insert(0, str(folder))
    dag_bag = DagBag(str(folder))
    mine = {path: error for path, error in dag_bag.import_errors.items() if Path(path).name == DAG_FILE}
    assert mine == {}, mine
    return dag_bag.dags[DAG_ID]


def test_bike_location_uses_kst_data_interval_end_for_t0_snapshot(dag):
    """08:00 Gold run의 ds(전날)가 아니라 당일 T0 파티션 날짜를 전달한다."""
    task = dag.get_task("build_bike_location")

    assert "data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')" in task.bash_command
    assert task.env["RENTAL_HISTORY_T0_ENABLED"] == "{{ var.value.get('RENTAL_HISTORY_T0_ENABLED', 'false') }}"
