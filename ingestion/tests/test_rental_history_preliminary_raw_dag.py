"""예비 Raw 수집 DAG 회귀 테스트.

이 DAG는 API 원본만 저장하는 단계라 Spark/Bronze/워터마크/Asset을 절대 건드리면 안 된다.
수집 기준시각 규칙은 일 배치 DAG와 공유해야 selection이 어긋나지 않으므로
dag_common의 공통 템플릿을 실제로 쓰고 있는지도 같이 고정한다.
"""
import ast
import sys
from pathlib import Path

import pytest



def _assert_imported(dag_bag, *file_names) -> None:
    """이 테스트가 담당하는 DAG 파일만 파싱 성공을 요구한다.

    dags 폴더 전체를 검사하면 무관한 DAG의 파싱 실패까지 여기서 터져서
    원인을 찾기 어렵고, 다른 사람의 변경이 이 테스트를 막는다.
    """
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
    return _dag_folder() / "rental_history_preliminary_raw_dag.py"


@pytest.fixture(scope="module")
def preliminary_task():
    from airflow.dag_processing.dagbag import DagBag

    folder = str(_dag_folder())
    if folder not in sys.path:
        sys.path.insert(0, folder)
    dag_bag = DagBag(folder)
    _assert_imported(dag_bag, "rental_history_preliminary_raw_dag.py")
    return dag_bag.dags["rental_history_preliminary_raw"].get_task(
        "collect_preliminary_raw"
    )


def test_preliminary_raw_dag_is_api_only():
    dag_file = _dag_file()
    source = dag_file.read_text(encoding="utf-8")
    tree = ast.parse(source)

    bash_tasks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "BashOperator"
    ]

    assert len(bash_tasks) == 1
    assert 'dag_id="rental_history_preliminary_raw"' in source
    assert 'task_id="collect_preliminary_raw"' in source
    assert 'bash_job("collect_rental_history_raw")' in source
    assert '"SNAPSHOT_TYPE": "PRELIMINARY"' in source
    assert "outlets=" not in source


def test_preliminary_task_publishes_no_asset_and_runs_no_spark_job(preliminary_task):
    assert preliminary_task.outlets == []
    assert "python -m jobs.collect_rental_history_raw" in preliminary_task.bash_command
    assert "promote_rental_history_raw" not in preliminary_task.bash_command
    assert "daily_batch_rental_history" not in preliminary_task.bash_command
    assert preliminary_task.env["SNAPSHOT_TYPE"] == "PRELIMINARY"


def test_preliminary_task_uses_the_shared_logical_cutoff_template(preliminary_task):
    from dag_common import COLLECTION_CUTOFF_AT_TEMPLATE

    cutoff = preliminary_task.env["COLLECTION_CUTOFF_AT"]

    assert cutoff == COLLECTION_CUTOFF_AT_TEMPLATE
    assert "data_interval_end" in cutoff
    assert 'dag_run.conf.get("collection_cutoff_at")' in cutoff
