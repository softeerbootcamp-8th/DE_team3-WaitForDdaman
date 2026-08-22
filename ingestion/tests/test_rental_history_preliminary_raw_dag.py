import ast
from pathlib import Path


def _dag_file() -> Path:
    repository_path = Path(__file__).resolve().parents[2] / "airflow" / "dags"
    if repository_path.exists():
        return repository_path / "rental_history_preliminary_raw_dag.py"
    return Path("/opt/airflow/dags/rental_history_preliminary_raw_dag.py")


def test_preliminary_raw_dag_is_api_only_and_uses_logical_cutoff():
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
    assert "data_interval_end" in source
    assert 'dag_run.conf.get("collection_cutoff_at")' in source
    assert '"SNAPSHOT_TYPE": "PRELIMINARY"' in source
    assert "outlets=" not in source
