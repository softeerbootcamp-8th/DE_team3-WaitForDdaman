"""bronze_initial_load_all_sources DAG 구조 테스트 (#232 - dynamic task mapping 재구성,
#249 - EMR 초기 적재 파일당 JobRun -> 배치당 JobRun)."""

import os
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


def _load_dag(app_env: str):
    """지정한 APP_ENV로 DAG 파일을 새로 파싱한다. is_aws_env()가 DAG 정의 함수 본문에서
    호출되는 시점(=DagBag이 파일을 import할 때)의 환경변수를 그대로 읽으므로, AWS
    분기(EMR 배치 태스크)와 로컬 분기(파일별 BashOperator)를 각각 검증하려면 그때마다
    APP_ENV를 바꾸고 DagBag을 새로 만들어야 한다."""
    from airflow.dag_processing.dagbag import DagBag

    folder = _dag_folder()
    if folder not in sys.path:
        sys.path.insert(0, folder)

    previous = os.environ.get("APP_ENV")
    os.environ["APP_ENV"] = app_env
    try:
        dag_bag = DagBag(folder)
    finally:
        if previous is None:
            os.environ.pop("APP_ENV", None)
        else:
            os.environ["APP_ENV"] = previous

    mine = {
        path: error
        for path, error in dag_bag.import_errors.items()
        if Path(path).name == DAG_FILE
    }
    assert mine == {}, mine
    return dag_bag


@pytest.fixture(scope="module")
def dag_bag():
    return _load_dag("local")


@pytest.fixture(scope="module")
def dag(dag_bag):
    return dag_bag.dags[DAG_ID]


@pytest.fixture(scope="module")
def aws_dag():
    return _load_dag("aws").dags[DAG_ID]


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


def test_local_env_keeps_per_file_task(dag):
    """로컬은 배치로 묶지 않는다 - 기존처럼 파일 하나당 태스크 인스턴스 하나."""
    task_ids = set(dag.task_ids)
    assert "initial_load_rental_history_file" in task_ids
    assert "initial_load_failure_report_file" in task_ids
    assert "initial_load_rental_history_batch" not in task_ids
    assert "chunk_rental_history_files" not in task_ids


def test_aws_env_uses_batched_emr_jobrun_tasks(aws_dag):
    """AWS는 파일 목록을 배치로 잘라(chunk_*_files) 배치당 EMR JobRun 하나를
    제출한다(initial_load_*_batch) - 파일당 JobRun 태스크는 존재하지 않는다 (#249)."""
    task_ids = set(aws_dag.task_ids)
    assert "chunk_rental_history_files" in task_ids
    assert "chunk_failure_report_files" in task_ids
    assert "initial_load_rental_history_batch" in task_ids
    assert "initial_load_failure_report_batch" in task_ids
    assert "initial_load_rental_history_file" not in task_ids
    assert "initial_load_failure_report_file" not in task_ids


def test_aws_env_batch_tasks_are_mapped_and_depend_on_chunk_task(aws_dag):
    rental_batch = aws_dag.get_task("initial_load_rental_history_batch")
    rental_chunk = aws_dag.get_task("chunk_rental_history_files")
    failure_batch = aws_dag.get_task("initial_load_failure_report_batch")
    failure_chunk = aws_dag.get_task("chunk_failure_report_files")

    assert rental_batch.is_mapped
    assert failure_batch.is_mapped
    assert rental_chunk.task_id in rental_batch.upstream_task_ids
    assert failure_chunk.task_id in failure_batch.upstream_task_ids


def test_aws_env_batch_tasks_use_emr_initial_load_pool(aws_dag):
    from dag_common import EMR_INITIAL_LOAD_POOL

    assert aws_dag.get_task("initial_load_rental_history_batch").pool == EMR_INITIAL_LOAD_POOL
    assert aws_dag.get_task("initial_load_failure_report_batch").pool == EMR_INITIAL_LOAD_POOL


def test_aws_env_allows_more_concurrent_tasks_than_local(dag, aws_dag):
    """max_active_tasks=2는 LocalStack 레이스 컨디션 방지용 로컬 전용 제약이라
    AWS 환경에서는 적용되지 않아야 한다 (#249)."""
    assert dag.max_active_tasks == 2
    assert aws_dag.max_active_tasks > 2


def test_emr_batch_size_params_exist(dag):
    assert dag.params["rental_history_emr_batch_size"] == "3"
    assert dag.params["failure_report_emr_batch_size"] == "6"
