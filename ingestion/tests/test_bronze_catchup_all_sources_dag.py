"""Bronze Catchup DAG 구조 테스트 (#195)."""

import sys
from pathlib import Path

import pytest

DAG_ID = "bronze_catchup_all_sources"
DAG_FILE = "bronze_catchup_all_sources_dag.py"


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


def test_reconciliation_runs_at_0030_without_airflow_catchup(dag):
    assert str(dag.schedule) == "30 0 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.max_active_tasks == 5


def test_reconciliation_has_separate_mapped_flows_for_both_sources(dag):
    assert {
        "check_rental_history_gap",
        "check_failure_report_gap",
        "prepare_rental_history_date",
        "promote_rental_history_batch",
        "catchup_failure_report_date",
        "advance_rental_history_watermark",
        "advance_failure_report_watermark",
    }.issubset(dag.task_ids)

    prepare = dag.get_task("prepare_rental_history_date")
    promote = dag.get_task("promote_rental_history_batch")
    failure = dag.get_task("catchup_failure_report_date")
    # API 풀을 원천별로 분리해 rental 3병렬이 failure 슬롯을 굶기지 못하게 한다.
    assert prepare.pool == "rental_history_api"
    assert failure.pool == "failure_report_api"
    assert prepare.pool != failure.pool
    assert prepare.max_active_tis_per_dag == 3
    assert failure.max_active_tis_per_dag == 1
    assert dag.get_task("advance_rental_history_watermark").trigger_rule == "all_done"
    assert dag.get_task("advance_failure_report_watermark").trigger_rule == "all_done"
    assert "assign_rental_history_api_keys" in dag.get_task(
        "advance_rental_history_watermark"
    ).upstream_task_ids
    assert "assign_failure_report_api_key" in dag.get_task(
        "advance_failure_report_watermark"
    ).upstream_task_ids


def test_promote_rental_history_batch_serializes_bronze_commit(dag):
    """prepare는 API 키별로 병렬 실행되지만 promote는 전용 풀에서 slot 1개로 직렬화되어야 한다."""
    prepare = dag.get_task("prepare_rental_history_date")
    promote = dag.get_task("promote_rental_history_batch")

    assert promote.pool == "bronze_rental_history_commit"
    assert promote.pool != prepare.pool
    assert promote.max_active_tis_per_dag == 1
    assert "promote_rental_history_batch" in dag.get_task(
        "advance_rental_history_watermark"
    ).upstream_task_ids


def test_failure_report_stays_a_single_task(dag):
    """failure_report는 수집+적재를 한 태스크에서 끝낸다 - rental처럼 나누지 않는다."""
    task_ids = set(dag.task_ids)

    assert "catchup_failure_report_date" in task_ids
    assert "prepare_failure_report_date" not in task_ids
    assert "promote_failure_report_date" not in task_ids


def test_rental_promote_consumes_prepare_output_not_original_requests(dag):
    """승격 대상은 수집에 성공한 날짜뿐이다 - manifest 재검증 대신 prepare가 막는다."""
    batches = dag.get_task("build_rental_history_promote_batches")
    promote = dag.get_task("promote_rental_history_batch")

    assert "prepare_rental_history_date" in batches.upstream_task_ids
    assert "build_rental_history_promote_batches" in promote.upstream_task_ids
    # prepare가 실패한 날짜는 배치에 들어오지 않으므로 ALL_DONE으로 억지로 돌리지 않는다.
    assert promote.trigger_rule == "all_success"


def test_watermark_tasks_publish_bronze_assets(dag):
    """catch-up 완료가 Silver까지 전달되려면 워터마크 전진 태스크가 Asset을 발행해야 한다 (#286).

    예전엔 이 DAG이 Bronze를 복구하고 워터마크만 올린 뒤 끝나서, 두 Bronze Asset의
    producer가 일배치 하나뿐이었다 - 일배치가 실패한 날은 복구가 끝나도 Silver가
    돌지 않았다.
    """
    expected = {
        "advance_rental_history_watermark": "rental_history_bronze",
        "advance_failure_report_watermark": "failure_report_bronze",
    }
    for task_id, asset_name in expected.items():
        outlets = dag.get_task(task_id).outlets
        assert [outlet.name for outlet in outlets] == [asset_name], task_id


def test_only_watermark_tasks_publish_assets(dag):
    """원천별 Asset 발행은 한 run에 최대 1회여야 한다.

    mapped 태스크에 outlet을 붙이면 gap 날짜 수만큼(실측 55일까지 관측) Silver가
    트리거된다. Asset을 발행하는 태스크가 원천별로 정확히 하나임을 고정한다.
    """
    publishers = {
        task.task_id: [outlet.name for outlet in task.outlets]
        for task in dag.tasks
        if task.outlets
    }
    assert publishers == {
        "advance_rental_history_watermark": ["rental_history_bronze"],
        "advance_failure_report_watermark": ["failure_report_bronze"],
    }


@pytest.mark.parametrize(
    "task_id",
    [
        "prepare_rental_history_date",
        "promote_rental_history_batch",
        "catchup_failure_report_date",
    ],
)
def test_mapped_tasks_do_not_publish_assets(dag, task_id):
    assert dag.get_task(task_id).outlets == []


@pytest.mark.parametrize(
    "task_id",
    ["advance_rental_history_watermark", "advance_failure_report_watermark"],
)
def test_asset_is_not_published_when_gap_remains(dag, task_id):
    """공백이 남으면 Asset이 발행되지 않아야 한다.

    outlets는 태스크 성공 시에만 발행되고, ALL_DONE으로 항상 실행되더라도
    RECONCILIATION_FAIL_ON_INCOMPLETE=true가 공백이 남은 실행을 실패시킨다.
    이 두 설정이 "발행 게이트"이므로 함께 고정한다 - 하나만 바뀌면 미확정 상태로
    Silver를 트리거하게 된다.
    """
    task = dag.get_task(task_id)
    assert task.trigger_rule == "all_done"
    assert "RECONCILIATION_FAIL_ON_INCOMPLETE='true'" in task.bash_command


def test_catchup_does_not_touch_silver(dag):
    """Silver 워터마크 writer 수가 늘지 않아야 한다 (#286).

    Silver를 이 DAG에서 직접 처리하면(initial_load_dag의 planner/청크/finalizer 복제)
    Silver 워터마크의 완료 정의가 marker 기반과 직접 기록 두 개로 갈리고,
    silver.rental_history 커밋 경로가 세 개가 된다. Asset만 발행하는 설계를 고정한다.
    """
    silver_jobs = ("advance_silver_rental_history_watermark", "transform_silver_rental_history")
    for task in dag.tasks:
        command = getattr(task, "bash_command", "") or ""
        for job in silver_jobs:
            assert job not in command, f"{task.task_id}가 {job}을 호출한다"
