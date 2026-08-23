"""Bronze 일 배치 DAG의 대여이력 TaskGroup 구조 테스트.

DagBag으로 실제 파싱한 DAG를 검증한다. 소스 문자열 검사로는 잡을 수 없는
체인/trigger rule/outlet/pool/원천 격리를 여기서 고정한다.
"""
from datetime import timedelta
from pathlib import Path

import pytest

DAG_ID = "bronze_daily_batch_all_sources"
GROUP = "rental_history"
CHAIN = [
    f"{GROUP}.collect_final_raw",
    f"{GROUP}.select_final_or_preliminary",
    f"{GROUP}.promote_to_bronze",
    f"{GROUP}.update_confirmed_watermark",
    f"{GROUP}.publish_bronze_asset",
]
OTHER_SOURCE_TASKS = [
    "daily_batch_station_master",
    "daily_batch_failure_report",
    "daily_batch_bikeman_event",
    "daily_batch_station_active",
]


def _dag_folder() -> str:
    repository_path = Path(__file__).resolve().parents[2] / "airflow" / "dags"
    if repository_path.exists():
        return str(repository_path)
    return "/opt/airflow/dags"



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


@pytest.fixture(scope="module")
def dag():
    import sys

    from airflow.dag_processing.dagbag import DagBag

    folder = _dag_folder()
    if folder not in sys.path:
        sys.path.insert(0, folder)
    dag_bag = DagBag(folder)
    _assert_imported(dag_bag, "bronze_daily_batch_all_sources_dag.py")
    return dag_bag.dags[DAG_ID]


def _task(dag, task_id):
    return dag.get_task(task_id)


def test_rental_history_task_group_has_the_five_expected_tasks(dag):
    group_tasks = sorted(t.task_id for t in dag.tasks if t.task_id.startswith(f"{GROUP}."))

    assert group_tasks == sorted(CHAIN)
    assert "daily_batch_rental_history" not in [t.task_id for t in dag.tasks]


def test_rental_history_chain_is_exact(dag):
    for upstream_id, downstream_id in zip(CHAIN, CHAIN[1:]):
        upstream = _task(dag, upstream_id)
        assert upstream.downstream_task_ids == {downstream_id}

    assert _task(dag, CHAIN[0]).upstream_task_ids == set()
    assert _task(dag, CHAIN[-1]).downstream_task_ids == set()


def test_only_the_selector_runs_on_all_done(dag):
    trigger_rules = {
        task_id: _task(dag, task_id).trigger_rule.value for task_id in CHAIN
    }

    assert trigger_rules[f"{GROUP}.select_final_or_preliminary"] == "all_done"
    assert {
        rule for task_id, rule in trigger_rules.items() if "select_final" not in task_id
    } == {"all_success"}


def test_final_collector_does_not_use_airflow_retries_or_long_timeout(dag):
    collector = _task(dag, f"{GROUP}.collect_final_raw")

    assert collector.retries == 0
    assert collector.execution_timeout == timedelta(minutes=15)
    # 나머지 task는 공통 재시도 정책을 유지한다.
    assert _task(dag, f"{GROUP}.select_final_or_preliminary").retries == 3
    assert _task(dag, f"{GROUP}.update_confirmed_watermark").retries == 3


def test_only_the_promotion_task_uses_the_bronze_pool(dag):
    pooled = {
        task_id for task_id in CHAIN if _task(dag, task_id).pool == "bronze_ingest"
    }

    assert pooled == {f"{GROUP}.promote_to_bronze"}
    assert _task(dag, f"{GROUP}.promote_to_bronze").execution_timeout == timedelta(hours=2)


def test_rental_history_asset_is_published_only_by_the_publish_task(dag):
    producers = {
        task.task_id
        for task in dag.tasks
        for outlet in (task.outlets or [])
        if getattr(outlet, "name", None) == "rental_history_bronze"
    }

    assert producers == {f"{GROUP}.publish_bronze_asset"}
    # #137: promotion metadata를 Asset event에 실어 보내야 해서 EmptyOperator에서
    # TaskFlow PythonOperator로 바뀌었다.
    assert _task(dag, f"{GROUP}.publish_bronze_asset").task_type == "_PythonDecoratedOperator"


def test_other_bronze_sources_stay_independent_of_the_rental_group(dag):
    for task_id in OTHER_SOURCE_TASKS:
        task = _task(dag, task_id)
        assert task.upstream_task_ids == set()
        assert task.downstream_task_ids == set()

    assert {task.task_id for task in dag.tasks if not task.task_id.startswith(f"{GROUP}.")} == set(
        OTHER_SOURCE_TASKS
    )


def test_other_bronze_source_commands_and_outlets_are_unchanged(dag):
    expected_outlets = {
        "daily_batch_station_master": "station_master_bronze",
        "daily_batch_failure_report": "failure_report_bronze",
        "daily_batch_bikeman_event": "bikeman_event_bronze",
        "daily_batch_station_active": "station_active_bronze",
    }

    for task_id, asset_name in expected_outlets.items():
        task = _task(dag, task_id)
        assert [outlet.name for outlet in task.outlets] == [asset_name]
        assert f"python -m jobs.{task_id}" in task.bash_command
        assert task.pool == "bronze_ingest"


def test_rental_history_tasks_run_the_expected_ingestion_modules(dag):
    expected_modules = {
        f"{GROUP}.collect_final_raw": "collect_rental_history_raw",
        f"{GROUP}.select_final_or_preliminary": "select_rental_history_snapshot",
        f"{GROUP}.promote_to_bronze": "promote_rental_history_raw",
        f"{GROUP}.update_confirmed_watermark": "update_rental_history_confirmed_watermark",
    }

    for task_id, module in expected_modules.items():
        assert f"python -m jobs.{module}" in _task(dag, task_id).bash_command


def test_flag_defaults_are_false_and_cutoff_uses_the_logical_interval(dag):
    collector = _task(dag, f"{GROUP}.collect_final_raw")
    selector = _task(dag, f"{GROUP}.select_final_or_preliminary")

    assert collector.env["SNAPSHOT_TYPE"] == "FINAL"
    for task in (collector, selector):
        cutoff = task.env["COLLECTION_CUTOFF_AT"]
        assert "data_interval_end" in cutoff
        assert 'dag_run.conf.get("collection_cutoff_at")' in cutoff

    assert "RENTAL_HISTORY_FALLBACK_ENABLED" in selector.env
    assert "'false'" in selector.env["RENTAL_HISTORY_FALLBACK_ENABLED"]
    assert "'false'" in selector.env["RENTAL_HISTORY_T0_ENABLED"]
    assert "'false'" in collector.env["RENTAL_HISTORY_T0_ENABLED"]
    assert "'120'" in selector.env["RENTAL_HISTORY_PRELIMINARY_MAX_AGE_MINUTES"]
    assert "params.max_days_per_run" in collector.env["MAX_DAYS_PER_RUN"]
    assert "params.max_days_per_run" in selector.env["MAX_DAYS_PER_RUN"]


def test_every_rental_task_receives_the_same_logical_cutoff(dag):
    cutoffs = {
        _task(dag, task_id).env["COLLECTION_CUTOFF_AT"]
        for task_id in CHAIN[:-1]
    }

    assert len(cutoffs) == 1


def test_catchup_dag_still_runs_the_legacy_rental_history_job():
    import sys

    from airflow.dag_processing.dagbag import DagBag

    folder = _dag_folder()
    if folder not in sys.path:
        sys.path.insert(0, folder)
    catchup = DagBag(folder).dags["bronze_catchup_all_sources"]
    task = catchup.get_task("catchup_rental_history")

    assert "python -m jobs.daily_batch_rental_history" in task.bash_command
    assert [outlet.name for outlet in task.outlets] == ["rental_history_bronze"]
