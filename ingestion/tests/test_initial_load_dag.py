"""initial_load DAG 구조 테스트 (#232 - dynamic task mapping 재구성,
#249 - EMR 초기 적재 파일당 JobRun -> 배치당 JobRun)."""

import os
import sys
from pathlib import Path

import pytest

DAG_ID = "initial_load"
DAG_FILE = "initial_load_dag.py"


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
    assert "build_rental_history_backfill_commands" in task_ids
    assert "load_silver_rental_history_chunk" in task_ids
    assert "finalize_rental_history_backfill_watermark" in task_ids
    # 기존 bash for-loop 단일 태스크는 더 이상 존재하지 않는다
    assert "load_silver_rental_history" not in task_ids
    # 연속 완료 구간을 finalizer가 marker로 직접 계산하므로 max_end 태스크는 사라졌다
    assert "max_rental_history_backfill_range_end" not in task_ids


def test_load_silver_rental_history_chunk_is_mapped(dag):
    chunk_task = dag.get_task("load_silver_rental_history_chunk")
    assert chunk_task.is_mapped


def test_chunk_task_expands_over_pending_ranges_only(dag):
    """청크 커맨드는 계획의 pending_ranges만 펼친다 - 완료 청크는 Task Instance를 안 만든다."""
    build = dag.get_task("build_rental_history_backfill_commands")
    assert "parse_rental_history_backfill_ranges" in build.upstream_task_ids
    assert build.task_id in dag.get_task("load_silver_rental_history_chunk").upstream_task_ids

    commands = build.python_callable(
        {
            "bronze_watermark_at_start": "2026-06-30",
            "contract_version": 1,
            "all_ranges": [
                {"start": "2015-01-01", "end": "2015-01-31"},
                {"start": "2015-02-01", "end": "2015-02-28"},
            ],
            "pending_ranges": [{"start": "2015-02-01", "end": "2015-02-28"}],
        }
    )
    assert len(commands) == 1
    assert "BACKFILL_RANGE_START='2015-02-01'" in commands[0]
    assert "BACKFILL_RANGE_END='2015-02-28'" in commands[0]
    assert "BRONZE_WATERMARK_AT_START='2026-06-30'" in commands[0]
    assert "2015-01-01" not in commands[0]


def test_chunk_task_keeps_silver_pool_and_serial_mapping(dag):
    """#232의 직렬 제약(pool + max_active_tis_per_dag=1)은 이번 변경에서도 유지한다."""
    chunk_task = dag.get_task("load_silver_rental_history_chunk")
    assert chunk_task.partial_kwargs["pool"] == "silver_process"
    assert chunk_task.partial_kwargs["max_active_tis_per_dag"] == 1


def test_finalize_watermark_runs_all_done_after_chunks(dag):
    """청크가 실패해도 finalizer가 돌아야 연속 완료 구간까지 워터마크가 전진한다."""
    finalize = dag.get_task("finalize_rental_history_backfill_watermark")
    chunk = dag.get_task("load_silver_rental_history_chunk")
    assert chunk.task_id in finalize.upstream_task_ids
    assert finalize.trigger_rule.value == "all_done"
    assert "advance_silver_rental_history_watermark" in finalize.bash_command
    assert "SILVER_BACKFILL_PLAN=" in finalize.bash_command


def test_finalizer_reads_planner_raw_xcom_not_the_parsed_dict(dag):
    """finalizer는 planner BashOperator의 XCom 원문(한 줄 JSON)을 받아야 한다.

    parse 태스크의 XCom(dict)을 쓰면 Jinja가 Python repr로 렌더링해서 두 가지가 동시에
    깨진다 - 작은따옴표라 JSON 파싱이 실패하고, bash의 '...' 인용도 그 자리에서 닫힌다.
    주석으로만 남아 있으면 나중에 "parse 태스크를 쓰는 게 자연스럽다"며 바뀌기 쉬워서
    구조로 고정한다.
    """
    finalize = dag.get_task("finalize_rental_history_backfill_watermark")

    assert 'task_ids="compute_rental_history_backfill_ranges"' in finalize.bash_command
    assert "parse_rental_history_backfill_ranges" not in finalize.bash_command
    # 계획 JSON은 셸에서 작은따옴표로 감싸 넘긴다 (JSON 자체에는 작은따옴표가 없다).
    assert "SILVER_BACKFILL_PLAN='{{" in finalize.bash_command


def test_finalizer_plan_argument_survives_shell_quoting(dag):
    """렌더된 계획 JSON이 셸을 거쳐도 그대로 JSON으로 되읽히는지 확인한다."""
    import json
    import shlex

    finalize = dag.get_task("finalize_rental_history_backfill_watermark")
    plan = {
        "silver_watermark_before": "2014-12-31",
        "bronze_watermark_at_start": "2026-06-30",
        "contract_version": 1,
        "all_ranges": [{"start": "2015-01-01", "end": "2015-01-31"}],
        "pending_ranges": [{"start": "2015-01-01", "end": "2015-01-31"}],
    }
    rendered = finalize.bash_command.replace(
        '{{ ti.xcom_pull(task_ids="compute_rental_history_backfill_ranges") }}',
        json.dumps(plan),
    )

    # shlex가 셸과 같은 규칙으로 쪼갠다 - 인용이 깨졌으면 여기서 토큰이 어긋난다.
    assignment = next(
        token for token in shlex.split(rendered) if token.startswith("SILVER_BACKFILL_PLAN=")
    )
    assert json.loads(assignment.split("=", 1)[1]) == plan


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
    assert dag.params["failure_report_emr_batch_size"] == "12"


def test_staging_batch_size_params_exist(dag):
    assert dag.params["rental_history_staging_batch_size"] == "6"
    assert dag.params["failure_report_staging_batch_size"] == "12"


def test_local_env_has_no_staging_batch_tasks(dag):
    """로컬은 list_input_files.py가 반환하는 로컬 경로를 그대로 쓴다 - S3 스테이징
    배치 태스크(#255)는 AWS 전용이다."""
    task_ids = set(dag.task_ids)
    assert "stage_rental_history_files_batch" not in task_ids
    assert "stage_failure_report_files_batch" not in task_ids
    assert "chunk_rental_history_staging_files" not in task_ids
    assert "chunk_failure_report_staging_files" not in task_ids


def test_aws_env_has_staging_batch_tasks_between_list_and_emr_chunk(aws_dag):
    """AWS는 다운로드(list_*_files)와 S3 업로드(stage_*_files_batch)를 분리한다(#255) -
    "파일 하나 = 태스크 하나"가 아니라 배치 단위 Dynamic Task Mapping이어야 한다."""
    task_ids = set(aws_dag.task_ids)
    assert "stage_rental_history_files_batch" in task_ids
    assert "stage_failure_report_files_batch" in task_ids
    assert "chunk_rental_history_staging_files" in task_ids
    assert "chunk_failure_report_staging_files" in task_ids

    stage_rental = aws_dag.get_task("stage_rental_history_files_batch")
    stage_failure = aws_dag.get_task("stage_failure_report_files_batch")
    assert stage_rental.is_mapped
    assert stage_failure.is_mapped

    chunk_rental_staging = aws_dag.get_task("chunk_rental_history_staging_files")
    chunk_failure_staging = aws_dag.get_task("chunk_failure_report_staging_files")
    assert chunk_rental_staging.task_id in stage_rental.upstream_task_ids
    assert chunk_failure_staging.task_id in stage_failure.upstream_task_ids

    # 스테이징 배치 결과(URI 목록)가 EMR 배치 청소 단계보다 먼저 와야 한다.
    chunk_rental_emr = aws_dag.get_task("chunk_rental_history_files")
    chunk_failure_emr = aws_dag.get_task("chunk_failure_report_files")
    assert "parse_rental_history_staging_uris" in chunk_rental_emr.upstream_task_ids
    assert "parse_failure_report_staging_uris" in chunk_failure_emr.upstream_task_ids


def test_aws_env_staging_batch_tasks_use_s3_staging_pool(aws_dag):
    from dag_common import S3_STAGING_POOL

    assert aws_dag.get_task("stage_rental_history_files_batch").pool == S3_STAGING_POOL
    assert aws_dag.get_task("stage_failure_report_files_batch").pool == S3_STAGING_POOL


def test_aws_env_emr_batch_tasks_use_entry_point_arguments_not_input_files_env(
    aws_dag, monkeypatch
):
    """#255: INPUT_FILES는 sparkSubmitParameters의 공백 파싱 버그를 다시 겪을 수 있으니
    entryPointArguments가 1차 전달 경로여야 한다. dag_common.run_emr_serverless_spark_job은
    내부에서 boto3.client(...)를 직접 호출하므로, boto3 자체를 페이크로 바꿔 실제 EMR
    Serverless start_job_run에 실린 jobDriver를 검사한다."""
    import boto3

    monkeypatch.setenv("EMR_SPARK_APPLICATION_ID", "app-id")
    monkeypatch.setenv("EMR_SPARK_EXECUTION_ROLE_ARN", "role-arn")

    captured_job_drivers = []

    class _FakeEmrServerlessClient:
        def start_job_run(self, **kwargs):
            captured_job_drivers.append(kwargs["jobDriver"])
            return {"jobRunId": "job-run-id"}

        def get_job_run(self, **kwargs):
            return {"jobRun": {"state": "SUCCESS"}}

    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeEmrServerlessClient())

    rental_batch_task = aws_dag.get_task("initial_load_rental_history_batch")
    rental_batch_task.python_callable(["s3://bucket/a.csv"])

    failure_batch_task = aws_dag.get_task("initial_load_failure_report_batch")
    failure_batch_task.python_callable(["s3://bucket/b.csv"])

    assert len(captured_job_drivers) == 2
    for job_driver, expected_files in zip(
        captured_job_drivers, ['["s3://bucket/a.csv"]', '["s3://bucket/b.csv"]']
    ):
        spark_submit = job_driver["sparkSubmit"]
        assert spark_submit["entryPointArguments"] == ["--input-files-json", expected_files]
        assert "INPUT_FILES" not in spark_submit["sparkSubmitParameters"]


def test_failure_report_silver_depends_on_both_watermarks(dag):
    """silver_failure_report가 확정 구간 증분으로 바뀐 뒤(#288) 하한 워터마크가 필요해졌다.

    Bronze 워터마크(상한)와 Silver 워터마크(하한)를 둘 다 읽어 구간을 정하므로,
    두 워터마크 태스크가 모두 upstream이어야 한다. bootstrap이 빠지면
    read_watermark가 backfill_start_date(기본 2015-01-01)로 폴백해 데이터가 없는
    6년치가 구간에 들어온다.
    """
    upstream = dag.get_task("load_silver_failure_report").upstream_task_ids

    assert "set_bronze_ingestion_watermark_failure_report" in upstream
    assert "bootstrap_silver_watermark_failure_report" in upstream


def test_failure_report_bootstrap_targets_its_own_dataset(dag):
    command = dag.get_task("bootstrap_silver_watermark_failure_report").bash_command

    assert "bootstrap_silver_watermark" in command
    assert "DATASET=failure_report" in command


def test_initial_load_lifts_failure_report_silver_day_cap(dag):
    """초기 적재는 2021-02부터를 한 번에 소화해야 한다.

    잡의 기본 상한은 31일이라, 초기 적재에서 상한을 풀지 않으면 워터마크가 31일씩만
    전진해 수십 번 재실행해야 한다.
    """
    command = dag.get_task("load_silver_failure_report").bash_command
    params = dag.params

    assert "MAX_DAYS_PER_RUN=" in command
    assert "failure_report_silver_total_days_cap" in command
    assert int(str(params["failure_report_silver_total_days_cap"])) >= 3650
