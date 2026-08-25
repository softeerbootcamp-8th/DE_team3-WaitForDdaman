"""initial_load_failure_report 배치 처리 테스트 (#249 - 파일당 JobRun -> 배치당 JobRun).

test_initial_load_rental_history.py와 동일한 이유/구조 - run()의 배치 집계 로직을
_process_one_input_file 모킹으로 Spark 없이 검증한다.
"""
from unittest import mock

import pytest

import jobs.initial_load_failure_report as job


def test_run_succeeds_when_all_files_succeed():
    with mock.patch.object(
        job, "_process_one_input_file", side_effect=[(30, False, False), (20, False, False)]
    ) as mocked, mock.patch.object(job, "build_spark_session"), mock.patch.object(
        job, "_ensure_bronze_table"
    ), mock.patch.object(job, "ensure_bucket"):
        job.run(["a.csv", "b.xlsx"])

    assert mocked.call_count == 2


def test_run_exits_nonzero_when_any_file_fails():
    with mock.patch.object(
        job, "_process_one_input_file", side_effect=[(0, True, False), (20, False, False)]
    ), mock.patch.object(job, "build_spark_session"), mock.patch.object(
        job, "_ensure_bronze_table"
    ), mock.patch.object(job, "ensure_bucket"):
        with pytest.raises(SystemExit) as exc_info:
            job.run(["bad.csv", "b.xlsx"])

    assert exc_info.value.code == 1


def test_run_continues_processing_remaining_files_after_one_fails():
    calls = []

    def fake_process(spark, input_file):
        calls.append(input_file)
        if input_file == "bad.csv":
            return 0, True, False
        return 5, False, False

    with mock.patch.object(job, "_process_one_input_file", side_effect=fake_process), \
        mock.patch.object(job, "build_spark_session"), \
        mock.patch.object(job, "_ensure_bronze_table"), \
        mock.patch.object(job, "ensure_bucket"):
        with pytest.raises(SystemExit):
            job.run(["a.csv", "bad.csv", "c.xlsx"])

    assert calls == ["a.csv", "bad.csv", "c.xlsx"]


def test_process_one_input_file_reports_failure_for_missing_local_file(tmp_path):
    missing = tmp_path / "does_not_exist.csv"
    row_count, failed, skipped = job._process_one_input_file(spark=None, input_file=str(missing))
    assert (row_count, failed, skipped) == (0, True, False)


def test_resolve_input_files_prefers_entry_point_argument():
    """entryPointArguments(--input-files-json)가 있으면 그걸 우선한다 (#255)."""
    files = job._resolve_input_files(["--input-files-json", '["a.csv", "b.xlsx"]'])
    assert files == ["a.csv", "b.xlsx"]


def test_resolve_input_files_falls_back_to_env_var(monkeypatch):
    """entryPointArguments가 없으면 INPUT_FILES 환경변수로 내려간다(하위호환, #255)."""
    monkeypatch.setenv("INPUT_FILES", '["c.csv"]')
    files = job._resolve_input_files([])
    assert files == ["c.csv"]


def test_resolve_input_files_exits_when_neither_is_set(monkeypatch):
    monkeypatch.delenv("INPUT_FILES", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        job._resolve_input_files([])
    assert exc_info.value.code == 1
