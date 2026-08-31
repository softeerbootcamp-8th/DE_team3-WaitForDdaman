"""initial_load_rental_history 배치 처리 테스트 (#249 - 파일당 JobRun -> 배치당 JobRun).

_process_one_input_file은 Spark 세션을 실제로 필요로 하지만, run()의 배치 집계/실패
판단 로직(파일 하나 실패가 배치 전체를 막지 않는지, 실패 파일이 하나라도 있으면
exit(1)하는지)은 그 함수를 모킹해서 Spark 없이도 검증할 수 있다. 실제 파일 파싱/스키마
검증 경로(모킹 대상 안쪽)는 schema/*_schema.py, common/encoding_utils.py 등 각자의
단위 테스트가 이미 다룬다.
"""
from unittest import mock
import logging

import pytest

import bronze.initial_load_rental_history as job


def test_run_succeeds_when_all_files_succeed():
    with mock.patch.object(
        job, "_process_one_input_file", side_effect=[(100, False, False), (50, False, False)]
    ) as mocked, mock.patch.object(job, "build_spark_session"), mock.patch.object(
        job, "_ensure_bronze_table"
    ), mock.patch.object(job, "ensure_bucket"):
        job.run(["a.csv", "b.csv"])

    assert mocked.call_count == 2


def test_run_logs_batch_progress_for_each_input_file(caplog):
    caplog.set_level(logging.INFO, logger=job.__name__)
    with mock.patch.object(
        job, "_process_one_input_file", side_effect=[(100, False, False), (50, False, False)]
    ), mock.patch.object(job, "build_spark_session"), mock.patch.object(
        job, "_ensure_bronze_table"
    ), mock.patch.object(job, "ensure_bucket"):
        job.run(["a.csv", "b.csv"])

    assert "초기 적재 배치 시작: 파일 2개" in caplog.text
    assert "[1/2] 파일 처리 시작: a.csv" in caplog.text
    assert "[1/2] 파일 처리 완료: a.csv (100행, 누적 100행)" in caplog.text
    assert "[2/2] 파일 처리 완료: b.csv (50행, 누적 150행)" in caplog.text


def test_run_exits_nonzero_when_any_file_fails():
    with mock.patch.object(
        job, "_process_one_input_file", side_effect=[(100, False, False), (0, True, False)]
    ), mock.patch.object(job, "build_spark_session"), mock.patch.object(
        job, "_ensure_bronze_table"
    ), mock.patch.object(job, "ensure_bucket"):
        with pytest.raises(SystemExit) as exc_info:
            job.run(["a.csv", "bad.csv"])

    assert exc_info.value.code == 1


def test_run_continues_processing_remaining_files_after_one_fails():
    """한 파일이 실패해도 나머지 배치 파일은 계속 처리된다 (전체를 한 번에 죽이지 않음)."""
    calls = []

    def fake_process(spark, input_file):
        calls.append(input_file)
        if input_file == "bad.csv":
            return 0, True, False
        return 10, False, False

    with mock.patch.object(job, "_process_one_input_file", side_effect=fake_process), \
        mock.patch.object(job, "build_spark_session"), \
        mock.patch.object(job, "_ensure_bronze_table"), \
        mock.patch.object(job, "ensure_bucket"):
        with pytest.raises(SystemExit):
            job.run(["a.csv", "bad.csv", "c.csv"])

    assert calls == ["a.csv", "bad.csv", "c.csv"]


def test_run_treats_skip_as_success_not_failure():
    """다른 데이터셋으로 스킵된 파일은 실패로 취급하지 않는다."""
    with mock.patch.object(
        job, "_process_one_input_file", side_effect=[(0, False, True)]
    ), mock.patch.object(job, "build_spark_session"), mock.patch.object(
        job, "_ensure_bronze_table"
    ), mock.patch.object(job, "ensure_bucket"):
        job.run(["other_dataset.csv"])  # SystemExit이 발생하지 않아야 함


def test_process_one_input_file_reports_failure_for_missing_local_file(tmp_path):
    missing = tmp_path / "does_not_exist.csv"
    row_count, failed, skipped = job._process_one_input_file(spark=None, input_file=str(missing))
    assert (row_count, failed, skipped) == (0, True, False)


def test_resolve_input_files_prefers_entry_point_argument():
    """entryPointArguments(--input-files-json)가 있으면 그걸 우선한다 (#255)."""
    files = job._resolve_input_files(["--input-files-json", '["a.csv", "b.csv"]'])
    assert files == ["a.csv", "b.csv"]


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
