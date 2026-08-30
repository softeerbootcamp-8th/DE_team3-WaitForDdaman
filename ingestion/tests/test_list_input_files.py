"""
list_input_files.py 테스트 (#255 - 다운로드/압축해제와 S3 업로드 분리)

S3 업로드는 jobs/stage_initial_load_files.py로 옮겨졌다 - 이 잡은 이제 환경(local/aws)
과 무관하게 항상 로컬 파일 경로 목록만 반환한다. 업로드/멱등성/레거시 재사용 테스트는
tests/test_stage_initial_load_files.py로 이동했다.
"""
from unittest.mock import patch


def test_run_returns_local_paths_regardless_of_env(tmp_path, monkeypatch):
    import config as config_module
    from bronze import list_input_files

    monkeypatch.setattr(config_module, "SETTINGS", config_module.Settings(env="aws"))

    input_dir = tmp_path / "raw_downloads"
    input_dir.mkdir()
    (input_dir / "2601.csv").write_bytes(b"csv-body")

    with patch("jobs.list_input_files.ensure_backfill_files"):
        files = list_input_files.run("rental_history", str(input_dir), "*")

    assert files == [str(input_dir / "2601.csv")]
