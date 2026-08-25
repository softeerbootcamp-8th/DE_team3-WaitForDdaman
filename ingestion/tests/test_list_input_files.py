"""
list_input_files.py의 AWS 초기 적재 스테이징 업로드 멱등성 테스트

ensure_backfill_files(열린데이터광장 다운로드)는 여기서 patch로 대체한다 - 이 테스트는
"로컬에 이미 있는 파일을 S3 스테이징에 올리는 단계"의 멱등성만 검증 대상이다.
"""
from pathlib import Path
from unittest.mock import patch

import pytest
from moto import mock_aws

import config as config_module

BUCKET = "test-initial-load-bucket"


@pytest.fixture
def aws_env(monkeypatch):
    test_settings = config_module.Settings(env="aws", raw_bucket=BUCKET, s3_region="ap-northeast-2")
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)
    with mock_aws():
        yield


def _run_with_local_file(tmp_path, content: bytes):
    from jobs import list_input_files

    input_dir = tmp_path / "raw_downloads"
    input_dir.mkdir(exist_ok=True)
    (input_dir / "2601.csv").write_bytes(content)

    with patch("jobs.list_input_files.ensure_backfill_files"):
        return list_input_files.run("rental_history", str(input_dir), "*")


def test_staging_upload_is_skipped_when_same_file_already_uploaded(aws_env, tmp_path, monkeypatch):
    from common import s3_utils

    uris_first = _run_with_local_file(tmp_path, b"csv-body")
    assert uris_first == ["s3://test-initial-load-bucket/raw/rental_history/_initial_load_staging/2601.csv"]

    real_client = s3_utils.get_s3_client()
    upload_calls = []
    orig_upload_file = real_client.upload_file

    def spying_upload_file(*args, **kwargs):
        upload_calls.append((args, kwargs))
        return orig_upload_file(*args, **kwargs)

    monkeypatch.setattr(real_client, "upload_file", spying_upload_file)
    monkeypatch.setattr(s3_utils, "get_s3_client", lambda: real_client)

    uris_second = _run_with_local_file(tmp_path, b"csv-body")

    assert uris_second == uris_first
    assert upload_calls == []  # 같은 내용이면 두 번째 실행에서 재업로드하지 않아야 함


def test_staging_upload_refreshes_when_local_file_changed(aws_env, tmp_path):
    from common import s3_utils

    uris = _run_with_local_file(tmp_path, b"csv-body-v1")
    bucket, key = s3_utils.split_s3_uri(uris[0])
    assert s3_utils.get_s3_client().get_object(Bucket=bucket, Key=key)["Body"].read() == b"csv-body-v1"

    uris_after = _run_with_local_file(tmp_path, b"csv-body-v2")  # 재다운로드로 로컬 원본이 갱신된 상황 재현

    assert uris_after == uris  # deterministic key는 그대로 유지
    bucket, key = s3_utils.split_s3_uri(uris_after[0])
    assert s3_utils.get_s3_client().get_object(Bucket=bucket, Key=key)["Body"].read() == b"csv-body-v2"
