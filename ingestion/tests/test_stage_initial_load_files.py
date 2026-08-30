"""
stage_initial_load_files.py 테스트 (#255)

- deterministic key로의 최초 업로드/멱등 스킵/내용 변경 시 재업로드는 upload_file_if_changed
  (tests/test_s3_utils.py)가 이미 커버하므로 여기서는 배치 잡 레벨 동작(여러 파일 -> URI
  목록, 배치 하나에 성공/legacy 재사용이 섞여도 각자 올바른 URI를 돌려주는지)만 검증한다.
- 레거시(한글/공백 원본 파일명) key 재사용은 여기서 직접 검증한다 - list_input_files.py가
  예전(#218 이전)에 쓰던 key 그대로를 미리 S3에 심어두고, 로컬 파일을 내려받지 않고도(=로컬
  MD5/업로드 없이) 서버사이드 CopyObject로만 재사용되는지 확인한다.
"""
from pathlib import Path

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


def test_run_uploads_new_files_and_returns_deterministic_uris(aws_env, tmp_path):
    from common import s3_utils
    from bronze import stage_initial_load_files

    local = tmp_path / "2601.csv"
    local.write_bytes(b"csv-body")

    uris = stage_initial_load_files.run("rental_history", [str(local)])

    assert uris == ["s3://test-initial-load-bucket/raw/rental_history/_initial_load_staging/input_2601.csv"]
    body = s3_utils.get_s3_client().get_object(
        Bucket=BUCKET, Key="raw/rental_history/_initial_load_staging/input_2601.csv"
    )["Body"].read()
    assert body == b"csv-body"


def test_run_skips_reupload_when_already_staged(aws_env, tmp_path, monkeypatch):
    from common import s3_utils
    from bronze import stage_initial_load_files

    local = tmp_path / "2601.csv"
    local.write_bytes(b"csv-body")
    stage_initial_load_files.run("rental_history", [str(local)])

    real_client = s3_utils.get_s3_client()
    upload_calls = []
    orig_upload_file = real_client.upload_file

    def spying_upload_file(*args, **kwargs):
        upload_calls.append((args, kwargs))
        return orig_upload_file(*args, **kwargs)

    monkeypatch.setattr(real_client, "upload_file", spying_upload_file)
    monkeypatch.setattr(s3_utils, "get_s3_client", lambda: real_client)

    uris = stage_initial_load_files.run("rental_history", [str(local)])

    assert uris == ["s3://test-initial-load-bucket/raw/rental_history/_initial_load_staging/input_2601.csv"]
    assert upload_calls == []


def test_run_reuses_legacy_key_via_server_side_copy_without_touching_local_file(
    aws_env, tmp_path, monkeypatch
):
    """예전(#218 이전) 초기 적재가 한글/공백 원본 파일명을 그대로 key로 썼던 객체가
    이미 S3에 있으면, 로컬 파일을 다시 읽어 MD5를 계산하거나 업로드하지 않고 서버사이드
    CopyObject만으로 새 deterministic key에 재사용해야 한다(#255)."""
    from common import s3_utils
    from bronze import stage_initial_load_files

    legacy_name = "서울시 공공자전거 대여이력_2601.csv"
    legacy_key = f"raw/rental_history/_initial_load_staging/{legacy_name}"
    s3_utils.ensure_bucket(BUCKET)
    s3_utils.get_s3_client().put_object(Bucket=BUCKET, Key=legacy_key, Body=b"legacy-body")

    local = tmp_path / legacy_name
    local.write_bytes(b"legacy-body")

    # legacy key 재사용 경로는 로컬 MD5 계산도, PutObject 업로드도 하지 않아야 한다 -
    # 둘 중 하나라도 호출되면 즉시 실패시켜서 서버사이드 CopyObject만 탔는지 드러낸다.
    def _fail_md5(path):
        raise AssertionError("legacy key 재사용 시 로컬 MD5를 계산하면 안 된다")

    monkeypatch.setattr(s3_utils, "_md5_hex", _fail_md5)
    real_client = s3_utils.get_s3_client()
    monkeypatch.setattr(
        real_client,
        "upload_file",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("legacy key 재사용 시 업로드하면 안 된다")),
    )
    monkeypatch.setattr(s3_utils, "get_s3_client", lambda: real_client)

    uris = stage_initial_load_files.run("rental_history", [str(local)])

    safe_key = "raw/rental_history/_initial_load_staging/input_2601.csv"
    assert uris == [f"s3://{BUCKET}/{safe_key}"]
    body = s3_utils.get_s3_client().get_object(Bucket=BUCKET, Key=safe_key)["Body"].read()
    assert body == b"legacy-body"


def test_run_never_returns_hidden_spark_input_key(aws_env, tmp_path):
    from bronze import stage_initial_load_files

    local = tmp_path / "서울특별시 공공자전거 대여이력 정보_1603.csv"
    local.write_bytes(b"csv-body")

    uris = stage_initial_load_files.run("rental_history", [str(local)])

    key = uris[0].rsplit("/", 1)[-1]
    assert key == "input_1603.csv"
    assert not key.startswith((".", "_"))


def test_run_handles_batch_of_multiple_files(aws_env, tmp_path):
    from bronze import stage_initial_load_files

    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_bytes(b"a-body")
    b.write_bytes(b"b-body")

    uris = stage_initial_load_files.run("failure_report", [str(a), str(b)])

    assert uris == [
        "s3://test-initial-load-bucket/raw/failure_report/_initial_load_staging/input_a.csv",
        "s3://test-initial-load-bucket/raw/failure_report/_initial_load_staging/input_b.csv",
    ]
