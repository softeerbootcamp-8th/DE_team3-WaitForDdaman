"""
s3_utils 테스트

NOTE: test_watermark.py와 같은 이유로 config.SETTINGS.env를 "aws"로 교체해 moto의
가상 AWS를 쓴다 (moto는 커스텀 endpoint_url을 가로채지 못함).

리전을 us-east-1이 아닌 값으로 두는 게 중요하다. 실제 S3는 us-east-1에서만 같은 소유자의
재생성을 200으로 통과시키고, 다른 리전에서는 BucketAlreadyOwnedByYou를 던진다.
운영/로컬 모두 ap-northeast-2를 쓰므로 그 동작을 재현해야 한다.
"""
import pytest
from moto import mock_aws

import config as config_module

BUCKET = "test-race-bucket"


@pytest.fixture
def s3_env(monkeypatch):
    test_settings = config_module.Settings(
        env="aws",
        raw_bucket=BUCKET,
        s3_region="ap-northeast-2",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)
    with mock_aws():
        yield


class _StaleListClient:
    """
    list_buckets만 "아직 버킷이 없다"고 답하는 클라이언트.

    ensure_bucket이 목록을 확인한 뒤 create_bucket을 호출하기 전에 다른 프로세스가
    같은 버킷을 만들어버린 상황을 결정적으로 재현한다. 실제로는 Bronze 일 배치의
    4개 태스크가 병렬로 뜨면서 밀리초 단위로 겹쳐 발생했다.
    """

    def __init__(self, inner):
        self._inner = inner

    def list_buckets(self, *args, **kwargs):
        return {"Buckets": []}

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_ensure_bucket_creates_when_missing(s3_env):
    from common.s3_utils import ensure_bucket, get_s3_client

    ensure_bucket(BUCKET)

    names = [b["Name"] for b in get_s3_client().list_buckets()["Buckets"]]
    assert BUCKET in names


def test_ensure_bucket_is_idempotent(s3_env):
    from common.s3_utils import ensure_bucket

    ensure_bucket(BUCKET)
    ensure_bucket(BUCKET)  # 두 번째 호출은 목록 확인에서 걸러져 아무 일도 하지 않는다


def test_ensure_bucket_survives_concurrent_creation(s3_env, monkeypatch):
    """
    다른 프로세스가 목록 확인과 생성 사이에 같은 버킷을 만들어도 실패하면 안 된다.

    "이미 내가 소유한 버킷이 있다"는 응답은 ensure_bucket의 목적(버킷이 존재하게 만든다)
    에서 보면 실패가 아니라 성공이다.
    """
    from common import s3_utils

    s3_utils.ensure_bucket(BUCKET)  # 경쟁자가 먼저 만들어둔 상태

    real_client = s3_utils.get_s3_client()
    monkeypatch.setattr(s3_utils, "get_s3_client", lambda: _StaleListClient(real_client))

    s3_utils.ensure_bucket(BUCKET)  # 여기서 BucketAlreadyOwnedByYou가 터지면 안 된다


def test_upload_file_if_changed_uploads_when_key_missing(s3_env, tmp_path):
    from common import s3_utils

    s3_utils.ensure_bucket(BUCKET)
    local = tmp_path / "a.csv"
    local.write_bytes(b"hello")

    uploaded = s3_utils.upload_file_if_changed(local, BUCKET, "raw/staging/a.csv")

    assert uploaded is True
    body = s3_utils.get_s3_client().get_object(Bucket=BUCKET, Key="raw/staging/a.csv")["Body"].read()
    assert body == b"hello"


def test_upload_file_if_changed_skips_when_content_unchanged(s3_env, tmp_path, monkeypatch):
    from common import s3_utils

    s3_utils.ensure_bucket(BUCKET)
    local = tmp_path / "a.csv"
    local.write_bytes(b"hello")

    first = s3_utils.upload_file_if_changed(local, BUCKET, "raw/staging/a.csv")
    assert first is True

    real_client = s3_utils.get_s3_client()
    upload_calls = []
    orig_upload_file = real_client.upload_file

    def spying_upload_file(*args, **kwargs):
        upload_calls.append((args, kwargs))
        return orig_upload_file(*args, **kwargs)

    monkeypatch.setattr(real_client, "upload_file", spying_upload_file)
    monkeypatch.setattr(s3_utils, "get_s3_client", lambda: real_client)

    second = s3_utils.upload_file_if_changed(local, BUCKET, "raw/staging/a.csv")

    assert second is False
    assert upload_calls == []  # 내용이 같으면 두 번째 호출은 실제 업로드를 하지 않아야 함


def test_upload_file_if_changed_reuploads_when_content_differs(s3_env, tmp_path):
    from common import s3_utils

    s3_utils.ensure_bucket(BUCKET)
    local = tmp_path / "a.csv"
    local.write_bytes(b"hello")
    s3_utils.upload_file_if_changed(local, BUCKET, "raw/staging/a.csv")

    local.write_bytes(b"changed-content")  # 로컬 원본이 재다운로드 등으로 갱신된 상황을 재현
    uploaded = s3_utils.upload_file_if_changed(local, BUCKET, "raw/staging/a.csv")

    assert uploaded is True
    body = s3_utils.get_s3_client().get_object(Bucket=BUCKET, Key="raw/staging/a.csv")["Body"].read()
    assert body == b"changed-content"


def test_list_keys_returns_only_matching_prefix_in_sorted_order(s3_env):
    from common import s3_utils

    s3_utils.ensure_bucket(BUCKET)
    s3_utils.put_json(BUCKET, "raw/rental/observed_at=0600/manifest.json", {"n": 2})
    s3_utils.put_json(BUCKET, "raw/rental/observed_at=0500/manifest.json", {"n": 1})
    s3_utils.put_json(BUCKET, "raw/failure/observed_at=0500/manifest.json", {"n": 3})

    assert s3_utils.list_keys(BUCKET, "raw/rental/") == [
        "raw/rental/observed_at=0500/manifest.json",
        "raw/rental/observed_at=0600/manifest.json",
    ]
