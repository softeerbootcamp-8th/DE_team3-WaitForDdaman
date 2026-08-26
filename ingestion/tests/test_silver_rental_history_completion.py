"""Silver 대여이력 청크 완료 marker 모듈 테스트.

marker 판정이 느슨하면 실제로는 처리되지 않은 구간이 영영 건너뛰어진다(데이터 유실).
반대로 너무 빡빡하면 이미 성공한 청크를 다시 돌 뿐이라 비용만 든다. 그래서 "확신할 수
없으면 pending"이 이 모듈의 규칙이고, 아래 테스트는 그 규칙의 경계를 고정한다.
"""
import pytest
from moto import mock_aws

import config as config_module
from common.s3_utils import ensure_bucket, put_json
from common.silver_rental_history_completion import (
    SILVER_RENTAL_HISTORY_CONTRACT_VERSION,
    build_completion_marker,
    completion_key,
    is_complete_marker,
    is_range_complete,
    read_completion_marker,
    write_completion_marker,
)

BUCKET = "test-silver-completion-bucket"
START = "2018-05-25"
END = "2018-06-24"


@pytest.fixture
def s3_env(monkeypatch):
    monkeypatch.setattr(
        config_module,
        "SETTINGS",
        config_module.Settings(env="aws", raw_bucket=BUCKET, s3_region="ap-northeast-2"),
    )
    with mock_aws():
        ensure_bucket(BUCKET)
        yield


def _marker(**overrides) -> dict:
    marker = build_completion_marker(
        range_start=START,
        range_end=END,
        bronze_watermark_at_start="2026-06-30",
        bronze_row_count=1280868,
        silver_row_count=1280232,
        quarantine_row_count=12,
        dag_run_id="manual__2026-08-26T00:00:00+00:00",
        processed_at="2026-08-26T00:00:00+00:00",
    )
    marker.update(overrides)
    return marker


def test_completion_key_layout():
    assert completion_key(START, END, 1) == (
        "_meta/completion/silver_rental_history"
        "/contract_version=1/range_start=2018-05-25/range_end=2018-06-24/completion.json"
    )


def test_completion_key_defaults_to_shared_contract_version():
    assert completion_key(START, END) == completion_key(
        START, END, SILVER_RENTAL_HISTORY_CONTRACT_VERSION
    )


def test_marker_document_shape():
    marker = _marker()
    assert marker["dataset"] == "silver_rental_history"
    assert marker["status"] == "COMPLETE"
    assert marker["contract_version"] == SILVER_RENTAL_HISTORY_CONTRACT_VERSION
    assert marker["bronze_row_count"] == 1280868
    assert marker["silver_row_count"] == 1280232
    assert marker["quarantine_row_count"] == 12


def test_complete_marker_is_reused(s3_env):
    write_completion_marker(BUCKET, _marker())
    assert is_range_complete(BUCKET, START, END) is True


def test_missing_marker_is_pending(s3_env):
    assert read_completion_marker(BUCKET, START, END) is None
    assert is_range_complete(BUCKET, START, END) is False


def test_different_dag_run_id_still_matches(s3_env):
    """새 DAG Run이 이전 Run의 성공 청크를 재사용하지 못하면 marker의 목적이 사라진다."""
    write_completion_marker(BUCKET, _marker(dag_run_id="manual__2020-01-01T00:00:00+00:00"))
    assert is_range_complete(BUCKET, START, END) is True


def test_other_contract_version_is_pending(s3_env):
    write_completion_marker(BUCKET, _marker(contract_version=1))
    assert is_range_complete(BUCKET, START, END, contract_version=2) is False


def test_corrupt_document_is_pending(s3_env):
    put_json(BUCKET, completion_key(START, END), ["not", "a", "marker"])
    assert is_range_complete(BUCKET, START, END) is False


def test_status_mismatch_is_pending(s3_env):
    put_json(BUCKET, completion_key(START, END), _marker(status="FAILED"))
    assert is_range_complete(BUCKET, START, END) is False


def test_range_mismatch_inside_document_is_pending(s3_env):
    """key는 맞는데 문서 안 범위가 다른 경우 - 완료로 착각하면 그 구간이 영영 안 돈다."""
    put_json(BUCKET, completion_key(START, END), _marker(range_end="2018-06-30"))
    assert is_range_complete(BUCKET, START, END) is False


@pytest.mark.parametrize("marker", [None, "COMPLETE", {}, {"status": "COMPLETE"}])
def test_is_complete_marker_rejects_non_marker_values(marker):
    assert is_complete_marker(marker, START, END, 1) is False
