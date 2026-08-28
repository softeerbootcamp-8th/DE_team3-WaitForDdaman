"""
common/dq_github.py 단위 테스트 (#217)

실제 GitHub API는 호출하지 않고 requests.get/post를 모킹한다.
"""
from unittest.mock import MagicMock, patch

import pytest

from common.dq_github import (
    AUTO_GENERATED_LABEL,
    FINGERPRINT_LABEL_PREFIX,
    compute_fingerprint,
    report_issue,
)


def test_compute_fingerprint_is_deterministic():
    a = compute_fingerprint("rental_history", "return_columns_null_rate", "return_dt")
    b = compute_fingerprint("rental_history", "return_columns_null_rate", "return_dt")
    c = compute_fingerprint("rental_history", "other_check", "return_dt")

    assert a == b
    assert a != c


def _mock_response(json_body, status_ok=True):
    resp = MagicMock()
    resp.json.return_value = json_body
    if status_ok:
        resp.raise_for_status.return_value = None
    else:
        resp.raise_for_status.side_effect = Exception("boom")
    return resp


def test_report_issue_creates_when_no_existing_open_issue():
    search_resp = _mock_response({"items": []})
    create_resp = _mock_response({"number": 42, "html_url": "https://github.com/x/y/issues/42"})

    with patch("common.dq_github.requests.get", return_value=search_resp) as mock_get, \
         patch("common.dq_github.requests.post", return_value=create_resp) as mock_post:
        result = report_issue(
            repo="x/y", token="tok", source_name="rental_history",
            check_name="return_columns_null_rate", target_column="return_dt",
            severity="warning", title="[DQ] title", body_for_new_issue="body",
            body_for_comment="comment",
        )

    assert result.is_new is True
    assert result.issue_number == 42
    mock_get.assert_called_once()
    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    assert AUTO_GENERATED_LABEL in kwargs["json"]["labels"]
    assert any(l.startswith(FINGERPRINT_LABEL_PREFIX) for l in kwargs["json"]["labels"])


def test_report_issue_comments_when_existing_open_issue_found():
    search_resp = _mock_response(
        {"items": [{"number": 7, "html_url": "https://github.com/x/y/issues/7"}]}
    )
    comment_resp = _mock_response({"id": 1})

    with patch("common.dq_github.requests.get", return_value=search_resp), \
         patch("common.dq_github.requests.post", return_value=comment_resp) as mock_post:
        result = report_issue(
            repo="x/y", token="tok", source_name="rental_history",
            check_name="return_columns_null_rate", target_column="return_dt",
            severity="warning", title="[DQ] title", body_for_new_issue="body",
            body_for_comment="comment",
        )

    assert result.is_new is False
    assert result.issue_number == 7
    mock_post.assert_called_once()
    args, _ = mock_post.call_args
    assert args[0].endswith("/repos/x/y/issues/7/comments")


def test_search_retries_once_then_raises():
    with patch("common.dq_github.requests.get", side_effect=Exception("network down")) as mock_get, \
         patch("common.dq_github.time.sleep", return_value=None):
        with pytest.raises(Exception, match="network down"):
            report_issue(
                repo="x/y", token="tok", source_name="rental_history",
                check_name="c", target_column="col", severity="warning",
                title="t", body_for_new_issue="b", body_for_comment="c",
            )

    assert mock_get.call_count == 2
