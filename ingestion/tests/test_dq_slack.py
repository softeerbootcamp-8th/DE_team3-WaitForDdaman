"""
common/dq_slack.py 단위 테스트 (#217)
"""
from unittest.mock import MagicMock, patch

from common.dq_slack import build_message, send_dq_alert


def test_build_message_for_new_issue():
    msg = build_message(
        source_name="rental_history", check_name="return_columns_null_rate",
        severity="warning", reasoning="결측률 급증", issue_url="https://x/1", is_new=True,
    )
    assert "신규 이슈" in msg
    assert "https://x/1" in msg
    assert ":warning:" in msg


def test_build_message_for_existing_issue_comment():
    msg = build_message(
        source_name="rental_history", check_name="return_columns_null_rate",
        severity="critical", reasoning="재발", issue_url="https://x/1", is_new=False,
    )
    assert "기존 이슈에 코멘트 추가" in msg
    assert ":rotating_light:" in msg


def test_build_message_for_github_error_case():
    msg = build_message(
        source_name="rental_history", check_name="c", severity="warning",
        reasoning="reason", issue_url=None, is_new=None, error="401 Unauthorized",
    )
    assert "GitHub 이슈 생성/코멘트 실패" in msg
    assert "401 Unauthorized" in msg


def test_send_dq_alert_posts_text_payload():
    fake_resp = MagicMock()
    with patch("common.dq_slack.requests.post", return_value=fake_resp) as mock_post:
        send_dq_alert("https://hooks.slack.example/webhook", "hello")

    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == "https://hooks.slack.example/webhook"
    assert kwargs["json"] == {"text": "hello"}
    fake_resp.raise_for_status.assert_called_once()
