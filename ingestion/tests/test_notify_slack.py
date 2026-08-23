"""
SNS(raw_fetch_alerts) -> Slack 웹훅 전달 Lambda 테스트 (Issue #180)
"""
import json
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from infra.lambdas.notify_slack.lambda_function import lambda_handler

WEBHOOK_URL = "https://hooks.slack.com/services/T000/B000/xxxxxxxx"

ALARM_MESSAGE = json.dumps({
    "AlarmName": "fetch_station_master_raw_errors",
    "NewStateValue": "ALARM",
    "NewStateReason": "Threshold Crossed: 1 datapoint [1.0] was greater than or equal to the threshold (1.0).",
    "StateChangeTime": "2026-08-23T01:23:45.000+0000",
    "Trigger": {"MetricName": "Errors", "Namespace": "AWS/Lambda"},
})


def _sns_event(message: str, subject: str = "ALARM: fetch_station_master_raw_errors") -> dict:
    return {"Records": [{"Sns": {"Message": message, "Subject": subject}}]}


def test_missing_webhook_url_raises(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    with pytest.raises(ValueError):
        lambda_handler(_sns_event(ALARM_MESSAGE), None)


def test_alarm_message_posted_to_webhook(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", WEBHOOK_URL)

    mock_resp = MagicMock()
    mock_resp.getcode.return_value = 200
    mock_urlopen = MagicMock()
    mock_urlopen.return_value.__enter__.return_value = mock_resp

    with patch("infra.lambdas.notify_slack.lambda_function.urllib.request.urlopen", mock_urlopen):
        result = lambda_handler(_sns_event(ALARM_MESSAGE), None)

    assert result == {"statusCode": 200, "notified": 1}

    sent_request = mock_urlopen.call_args[0][0]
    assert sent_request.full_url == WEBHOOK_URL
    sent_payload = json.loads(sent_request.data.decode("utf-8"))
    assert "fetch_station_master_raw_errors" in sent_payload["text"]
    assert "ALARM" in sent_payload["text"]


def test_null_trigger_field_does_not_crash(monkeypatch):
    """Trigger가 명시적 null이면 alarm.get('Trigger', {})는 기본값 대신 None을 반환해
    trigger.get(...)에서 AttributeError가 났었다 - or {} 로 방어한다."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", WEBHOOK_URL)
    message = json.dumps({
        "AlarmName": "some-alarm",
        "NewStateValue": "ALARM",
        "NewStateReason": "reason",
        "StateChangeTime": "2026-08-23T01:23:45.000+0000",
        "Trigger": None,
    })

    mock_resp = MagicMock()
    mock_resp.getcode.return_value = 200
    mock_urlopen = MagicMock()
    mock_urlopen.return_value.__enter__.return_value = mock_resp

    with patch("infra.lambdas.notify_slack.lambda_function.urllib.request.urlopen", mock_urlopen):
        result = lambda_handler(_sns_event(message), None)

    assert result == {"statusCode": 200, "notified": 1}


def test_non_json_message_falls_back_to_raw_text(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", WEBHOOK_URL)

    mock_resp = MagicMock()
    mock_resp.getcode.return_value = 200
    mock_urlopen = MagicMock()
    mock_urlopen.return_value.__enter__.return_value = mock_resp

    with patch("infra.lambdas.notify_slack.lambda_function.urllib.request.urlopen", mock_urlopen):
        result = lambda_handler(_sns_event("plain text alert", subject="manual-publish"), None)

    assert result == {"statusCode": 200, "notified": 1}
    sent_request = mock_urlopen.call_args[0][0]
    sent_payload = json.loads(sent_request.data.decode("utf-8"))
    assert "plain text alert" in sent_payload["text"]


def test_webhook_http_error_raises(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", WEBHOOK_URL)

    mock_urlopen = MagicMock(side_effect=urllib.error.URLError("connection refused"))

    with patch("infra.lambdas.notify_slack.lambda_function.urllib.request.urlopen", mock_urlopen):
        with pytest.raises(RuntimeError):
            lambda_handler(_sns_event(ALARM_MESSAGE), None)


def test_empty_records_returns_zero_notified(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", WEBHOOK_URL)
    result = lambda_handler({"Records": []}, None)
    assert result == {"statusCode": 200, "notified": 0}


def test_one_failing_record_does_not_block_other_records(monkeypatch):
    """한 레코드가 실패해도 나머지 레코드는 계속 전송돼야 한다 (재시도 시 중복 발송 방지)."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", WEBHOOK_URL)

    ok_resp = MagicMock()
    ok_resp.getcode.return_value = 200
    ok_cm = MagicMock()
    ok_cm.__enter__.return_value = ok_resp

    mock_urlopen = MagicMock(
        side_effect=[ok_cm, urllib.error.URLError("connection refused")]
    )

    event = {
        "Records": [
            {"Sns": {"Message": ALARM_MESSAGE, "Subject": "ok-alarm"}},
            {"Sns": {"Message": ALARM_MESSAGE, "Subject": "bad-alarm"}},
        ]
    }

    with patch("infra.lambdas.notify_slack.lambda_function.urllib.request.urlopen", mock_urlopen):
        with pytest.raises(RuntimeError):
            lambda_handler(event, None)

    assert mock_urlopen.call_count == 2
