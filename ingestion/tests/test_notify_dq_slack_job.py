"""
jobs/notify_dq_slack.py 단위 테스트 (#217)
"""
import operations.notify_dq_slack as job


def test_skips_when_no_github_issues_file(monkeypatch):
    monkeypatch.setattr(job, "get_json", lambda bucket, key: None)

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert result == 0


def test_skips_when_webhook_not_configured(monkeypatch):
    issues = [{"check_name": "a", "source_name": "rental_history", "severity": "warning",
               "reasoning": "r", "issue_url": "https://x/1", "is_new": True, "error": None}]
    monkeypatch.setattr(job, "get_json", lambda bucket, key: issues)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert result == 0


def test_sends_one_message_per_issue(monkeypatch):
    issues = [
        {"check_name": "a", "source_name": "rental_history", "severity": "warning",
         "reasoning": "r1", "issue_url": "https://x/1", "is_new": True, "error": None},
        {"check_name": "b", "source_name": "rental_history", "severity": "critical",
         "reasoning": "r2", "issue_url": "https://x/2", "is_new": False, "error": None},
    ]
    monkeypatch.setattr(job, "get_json", lambda bucket, key: issues)
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/webhook")

    sent_messages = []
    monkeypatch.setattr(job, "send_dq_alert", lambda webhook, message: sent_messages.append(message))

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert result == 2
    assert len(sent_messages) == 2


def test_send_failure_does_not_raise_and_still_reports_count(monkeypatch):
    issues = [{"check_name": "a", "source_name": "rental_history", "severity": "warning",
               "reasoning": "r", "issue_url": "https://x/1", "is_new": True, "error": None}]
    monkeypatch.setattr(job, "get_json", lambda bucket, key: issues)
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/webhook")

    def raise_err(webhook, message):
        raise RuntimeError("webhook expired")

    monkeypatch.setattr(job, "send_dq_alert", raise_err)
    monkeypatch.setattr(job, "time", type("T", (), {"sleep": staticmethod(lambda s: None)}))

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert result == 0
