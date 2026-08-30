"""
jobs/report_dq_issue.py 단위 테스트 (#217)
"""
from unittest.mock import MagicMock

import operations.report_dq_issue as job


def _interpretation(checks):
    return {"source_name": "rental_history", "execution_date": "2026-08-22", "checks": checks}


def test_skips_when_no_interpretation_file(monkeypatch):
    monkeypatch.setattr(job, "get_json", lambda bucket, key: None)

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert result == []


def test_skips_when_no_anomalous_checks(monkeypatch):
    interp = _interpretation([{"check_name": "a", "is_anomaly": False}])
    monkeypatch.setattr(job, "get_json", lambda bucket, key: interp)

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert result == []


def test_returns_error_entries_when_github_env_missing(monkeypatch):
    interp = _interpretation(
        [{"check_name": "return_columns_null_rate", "is_anomaly": True, "severity": "warning", "reasoning": "r"}]
    )
    monkeypatch.setattr(job, "get_json", lambda bucket, key: interp)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert len(result) == 1
    assert result[0]["error"] is not None
    assert result[0]["issue_url"] is None


def test_reports_issue_and_persists_results(monkeypatch):
    interp = _interpretation(
        [{"check_name": "return_columns_null_rate", "is_anomaly": True, "severity": "warning", "reasoning": "r",
          "suggested_action": "확인"}]
    )
    pending = {"results": [{"check_name": "return_columns_null_rate", "target_column": "return_dt",
                             "metric_value": 0.0056, "threshold": 0.005, "pass_fail": "FAIL"}]}

    def fake_get_json(bucket, key):
        if "github_issues" in key:
            return None
        if "interpretation" in key:
            return interp
        return pending

    monkeypatch.setattr(job, "get_json", fake_get_json)
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("GITHUB_REPOSITORY", "x/y")
    monkeypatch.setattr(job, "build_iceberg_catalog", lambda: MagicMock())
    monkeypatch.setattr(job, "fetch_history", lambda *a, **k: [])
    monkeypatch.setattr(job, "ensure_bucket", lambda bucket: None)

    saved = {}
    monkeypatch.setattr(job, "put_json", lambda bucket, key, payload: saved.update({key: payload}))

    fake_issue = MagicMock(issue_number=1, issue_url="https://github.com/x/y/issues/1", is_new=True)
    monkeypatch.setattr(job, "report_issue", lambda **kwargs: fake_issue)

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert len(result) == 1
    assert result[0]["issue_number"] == 1
    assert result[0]["is_new"] is True
    assert result[0]["error"] is None
    assert saved  # put_json이 호출되어 github_issues 결과가 저장됨


def test_continues_and_records_error_when_report_issue_raises(monkeypatch):
    interp = _interpretation(
        [{"check_name": "return_columns_null_rate", "is_anomaly": True, "severity": "warning", "reasoning": "r"}]
    )
    pending = {"results": [{"check_name": "return_columns_null_rate", "target_column": "return_dt",
                             "metric_value": 0.0056, "threshold": 0.005, "pass_fail": "FAIL"}]}

    def fake_get_json(bucket, key):
        if "interpretation" in key:
            return interp
        return pending

    monkeypatch.setattr(job, "get_json", fake_get_json)
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("GITHUB_REPOSITORY", "x/y")
    monkeypatch.setattr(job, "build_iceberg_catalog", lambda: MagicMock())
    monkeypatch.setattr(job, "fetch_history", lambda *a, **k: [])
    monkeypatch.setattr(job, "ensure_bucket", lambda bucket: None)
    monkeypatch.setattr(job, "put_json", lambda bucket, key, payload: None)

    def raise_err(**kwargs):
        raise RuntimeError("GitHub API down")

    monkeypatch.setattr(job, "report_issue", raise_err)

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert len(result) == 1
    assert result[0]["error"] == "GitHub API down"
    assert result[0]["issue_url"] is None
