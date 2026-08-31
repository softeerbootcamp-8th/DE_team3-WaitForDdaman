"""
jobs/interpret_dq_results.py 단위 테스트 (#217)

FAIL이 없으면 LLM 호출을 스킵하는 비용 절감 분기를 검증한다.
"""
from unittest.mock import MagicMock

import operations.interpret_dq_results as job


def _pending(results):
    return {"source_name": "rental_history", "execution_date": "2026-08-22", "results": results}


def test_skips_llm_call_when_no_fail(monkeypatch):
    pending = _pending(
        [
            {"check_name": "a", "pass_fail": "PASS"},
            {"check_name": "b", "pass_fail": "MONITOR"},
        ]
    )
    monkeypatch.setattr(job, "get_json", lambda bucket, key: pending)
    fake_interpret = MagicMock()
    monkeypatch.setattr(job, "interpret", fake_interpret)

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert result is None
    fake_interpret.assert_not_called()


def test_calls_llm_when_any_fail(monkeypatch):
    pending = _pending(
        [
            {"check_name": "a", "pass_fail": "PASS"},
            {"check_name": "b", "pass_fail": "FAIL"},
        ]
    )
    monkeypatch.setattr(job, "get_json", lambda bucket, key: pending)
    monkeypatch.setattr(job, "fetch_history", lambda *a, **k: [])
    monkeypatch.setattr(job, "build_iceberg_catalog", lambda: MagicMock())
    monkeypatch.setattr(job, "ensure_bucket", lambda bucket: None)
    monkeypatch.setattr(job, "put_json", lambda bucket, key, payload: None)
    fake_interpret = MagicMock(return_value={"overall_severity": "warning"})
    monkeypatch.setattr(job, "interpret", fake_interpret)

    result = job.run(source_name="rental_history", execution_date_str="2026-08-22")

    assert result == {"overall_severity": "warning"}
    fake_interpret.assert_called_once()
