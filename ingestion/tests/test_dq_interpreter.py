"""
common/dq_interpreter.py 단위 테스트 (#217)

OpenAI API는 실제로 호출하지 않고 client.chat.completions.create를 모킹한다.
"""
import json
from unittest.mock import MagicMock, patch

from pyiceberg.exceptions import NoSuchTableError

from common.dq_interpreter import build_prompt, fetch_history, interpret


def test_fetch_history_returns_empty_when_table_missing():
    mock_catalog = MagicMock()
    mock_catalog.load_table.side_effect = NoSuchTableError

    history = fetch_history(mock_catalog, "rental_history", "2026-08-24")

    assert history == []


def test_fetch_history_filters_by_source_and_date_range():
    mock_arrow = MagicMock()
    mock_arrow.__len__.return_value = 1
    mock_arrow.to_pylist.return_value = [{"check_name": "sex_cd_null_rate", "execution_date": "2026-08-20"}]

    mock_scan = MagicMock()
    mock_scan.to_arrow.return_value = mock_arrow

    mock_table = MagicMock()
    mock_table.scan.return_value = mock_scan

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    history = fetch_history(mock_catalog, "rental_history", "2026-08-24", lookback_days=14)

    assert history == [{"check_name": "sex_cd_null_rate", "execution_date": "2026-08-20"}]
    mock_table.scan.assert_called_once()


def test_build_prompt_includes_current_run_and_sorted_history():
    prompt = build_prompt(
        source_name="rental_history",
        execution_date="2026-08-24",
        lookback_days=14,
        current_run=[{"check_name": "sex_cd_null_rate", "metric_value": 0.4}],
        history=[
            {"check_name": "sex_cd_null_rate", "execution_date": "2026-08-22", "metric_value": 0.23},
            {"check_name": "sex_cd_null_rate", "execution_date": "2026-08-10", "metric_value": 0.21},
        ],
    )

    assert "rental_history" in prompt
    assert "2026-08-24" in prompt
    assert prompt.index("2026-08-10") < prompt.index("2026-08-22")  # 오래된 순 정렬


def test_interpret_parses_json_response():
    fake_message = MagicMock()
    fake_message.content = json.dumps(
        {
            "source_name": "rental_history",
            "execution_date": "2026-08-24",
            "checks": [],
            "overall_severity": "info",
        }
    )
    fake_response = MagicMock()
    fake_response.choices = [MagicMock(message=fake_message)]

    fake_client = MagicMock()
    fake_client.chat.completions.create.return_value = fake_response
    fake_openai = MagicMock()
    fake_openai.OpenAI.return_value = fake_client

    with patch.dict("sys.modules", {"openai": fake_openai}):
        result = interpret(
            source_name="rental_history",
            execution_date="2026-08-24",
            current_run=[],
            history=[],
        )

    assert result["overall_severity"] == "info"
    fake_client.chat.completions.create.assert_called_once()
