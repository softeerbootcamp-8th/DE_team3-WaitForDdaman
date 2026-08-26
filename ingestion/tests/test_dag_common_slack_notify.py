"""
dag_common.notify_slack_on_failure 테스트 (Issue #180)

dag_common.py는 Airflow 본체에 의존하지 않는 순수 모듈이라, airflow 패키지가
설치되지 않은 환경(이 저장소의 로컬 venv 포함)에서도 airflow/dags를 sys.path에
얹어 직접 import해서 테스트할 수 있다. DagBag 기반 DAG 구조 테스트와 달리
Airflow 본체가 필요 없다.
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

DAGS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
)
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

import dag_common  # noqa: E402


def _context(dag_id="my_dag", task_id="my_task", ds="2026-08-23", log_url="http://airflow/log"):
    ti = MagicMock()
    ti.dag_id = dag_id
    ti.task_id = task_id
    ti.log_url = log_url
    return {"task_instance": ti, "ds": ds}


def test_default_args_wires_the_callback():
    assert dag_common.DEFAULT_ARGS["on_failure_callback"] is dag_common.notify_slack_on_failure


def test_missing_webhook_url_is_a_noop(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    with patch("dag_common.requests.post") as mock_post:
        dag_common.notify_slack_on_failure(_context())
    mock_post.assert_not_called()


def test_posts_dag_and_task_info_to_webhook(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    with patch("dag_common.requests.post") as mock_post:
        dag_common.notify_slack_on_failure(
            _context(dag_id="bronze_daily_batch_all_sources", task_id="daily_batch_station_master")
        )

    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == "https://hooks.slack.com/services/T/B/X"
    text = kwargs["json"]["text"]
    assert "bronze_daily_batch_all_sources.daily_batch_station_master" in text
    assert "2026-08-23" in text
    assert kwargs["timeout"] == 10


def test_webhook_request_failure_is_swallowed(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    with patch("dag_common.requests.post", side_effect=dag_common.requests.RequestException("boom")):
        dag_common.notify_slack_on_failure(_context())  # 예외가 올라오면 안 된다


def test_replaces_localhost_with_airflow_base_url(monkeypatch):
    """AIRFLOW_BASE_URL이 설정된 경우 localhost:8080 로그 URL을 실제 base URL로 치환한다."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setenv("AIRFLOW_BASE_URL", "http://ec2-1-2-3-4.compute.amazonaws.com:8080")

    with patch("dag_common.requests.post") as mock_post:
        dag_common.notify_slack_on_failure(
            _context(
                dag_id="my_dag",
                task_id="my_task",
                log_url="http://localhost:8080/dags/my_dag/runs/scheduled__2026-08-25/tasks/my_task",
            )
        )

    mock_post.assert_called_once()
    text = mock_post.call_args[1]["json"]["text"]
    assert "http://ec2-1-2-3-4.compute.amazonaws.com:8080/dags/my_dag/runs/scheduled__2026-08-25/tasks/my_task" in text
    assert "localhost:8080" not in text


@pytest.mark.parametrize(
    "module_name",
    [
        "bikeman_event_generator_dag",
        "gold_to_serving_sync_dag",
        "gold_dim_fact_dag",
        "gold_maintenance_dag",
        "gold_risk_decision_dag",
        "bronze_initial_load_all_sources_dag",
        "risk_model_train_dag",
        "set_watermark_dag",
    ],
)
def test_dag_files_wire_the_shared_callback(module_name):
    """개별 default_args를 쓰는 DAG들이 dag_common의 공유 콜백을 참조하는지 소스로 확인.

    실제 import는 각 DAG 모듈이 Airflow 본체/다른 무거운 의존성을 필요로 해서
    (예: pipeline/*.jobs, DB 연결) 이 테스트 범위에서는 부담이 크다 - 소스 텍스트
    검사로 "중복 함수 재도입" 회귀만 잡는다.
    """
    path = os.path.join(DAGS_DIR, f"{module_name}.py")
    with open(path, encoding="utf-8") as f:
        source = f.read()
    assert "notify_slack_on_failure" in source
    assert "_notify_slack_on_failure" not in source  # 예전 중복 정의가 되살아나지 않았는지
