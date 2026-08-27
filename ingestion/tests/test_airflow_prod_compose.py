"""운영 Airflow 초기화 설정의 필수 pool 회귀 검증."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPO_ROOT / "docker-compose.airflow.prod.yml"


def test_airflow_init_creates_catchup_api_pools():
    """DAG가 참조하는 전용 pool은 초기화 단계에서 반드시 생성한다."""
    compose = COMPOSE_PATH.read_text()

    expected_slots = {
        "seoul_api": 4,
        "rental_history_api": 3,
        "failure_report_api": 1,
        "bronze_rental_history_commit": 1,
        "emr_initial_load": 3,
        "s3_initial_load_staging": 2,
    }

    for pool, slots in expected_slots.items():
        assert f"airflow pools set {pool} {slots}" in compose
