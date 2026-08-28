"""
COLLECT/DEPLOY 이벤트 dict 생성.

occurred_at/received_at은 완전히 결정론적인 고정 시각을 쓴다(target_date 09:00/09:15) -
시드 데이터처럼 하루 중 무작위 분산은 재실행 시 동일 결과를 보장해야 한다는 요구사항과
맞지 않는다. worker_id만 호출부(generate_collect_events.py/deploy_returned_bikes.py)에서
매 실행 랜덤으로 골라 넘긴다 - event_id가 이미 (bike_id, event_type, target_date) 기반
결정론적 키라 worker_id가 실행마다 달라져도 재실행 시 중복 삽입은 발생하지 않는다.
"""
from datetime import datetime, timedelta

from event_ids import make_event_id

WORKER_POOL = [f"worker_{i:04d}" for i in range(1, 21)]

OCCURRED_HOUR = 9
RECEIVED_DELAY_MINUTES = 15


def build_collect_event(bike_id: str, station_id: str | None, target_date: str, worker_id: str) -> dict:
    return _build_event(bike_id, "COLLECT", station_id, target_date, worker_id)


def build_deploy_event(bike_id: str, station_id: str | None, target_date: str, worker_id: str) -> dict:
    return _build_event(bike_id, "DEPLOY", station_id, target_date, worker_id)


def _build_event(bike_id: str, event_type: str, station_id: str | None, target_date: str, worker_id: str) -> dict:
    occurred_at = datetime.strptime(target_date, "%Y-%m-%d").replace(hour=OCCURRED_HOUR, minute=0, second=0)
    received_at = occurred_at + timedelta(minutes=RECEIVED_DELAY_MINUTES)
    return {
        "event_id": str(make_event_id(bike_id, event_type, target_date)),
        "event_type": event_type,
        "bike_id": bike_id,
        "station_id": station_id,
        "worker_id": worker_id,
        "occurred_at": occurred_at,
        "received_at": received_at,
    }
