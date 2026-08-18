"""
결정론적 이벤트 UUID5 생성.

같은 (bike_id, event_type, target_date) 조합이면 몇 번을 재실행해도 항상 같은
event_id가 나온다 - bikeman.fact_worker_event.event_id(PK)에 대한
INSERT ... ON CONFLICT DO NOTHING과 결합해 재실행 시 중복 삽입을 막는다.
"""
import uuid

EVENT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "bikeman.fact_worker_event")


def make_event_id(bike_id: str, event_type: str, target_date: str) -> uuid.UUID:
    return uuid.uuid5(EVENT_NAMESPACE, f"{bike_id}:{event_type}:{target_date}")
