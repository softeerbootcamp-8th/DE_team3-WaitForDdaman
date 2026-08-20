from datetime import datetime

from event_builder import WORKER_POOL, build_collect_event, build_deploy_event
from event_ids import make_event_id


def test_worker_pool_has_20_workers_zero_padded():
    assert len(WORKER_POOL) == 20
    assert WORKER_POOL[0] == "worker_0001"
    assert WORKER_POOL[-1] == "worker_0020"


def test_build_collect_event_fields():
    event = build_collect_event("SPB-12345", "ST-0001", "2026-08-18", "worker_0007")
    assert event["event_type"] == "COLLECT"
    assert event["bike_id"] == "SPB-12345"
    assert event["station_id"] == "ST-0001"
    assert event["worker_id"] == "worker_0007"
    assert event["occurred_at"] == datetime(2026, 8, 18, 9, 0, 0)
    assert event["received_at"] == datetime(2026, 8, 18, 9, 15, 0)
    assert event["event_id"] == str(make_event_id("SPB-12345", "COLLECT", "2026-08-18"))


def test_build_deploy_event_fields():
    event = build_deploy_event("SPB-12345", "ST-0001", "2026-08-18", "worker_0007")
    assert event["event_type"] == "DEPLOY"
    assert event["event_id"] == str(make_event_id("SPB-12345", "DEPLOY", "2026-08-18"))


def test_build_collect_event_allows_null_station_id():
    event = build_collect_event("SPB-12345", None, "2026-08-18", "worker_0007")
    assert event["station_id"] is None


def test_worker_id_does_not_affect_event_id_or_timestamps():
    e1 = build_collect_event("SPB-12345", "ST-0001", "2026-08-18", "worker_0001")
    e2 = build_collect_event("SPB-12345", "ST-0001", "2026-08-18", "worker_0002")
    assert e1["event_id"] == e2["event_id"]
    assert e1["occurred_at"] == e2["occurred_at"]
    assert e1["received_at"] == e2["received_at"]
