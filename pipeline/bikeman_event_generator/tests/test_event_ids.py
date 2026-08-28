from event_ids import make_event_id


def test_same_inputs_produce_same_id():
    id1 = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    id2 = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    assert id1 == id2


def test_different_event_type_produces_different_id():
    collect_id = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    deploy_id = make_event_id("SPB-12345", "DEPLOY", "2026-08-18")
    assert collect_id != deploy_id


def test_different_date_produces_different_id():
    id1 = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    id2 = make_event_id("SPB-12345", "COLLECT", "2026-08-19")
    assert id1 != id2


def test_different_bike_produces_different_id():
    id1 = make_event_id("SPB-12345", "COLLECT", "2026-08-18")
    id2 = make_event_id("SPB-99999", "COLLECT", "2026-08-18")
    assert id1 != id2
