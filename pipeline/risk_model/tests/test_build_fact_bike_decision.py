"""
gold.fact_bike_decision의 재고 조인 + 랭킹 기반 대여중단/보류 결정 로직 테스트 (#171)

Iceberg 카탈로그를 직접 읽는 부분은 여기서 테스트하지 않는다. 대신 세 PyArrow
Table만으로 동작하는 순수 함수 _decide_actions()만 검증한다 - build_dim_bike.py의
_compute_new_bikes()와 동일한 DuckDB 기반 패턴.
"""
from datetime import date

import pyarrow as pa

from jobs.build_fact_bike_decision import HOLD, SUSPEND, _decide_actions

SNAPSHOT_DATE = date(2026, 8, 17)


def risk_table(rows: list[tuple[str, float, str]]) -> pa.Table:
    return pa.table({
        "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
        "risk_score": pa.array([r[1] for r in rows], type=pa.float64()),
        "risk_grade": pa.array([r[2] for r in rows], type=pa.string()),
    })


def location_table(rows: list[tuple[str, str]]) -> pa.Table:
    return pa.table({
        "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
        "last_station_id": pa.array([r[1] for r in rows], type=pa.string()),
    })


def inventory_table(rows: list[tuple[str, int, int]]) -> pa.Table:
    return pa.table({
        "station_id": pa.array([r[0] for r in rows], type=pa.string()),
        "bike_cnt": pa.array([r[1] for r in rows], type=pa.int64()),
        "target_bike_cnt": pa.array([r[2] for r in rows], type=pa.int64()),
    })


def action_of(result: pa.Table, bike_id: str) -> str:
    by_bike = {r["bike_id"]: r["action"] for r in result.to_pylist()}
    return by_bike[bike_id]


def test_critical_grade_is_always_suspended():
    risk = risk_table([("SPB-001", 95.0, "Critical")])
    location = location_table([("SPB-001", "ST-1")])
    # suspendable_bike_cnt = 0 이라도 Critical은 예산 확인 없이 무조건 대여중단.
    inventory = inventory_table([("ST-1", 5, 5)])

    result = _decide_actions(risk, location, inventory, SNAPSHOT_DATE)

    assert action_of(result, "SPB-001") == SUSPEND


def test_warning_within_available_slots_is_suspended():
    risk = risk_table([("SPB-001", 80.0, "Warning")])
    location = location_table([("SPB-001", "ST-1")])
    # suspendable_bike_cnt = 10 - 5 = 5, critical_cnt = 0 -> warning_available_cnt = 5
    inventory = inventory_table([("ST-1", 10, 5)])

    result = _decide_actions(risk, location, inventory, SNAPSHOT_DATE)

    assert action_of(result, "SPB-001") == SUSPEND


def test_warning_beyond_available_slots_is_held():
    risk = risk_table([("SPB-001", 90.0, "Warning"), ("SPB-002", 80.0, "Warning")])
    location = location_table([("SPB-001", "ST-1"), ("SPB-002", "ST-1")])
    # suspendable_bike_cnt = 6 - 5 = 1 -> warning_available_cnt = 1, SPB-001만 랭킹 1위
    inventory = inventory_table([("ST-1", 6, 5)])

    result = _decide_actions(risk, location, inventory, SNAPSHOT_DATE)

    assert action_of(result, "SPB-001") == SUSPEND
    assert action_of(result, "SPB-002") == HOLD


def test_normal_grade_is_held():
    risk = risk_table([("SPB-001", 10.0, "Normal")])
    location = location_table([("SPB-001", "ST-1")])
    inventory = inventory_table([("ST-1", 10, 5)])

    result = _decide_actions(risk, location, inventory, SNAPSHOT_DATE)

    assert action_of(result, "SPB-001") == HOLD


def test_bike_without_location_is_dropped():
    risk = risk_table([("SPB-001", 95.0, "Critical"), ("SPB-002", 90.0, "Critical")])
    location = location_table([("SPB-001", "ST-1")])  # SPB-002는 위치 정보 없음
    inventory = inventory_table([("ST-1", 5, 5)])

    result = _decide_actions(risk, location, inventory, SNAPSHOT_DATE)

    assert [r["bike_id"] for r in result.to_pylist()] == ["SPB-001"]


def test_station_without_inventory_falls_back_to_hold_for_warning():
    risk = risk_table([("SPB-001", 80.0, "Warning")])
    location = location_table([("SPB-001", "ST-1")])
    inventory = inventory_table([])  # ST-1 재고 정보 없음 (운영 중단 등)

    result = _decide_actions(risk, location, inventory, SNAPSHOT_DATE)

    assert action_of(result, "SPB-001") == HOLD


def test_snapshot_date_is_the_given_target_date():
    risk = risk_table([("SPB-001", 95.0, "Critical")])
    location = location_table([("SPB-001", "ST-1")])
    inventory = inventory_table([("ST-1", 5, 5)])

    result = _decide_actions(risk, location, inventory, SNAPSHOT_DATE)

    assert result.to_pylist()[0]["snapshot_date"].isoformat() == "2026-08-17"