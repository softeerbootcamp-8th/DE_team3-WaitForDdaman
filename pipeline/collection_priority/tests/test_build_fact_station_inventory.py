"""
gold.fact_station_inventory 핵심 로직 테스트 (#170)

Iceberg 카탈로그를 직접 읽는 부분은 여기서 테스트하지 않는다. 대신 PyArrow Table만으로
동작하는 순수 함수 셋(_merge_bike_last_action / _resolve_bike_station /
_aggregate_station_inventory)만 검증한다 -
staging/tests/test_transform_silver_rental_history.py와 동일한 DuckDB 기반 패턴.
"""
from datetime import datetime, timezone

import pyarrow as pa

from gold.build_fact_station_inventory import (
    _aggregate_station_inventory,
    _dedup_by,
    _merge_bike_last_action,
    _resolve_bike_station,
)

SNAPSHOT_DATE = "2026-08-17"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _table(spec: dict) -> pa.Table:
    return pa.table(spec)


def bike_last_action_baseline(rows: list[tuple]) -> pa.Table:
    return _table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "base_event_type": pa.array([r[1] for r in rows], type=pa.string()),
            "base_station_id": pa.array([r[2] for r in rows], type=pa.string()),
            "base_at": pa.array([r[3] for r in rows], type=pa.timestamp("us", tz="UTC")),
        }
    )


def bike_last_action_delta(rows: list[tuple]) -> pa.Table:
    return _table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "delta_event_type": pa.array([r[1] for r in rows], type=pa.string()),
            "delta_station_id": pa.array([r[2] for r in rows], type=pa.string()),
            "delta_at": pa.array([r[3] for r in rows], type=pa.timestamp("us", tz="UTC")),
        }
    )


def bike_location(rows: list[tuple]) -> pa.Table:
    return _table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "last_station_id": pa.array([r[1] for r in rows], type=pa.string()),
            "last_event_at": pa.array([r[2] for r in rows], type=pa.timestamp("us", tz="UTC")),
        }
    )


def latest_action(rows: list[tuple]) -> pa.Table:
    return _table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "action_event_type": pa.array([r[1] for r in rows], type=pa.string()),
            "action_station_id": pa.array([r[2] for r in rows], type=pa.string()),
            "action_at": pa.array([r[3] for r in rows], type=pa.timestamp("us", tz="UTC")),
        }
    )


def resolved(rows: list[tuple]) -> pa.Table:
    return _table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "effective_station_id": pa.array([r[1] for r in rows], type=pa.string()),
            "excluded": pa.array([r[2] for r in rows], type=pa.bool_()),
        }
    )


def station_active(rows: list[tuple]) -> pa.Table:
    return _table(
        {
            "station_id": pa.array([r[0] for r in rows], type=pa.string()),
            "hold_num": pa.array([r[1] for r in rows], type=pa.int32()),
        }
    )


def by_key(table: pa.Table, key: str) -> dict:
    return {r[key]: r for r in table.to_pylist()}


# ---------------------------------------------------------- _merge_bike_last_action


def test_bike_last_action_delta_wins_when_newer():
    baseline = bike_last_action_baseline([("B1", "DEPLOY", "ST-1", utc(2026, 8, 15, 9, 0))])
    delta = bike_last_action_delta([("B1", "COLLECT", "ST-2", utc(2026, 8, 16, 9, 0))])

    result = by_key(_merge_bike_last_action(baseline, delta, SNAPSHOT_DATE), "bike_id")

    assert result["B1"]["action_event_type"] == "COLLECT"
    assert result["B1"]["action_station_id"] == "ST-2"


def test_bike_last_action_baseline_wins_when_delta_not_newer():
    baseline = bike_last_action_baseline([("B1", "DEPLOY", "ST-1", utc(2026, 8, 16, 9, 0))])
    delta = bike_last_action_delta([("B1", "COLLECT", "ST-2", utc(2026, 8, 15, 9, 0))])

    result = by_key(_merge_bike_last_action(baseline, delta, SNAPSHOT_DATE), "bike_id")

    assert result["B1"]["action_event_type"] == "DEPLOY"
    assert result["B1"]["action_station_id"] == "ST-1"


def test_bike_last_action_carry_forward_without_delta():
    baseline = bike_last_action_baseline([("B1", "DEPLOY", "ST-1", utc(2026, 8, 15, 9, 0))])
    delta = bike_last_action_delta([])

    result = by_key(_merge_bike_last_action(baseline, delta, SNAPSHOT_DATE), "bike_id")

    assert result["B1"]["action_station_id"] == "ST-1"


# --------------------------------------------------------------- _resolve_bike_station


def test_resolve_uses_bike_location_when_no_action():
    bl = bike_location([("B1", "ST-1", utc(2026, 8, 15, 9, 0))])
    la = latest_action([])

    result = by_key(_resolve_bike_station(bl, la), "bike_id")

    assert result["B1"]["effective_station_id"] == "ST-1"
    assert result["B1"]["excluded"] is False


def test_resolve_uses_bike_location_when_action_is_older():
    bl = bike_location([("B1", "ST-1", utc(2026, 8, 16, 9, 0))])
    la = latest_action([("B1", "COLLECT", None, utc(2026, 8, 15, 9, 0))])

    result = by_key(_resolve_bike_station(bl, la), "bike_id")

    assert result["B1"]["effective_station_id"] == "ST-1"
    assert result["B1"]["excluded"] is False


def test_resolve_newest_collect_excludes_bike():
    bl = bike_location([("B1", "ST-1", utc(2026, 8, 15, 9, 0))])
    la = latest_action([("B1", "COLLECT", None, utc(2026, 8, 16, 9, 0))])

    result = by_key(_resolve_bike_station(bl, la), "bike_id")

    assert result["B1"]["effective_station_id"] is None
    assert result["B1"]["excluded"] is True


def test_resolve_newest_deploy_overwrites_station():
    bl = bike_location([("B1", "ST-1", utc(2026, 8, 15, 9, 0))])
    la = latest_action([("B1", "DEPLOY", "ST-9", utc(2026, 8, 16, 9, 0))])

    result = by_key(_resolve_bike_station(bl, la), "bike_id")

    assert result["B1"]["effective_station_id"] == "ST-9"
    assert result["B1"]["excluded"] is False


def test_resolve_deploy_without_station_falls_back_to_bike_location():
    """DEPLOY인데 station_id가 없는 이례적 케이스 - bike_location 값으로 폴백."""
    bl = bike_location([("B1", "ST-1", utc(2026, 8, 15, 9, 0))])
    la = latest_action([("B1", "DEPLOY", None, utc(2026, 8, 16, 9, 0))])

    result = by_key(_resolve_bike_station(bl, la), "bike_id")

    assert result["B1"]["effective_station_id"] == "ST-1"
    assert result["B1"]["excluded"] is False


# --------------------------------------------------------- _aggregate_station_inventory


def test_aggregate_station_with_no_bikes_gets_zero():
    res = resolved([])
    sa = station_active([("ST-1", 10)])

    result = by_key(_aggregate_station_inventory(res, sa, SNAPSHOT_DATE), "station_id")

    assert result["ST-1"]["bike_cnt"] == 0
    assert result["ST-1"]["target_bike_cnt"] == 10


def test_aggregate_counts_only_non_excluded_bikes_with_station():
    res = resolved(
        [
            ("B1", "ST-1", False),
            ("B2", "ST-1", False),
            ("B3", "ST-1", True),  # excluded(COLLECT) - 집계 제외
            ("B4", None, False),  # 대여소 없음(노상 방치) - 집계 제외
        ]
    )
    sa = station_active([("ST-1", 10)])

    result = by_key(_aggregate_station_inventory(res, sa, SNAPSHOT_DATE), "station_id")

    assert result["ST-1"]["bike_cnt"] == 2


def test_aggregate_snapshot_date_is_set():
    res = resolved([])
    sa = station_active([("ST-1", 10)])

    result = by_key(_aggregate_station_inventory(res, sa, SNAPSHOT_DATE), "station_id")

    assert result["ST-1"]["snapshot_date"].isoformat() == SNAPSHOT_DATE


def test_dedup_by_keeps_one_row_per_key():
    """has_uniqueness(threshold=0.99) 하드 게이트가 1%까지는 통과시키는 위험을
    쓰기 전에 미리 제거한다(#332 PR 리뷰) - bike_last_action(bike_id)/
    fact_station_inventory(station_id) 둘 다 이 함수 하나를 공유해서 쓴다."""
    table = pa.table({
        "station_id": ["ST-1", "ST-1", "ST-2"],
        "bike_cnt": [3, 5, 1],
    })

    result = _dedup_by(table, "station_id")

    assert len(result) == 2
    assert sorted(result["station_id"].to_pylist()) == ["ST-1", "ST-2"]
