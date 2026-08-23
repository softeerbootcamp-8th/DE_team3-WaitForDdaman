"""
gold.bike_location 증분 병합(baseline+delta) 로직 테스트 (#170)

_baseline()/_delta()는 Iceberg 카탈로그를 직접 읽으므로 여기서는 테스트하지 않는다.
대신 두 PyArrow Table만으로 동작하는 순수 함수 _merge_baseline_delta()만 검증한다 -
staging/tests/test_transform_silver_rental_history.py와 동일한 DuckDB 기반 패턴.
"""
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa

from jobs.build_bike_location import COLD_START_LOOKBACK_DAYS, _effective_delta_start, _merge_baseline_delta

SNAPSHOT_DATE = "2026-08-17"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def baseline_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "base_station_id": pa.array([r[1] for r in rows], type=pa.string()),
            "base_event_at": pa.array([r[2] for r in rows], type=pa.timestamp("us", tz="UTC")),
        }
    )


def delta_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "delta_station_id": pa.array([r[1] for r in rows], type=pa.string()),
            "delta_event_at": pa.array([r[2] for r in rows], type=pa.timestamp("us", tz="UTC")),
        }
    )


def by_bike(table: pa.Table) -> dict:
    return {r["bike_id"]: r for r in table.to_pylist()}


def test_cold_start_uses_delta_when_baseline_empty():
    """최초 실행(gold.bike_location이 비어있음) - baseline 없이 delta만으로 채워진다."""
    baseline = baseline_table([])
    delta = delta_table([("B1", "ST-1", utc(2026, 8, 16, 9, 0))])

    result = by_bike(_merge_baseline_delta(baseline, delta, SNAPSHOT_DATE))

    assert result["B1"]["last_station_id"] == "ST-1"
    assert result["B1"]["snapshot_date"].isoformat() == SNAPSHOT_DATE


def test_carry_forward_when_no_delta_activity():
    """오늘 활동이 없는 자전거는 baseline 값을 그대로 유지한다."""
    baseline = baseline_table([("B1", "ST-1", utc(2026, 8, 15, 9, 0))])
    delta = delta_table([])

    result = by_bike(_merge_baseline_delta(baseline, delta, SNAPSHOT_DATE))

    assert result["B1"]["last_station_id"] == "ST-1"
    assert result["B1"]["last_event_at"] == utc(2026, 8, 15, 9, 0)


def test_delta_wins_when_strictly_newer():
    """baseline/delta 둘 다 있으면 더 최신 이벤트(반납 시각)를 채택한다."""
    baseline = baseline_table([("B1", "ST-1", utc(2026, 8, 15, 9, 0))])
    delta = delta_table([("B1", "ST-2", utc(2026, 8, 16, 9, 0))])

    result = by_bike(_merge_baseline_delta(baseline, delta, SNAPSHOT_DATE))

    assert result["B1"]["last_station_id"] == "ST-2"
    assert result["B1"]["last_event_at"] == utc(2026, 8, 16, 9, 0)


def test_baseline_wins_when_delta_not_newer():
    """
    같은 날 재실행되거나 백필로 날짜 순서가 어긋나면 delta가 baseline보다 과거일
    수 있다 - 이 경우 과거 데이터로 최신 상태를 덮어쓰면 안 된다(idempotent).
    """
    baseline = baseline_table([("B1", "ST-2", utc(2026, 8, 16, 9, 0))])
    delta = delta_table([("B1", "ST-1", utc(2026, 8, 15, 9, 0))])

    result = by_bike(_merge_baseline_delta(baseline, delta, SNAPSHOT_DATE))

    assert result["B1"]["last_station_id"] == "ST-2"
    assert result["B1"]["last_event_at"] == utc(2026, 8, 16, 9, 0)


def test_null_station_id_from_delta_means_not_at_any_station():
    """대여소가 아닌 곳에 반납된 경우(노상 방치) - null이 결측이 아니라 유효값이다."""
    baseline = baseline_table([("B1", "ST-1", utc(2026, 8, 15, 9, 0))])
    delta = delta_table([("B1", None, utc(2026, 8, 16, 9, 0))])

    result = by_bike(_merge_baseline_delta(baseline, delta, SNAPSHOT_DATE))

    assert result["B1"]["last_station_id"] is None
    assert result["B1"]["last_event_at"] == utc(2026, 8, 16, 9, 0)


def test_effective_delta_start_uses_baseline_when_present():
    """baseline이 있으면(정상 증분) lookback 안 걸고 그대로 쓴다."""
    start = _effective_delta_start(date(2026, 8, 1), date(2026, 8, 17))
    assert start == date(2026, 8, 1)


def test_effective_delta_start_applies_lookback_on_cold_start():
    """#147: cold start(baseline 없음)는 전체 스캔 대신 최근 N일만 본다."""
    end = date(2026, 8, 17)
    start = _effective_delta_start(None, end)
    assert start == end - timedelta(days=COLD_START_LOOKBACK_DAYS - 1)
    assert (end - start).days == COLD_START_LOOKBACK_DAYS - 1


def test_multiple_bikes_are_independent():
    baseline = baseline_table([("B1", "ST-1", utc(2026, 8, 15, 9, 0))])
    delta = delta_table([("B2", "ST-2", utc(2026, 8, 16, 9, 0))])

    result = by_bike(_merge_baseline_delta(baseline, delta, SNAPSHOT_DATE))

    assert result["B1"]["last_station_id"] == "ST-1"
    assert result["B2"]["last_station_id"] == "ST-2"
