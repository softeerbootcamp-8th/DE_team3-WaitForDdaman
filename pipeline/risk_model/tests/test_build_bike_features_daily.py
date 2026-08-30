"""
gold.bike_features_daily의 SUSPEND 당일 대여 제외 로직 테스트 (#171)

Iceberg 카탈로그를 직접 읽는 부분은 여기서 테스트하지 않는다. 대신 두 PyArrow
Table만으로 동작하는 순수 함수 _exclude_suspended_rental_days()만 검증한다 -
build_dim_bike.py의 _compute_new_bikes()와 동일한 DuckDB 기반 패턴.
"""
from datetime import date, datetime, timezone

import pyarrow as pa
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from gold.build_bike_features_daily import (
    _dedup_by_bike_id,
    _exclude_suspended_rental_days,
    _parse_bool_env,
    _rental_scan_row_filter,
)


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def rent_table(rows: list[tuple[str, datetime]]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "rent_at": pa.array([r[1] for r in rows], type=pa.timestamp("us", tz="UTC")),
        }
    )


def suspended_table(rows: list[tuple[str, date]]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "snapshot_date": pa.array([r[1] for r in rows], type=pa.date32()),
        }
    )


def test_rental_on_suspend_day_is_excluded():
    rent = rent_table([("SPB-001", utc(2026, 8, 20, 9, 0, 0))])
    suspended = suspended_table([("SPB-001", date(2026, 8, 20))])

    result = _exclude_suspended_rental_days(rent, suspended).to_pylist()

    assert result == []


def test_rental_on_different_day_is_kept():
    rent = rent_table([("SPB-001", utc(2026, 8, 21, 9, 0, 0))])
    suspended = suspended_table([("SPB-001", date(2026, 8, 20))])

    result = _exclude_suspended_rental_days(rent, suspended).to_pylist()

    assert [r["bike_id"] for r in result] == ["SPB-001"]


def test_rental_of_other_bike_is_kept():
    rent = rent_table([("SPB-002", utc(2026, 8, 20, 9, 0, 0))])
    suspended = suspended_table([("SPB-001", date(2026, 8, 20))])

    result = _exclude_suspended_rental_days(rent, suspended).to_pylist()

    assert [r["bike_id"] for r in result] == ["SPB-002"]


def test_empty_suspended_keeps_all_rentals():
    rent = rent_table([("SPB-001", utc(2026, 8, 20, 9, 0, 0)), ("SPB-002", utc(2026, 8, 20, 9, 0, 0))])
    suspended = suspended_table([])

    result = _exclude_suspended_rental_days(rent, suspended).to_pylist()

    assert {r["bike_id"] for r in result} == {"SPB-001", "SPB-002"}


def test_rental_scan_row_filter_matches_window_days():
    """[target_date - window_days, target_date) - build_usage_features()의 JOIN 경계와 동일해야 한다."""
    result = _rental_scan_row_filter(date(2026, 8, 20), window_days=14)

    assert result == And(
        GreaterThanOrEqual("rent_date_partition", "2026-08-06"),
        LessThan("rent_date_partition", "2026-08-20"),
    )


def test_rental_scan_row_filter_excludes_target_date_itself():
    """target_date 당일은 포함하지 않는다 (window_days=1이어도 시작일=종료일 전날)."""
    result = _rental_scan_row_filter(date(2026, 1, 1), window_days=1)

    assert result == And(
        GreaterThanOrEqual("rent_date_partition", "2025-12-31"),
        LessThan("rent_date_partition", "2026-01-01"),
    )


def test_parse_bool_env_handles_failure_report_t0(monkeypatch):
    monkeypatch.delenv("FAILURE_REPORT_T0_ENABLED", raising=False)
    assert _parse_bool_env("FAILURE_REPORT_T0_ENABLED", default=False) is False
    assert _parse_bool_env("FAILURE_REPORT_T0_ENABLED", default=True) is True

    monkeypatch.setenv("FAILURE_REPORT_T0_ENABLED", "true")
    assert _parse_bool_env("FAILURE_REPORT_T0_ENABLED") is True

    monkeypatch.setenv("FAILURE_REPORT_T0_ENABLED", "false")
    assert _parse_bool_env("FAILURE_REPORT_T0_ENABLED") is False

def test_dedup_by_bike_id_keeps_one_row_per_bike_id():
    """has_uniqueness(threshold=0.99) 하드 게이트가 1%까지는 통과시키는 위험을
    쓰기 전에 미리 제거한다(#332 PR 리뷰) - 중복이 있어도 결과는 bike_id당 1행."""
    table = pa.table({
        "bike_id": ["B1", "B1", "B2"],
        "trips": [3, 5, 1],
    })

    result = _dedup_by_bike_id(table)

    assert len(result) == 2
    assert sorted(result["bike_id"].to_pylist()) == ["B1", "B2"]
