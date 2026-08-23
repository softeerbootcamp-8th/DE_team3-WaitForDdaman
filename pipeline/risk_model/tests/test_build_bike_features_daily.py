"""
gold.bike_features_daily의 SUSPEND 당일 대여 제외 로직 테스트 (#171)

Iceberg 카탈로그를 직접 읽는 부분은 여기서 테스트하지 않는다. 대신 두 PyArrow
Table만으로 동작하는 순수 함수 _exclude_suspended_rental_days()만 검증한다 -
build_dim_bike.py의 _compute_new_bikes()와 동일한 DuckDB 기반 패턴.
"""
from datetime import date, datetime, timezone

import pyarrow as pa

from jobs.build_bike_features_daily import _exclude_suspended_rental_days


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