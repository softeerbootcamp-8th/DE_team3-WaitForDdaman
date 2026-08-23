"""
gold.dim_bike 신규 자전거 계산 로직 테스트 (#170)

Iceberg 카탈로그를 직접 읽는 부분은 여기서 테스트하지 않는다. 대신 두 PyArrow
Table만으로 동작하는 순수 함수 _compute_new_bikes()만 검증한다 -
staging/tests/test_transform_silver_rental_history.py와 동일한 DuckDB 기반 패턴.
"""
from datetime import datetime, timezone

import pyarrow as pa

from jobs.build_dim_bike import _compute_new_bikes

RENTAL_COLUMNS = ["bike_id", "rent_dt"]


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def rental_table(rows: list[tuple[str, datetime]]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "rent_dt": pa.array([r[1] for r in rows], type=pa.timestamp("us", tz="UTC")),
        }
    )


def existing_bike_ids(bike_ids: list[str]) -> pa.Table:
    return pa.table({"bike_id": pa.array(bike_ids, type=pa.string())})


def test_first_seen_at_is_earliest_rent_dt_per_bike():
    silver = rental_table(
        [
            ("SPB-001", utc(2026, 8, 20, 9, 0, 0)),
            ("SPB-001", utc(2026, 8, 18, 7, 0, 0)),  # 더 이른 시각
            ("SPB-001", utc(2026, 8, 21, 10, 0, 0)),
        ]
    )

    result = _compute_new_bikes(silver, existing_bike_ids([])).to_pylist()

    assert len(result) == 1
    assert result[0]["bike_id"] == "SPB-001"
    assert result[0]["first_seen_at"] == utc(2026, 8, 18, 7, 0, 0)


def test_snapshot_date_and_start_year_derived_from_first_seen_at():
    silver = rental_table([("SPB-001", utc(2026, 8, 18, 7, 30, 0))])

    result = _compute_new_bikes(silver, existing_bike_ids([])).to_pylist()

    assert result[0]["snapshot_date"].isoformat() == "2026-08-18"
    assert result[0]["start_year"] == 2026


def test_bikes_already_in_gold_are_excluded():
    silver = rental_table(
        [
            ("SPB-001", utc(2026, 8, 20, 9, 0, 0)),  # 이미 gold.dim_bike에 있음
            ("SPB-002", utc(2026, 8, 20, 9, 0, 0)),  # 신규
        ]
    )

    result = _compute_new_bikes(silver, existing_bike_ids(["SPB-001"])).to_pylist()

    assert [r["bike_id"] for r in result] == ["SPB-002"]
