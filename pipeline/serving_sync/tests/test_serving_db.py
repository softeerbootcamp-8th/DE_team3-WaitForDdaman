"""serving_db의 순수 SQL 빌더 로직 테스트 (DB 연결 없이 검증 가능한 부분만)."""
from serving_db import _build_upsert_query


def test_upsert_query_has_all_columns_in_insert_list():
    query = _build_upsert_query(
        table="station_daily",
        columns=["snapshot_date", "station_id", "bike_cnt"],
        conflict_keys=["station_id", "snapshot_date"],
    )
    assert "INSERT INTO station_daily (snapshot_date, station_id, bike_cnt)" in query


def test_upsert_query_excludes_conflict_keys_from_update_set():
    query = _build_upsert_query(
        table="station_daily",
        columns=["snapshot_date", "station_id", "bike_cnt"],
        conflict_keys=["station_id", "snapshot_date"],
    )
    assert "ON CONFLICT (station_id, snapshot_date) DO UPDATE SET bike_cnt = EXCLUDED.bike_cnt" in query
    assert "station_id = EXCLUDED.station_id" not in query
    assert "snapshot_date = EXCLUDED.snapshot_date" not in query


def test_upsert_query_uses_values_placeholder():
    query = _build_upsert_query(table="t", columns=["a", "b"], conflict_keys=["a"])
    assert query.strip().endswith("VALUES %s ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b")
