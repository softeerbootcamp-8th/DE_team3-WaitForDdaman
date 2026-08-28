"""
gold.station_active 조인 로직 테스트 (#170)

_latest_snapshot()은 Iceberg 카탈로그를 직접 읽으므로 여기서는 테스트하지 않는다.
대신 두 PyArrow Table만으로 동작하는 순수 함수 _join_active_stations()만
검증한다 - staging/tests/test_transform_silver_rental_history.py와 동일한 DuckDB
기반 패턴.
"""
import pyarrow as pa

from jobs.build_station_active import _join_active_stations

SNAPSHOT_DATE = "2026-08-17"

MASTER_COLUMNS = ["station_id", "station_name", "region", "district", "hold_num", "latitude", "longitude"]


def master_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "station_id": pa.array([r[0] for r in rows], type=pa.string()),
            "station_name": pa.array([r[1] for r in rows], type=pa.string()),
            "region": pa.array([r[2] for r in rows], type=pa.string()),
            "district": pa.array([r[3] for r in rows], type=pa.string()),
            "hold_num": pa.array([r[4] for r in rows], type=pa.int32()),
            "latitude": pa.array([r[5] for r in rows], type=pa.float64()),
            "longitude": pa.array([r[6] for r in rows], type=pa.float64()),
        }
    )


def active_ids_table(station_ids: list[str]) -> pa.Table:
    return pa.table({"station_id": pa.array(station_ids, type=pa.string())})


def by_station(table: pa.Table) -> dict:
    return {r["station_id"]: r for r in table.to_pylist()}


def test_inner_join_keeps_only_active_stations():
    master = master_table(
        [
            ("ST-1", "1번 대여소", "강북", "마포구", 10, 37.5, 126.9),
            ("ST-2", "2번 대여소", "강남", "강남구", 20, 37.4, 127.0),
        ]
    )
    active_ids = active_ids_table(["ST-1"])

    result = by_station(_join_active_stations(master, active_ids, SNAPSHOT_DATE))

    assert set(result.keys()) == {"ST-1"}


def test_description_columns_come_from_master():
    master = master_table([("ST-1", "1번 대여소", "강북", "마포구", 10, 37.5, 126.9)])
    active_ids = active_ids_table(["ST-1"])

    result = by_station(_join_active_stations(master, active_ids, SNAPSHOT_DATE))

    assert result["ST-1"]["station_name"] == "1번 대여소"
    assert result["ST-1"]["hold_num"] == 10


def test_snapshot_date_is_set():
    master = master_table([("ST-1", "1번 대여소", "강북", "마포구", 10, 37.5, 126.9)])
    active_ids = active_ids_table(["ST-1"])

    result = by_station(_join_active_stations(master, active_ids, SNAPSHOT_DATE))

    assert result["ST-1"]["snapshot_date"].isoformat() == SNAPSHOT_DATE
