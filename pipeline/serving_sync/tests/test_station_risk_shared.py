"""대여소별 위험도 집계(station_risk_agg) 순수 로직 테스트 (#172)."""
import pyarrow as pa

from station_risk_shared import station_risk_agg


def risk_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "risk_grade": pa.array([r[1] for r in rows], type=pa.string()),
        }
    )


def location_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "last_station_id": pa.array([r[1] for r in rows], type=pa.string()),
        }
    )


def rows_by_station(risk_rows, location_rows) -> dict:
    result = station_risk_agg(risk_table(risk_rows), location_table(location_rows))
    return {r["station_id"]: r for r in result.to_pylist()}


def test_all_normal_bikes_have_full_healthy_ratio():
    result = rows_by_station(
        [("B1", "Normal"), ("B2", "Normal")],
        [("B1", "ST-1"), ("B2", "ST-1")],
    )
    assert result["ST-1"]["risk_cnt"] == 0
    assert result["ST-1"]["healthy_ratio"] == 100.0


def test_warning_and_critical_count_toward_risk_cnt():
    result = rows_by_station(
        [("B1", "Normal"), ("B2", "Warning"), ("B3", "Critical")],
        [("B1", "ST-1"), ("B2", "ST-1"), ("B3", "ST-1")],
    )
    assert result["ST-1"]["risk_cnt"] == 2
    assert abs(result["ST-1"]["healthy_ratio"] - 33.3) < 0.1


def test_bikes_with_no_station_are_excluded():
    result = rows_by_station([("B1", "Critical")], [("B1", None)])
    assert result == {}


def test_stations_are_independent():
    result = rows_by_station(
        [("B1", "Critical"), ("B2", "Normal")],
        [("B1", "ST-1"), ("B2", "ST-2")],
    )
    assert result["ST-1"]["healthy_ratio"] == 0.0
    assert result["ST-2"]["healthy_ratio"] == 100.0
