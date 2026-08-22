"""build_mart_station_daily 조인/urgency 로직 테스트 (#172)."""
import pyarrow as pa

from build_mart_station_daily import build_mart_station_daily

SNAPSHOT_DATE = "2026-08-18"


def station_active_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "station_id": pa.array([r[0] for r in rows], type=pa.string()),
            "station_name": pa.array([r[1] for r in rows], type=pa.string()),
            "region": pa.array([r[2] for r in rows], type=pa.string()),
            "district": pa.array([r[3] for r in rows], type=pa.string()),
            "latitude": pa.array([r[4] for r in rows], type=pa.float64()),
            "longitude": pa.array([r[5] for r in rows], type=pa.float64()),
            "hold_num": pa.array([r[6] for r in rows], type=pa.int32()),
        }
    )


def inventory_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "station_id": pa.array([r[0] for r in rows], type=pa.string()),
            "bike_cnt": pa.array([r[1] for r in rows], type=pa.int32()),
            "target_bike_cnt": pa.array([r[2] for r in rows], type=pa.int32()),
        }
    )


def station_risk_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "station_id": pa.array([r[0] for r in rows], type=pa.string()),
            "risk_cnt": pa.array([r[1] for r in rows], type=pa.int32()),
            "healthy_ratio": pa.array([r[2] for r in rows], type=pa.float64()),
        }
    )


def build(station_active_rows, inventory_rows, station_risk_rows) -> dict:
    result = build_mart_station_daily(
        station_active_table(station_active_rows),
        inventory_table(inventory_rows),
        station_risk_table(station_risk_rows),
        SNAPSHOT_DATE,
    )
    return {r["station_id"]: r for r in result.to_pylist()}


def test_latitude_longitude_pass_through_unchanged():
    result = build(
        [("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10)],
        [("ST-1", 5, 10)],
        [],
    )
    assert result["ST-1"]["latitude"] == 37.5
    assert result["ST-1"]["longitude"] == 127.0


def test_healthy_ratio_ge_70_is_sufficient():
    result = build(
        [("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10)],
        [("ST-1", 5, 10)],
        [("ST-1", 1, 80.0)],
    )
    assert result["ST-1"]["urgency"] == "여유있음"


def test_healthy_ratio_below_70_is_insufficient():
    result = build(
        [("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10)],
        [("ST-1", 5, 10)],
        [("ST-1", 4, 50.0)],
    )
    assert result["ST-1"]["urgency"] == "부족함"


def test_missing_inventory_defaults_bike_cnt_to_zero():
    result = build(
        [("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10)],
        [],
        [],
    )
    assert result["ST-1"]["bike_cnt"] == 0
    assert result["ST-1"]["risk_cnt"] == 0
    assert result["ST-1"]["healthy_ratio"] == 100.0
    assert result["ST-1"]["urgency"] == "여유있음"
