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


def test_mart_dedups_duplicate_station_active_and_inventory_rows():
    """gold.station_active/gold.fact_station_inventory는 has_uniqueness(threshold=0.99)
    하드 게이트라 station_id 중복이 최대 1%까지 통과해서 여기로 들어올 수 있다 - 그런
    입력이 와도 마트는 대여소당 한 행만 내야 한다(fan-out 방지, mart_bike_risk_daily와
    동일 원칙). build()는 station_id로 dict를 만들어 행 중복 여부를 못 잡으므로,
    여기서는 결과 테이블 자체의 행 수를 직접 확인한다."""
    result_table = build_mart_station_daily(
        station_active_table(
            [
                ("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10),
                ("ST-1", "역삼역", "강남", "강남구", 37.5, 127.0, 10),  # 중복 2행
            ]
        ),
        inventory_table([("ST-1", 5, 10), ("ST-1", 9, 10)]),  # 중복 2행
        station_risk_table([]),
        SNAPSHOT_DATE,
    )
    assert len(result_table) == 1


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
