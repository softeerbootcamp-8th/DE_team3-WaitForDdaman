"""write_station_daily의 순수 변환 로직(_rows_for_insert) 테스트 (#172)."""
import pyarrow as pa

from write_station_daily import COLUMNS, _rows_for_insert


def test_rows_for_insert_matches_column_order():
    table = pa.table(
        {
            "station_id": pa.array(["ST-1"], type=pa.string()),
            "snapshot_date": pa.array(["2026-08-18"], type=pa.string()),
            "bike_cnt": pa.array([5], type=pa.int32()),
        }
    )
    rows = _rows_for_insert(table, ["snapshot_date", "station_id", "bike_cnt"])
    assert rows == [("2026-08-18", "ST-1", 5)]


def test_columns_match_mart_output_shape():
    assert COLUMNS == [
        "snapshot_date", "station_id", "station_name", "region", "district",
        "latitude", "longitude", "hold_num", "bike_cnt", "risk_cnt", "healthy_ratio", "urgency",
    ]
