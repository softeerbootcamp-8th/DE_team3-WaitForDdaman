"""write_bike_risk_daily의 순수 변환 로직(_rows_for_insert) 테스트 (#172)."""
import pyarrow as pa

from write_bike_risk_daily import COLUMNS, _rows_for_insert


def test_rows_for_insert_matches_column_order():
    table = pa.table(
        {
            "bike_id": pa.array(["B1"], type=pa.string()),
            "snapshot_date": pa.array(["2026-08-18"], type=pa.string()),
            "healthy_ratio": pa.array([100.0], type=pa.float64()),
        }
    )
    rows = _rows_for_insert(table, ["snapshot_date", "bike_id", "healthy_ratio"])
    assert rows == [("2026-08-18", "B1", 100.0)]


def test_rows_for_insert_preserves_fail_history_as_list():
    table = pa.table(
        {
            "bike_id": pa.array(["B1"], type=pa.string()),
            "fail_history": pa.array([["2026-08-01 펑크"]], type=pa.list_(pa.string())),
        }
    )
    rows = _rows_for_insert(table, ["bike_id", "fail_history"])
    assert rows == [("B1", ["2026-08-01 펑크"])]


def test_columns_match_mart_output_shape():
    assert COLUMNS == [
        "snapshot_date", "bike_id", "station_id", "station_name", "region", "district",
        "healthy_ratio", "risk_grade", "risk_score", "dist_km", "start_year", "aging",
        "fail_history",
    ]
