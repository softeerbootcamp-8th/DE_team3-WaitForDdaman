"""build_mart_bike_risk_daily 조인/집계 순수 로직 테스트 (#172)."""
from datetime import datetime, timezone

import pyarrow as pa

from build_mart_bike_risk_daily import _fail_history_agg, build_mart_bike_risk_daily

SNAPSHOT_DATE = "2026-08-18"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def risk_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "risk_score": pa.array([r[1] for r in rows], type=pa.float64()),
            "risk_grade": pa.array([r[2] for r in rows], type=pa.string()),
        }
    )


def decision_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "action": pa.array([r[1] for r in rows], type=pa.string()),
        }
    )


def location_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "last_station_id": pa.array([r[1] for r in rows], type=pa.string()),
        }
    )


def station_active_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "station_id": pa.array([r[0] for r in rows], type=pa.string()),
            "station_name": pa.array([r[1] for r in rows], type=pa.string()),
            "region": pa.array([r[2] for r in rows], type=pa.string()),
            "district": pa.array([r[3] for r in rows], type=pa.string()),
        }
    )


def dim_bike_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "start_year": pa.array([r[1] for r in rows], type=pa.int32()),
        }
    )


def features_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "dist_km": pa.array([r[1] for r in rows], type=pa.float64()),
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


def failure_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "fail_history": pa.array([r[1] for r in rows], type=pa.list_(pa.string())),
        }
    )


def raw_failure_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "bike_no": pa.array([r[0] for r in rows], type=pa.string()),
            "reg_dttm": pa.array([r[1] for r in rows], type=pa.timestamp("us", tz="UTC")),
            "failure_type": pa.array([r[2] for r in rows], type=pa.string()),
        }
    )


def build(risk_rows, decision_rows) -> dict:
    location_rows = [(r[0], "ST-1") for r in risk_rows]
    result = build_mart_bike_risk_daily(
        risk_table(risk_rows),
        decision_table(decision_rows),
        location_table(location_rows),
        station_active_table([("ST-1", "테스트대여소", "강북", "마포구")]),
        dim_bike_table([(r[0], 2020) for r in risk_rows]),
        features_table([(r[0], 12.5) for r in risk_rows]),
        station_risk_table([]),
        failure_table([]),
        SNAPSHOT_DATE,
    )
    return {r["bike_id"]: r for r in result.to_pylist()}


def test_mart_omits_action_column():
    result = build([("B1", 10.0, "Normal")], [("B1", "보류")])
    assert "action" not in result["B1"]


def test_mart_keeps_only_decided_bikes():
    result = build(
        [("B1", 90.0, "Critical"), ("B2", 10.0, "Normal")],
        [("B1", "대여중단")],
    )
    assert sorted(result) == ["B1"]


def test_aging_is_snapshot_year_minus_start_year():
    result = build([("B1", 10.0, "Normal")], [("B1", "보류")])
    assert result["B1"]["aging"] == 2026 - 2020


def test_no_risk_scored_station_defaults_to_full_health():
    result = build([("B1", 10.0, "Normal")], [("B1", "보류")])
    assert result["B1"]["healthy_ratio"] == 100.0


def test_fail_history_agg_orders_most_recent_first():
    raw = raw_failure_table(
        [
            ("B1", utc(2026, 1, 1), "펑크"),
            ("B1", utc(2026, 3, 1), "타이어마모"),
            ("B1", utc(2026, 2, 1), "체인끊김"),
        ]
    )
    result = {r["bike_id"]: r for r in _fail_history_agg(raw, "2026-08-18").to_pylist()}
    assert result["B1"]["fail_history"] == [
        "2026-03-01 타이어마모",
        "2026-02-01 체인끊김",
        "2026-01-01 펑크",
    ]


def test_fail_history_agg_respects_limit():
    raw = raw_failure_table([("B2", utc(2026, 1, i), f"고장{i}") for i in range(1, 8)])
    result = {r["bike_id"]: r for r in _fail_history_agg(raw, "2026-08-18", limit=5).to_pylist()}
    assert result["B2"]["fail_history"] == [
        "2026-01-07 고장7",
        "2026-01-06 고장6",
        "2026-01-05 고장5",
        "2026-01-04 고장4",
        "2026-01-03 고장3",
    ]
