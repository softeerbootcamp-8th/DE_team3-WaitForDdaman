"""
gold.fact_bike_risk의 "정비중(수거→미배치) 자전거 판정" 로직 테스트 (#171)

Iceberg 카탈로그를 직접 읽는 부분은 여기서 테스트하지 않는다. 대신 PyArrow Table
하나만으로 동작하는 순수 함수 _latest_collected_bike_ids()만 검증한다 -
build_dim_bike.py의 _compute_new_bikes()와 동일한 DuckDB 기반 패턴.
"""
from datetime import datetime, timezone

import pyarrow as pa

from gold.build_fact_bike_risk import _dedup_by_bike_id, _exclude_collected_bikes, _latest_collected_bike_ids


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def actions_table(rows: list[tuple[str, str, datetime]]) -> pa.Table:
    return pa.table(
        {
            "bike_id": pa.array([r[0] for r in rows], type=pa.string()),
            "event_type": pa.array([r[1] for r in rows], type=pa.string()),
            "occurred_at": pa.array([r[2] for r in rows], type=pa.timestamp("us", tz="UTC")),
        }
    )


def test_bike_whose_latest_event_is_collect_is_included():
    actions = actions_table([("SPB-001", "COLLECT", utc(2026, 8, 20, 9, 0, 0))])

    result = _latest_collected_bike_ids(actions).to_pylist()

    assert [r["bike_id"] for r in result] == ["SPB-001"]


def test_bike_deployed_after_collect_is_excluded():
    actions = actions_table([
        ("SPB-001", "COLLECT", utc(2026, 8, 18, 9, 0, 0)),
        ("SPB-001", "DEPLOY", utc(2026, 8, 20, 9, 0, 0)),  # 더 최신
    ])

    result = _latest_collected_bike_ids(actions).to_pylist()

    assert result == []


def test_bike_with_only_deploy_events_is_excluded():
    actions = actions_table([("SPB-001", "DEPLOY", utc(2026, 8, 20, 9, 0, 0))])

    result = _latest_collected_bike_ids(actions).to_pylist()

    assert result == []


def test_multiple_bikes_are_judged_independently():
    actions = actions_table([
        ("SPB-001", "COLLECT", utc(2026, 8, 20, 9, 0, 0)),
        ("SPB-002", "DEPLOY", utc(2026, 8, 20, 9, 0, 0)),
    ])

    result = _latest_collected_bike_ids(actions).to_pylist()

    assert [r["bike_id"] for r in result] == ["SPB-001"]


def feature_row_table(bike_ids: list[str]) -> pa.Table:
    return pa.table({
        "bike_id": pa.array(bike_ids, type=pa.string()),
        "trips": pa.array([1] * len(bike_ids), type=pa.int32()),
    })


def bike_id_table(bike_ids: list[str]) -> pa.Table:
    return pa.table({"bike_id": pa.array(bike_ids, type=pa.string())})


def test_excludes_currently_collected_bikes():
    features = feature_row_table(["SPB-001", "SPB-002"])
    collected = bike_id_table(["SPB-001"])

    result = _exclude_collected_bikes(features, collected).to_pylist()

    assert [r["bike_id"] for r in result] == ["SPB-002"]


def test_no_bikes_collected_keeps_all():
    features = feature_row_table(["SPB-001", "SPB-002"])
    collected = bike_id_table([])

    result = _exclude_collected_bikes(features, collected).to_pylist()

    assert {r["bike_id"] for r in result} == {"SPB-001", "SPB-002"}

def test_dedup_by_bike_id_keeps_one_row_per_bike_id():
    """has_uniqueness(threshold=0.99) 하드 게이트가 1%까지는 통과시키는 위험을
    쓰기 전에 미리 제거한다(#332 PR 리뷰) - 중복이 있어도 결과는 bike_id당 1행."""
    table = pa.table({
        "bike_id": ["B1", "B1", "B2"],
        "risk_score": [90.0, 10.0, 50.0],
    })

    result = _dedup_by_bike_id(table)

    assert len(result) == 2
    assert sorted(result["bike_id"].to_pylist()) == ["B1", "B2"]
