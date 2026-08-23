"""Spark 없이 도는 단위 테스트. 앵커 계산과 게이트 판정이 핵심 계약이다.

  pytest tests/test_risk_model.py
"""

from __future__ import annotations

from datetime import date

import pytest
from moto import mock_aws

from pipeline.train_risk_model.evaluate import apply_gate, select_best
from pipeline.train_risk_model.features import FEATURE_COLS
from pipeline.train_risk_model.samples import resolve_anchors
from pipeline.train_risk_model.settings import Config

CFG = Config(
    {
        "run": {
            "horizon_days": 14,
            "holdout_days": 60,
            "anchor_step_days": 7,
            "rolling_months": 12,
        },
        "gate": {"metric": "capture_at_k", "max_drop_pp": 2.0},
        "models": {"primary": "lgbm"},
    }
)

ANCHOR_CFG = Config(
    {
        **CFG,
        "sources": {
            "rental_history": "silver.rental_history",
            "failure_report": "silver.failure_report",
        },
    }
)

BUCKET = "test-warehouse-bucket"


@pytest.fixture
def s3_env(monkeypatch):
    """#148: detect_label_ready_max가 Spark 없이 boto3 파티션 listing만 쓰는지 검증하는 환경.

    ingestion 쪽 테스트(test_check_silver_snapshot_date.py)와 동일하게 moto로 S3를 흉내낸다.
    """
    import config as config_module

    test_settings = config_module.Settings(
        env="aws",
        warehouse_bucket=BUCKET,
        iceberg_warehouse_path=f"s3a://{BUCKET}/warehouse",
        s3_region="ap-northeast-2",
    )
    monkeypatch.setattr(config_module, "SETTINGS", test_settings)

    with mock_aws():
        from common.s3_utils import ensure_bucket

        ensure_bucket(BUCKET)
        yield


def _put_partition(table: str, partition_col: str, value: str) -> None:
    from common.s3_utils import get_s3_client

    namespace, name = table.split(".", 1)
    key = f"warehouse/{namespace}/{name}/data/{partition_col}={value}/00000-0-x.parquet"
    get_s3_client().put_object(Bucket=BUCKET, Key=key, Body=b"x")


def test_anchor_gap_equals_horizon():
    """학습 마지막 앵커와 홀드아웃 첫날 사이 간격이 라벨창과 같아야 누수가 없다."""
    p = resolve_anchors(CFG, None, date(2026, 6, 16))
    train_end = date.fromisoformat(p["train_range"][1])
    holdout_start = date.fromisoformat(p["holdout_range"][0])
    assert (holdout_start - train_end).days == CFG["run"]["horizon_days"]


def test_anchors_are_deterministic():
    a = resolve_anchors(CFG, "2026-05-01", date(2026, 6, 16))
    b = resolve_anchors(CFG, "2026-05-01", date(2026, 6, 16))
    assert a == b
    assert a["run_key"] == "20260501"


def test_reject_unconfirmed_labels():
    with pytest.raises(ValueError, match="라벨 확정 한계"):
        resolve_anchors(CFG, "2026-06-25", date(2026, 6, 16))


def test_holdout_anchors_are_daily_and_end_at_as_of_end():
    p = resolve_anchors(CFG, None, date(2026, 6, 16))
    assert len(p["holdout_anchors"]) == CFG["run"]["holdout_days"]
    assert p["holdout_anchors"][-1] == "2026-06-16"


def test_gate_blocks_regression():
    m = {"lgbm": {"capture_at_k": 10.0}}
    g = apply_gate("lgbm", m, {"metrics": {"capture_at_k": 13.0}}, CFG)
    assert g["passed"] is False and g["delta_pp"] == -3.0


def test_gate_allows_small_drop_and_first_run():
    m = {"lgbm": {"capture_at_k": 12.0}}
    assert apply_gate("lgbm", m, {"metrics": {"capture_at_k": 13.0}}, CFG)["passed"] is True
    assert apply_gate("lgbm", m, None, CFG)["passed"] is True


def test_select_best_prefers_primary_on_tie():
    m = {"lgbm": {"capture_at_k": 12.0}, "logreg": {"capture_at_k": 12.0}}
    assert select_best(m, CFG) == "lgbm"


def test_detect_label_ready_max_from_partitions(s3_env):
    """#148: Spark 세션 없이 파티션 디렉터리만 보고 fault/rent min·max, 공백 월을 구한다."""
    from pipeline.train_risk_model.samples import detect_label_ready_max

    for d in ["2026-01-05", "2026-01-20", "2026-03-10"]:  # 2026-02는 비움
        _put_partition("silver.failure_report", "reg_date_partition", d)
    for d in ["2026-01-01", "2026-03-15"]:
        _put_partition("silver.rental_history", "rent_date_partition", d)

    result = detect_label_ready_max(ANCHOR_CFG)

    assert result["fault_min"] == "2026-01-05"
    assert result["fault_max"] == "2026-03-10"
    assert result["rent_min"] == "2026-01-01"
    assert result["rent_max"] == "2026-03-15"
    assert result["missing_fault_months"] == ["2026-02"]
    # label_ready_max = min(rent_max, fault_max - horizon(14일)) = min(03-15, 02-24)
    assert result["label_ready_max"] == "2026-02-24"


def test_detect_label_ready_max_raises_on_empty_source(s3_env):
    from pipeline.train_risk_model.samples import detect_label_ready_max

    with pytest.raises(ValueError, match="silver 원천이 비어"):
        detect_label_ready_max(ANCHOR_CFG)


def test_feature_contract_is_seven_columns():
    """아티팩트와 추론 코드가 공유하는 계약. 변경 시 FEATURE_VERSION 을 올려야 한다."""
    assert FEATURE_COLS == [
        "trips",
        "dist_km",
        "instant_ret",
        "fail_150d",
        "days_since_fail",
        "days_since_last_rent",
        "trend_ratio",
    ]
