"""Spark 없이 도는 단위 테스트. 앵커 계산과 게이트 판정이 핵심 계약이다.

  pytest tests/test_risk_model.py
"""

from __future__ import annotations

from datetime import date

import pytest

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
