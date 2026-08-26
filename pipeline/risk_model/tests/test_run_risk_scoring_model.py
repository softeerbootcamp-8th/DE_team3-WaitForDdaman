"""run_risk_scoring_model 백분율(%) 기반 컷오프 단위 테스트."""
import numpy as np
import pandas as pd
import pytest

from jobs.run_risk_scoring_model import (
    DEFAULT_CRITICAL_PERCENTILE,
    DEFAULT_WARNING_PERCENTILE,
    score,
)


def _fake_artifact(scores: np.ndarray | None = None) -> dict:
    """테스트용 fake artifact."""
    return {
        "model_type": "rule_trips",
        "features": ["trips"],
        "model_version": "v_test",
        "_predict_scores": scores,
    }


def _fake_train_score(art: dict, feat: pd.DataFrame) -> np.ndarray:
    if "_predict_scores" in art and art["_predict_scores"] is not None:
        return art["_predict_scores"]
    # 기본: feat['trips']를 0~1 스케일로 반환
    trips = feat["trips"].values
    return trips / np.max(trips) if np.max(trips) > 0 else trips


def test_score_percentile_cutoff_default(monkeypatch):
    """기본 컷오프: 상위 1% Critical (99th), 상위 3% Warning (97th), 나머지 Normal."""
    monkeypatch.setattr("jobs.run_risk_scoring_model._train_score", _fake_train_score)

    # 100개 자전거의 trips가 1부터 100까지 정렬된 경우
    n = 100
    feat = pd.DataFrame({"trips": np.arange(1, n + 1)}, index=[f"BK{i:03d}" for i in range(1, n + 1)])
    art = _fake_artifact()

    result = score(feat, art)

    assert len(result) == n
    assert "risk_score" in result.columns
    assert "risk_grade" in result.columns
    assert result["model_version"].iloc[0] == "v_test"

    grade_counts = result["risk_grade"].value_counts()
    # 100개 기준: 100번째(1개) = Critical, 98, 99번째(2개) = Warning, 1~97번째(97개) = Normal
    assert grade_counts.get("Critical", 0) == 1
    assert grade_counts.get("Warning", 0) == 2
    assert grade_counts.get("Normal", 0) == 97

    # 가장 높은 점수의 자전거가 Critical인지 확인
    assert result.loc["BK100", "risk_grade"] == "Critical"
    assert result.loc["BK099", "risk_grade"] == "Warning"
    assert result.loc["BK098", "risk_grade"] == "Warning"
    assert result.loc["BK097", "risk_grade"] == "Normal"


def test_score_custom_percentiles(monkeypatch):
    """함수 인자로 커스텀 백분위를 전달했을 때 정상 적용되는지 검증."""
    monkeypatch.setattr("jobs.run_risk_scoring_model._train_score", _fake_train_score)

    n = 100
    feat = pd.DataFrame({"trips": np.arange(1, n + 1)}, index=[f"BK{i:03d}" for i in range(1, n + 1)])
    art = _fake_artifact()

    # 상위 10% Warning (90th percentile), 상위 5% Critical (95th percentile)
    result = score(feat, art, warning_percentile=90.0, critical_percentile=95.0)

    grade_counts = result["risk_grade"].value_counts()
    assert grade_counts.get("Critical", 0) == 5
    assert grade_counts.get("Warning", 0) == 5
    assert grade_counts.get("Normal", 0) == 90


def test_score_env_var_percentiles(monkeypatch):
    """환경변수로 백분위 설정 시 정상 적용되는지 검증."""
    monkeypatch.setattr("jobs.run_risk_scoring_model._train_score", _fake_train_score)
    monkeypatch.setenv("WARNING_RISK_PERCENTILE", "80.0")
    monkeypatch.setenv("CRITICAL_RISK_PERCENTILE", "90.0")

    n = 100
    feat = pd.DataFrame({"trips": np.arange(1, n + 1)}, index=[f"BK{i:03d}" for i in range(1, n + 1)])
    art = _fake_artifact()

    result = score(feat, art)

    grade_counts = result["risk_grade"].value_counts()
    assert grade_counts.get("Critical", 0) == 10
    assert grade_counts.get("Warning", 0) == 10
    assert grade_counts.get("Normal", 0) == 80


def test_score_empty_dataframe(monkeypatch):
    """빈 DataFrame이 들어왔을 때 에러 없이 빈 결과 반환."""
    monkeypatch.setattr("jobs.run_risk_scoring_model._train_score", lambda _art, _feat: np.array([]))

    feat = pd.DataFrame(columns=["trips"])
    art = _fake_artifact()

    result = score(feat, art)
    assert len(result) == 0
    assert "risk_score" in result.columns
    assert "risk_grade" in result.columns
