"""
gold_risk_decision 원안의 "5. run_risk_scoring_model" 단계 구현 - gold.bike_features_daily
-> risk_score/risk_grade 추론. build_fact_bike_risk.py가 이 모듈을 그대로 불러 쓴다.

모델 로딩(registry.json의 champion)과 스코어링(model_type별 predict_proba 분기)은
pipeline/train_risk_model의 registry.get_champion() / train.score()를 그대로 쓴다 -
학습 쪽과 추론 쪽이 이 분기를 따로 구현하면 모델 타입이 늘어날 때마다 두 곳을 같이
고쳐야 하고, 하나만 고치고 잊어버리기 쉽다(train-serving skew). champion은 항상
models.primary 타입만 승격되도록 학습 쪽에서 보장하므로(risk_model_train_dag.py),
여기서는 model_type 분기를 신경 쓸 필요 없이 train.score()에 그대로 위임한다.
"""
import io
import logging
import os

import joblib
import pandas as pd

from ml.registry import get_champion
from ml.settings import load_config, read_bytes
from ml.train import score as _train_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# 상위 백분위(%) 기준 컷오프 기본값
# 고정 점수 컷오프는 모델 재학습이나 데이터 분포 변화 시 특정 등급이 미발생할 수 있어
# 상위 %를 기준으로 동적 컷오프를 계산한다: 상위 1% Critical, 상위 3% Warning, 하위 97% Normal.
DEFAULT_WARNING_PERCENTILE = 97.0   # 상위 3% (Warning)
DEFAULT_CRITICAL_PERCENTILE = 99.0  # 상위 1% (Critical)


def load_model(cfg=None) -> dict:
    cfg = cfg or load_config()
    champion = get_champion(cfg)
    if champion is None:
        raise RuntimeError(
            "champion 모델이 없습니다 - risk_model_train DAG을 먼저 실행해 모델을 승격해야 합니다"
        )
    art = joblib.load(io.BytesIO(read_bytes(champion["artifact_uri"])))
    logger.info(
        "모델 로드: %s (features=%s, model_version=%s)",
        art["model_type"], art["features"], champion["model_version"],
    )
    return art


def score(
    feat: pd.DataFrame,
    art: dict,
    warning_percentile: float | None = None,
    critical_percentile: float | None = None,
) -> pd.DataFrame:
    """feat: bike_id를 index로 갖는 feature DataFrame (art['features'] 컬럼 포함)."""
    p = _train_score(art, feat)

    out = pd.DataFrame(index=feat.index)
    out["risk_score"] = (p * 100).round(3)

    if len(out) == 0:
        out["risk_grade"] = pd.Series(dtype="object")
    else:
        warn_pct = (
            warning_percentile
            if warning_percentile is not None
            else float(os.getenv("WARNING_RISK_PERCENTILE", str(DEFAULT_WARNING_PERCENTILE)))
        )
        crit_pct = (
            critical_percentile
            if critical_percentile is not None
            else float(os.getenv("CRITICAL_RISK_PERCENTILE", str(DEFAULT_CRITICAL_PERCENTILE)))
        )

        warn_cut = out["risk_score"].quantile(warn_pct / 100.0)
        crit_cut = out["risk_score"].quantile(crit_pct / 100.0)

        logger.info(
            "위험도 백분위 컷오프 적용: Warning(>=%.1f%%, 점수>=%.3f), Critical(>=%.1f%%, 점수>=%.3f)",
            100.0 - warn_pct,
            warn_cut,
            100.0 - crit_pct,
            crit_cut,
        )

        def _to_grade(s: float) -> str:
            if s >= crit_cut:
                return "Critical"
            elif s >= warn_cut:
                return "Warning"
            else:
                return "Normal"

        out["risk_grade"] = out["risk_score"].apply(_to_grade)

    out["model_version"] = art.get("model_version", "unknown")
    return out


if __name__ == "__main__":
    art = load_model()

    # 0~1 랜덤값 대신, 실제 feature 스케일에 가까운 값으로 넣어야 모델이 서로
    # 다른 leaf로 분류한다 (스케일이 다 다른데 랜덤값은 전부 0~1이라 구분이 안 됨).
    fake = pd.DataFrame(
        {
            "trips": [2, 15, 8, 25, 1],
            "dist_km": [10, 200, 80, 450, 3],
            "dur_h": [1, 20, 8, 40, 0.5],
            "instant_ret": [0, 3, 1, 5, 0],
            "fail_150d": [0, 2, 0, 4, 0],
            "days_since_fail": [9999, 20, 9999, 5, 9999],
            "days_since_last_rent": [5, 0, 2, 0, 8],
            "trend_ratio": [1.0, 2.5, 1.1, 3.0, 0.3],
        },
        index=[f"BK{i:02d}" for i in range(5)],
    )
    result = score(fake, art)
    print(result)
