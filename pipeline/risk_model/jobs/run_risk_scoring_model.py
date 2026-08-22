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

import joblib
import pandas as pd

from pipeline.train_risk_model.registry import get_champion
from pipeline.train_risk_model.settings import load_config, read_bytes
from pipeline.train_risk_model.train import score as _train_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# 임시값 - #132 재조정. 0812_risk_model_code_optimize.ipynb에서 lgbm3(num_leaves=31,
# min_child_samples=50, reg_lambda=1, is_unbalance=True) 기준 held-out(2026-04-15~06-16,
# 187만 건) 분포로 다시 잡음: Warning 2.45%, Critical 0.77%. risk_score가 raw 확률이라
# 재학습마다(특히 양성비율이 바뀌면) 분포가 드리프트할 수 있어 고정 컷오프는 근본 해법이
# 아님 - 컷오프를 하드코딩하는 대신 그때그때의 분포에 맞춰 재계산하는 방식으로 바꿀지 검토 필요.
RISK_GRADE_BINS = [-1, 66, 70, 101]
RISK_GRADE_LABELS = ["Normal", "Warning", "Critical"]


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


def score(feat: pd.DataFrame, art: dict) -> pd.DataFrame:
    """feat: bike_id를 index로 갖는 feature DataFrame (art['features'] 컬럼 포함)."""
    p = _train_score(art, feat)

    out = pd.DataFrame(index=feat.index)
    out["risk_score"] = (p * 100).round(3)
    out["risk_grade"] = pd.cut(
        out["risk_score"], bins=RISK_GRADE_BINS, labels=RISK_GRADE_LABELS
    )
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
