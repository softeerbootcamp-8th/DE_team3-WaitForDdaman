"""
dag_risk_decision 원안의 "5. run_risk_scoring_model" 단계 구현 - gold.bike_features_daily
-> risk_score/risk_grade 추론. build_fact_bike_risk.py가 이 모듈을 그대로 불러 쓴다.

모델 파일(risk_model_v1.joblib)은 {scaler, model, features, model_type, ...} 형태의
딕셔너리로 저장돼 있다. model은 features 순서 그대로(numpy array, 컬럼명 정보 없음)
학습됐으므로, 추론 시에도 반드시 art['features'] 순서를 그대로 지켜서 컬럼을 선택해야 한다.
"""
import logging
from pathlib import Path

import joblib
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MODEL_DIR = Path(__file__).resolve().parent.parent / "models"

RISK_GRADE_BINS = [-1, 95, 99, 101]
RISK_GRADE_LABELS = ["Normal", "Warning", "Critical"]


def load_model(model_file: str = "risk_model_v1.joblib") -> dict:
    art = joblib.load(MODEL_DIR / model_file)
    logger.info("모델 로드: %s (features=%s)", art["model_type"], art["features"])
    return art


def score(feat: pd.DataFrame, art: dict) -> pd.DataFrame:
    """feat: bike_id를 index로 갖는 feature DataFrame (art['features'] 컬럼 포함)."""
    X = feat[art["features"]].fillna(0)

    if art["model_type"] == "lgbm":
        p = art["model"].predict_proba(X.values)[:, 1]
    else:
        p = art["model"].predict_proba(art["scaler"].transform(X.values))[:, 1]

    out = pd.DataFrame(index=feat.index)
    out["risk_raw"] = p
    out["risk_score"] = (pd.Series(p, index=feat.index).rank(pct=True) * 100).round(3)
    out["risk_grade"] = pd.cut(
        out["risk_score"], bins=RISK_GRADE_BINS, labels=RISK_GRADE_LABELS
    )
    out["model_version"] = art.get("model_file", "risk_model_v1.joblib")
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
