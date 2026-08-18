"""후보 모델 학습. Spark 없이 pandas + sklearn/lightgbm 으로 단일 노드 실행.

표본 규모(수십만~수백만 행 × 7컬럼)에서 분산 학습은 오버헤드만 늘린다.
Spark 는 피처 생성까지만 쓴다 — 기술 선택 근거로 그대로 쓸 수 있는 지점.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from .features import FEATURE_COLS


def _open_dataset(uri: str):
    """파티션 parquet 디렉터리를 연다.

    pyarrow 는 Spark/Hadoop 전용 스킴인 s3a:// 를 모른다(s3:// 만 인식).
    같은 LocalStack/S3 버킷을 Spark 는 s3a:// 로 쓰고 여기서는 s3:// 로 읽어야 해서
    s3a 는 커스텀 S3FileSystem(엔드포인트=S3_ENDPOINT/AWS_ENDPOINT_URL)으로 직접 연다.
    """
    import pyarrow.dataset as ds

    if uri.startswith("s3a://") or uri.startswith("s3://"):
        import os

        from pyarrow import fs as pafs

        endpoint = os.environ.get("S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
        kwargs = {
            "access_key": os.environ.get("AWS_ACCESS_KEY_ID"),
            "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        }
        if endpoint:
            scheme, host = endpoint.split("://", 1)
            kwargs.update(endpoint_override=host, scheme=scheme)
        filesystem = pafs.S3FileSystem(**kwargs)
        path = uri.split("://", 1)[1]
        return ds.dataset(path, format="parquet", partitioning="hive", filesystem=filesystem)
    return ds.dataset(uri, format="parquet", partitioning="hive")


def load_samples(sample_path: str, anchor_type: str, anchors: list[str] | None = None) -> pd.DataFrame:
    """파티션 디렉터리에서 읽는다. anchors 를 주면 해당 파티션만 읽는다."""
    root = f"{sample_path}/anchor_type={anchor_type}"
    df = _open_dataset(root).to_table().to_pandas()
    if "snapshot_date" in df.columns:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    if anchors:
        keep = {date.fromisoformat(a) for a in anchors}
        df = df[df["snapshot_date"].isin(keep)]
    return df


def load_pos_new(pos_path: str) -> pd.DataFrame:
    df = _open_dataset(pos_path).to_table().to_pandas()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    return df


def assert_quality(df: pd.DataFrame, cfg) -> dict:
    """학습 전 품질 게이트. 통과 못 하면 조용히 학습하지 않고 실패시킨다."""
    q = cfg["quality"]
    eligible = df[~df["excluded"].astype(bool)]
    report = {
        "rows_total": int(len(df)),
        "rows_eligible": int(len(eligible)),
        "anchors": int(eligible["snapshot_date"].nunique()),
        "pos_rate": float(eligible["label"].mean()) if len(eligible) else 0.0,
        "pos_count": int(eligible["label"].sum()),
        "excluded_rate": float(df["excluded"].astype(bool).mean()) if len(df) else 0.0,
        "bike_class_dist": eligible["bike_class"].value_counts().to_dict(),
        "null_rate": {c: float(eligible[c].isna().mean()) for c in FEATURE_COLS},
    }

    errors = []
    if report["rows_eligible"] < int(q["min_train_rows"]):
        errors.append(f"학습 행수 부족: {report['rows_eligible']} < {q['min_train_rows']}")
    if report["anchors"] < int(q["min_anchors"]):
        errors.append(f"앵커 수 부족: {report['anchors']} < {q['min_anchors']}")
    if not (float(q["min_pos_rate"]) <= report["pos_rate"] <= float(q["max_pos_rate"])):
        errors.append(
            f"양성비율 이상: {report['pos_rate']:.4f} (허용 {q['min_pos_rate']}~{q['max_pos_rate']})"
        )
    max_null = float(q["max_feature_null_rate"])
    bad = {c: r for c, r in report["null_rate"].items() if r > max_null}
    if bad:
        errors.append(f"피처 결측률 초과: {bad}")

    report["errors"] = errors
    return report


def to_model_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """모든 후보가 같은 입력 행렬을 쓴다 — 모델 차이만 순수하게 비교한다."""
    return df[FEATURE_COLS].fillna(0).astype("float64")


def train_candidates(train_df: pd.DataFrame, cfg) -> dict:
    from lightgbm import LGBMClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    names = list(cfg.get_path("models.candidates", ["lgbm"]))
    eligible = train_df[~train_df["excluded"].astype(bool)]
    X = to_model_matrix(eligible)
    y = eligible["label"].astype(int).values

    out: dict[str, dict] = {}

    if "rule_trips" in names:
        # 대조군. 학습 불필요 — trips 정렬 그대로.
        out["rule_trips"] = {"model_type": "rule_trips", "model": None, "scaler": None}

    if "logreg" in names:
        scaler = StandardScaler().fit(X.values)
        logreg = LogisticRegression(**(cfg.get_path("models.logreg") or {})).fit(
            scaler.transform(X.values), y
        )
        out["logreg"] = {"model_type": "logreg", "model": logreg, "scaler": scaler}

    if "lgbm" in names:
        lgbm = LGBMClassifier(**(cfg.get_path("models.lgbm") or {})).fit(X.values, y)
        out["lgbm"] = {"model_type": "lgbm", "model": lgbm, "scaler": None}

    for art in out.values():
        art["features"] = list(FEATURE_COLS)

    return {
        "candidates": out,
        "train_stats": {
            "rows": int(len(X)),
            "pos": int(y.sum()),
            "pos_rate": float(y.mean()) if len(y) else 0.0,
        },
    }


def score(artifact: dict, feat: pd.DataFrame) -> pd.Series:
    """학습·평가·추론이 공유하는 스코어링. 인덱스는 bike_id."""
    idx = feat["bike_id"].values if "bike_id" in feat.columns else feat.index
    if artifact["model_type"] == "rule_trips":
        return pd.Series(feat["trips"].astype(float).values, index=idx)
    X = to_model_matrix(feat)
    if artifact["model_type"] == "logreg":
        p = artifact["model"].predict_proba(artifact["scaler"].transform(X.values))[:, 1]
    else:
        p = artifact["model"].predict_proba(X.values)[:, 1]
    return pd.Series(p, index=idx)


def feature_importance(artifact: dict) -> dict:
    if artifact["model_type"] == "lgbm":
        return dict(
            zip(FEATURE_COLS, [float(v) for v in artifact["model"].feature_importances_])
        )
    if artifact["model_type"] == "logreg":
        return dict(zip(FEATURE_COLS, [float(v) for v in artifact["model"].coef_[0]]))
    return {}
