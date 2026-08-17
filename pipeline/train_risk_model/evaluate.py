"""홀드아웃 walk-forward 평가.

메인지표: 매 기준일 top-K 명단에, 직전 30일 신고 이력이 없는 '신규' 고장
자전거가 신고 전에 들어 있었던 비율.

분모는 pos_new 테이블(피처 테이블에 없는 자전거까지 포함)을 쓴다.
후보군에 없는 자전거를 분모에서 빼면 지표가 실제보다 좋게 나온다.
"""

from __future__ import annotations

import pandas as pd

from .train import score


def walk_forward(candidates: dict, holdout_df: pd.DataFrame, pos_new: pd.DataFrame, k: int) -> dict:
    from sklearn.metrics import average_precision_score

    pos_by_day = pos_new.groupby("snapshot_date")["bike_id"].apply(set).to_dict()
    eligible_all = holdout_df[~holdout_df["excluded"].astype(bool)]

    curves = {name: {} for name in candidates}
    pooled = {name: [] for name in candidates}
    skipped = []

    for as_of, day in eligible_all.groupby("snapshot_date"):
        pos_set = pos_by_day.get(as_of, set())
        key = as_of.isoformat()
        if not pos_set or day.empty:
            skipped.append(key)
            for name in candidates:
                curves[name][key] = None
            continue
        for name, art in candidates.items():
            s = score(art, day)
            topk = set(s.sort_values(ascending=False).head(k).index)
            curves[name][key] = round(len(topk & pos_set) / len(pos_set) * 100, 2)
            pooled[name].append(pd.DataFrame({"y": day["label"].values, "p": s.values}))

    metrics = {}
    for name in candidates:
        vals = [v for v in curves[name].values() if v is not None]
        pl = pd.concat(pooled[name]) if pooled[name] else pd.DataFrame({"y": [], "p": []})
        metrics[name] = {
            "capture_at_k": round(float(pd.Series(vals).mean()), 3) if vals else None,
            "capture_std": round(float(pd.Series(vals).std()), 3) if len(vals) > 1 else None,
            "capture_min": round(float(min(vals)), 3) if vals else None,
            "pr_auc": (
                round(float(average_precision_score(pl["y"], pl["p"])), 5)
                if pl["y"].nunique() > 1
                else None
            ),
            "eval_days": len(vals),
            "top_k": k,
        }
    return {"metrics": metrics, "curves": curves, "skipped_days": skipped}


def select_best(metrics: dict, cfg) -> str:
    """capture_at_k 최대. 동점이면 primary 우선."""
    primary = cfg.get_path("models.primary", "lgbm")
    scored = [(n, m["capture_at_k"]) for n, m in metrics.items() if m["capture_at_k"] is not None]
    if not scored:
        raise ValueError("평가 가능한 후보가 없습니다 — 홀드아웃 구간에 신규 고장이 없습니다.")
    best_val = max(v for _, v in scored)
    tied = [n for n, v in scored if v == best_val]
    return primary if primary in tied else tied[0]


def apply_gate(best_name: str, metrics: dict, champion: dict | None, cfg) -> dict:
    """champion 대비 성능 하락 폭 판정. 첫 실행이면 무조건 통과."""
    metric_key = cfg.get_path("gate.metric", "capture_at_k")
    max_drop = float(cfg.get_path("gate.max_drop_pp", 2.0))
    new_val = metrics[best_name][metric_key]

    if champion is None:
        return {
            "passed": True,
            "reason": "champion 없음 (최초 등록)",
            "new": new_val,
            "champion": None,
            "delta_pp": None,
        }

    old_val = (champion.get("metrics") or {}).get(metric_key)
    if old_val is None:
        return {
            "passed": True,
            "reason": "champion 지표 없음",
            "new": new_val,
            "champion": None,
            "delta_pp": None,
        }

    delta = round(new_val - old_val, 3)
    passed = delta >= -max_drop
    return {
        "passed": passed,
        "reason": (
            f"{metric_key} {old_val} → {new_val} ({delta:+}pp), 허용 -{max_drop}pp"
        ),
        "new": new_val,
        "champion": old_val,
        "delta_pp": delta,
    }
