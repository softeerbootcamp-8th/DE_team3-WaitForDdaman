"""모델 아티팩트 저장과 champion 포인터 관리.

MLflow 를 켜도 joblib 은 S3 에 직접 저장하고 그 경로를 MLflow 태그에 남긴다.
tracking server 가 죽어도 추론 DAG 이 S3 직접 로드로 폴백할 수 있어야 한다.
"""

from __future__ import annotations

import io
import json
from datetime import datetime

import joblib

from .features import FEATURE_COLS, FEATURE_VERSION
from .settings import exists, join_uri, read_json, write_bytes, write_json

REGISTRY_FILE = "registry.json"


def registry_uri(cfg) -> str:
    return join_uri(cfg.get_path("paths.model_root"), REGISTRY_FILE)


def load_registry(cfg) -> dict:
    uri = registry_uri(cfg)
    if not exists(uri):
        return {"champion": None, "history": []}
    return read_json(uri)


def get_champion(cfg) -> dict | None:
    return load_registry(cfg).get("champion")


def _artifact_bytes(payload: dict) -> bytes:
    buf = io.BytesIO()
    joblib.dump(payload, buf)
    return buf.getvalue()


def save_run(cfg, run_key: str, best_name: str, candidates: dict, eval_result: dict,
             anchor_plan: dict, train_stats: dict, quality: dict, gate: dict,
             importance: dict) -> dict:
    """{model_root}/{run_key}/ 아래에 아티팩트와 메타를 쓴다. 같은 run_key 는 덮어쓴다."""
    root = join_uri(cfg.get_path("paths.model_root"), run_key)
    art = candidates[best_name]

    payload = {
        "scaler": art.get("scaler"),
        "model": art.get("model"),
        "features": list(FEATURE_COLS),
        "model_type": art["model_type"],
        "feature_version": FEATURE_VERSION,
        "model_version": run_key,
        "as_of_end": anchor_plan["as_of_end"],
        "window_days": int(cfg.get_path("run.window_days")),
        "horizon_days": int(cfg.get_path("run.horizon_days")),
        "exclude_recent_days": int(cfg.get_path("run.exclude_recent_days")),
        "train_anchors": anchor_plan["train_anchors"],
        "trained_at": datetime.utcnow().isoformat(timespec="seconds"),
    }
    model_uri = write_bytes(join_uri(root, "model.joblib"), _artifact_bytes(payload))

    metrics_uri = write_json(
        join_uri(root, "metrics.json"),
        {
            "model_version": run_key,
            "selected": best_name,
            "metrics": eval_result["metrics"],
            "curves": eval_result["curves"],
            "skipped_days": eval_result["skipped_days"],
            "gate": gate,
            "train_stats": train_stats,
            "quality": quality,
            "feature_importance": importance,
        },
    )
    spec_uri = write_json(
        join_uri(root, "feature_spec.json"),
        {
            "feature_version": FEATURE_VERSION,
            "features": list(FEATURE_COLS),
            "window_days": int(cfg.get_path("run.window_days")),
            "horizon_days": int(cfg.get_path("run.horizon_days")),
            "exclude_recent_days": int(cfg.get_path("run.exclude_recent_days")),
            "candidate_pool": "창 내 대여 1건 이상",
            "anchor_plan": anchor_plan,
        },
    )

    entry = {
        "model_version": run_key,
        "candidate": best_name,
        "model_type": art["model_type"],
        "artifact_uri": model_uri,
        "metrics_uri": metrics_uri,
        "feature_spec_uri": spec_uri,
        "feature_version": FEATURE_VERSION,
        "metrics": eval_result["metrics"][best_name],
        "as_of_end": anchor_plan["as_of_end"],
        "gate": gate,
        "registered_at": datetime.utcnow().isoformat(timespec="seconds"),
    }
    return entry


def promote(cfg, entry: dict) -> dict:
    """게이트 통과 시에만 champion 포인터를 옮긴다. 미통과면 history 만 남긴다."""
    reg = load_registry(cfg)
    reg["history"] = [h for h in reg.get("history", []) if h["model_version"] != entry["model_version"]]
    reg["history"].append(entry)
    reg["history"] = sorted(reg["history"], key=lambda h: h["model_version"])[-50:]
    if entry["gate"]["passed"]:
        reg["champion"] = entry
    write_json(registry_uri(cfg), reg)
    return reg


# ── MLflow (옵션) ─────────────────────────────────────────────────────
def log_to_mlflow(cfg, run_key: str, best_name: str, eval_result: dict, entry: dict,
                  train_stats: dict, importance: dict) -> str | None:
    if not cfg.get_path("mlflow.enabled", False):
        return None
    try:
        import mlflow
    except ImportError:
        print("[warn] mlflow 미설치 — 로깅 생략")
        return None

    mlflow.set_tracking_uri(cfg.get_path("mlflow.tracking_uri"))
    mlflow.set_experiment(cfg.get_path("mlflow.experiment"))

    with mlflow.start_run(run_name=f"train-{run_key}") as parent:
        mlflow.set_tags(
            {
                "model_version": run_key,
                "as_of_end": entry["as_of_end"],
                "feature_version": entry["feature_version"],
                "selected": best_name,
                "gate_passed": str(entry["gate"]["passed"]),
                "artifact_uri": entry["artifact_uri"],  # S3 직접 로드 폴백 경로
            }
        )
        mlflow.log_params(
            {
                "window_days": cfg.get_path("run.window_days"),
                "horizon_days": cfg.get_path("run.horizon_days"),
                "exclude_recent_days": cfg.get_path("run.exclude_recent_days"),
                "top_k": cfg.get_path("run.top_k"),
                "rolling_months": cfg.get_path("run.rolling_months"),
                "anchor_step_days": cfg.get_path("run.anchor_step_days"),
                **{f"lgbm_{k}": v for k, v in (cfg.get_path("models.lgbm") or {}).items()},
            }
        )
        mlflow.log_metrics({"train_rows": train_stats["rows"], "train_pos": train_stats["pos"]})

        for name, m in eval_result["metrics"].items():
            for mk, mv in m.items():
                if isinstance(mv, (int, float)):
                    mlflow.log_metric(f"{name}__{mk}", mv)
        for fname, fval in importance.items():
            mlflow.log_metric(f"importance__{fname}", fval)

        return parent.info.run_id
