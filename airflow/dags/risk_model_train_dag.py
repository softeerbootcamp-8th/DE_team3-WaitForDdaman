"""위험도 모델 학습(재학습) DAG — 수동 트리거 전용.

  resolve_anchors → validate_inputs → build_train_samples → assert_train_table
                  → train_candidates → evaluate → select_and_gate → persist_and_register

설계 원칙 반영
  · 멱등성   : run_key(=as_of_end) 기준 파티션/아티팩트 덮어쓰기. append 없음.
  · 원자성   : 앵커 산출 / 검증 / 적재 / 학습 / 평가 / 저장을 분리.
  · 결정론   : datetime.now() 미사용. as_of_end 를 데이터에서 도출하거나 param 으로 고정.
  · 포인터   : XCom 에는 경로와 지표 요약만. 데이터프레임은 절대 안 넘긴다.
  · 설정분리 : 임계값·경로·하이퍼파라미터 전부 config/risk_model.yaml.

APP_ENV=aws 에서는 build_train_samples 태스크가 EMR Serverless StartJobRun으로
pipeline/train_risk_model/samples.py 의 main() 을 spark-submit 엔트리포인트로 실행한다.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import timedelta

# 소스는 /opt/airflow/src에 있으므로 공통 PYTHONPATH 설정을 사용한다.
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/airflow")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

PYLIB_DIR = os.environ.get("PYLIB_DIR", "/opt/airflow/pylib")
if PYLIB_DIR not in sys.path:
    sys.path.insert(0, PYLIB_DIR)

# resolve_anchors가 Spark 대신 common.partition_listing(boto3)을 쓰므로(#148),
# gold_dim_fact_dag.py와 동일한 패턴으로 ingestion을 네임스페이스 패키지 루트로
# sys.path에 얹는다. .env도 같은 이유로 먼저 로드한다 - 안 하면 컨테이너의
# 루트 .env(배포용, APP_ENV=aws)가 그대로 새어 들어와 LocalStack 대신 실제
# AWS S3로 나가는 문제가 재현된다(#145/#153에서 Gold 센서가 겪었던 것과 동일).
INGESTION_DIR = os.environ.get("INGESTION_DIR", "/opt/airflow/src")


def _load_ingestion_env(env_path: str) -> None:
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip()


_load_ingestion_env("/opt/airflow/.env")
if INGESTION_DIR not in sys.path:
    sys.path.insert(0, INGESTION_DIR)

try:  # Airflow 3.x
    from airflow.sdk import Param, dag, task
except ImportError:  # 2.x 호환
    from airflow.decorators import dag, task
    from airflow.models.param import Param

from dag_common import notify_slack_on_failure, run_emr_serverless_spark_job


def _params() -> dict:
    """TaskFlow 안에서 DAG params 를 읽는다."""
    try:
        from airflow.sdk import get_current_context
    except ImportError:
        from airflow.operators.python import get_current_context
    return dict(get_current_context().get("params") or {})


DEFAULT_ARGS = {
    "owner": "de-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify_slack_on_failure,
}


@dag(
    dag_id="risk_model_train",
    description="따릉이 위험도 모델 학습/재학습 — 앵커 샘플 생성 → 학습 → 홀드아웃 평가 → 아티팩트 저장",
    schedule=None,          # 수동 트리거 전용
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["utils", "manual"],
    params={
        "as_of_end": Param(
            None,
            type=["null", "string"],
            description="학습 기준 종료일(YYYY-MM-DD). 비우면 라벨 확정 가능한 최신일 자동 사용.",
        ),
        "rolling_months": Param(
            None, type=["null", "integer"], description="학습 rolling 윈도우(개월). 비우면 config 값."
        ),
        "skip_gate": Param(
            False, type="boolean", description="True 면 게이트 결과와 무관하게 champion 승격."
        ),
        "dry_run": Param(
            False, type="boolean", description="True 면 아티팩트 저장/승격을 하지 않는다."
        ),
    },
)
def risk_model_train():

    @task
    def resolve_anchors() -> dict:
        """원천 최신일에서 as_of_end 를 도출하고 학습/홀드아웃 앵커를 확정한다."""
        from ml.samples import (
            detect_label_ready_max,
            resolve_anchors as _resolve,
        )
        from ml.settings import load_config

        params = _params()
        cfg = load_config()
        if params and params.get("rolling_months"):
            cfg["run"]["rolling_months"] = int(params["rolling_months"])

        probe = detect_label_ready_max(cfg)

        from datetime import date

        plan = _resolve(
            cfg,
            as_of_end=(params or {}).get("as_of_end"),
            label_ready_max=date.fromisoformat(probe["label_ready_max"]),
        )
        plan["source_probe"] = probe
        plan["rolling_months"] = int(cfg.get_path("run.rolling_months"))
        plan["window_days"] = int(cfg.get_path("run.window_days"))
        plan["_params"] = {k: params.get(k) for k in ("skip_gate", "dry_run")}
        print(
            f"run_key={plan['run_key']}  "
            f"train {plan['train_range'][0]}~{plan['train_range'][1]} "
            f"({len(plan['train_anchors'])}개, {plan['gap_days']}일 gap)  "
            f"holdout {plan['holdout_range'][0]}~{plan['holdout_range'][1]} "
            f"({len(plan['holdout_anchors'])}개)"
        )
        return plan

    @task
    def validate_inputs(plan: dict) -> dict:
        """라벨 확정 여부, 대여이력 하한, 고장신고 공백 구간을 확인한다."""
        probe = plan["source_probe"]
        window = int(plan.get("window_days") or 14)
        errors, warnings = [], []

        if plan["as_of_end"] > probe["label_ready_max"]:
            errors.append(
                f"as_of_end({plan['as_of_end']}) > 라벨 확정 한계({probe['label_ready_max']})"
            )
        from datetime import date, timedelta

        earliest_anchor = date.fromisoformat(probe["rent_min"]) + timedelta(days=window)
        if date.fromisoformat(plan["train_range"][0]) < earliest_anchor:
            warnings.append(
                f"학습 시작({plan['train_range'][0]})이 피처 계산 가능 하한"
                f"({earliest_anchor.isoformat()}, rent_min+{window}일)보다 이릅니다 "
                "— 해당 구간 앵커는 피처가 불완전합니다."
            )
        if plan["train_range"][0] < probe["fault_min"]:
            warnings.append(
                f"학습 시작({plan['train_range'][0]})이 고장신고 시작({probe['fault_min']})보다 이릅니다 "
                "— 해당 구간 앵커는 라벨이 전부 0입니다."
            )
        blocking = [
            m
            for m in probe["missing_fault_months"]
            if plan["train_range"][0][:7] <= m <= plan["holdout_range"][1][:7]
        ]
        if blocking:
            errors.append(f"학습/평가 구간에 고장신고 없는 달: {blocking}")

        for w in warnings:
            print(f"[warn] {w}")
        if errors:
            raise ValueError("입력 검증 실패:\n  - " + "\n  - ".join(errors))
        return {"ok": True, "warnings": warnings}

    @task(retries=0)  # Spark job 은 재시도보다 로그 확인이 먼저다
    def build_train_samples(plan: dict, _gate: dict) -> dict:
        """앵커별 피처+라벨을 gold.fact_bike_train_sample 파티션에 dynamic overwrite."""
        from ml.settings import load_config

        cfg = load_config()
        app_env = os.getenv("APP_ENV", "local")

        if app_env == "aws":
            emr_plan = {k: v for k, v in plan.items() if k != "source_probe"}
            run_emr_serverless_spark_job(
                entry_point="local:///opt/app/src/ml/samples.py",
                entry_point_arguments=[
                    "--anchor-plan-json",
                    json.dumps(emr_plan, ensure_ascii=False, separators=(",", ":")),
                ],
                name=f"risk-model-build-samples-{plan['run_key']}",
                log_group_name="/emr-serverless/risk-model-train",
                log_stream_name_prefix=plan["run_key"],
                tags={
                    "dag_id": "risk_model_train",
                    "task_id": "build_train_samples",
                    "run_key": plan["run_key"],
                },
            )

            stats = {
                "train_anchors": len(plan["train_anchors"]),
                "holdout_anchors": len(plan["holdout_anchors"]),
                "sample_path": cfg.get_path("paths.train_sample"),
                "pos_new_path": cfg.get_path("paths.label_pos_new"),
            }
            print(f"샘플 적재 완료(EMR Serverless): {stats}")
            return stats

        if app_env != "local":
            raise ValueError(f"지원하지 않는 APP_ENV={app_env!r} 입니다. local 또는 aws만 허용합니다.")

        from ml.samples import get_spark, write_samples

        spark = get_spark(cfg, f"risk-model-samples-{plan['run_key']}")
        try:
            stats = write_samples(spark, cfg, plan)
        finally:
            spark.stop()
        print(f"샘플 적재 완료: {stats}")
        return stats

    @task
    def assert_train_table(plan: dict, stats: dict) -> dict:
        """행수·앵커수·양성비율·결측률 게이트."""
        from ml.settings import load_config
        from ml.train import assert_quality, load_samples

        cfg = load_config()
        df = load_samples(stats["sample_path"], "train", plan["train_anchors"])
        report = assert_quality(df, cfg)
        print(
            f"학습 후보 {report['rows_eligible']:,}행 / 앵커 {report['anchors']}개 / "
            f"양성 {report['pos_count']:,}건 ({report['pos_rate']*100:.2f}%) / "
            f"제외율 {report['excluded_rate']*100:.1f}% / 차종 {report['bike_class_dist']}"
        )
        if report["errors"]:
            raise ValueError("학습 테이블 검증 실패:\n  - " + "\n  - ".join(report["errors"]))
        return report

    @task
    def train_and_evaluate(plan: dict, stats: dict, quality: dict) -> dict:
        """후보 학습 → 홀드아웃 walk-forward 평가 → 승격 → 게이트 판정.

        rule_trips/logreg/lgbm 3개를 전부 학습·평가하는 건 "baseline보다 실제로
        나은가"를 매번 리포트로 확인하기 위한 것뿐이다(select_best는 그 리포트용
        1등을 보여준다) - 승격 후보는 항상 models.primary 하나로 고정한다.
        타입이 재학습마다 바뀌면 (a) rule_trips처럼 score()가 확률이 아니라 다른
        스케일 값을 반환하는 타입이 승격돼 추론 쪽 검증이 깨질 수 있고, (b) logreg/
        lgbm처럼 서로 확률을 반환하는 타입끼리도 캘리브레이션이 달라서 risk_grade
        컷오프가 조용히 안 맞게 된다. 모델 타입을 바꾸는 건 재학습이 자동으로 정할
        일이 아니라 models.primary를 사람이 바꾸는 명시적 결정이어야 한다.

        학습된 모델 객체는 XCom 으로 넘기지 않는다. 저장까지 이 태스크에서 끝낸다.
        """
        from ml import registry
        from ml.evaluate import apply_gate, select_best, walk_forward
        from ml.settings import load_config
        from ml.train import (
            feature_importance,
            load_pos_new,
            load_samples,
            train_candidates,
        )

        cfg = load_config()
        params = plan.get("_params", {})

        train_df = load_samples(stats["sample_path"], "train", plan["train_anchors"])
        trained = train_candidates(train_df, cfg)
        candidates = trained["candidates"]
        print(f"학습 완료: {list(candidates)}  {trained['train_stats']}")

        holdout_df = load_samples(stats["sample_path"], "holdout", plan["holdout_anchors"])
        pos_new = load_pos_new(stats["pos_new_path"])
        eval_result = walk_forward(
            candidates, holdout_df, pos_new, int(cfg.get_path("run.top_k", 500))
        )
        for name, m in eval_result["metrics"].items():
            print(
                f"  {name:<12} capture@{m['top_k']}={m['capture_at_k']}%  "
                f"std={m['capture_std']}  min={m['capture_min']}  "
                f"pr_auc={m['pr_auc']}  days={m['eval_days']}"
            )

        report_best = select_best(eval_result["metrics"], cfg)
        primary = cfg.get_path("models.primary", "lgbm")
        if primary not in candidates:
            raise ValueError(
                f"models.primary={primary!r}가 학습된 후보({list(candidates)})에 없습니다 - "
                "models.candidates 설정을 확인하세요"
            )
        if report_best != primary:
            print(f"참고: 이번 평가는 {report_best}가 1등이었지만, 승격 후보는 항상 primary({primary})로 고정")

        champion = registry.get_champion(cfg)
        gate = apply_gate(primary, eval_result["metrics"], champion, cfg)
        if params.get("skip_gate"):
            gate = {**gate, "passed": True, "reason": gate["reason"] + " (skip_gate=True)"}
        print(f"승격 후보: {primary}  게이트: {gate['passed']} — {gate['reason']}")

        importance = feature_importance(candidates[primary])

        if params.get("dry_run"):
            print("[dry_run] 아티팩트 저장/승격 생략")
            return {
                "run_key": plan["run_key"],
                "selected": primary,
                "report_best": report_best,
                "metrics": eval_result["metrics"],
                "gate": gate,
                "dry_run": True,
            }

        entry = registry.save_run(
            cfg,
            run_key=plan["run_key"],
            best_name=primary,
            candidates=candidates,
            eval_result=eval_result,
            anchor_plan={k: v for k, v in plan.items() if k != "source_probe"},
            train_stats=trained["train_stats"],
            quality=quality,
            gate=gate,
            importance=importance,
        )
        registry.promote(cfg, entry)
        mlflow_run = registry.log_to_mlflow(
            cfg, plan["run_key"], primary, eval_result, entry, trained["train_stats"], importance
        )

        print(f"아티팩트 → {entry['artifact_uri']}")
        print(f"champion 갱신: {gate['passed']}")
        return {
            "run_key": entry["model_version"],
            "selected": primary,
            "report_best": report_best,
            "artifact_uri": entry["artifact_uri"],
            "metrics_uri": entry["metrics_uri"],
            "metrics": eval_result["metrics"],
            "gate": gate,
            "mlflow_run_id": mlflow_run,
            "importance": importance,
        }

    @task
    def report(result: dict) -> dict:
        """실행 요약. 게이트 미통과는 실패가 아니라 '승격 보류' 로 기록한다."""
        g = result["gate"]
        status = "PROMOTED" if g["passed"] else "HELD (champion 유지)"
        note = "" if result["report_best"] == result["selected"] else f"  (참고: 리포트 1등은 {result['report_best']})"
        print(
            f"[{result['run_key']}] {result['selected']} → {status}{note}\n"
            f"  {g['reason']}\n"
            f"  아티팩트: {result.get('artifact_uri', '(dry_run)')}"
        )
        return result

    plan = resolve_anchors()
    checked = validate_inputs(plan)
    stats = build_train_samples(plan, checked)
    quality = assert_train_table(plan, stats)
    result = train_and_evaluate(plan, stats, quality)
    report(result)


risk_model_train()
