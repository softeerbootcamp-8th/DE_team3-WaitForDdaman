"""위험도 모델 학습(재학습) DAG — 로컬 Airflow에서 실 AWS(EMR Serverless/S3)를 대상으로 실행.

risk_model_train_dag.py(prod EC2용)와 태스크 로직은 완전히 동일하다. 유일한 차이는
dag_id/tags뿐이다 - prod EC2(t4g.large, 8GB)가 assert_train_table/train_and_evaluate의
pandas+sklearn+lightgbm 학습 부하를 감당하기 빠듯해서(#실측: 유휴 상태에서도 available
2.1GB), 그 무거운 계산을 prod 워커가 아니라 이 DAG을 트리거하는 로컬 머신(docker-compose.local.yml)
에서 돌리기 위해 별도 dag_id로 분리했다. build_train_samples(EMR Serverless)는 원래도
AWS 관리형 컴퓨트라 어디서 트리거하든 동일하게 돈다.

실행 전 준비 (docker-compose.local.yml 컨테이너에 아래 환경변수가 실 AWS 값으로 채워져야 함
- .env.prod에서 그대로 복사):
  APP_ENV=aws
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN (유효한 자격증명)
  EMR_SPARK_APPLICATION_ID / EMR_SPARK_EXECUTION_ROLE_ARN
  ICEBERG_CATALOG_NAME / ICEBERG_WAREHOUSE_PATH / RAW_BUCKET / WAREHOUSE_BUCKET
  ICEBERG_JDBC_CATALOG_URI / ICEBERG_JDBC_CATALOG_USER / ICEBERG_JDBC_CATALOG_PASSWORD
  RISK_MODEL_CONFIG=/opt/airflow/pylib/config/risk_model.yaml
    (docker-compose.local.yml 기본값은 risk_model.local.yaml이라 이 DAG을 쓸 때는
     반드시 오버라이드해야 한다 - 안 그러면 로컬 개발용 경로/카탈로그로 돈다.)

APP_ENV가 여전히 local이면 build_train_samples가 로컬 Spark(LocalStack)로 폴백한다
(risk_model_train_dag.py와 동일 분기) - 이 DAG의 의미가 없어지므로 반드시 aws로 켜고 쓸 것.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import timedelta

# /opt/airflow/pipeline 은 PYTHONPATH 에 없으므로 부모 경로를 넣어준다.
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/airflow")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

PYLIB_DIR = os.environ.get("PYLIB_DIR", "/opt/airflow/pylib")
if PYLIB_DIR not in sys.path:
    sys.path.insert(0, PYLIB_DIR)

# resolve_anchors가 Spark 대신 common.partition_listing(boto3)을 쓰므로(#148),
# gold_dim_fact_dag.py와 동일한 패턴으로 ingestion을 네임스페이스 패키지 루트로
# sys.path에 얹는다.
#
# risk_model_train_dag.py(prod)와 달리 이 DAG은 로컬 Airflow 컨테이너에서 "실
# AWS"(APP_ENV=aws, 실 자격증명)를 의도적으로 쓰는 용도라, ingestion/.env(LocalStack
# 기본값 - APP_ENV=local, AWS_ACCESS_KEY_ID=빈값)를 무조건 덮어쓰면 docker-compose가
# 넘겨준 실 값이 DAG 파싱/태스크 실행 때마다 도로 지워진다(#실측). 그래서 여기서는
# 이미 채워진 값(compose environment: 블록이 준 값)은 덮지 않는다 - ingestion/.env는
# compose가 안 채워준 나머지 로컬 전용 기본값만 보충하는 용도로 남긴다.
INGESTION_DIR = os.environ.get("INGESTION_DIR", "/opt/airflow/ingestion")


def _load_ingestion_env(env_path: str) -> None:
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            if os.environ.get(key):  # 이미 실 값이 채워져 있으면 덮지 않는다
                continue
            os.environ[key] = value.strip()


_load_ingestion_env(f"{INGESTION_DIR}/.env")
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
    dag_id="risk_model_train_local",
    description="위험도 모델 학습 — 로컬 Airflow에서 실 EMR Serverless/S3 대상 실행 (prod EC2 미사용)",
    schedule=None,          # 수동 트리거 전용
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["independent", "manual", "local-aws"],
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
def risk_model_train_local():

    @task
    def resolve_anchors() -> dict:
        """원천 최신일에서 as_of_end 를 도출하고 학습/홀드아웃 앵커를 확정한다."""
        from pipeline.train_risk_model.samples import (
            detect_label_ready_max,
            resolve_anchors as _resolve,
        )
        from pipeline.train_risk_model.settings import load_config

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
        """앵커별 피처+라벨을 gold.fact_bike_train_sample 파티션에 dynamic overwrite.

        EMR Serverless는 AWS 관리형 컴퓨트라 이 태스크 자체는 prod와 동일하게
        가볍다 - 무거운 건 아래 assert_train_table/train_and_evaluate 쪽이고,
        그건 이 DAG을 트리거한 로컬 Airflow 워커(당신 컴퓨터)에서 돈다.
        """
        from pipeline.train_risk_model.settings import load_config

        cfg = load_config()
        app_env = os.getenv("APP_ENV", "local")

        if app_env == "aws":
            emr_plan = {k: v for k, v in plan.items() if k != "source_probe"}
            run_emr_serverless_spark_job(
                entry_point="local:///opt/app/pipeline/train_risk_model/samples.py",
                entry_point_arguments=[
                    "--anchor-plan-json",
                    json.dumps(emr_plan, ensure_ascii=False, separators=(",", ":")),
                ],
                name=f"risk-model-build-samples-local-{plan['run_key']}",
                log_group_name="/emr-serverless/risk-model-train",
                log_stream_name_prefix=plan["run_key"],
                tags={
                    "dag_id": "risk_model_train_local",
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

        from pipeline.train_risk_model.samples import get_spark, write_samples

        spark = get_spark(cfg, f"risk-model-samples-{plan['run_key']}")
        try:
            stats = write_samples(spark, cfg, plan)
        finally:
            spark.stop()
        print(f"샘플 적재 완료: {stats}")
        return stats

    @task
    def assert_train_table(plan: dict, stats: dict) -> dict:
        """행수·앵커수·양성비율·결측률 게이트. pandas 로 로컬 머신 메모리에서 실행된다."""
        from pipeline.train_risk_model.settings import load_config
        from pipeline.train_risk_model.train import assert_quality, load_samples

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

        risk_model_train_dag.py의 동명 태스크와 완전히 동일한 로직 - sklearn/lightgbm
        학습이 여기서 실제로 로컬 머신 CPU/메모리를 쓴다(prod EC2 아님).
        """
        from pipeline.train_risk_model import registry
        from pipeline.train_risk_model.evaluate import apply_gate, select_best, walk_forward
        from pipeline.train_risk_model.settings import load_config
        from pipeline.train_risk_model.train import (
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


risk_model_train_local()
