"""risk_model_train DAG을 Airflow 없이(prod EC2 밖에서) 실행한다.

build_train_samples(EMR Serverless)만 원격이고, 나머지(assert_train_table/
train_and_evaluate)는 이 스크립트를 실행하는 머신에서 pandas/sklearn/lightgbm으로
돈다 - prod Airflow 워커 EC2 메모리를 전혀 쓰지 않는다. S3가 유일한 접점이라
risk_model_train_dag.py(airflow/dags/risk_model_train_dag.py)의 태스크 바디를
그대로 옮긴 것뿐, 로직 자체는 바꾸지 않는다. Slack 알림/XCom 기록/게이트 감사이력 같은
Airflow 오케스트레이션 부가기능은 여기 없다 - 그건 이 스크립트가 대신하지 않는다.

준비물
  - APP_ENV=aws
  - 유효한 AWS 자격증명: emr-serverless:StartJobRun/GetJobRun, iam:PassRole
    (EMR_SPARK_EXECUTION_ROLE_ARN), S3 read(원천 silver)/write(train_sample,
    label_pos_new, model_root)
  - EMR_SPARK_APPLICATION_ID, EMR_SPARK_EXECUTION_ROLE_ARN, AWS_DEFAULT_REGION
    (.env.prod 값과 동일 - 그 파일에서 복사해서 export)
  - RISK_MODEL_CONFIG=<레포 경로>/config/risk_model.yaml
  - ICEBERG_WAREHOUSE_PATH 등 config.SETTINGS가 읽는 값(원천 조회용, detect_label_ready_max)
  - pip install -r spark/requirements-emr.txt (pandas/pyarrow/boto3/scikit-learn/lightgbm/PyYAML)

실행
  python -m pipeline.train_risk_model.local_run
  python -m pipeline.train_risk_model.local_run --as-of-end 2026-08-20 --dry-run
  python -m pipeline.train_risk_model.local_run --skip-emr   # train_sample이 이미 S3에 있을 때
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
from datetime import date, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INGESTION_DIR = os.path.join(PROJECT_ROOT, "ingestion")
for _p in (PROJECT_ROOT, INGESTION_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"환경변수 {name}가 필요합니다.")
    return value


# ── EMR Serverless 제출 (dag_common.run_emr_serverless_spark_job과 동일 로직,
#    airflow 패키지 없이 로컬에서 돌 수 있도록 재구현) ──────────────────────
def _spark_submit_parameters() -> str:
    env = {
        "APP_ENV": "aws",
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", ""),
        "RISK_MODEL_CONFIG": os.getenv("EMR_RISK_MODEL_CONFIG", "/opt/app/config/risk_model.yaml"),
    }
    for key in (
        "ICEBERG_CATALOG_TYPE",
        "ICEBERG_CATALOG_NAME",
        "ICEBERG_WAREHOUSE_PATH",
        "ICEBERG_JDBC_CATALOG_URI",
        "ICEBERG_CATALOG_SECRET_ARN",
        "RAW_BUCKET",
        "WAREHOUSE_BUCKET",
    ):
        value = os.getenv(key)
        if value:
            env[key] = value
    if not env.get("ICEBERG_CATALOG_SECRET_ARN"):
        for key in ("ICEBERG_JDBC_CATALOG_USER", "ICEBERG_JDBC_CATALOG_PASSWORD"):
            value = os.getenv(key)
            if value:
                env[key] = value

    params = []
    for key, value in env.items():
        if value == "":
            continue
        params += [
            "--conf",
            f"spark.emr-serverless.driverEnv.{key}={value}",
            "--conf",
            f"spark.executorEnv.{key}={value}",
        ]
    return " ".join(shlex.quote(p) for p in params)


def submit_emr_job(
    *,
    entry_point: str,
    name: str,
    entry_point_arguments: list[str] | None = None,
    log_group_name: str,
    log_stream_name_prefix: str | None = None,
    tags: dict[str, str] | None = None,
) -> str:
    import boto3

    application_id = _require_env("EMR_SPARK_APPLICATION_ID")
    execution_role_arn = _require_env("EMR_SPARK_EXECUTION_ROLE_ARN")
    region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")
    client = boto3.client("emr-serverless", region_name=region)

    response = client.start_job_run(
        applicationId=application_id,
        executionRoleArn=execution_role_arn,
        name=name,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_point,
                "entryPointArguments": entry_point_arguments or [],
                "sparkSubmitParameters": _spark_submit_parameters(),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "cloudWatchLoggingConfiguration": {
                    "enabled": True,
                    "logGroupName": log_group_name,
                    "logStreamNamePrefix": log_stream_name_prefix or name,
                }
            }
        },
        tags=tags or {},
    )
    job_run_id = response["jobRunId"]
    print(f"EMR Serverless job submitted: application={application_id} job_run_id={job_run_id}")

    terminal = {"SUCCESS", "FAILED", "CANCELLED"}
    poll_seconds = int(os.getenv("EMR_SPARK_POLL_INTERVAL_SECONDS", "30"))
    max_seconds = int(os.getenv("EMR_SPARK_POLL_MAX_SECONDS", str(6 * 60 * 60)))
    deadline = time.monotonic() + max_seconds
    last_state = None
    while True:
        job = client.get_job_run(applicationId=application_id, jobRunId=job_run_id)["jobRun"]
        state = job["state"]
        if state != last_state:
            print(f"EMR Serverless job {job_run_id} state={state}: {job.get('stateDetails', '')}")
            last_state = state
        if state == "SUCCESS":
            return job_run_id
        if state in terminal:
            raise RuntimeError(
                f"EMR Serverless job {job_run_id} failed with state={state}: {job.get('stateDetails', '')}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"EMR Serverless job {job_run_id} did not finish within {max_seconds} seconds "
                f"(last_state={state})"
            )
        time.sleep(poll_seconds)


# ── DAG 태스크 바디 그대로 ──────────────────────────────────────────────
def _validate_inputs(cfg, plan: dict, probe: dict) -> None:
    window = int(plan.get("window_days") or cfg.get_path("run.window_days", 14))
    errors, warnings = [], []

    if plan["as_of_end"] > probe["label_ready_max"]:
        errors.append(f"as_of_end({plan['as_of_end']}) > 라벨 확정 한계({probe['label_ready_max']})")

    earliest_anchor = date.fromisoformat(probe["rent_min"]) + timedelta(days=window)
    if date.fromisoformat(plan["train_range"][0]) < earliest_anchor:
        warnings.append(
            f"학습 시작({plan['train_range'][0]})이 피처 계산 가능 하한"
            f"({earliest_anchor.isoformat()}, rent_min+{window}일)보다 이릅니다."
        )
    if plan["train_range"][0] < probe["fault_min"]:
        warnings.append(
            f"학습 시작({plan['train_range'][0]})이 고장신고 시작({probe['fault_min']})보다 이릅니다."
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
        raise SystemExit("입력 검증 실패:\n  - " + "\n  - ".join(errors))


def main() -> None:
    ap = argparse.ArgumentParser(description="risk_model_train DAG을 Airflow 없이 로컬에서 실행")
    ap.add_argument("--as-of-end", default=None, help="학습 기준 종료일(YYYY-MM-DD). 비우면 자동 산출.")
    ap.add_argument("--rolling-months", type=int, default=None)
    ap.add_argument("--skip-gate", action="store_true", help="게이트 결과와 무관하게 champion 승격")
    ap.add_argument("--dry-run", action="store_true", help="아티팩트 저장/승격 생략")
    ap.add_argument(
        "--skip-emr", action="store_true", help="train_sample이 이미 S3에 있으면 EMR 제출을 생략"
    )
    args = ap.parse_args()

    if os.environ.get("APP_ENV") != "aws":
        raise SystemExit("APP_ENV=aws 로 설정하세요 (실 S3/EMR Serverless 대상).")

    from pipeline.train_risk_model import registry
    from pipeline.train_risk_model.evaluate import apply_gate, select_best, walk_forward
    from pipeline.train_risk_model.samples import detect_label_ready_max, resolve_anchors
    from pipeline.train_risk_model.settings import load_config
    from pipeline.train_risk_model.train import (
        assert_quality,
        feature_importance,
        load_pos_new,
        load_samples,
        train_candidates,
    )

    cfg = load_config()
    if args.rolling_months:
        cfg["run"]["rolling_months"] = args.rolling_months

    # ── resolve_anchors ──
    probe = detect_label_ready_max(cfg)
    plan = resolve_anchors(
        cfg,
        as_of_end=args.as_of_end,
        label_ready_max=date.fromisoformat(probe["label_ready_max"]),
    )
    plan["source_probe"] = probe
    plan["window_days"] = int(cfg.get_path("run.window_days"))
    print(
        f"run_key={plan['run_key']}  "
        f"train {plan['train_range'][0]}~{plan['train_range'][1]} ({len(plan['train_anchors'])}개)  "
        f"holdout {plan['holdout_range'][0]}~{plan['holdout_range'][1]} ({len(plan['holdout_anchors'])}개)"
    )

    # ── validate_inputs ──
    _validate_inputs(cfg, plan, probe)

    # ── build_train_samples (EMR Serverless) ──
    sample_path = cfg.get_path("paths.train_sample")
    pos_path = cfg.get_path("paths.label_pos_new")
    if args.skip_emr:
        print(f"--skip-emr: EMR 제출 생략, 기존 {sample_path} 사용")
    else:
        emr_plan = {k: v for k, v in plan.items() if k != "source_probe"}
        submit_emr_job(
            entry_point="local:///opt/app/pipeline/train_risk_model/samples.py",
            entry_point_arguments=[
                "--anchor-plan-json",
                json.dumps(emr_plan, ensure_ascii=False, separators=(",", ":")),
            ],
            name=f"risk-model-build-samples-{plan['run_key']}",
            log_group_name="/emr-serverless/risk-model-train",
            log_stream_name_prefix=plan["run_key"],
            tags={
                "dag_id": "risk_model_train_local",
                "task_id": "build_train_samples",
                "run_key": plan["run_key"],
            },
        )
    stats = {"sample_path": sample_path, "pos_new_path": pos_path}

    # ── assert_train_table (여기서부터 로컬 pandas) ──
    train_df = load_samples(stats["sample_path"], "train", plan["train_anchors"])
    quality = assert_quality(train_df, cfg)
    print(
        f"학습 후보 {quality['rows_eligible']:,}행 / 앵커 {quality['anchors']}개 / "
        f"양성 {quality['pos_count']:,}건 ({quality['pos_rate']*100:.2f}%) / "
        f"제외율 {quality['excluded_rate']*100:.1f}% / 차종 {quality['bike_class_dist']}"
    )
    if quality["errors"]:
        raise SystemExit("학습 테이블 검증 실패:\n  - " + "\n  - ".join(quality["errors"]))

    # ── train_and_evaluate ──
    trained = train_candidates(train_df, cfg)
    candidates = trained["candidates"]
    print(f"학습 완료: {list(candidates)}  {trained['train_stats']}")

    holdout_df = load_samples(stats["sample_path"], "holdout", plan["holdout_anchors"])
    pos_new = load_pos_new(stats["pos_new_path"])
    eval_result = walk_forward(candidates, holdout_df, pos_new, int(cfg.get_path("run.top_k", 500)))
    for name, m in eval_result["metrics"].items():
        print(
            f"  {name:<12} capture@{m['top_k']}={m['capture_at_k']}%  "
            f"std={m['capture_std']}  min={m['capture_min']}  "
            f"pr_auc={m['pr_auc']}  days={m['eval_days']}"
        )

    report_best = select_best(eval_result["metrics"], cfg)
    primary = cfg.get_path("models.primary", "lgbm")
    if primary not in candidates:
        raise SystemExit(
            f"models.primary={primary!r}가 학습된 후보({list(candidates)})에 없습니다 - "
            "models.candidates 설정을 확인하세요"
        )
    if report_best != primary:
        print(f"참고: 이번 평가는 {report_best}가 1등이었지만, 승격 후보는 항상 primary({primary})로 고정")

    champion = registry.get_champion(cfg)
    gate = apply_gate(primary, eval_result["metrics"], champion, cfg)
    if args.skip_gate:
        gate = {**gate, "passed": True, "reason": gate["reason"] + " (skip_gate=True)"}
    print(f"승격 후보: {primary}  게이트: {gate['passed']} — {gate['reason']}")

    importance = feature_importance(candidates[primary])

    if args.dry_run:
        print("[dry_run] 아티팩트 저장/승격 생략")
        return

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

    print(f"아티팩트 → {entry['artifact_uri']}")
    print(f"champion 갱신: {gate['passed']}")


if __name__ == "__main__":
    main()
