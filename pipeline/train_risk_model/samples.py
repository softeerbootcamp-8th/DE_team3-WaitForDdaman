"""앵커 산출과 학습 샘플 적재.

이 모듈이 파이프라인에서 유일한 Spark 작업이다.
EMR 로 옮길 때는 아래 main() 을 spark-submit 엔트리포인트로 그대로 쓰고,
DAG 의 build_train_samples 태스크만 EmrAddStepsOperator 로 바꾸면 된다.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

from pyspark.sql import SparkSession

from pipeline.train_risk_model import features as ft
from pipeline.train_risk_model.settings import load_config


# ── Spark 세션 ────────────────────────────────────────────────────────
def get_spark(cfg, app_name: str = "risk-model-train") -> SparkSession:
    """팀 표준 빌더(ingestion/common/spark_session.py)를 그대로 재사용한다.

    카탈로그 등록(bike_catalog, hadoop/glue 분기), S3A 설정, 로컬 driver
    bind/memory 튜닝을 전부 그쪽에서 담당하므로 여기서 중복 구현하지 않는다.
    risk_model.yaml 의 spark.* 값은 참고용일 뿐 실제로는 쓰이지 않는다 —
    로컬 튜닝은 SPARK_LOCAL_MASTER / SPARK_LOCAL_DRIVER_MEMORY /
    SPARK_LOCAL_SHUFFLE_PARTITIONS 환경변수로 한다 (팀 빌더 쪽 규칙과 동일).
    """
    from ingestion.common.spark_session import build_spark_session

    return build_spark_session(app_name)


# ── 앵커 계산 (결정론적) ──────────────────────────────────────────────
def resolve_anchors(cfg, as_of_end: str | date | None = None, label_ready_max: date | None = None) -> dict:
    """as_of_end 를 데이터에서 뽑고 앵커 두 세트를 만든다.

    datetime.now() 를 쓰지 않는다. 수동 트리거라 data_interval 을 신뢰할 수 없으므로
    '라벨이 확정된 마지막 날짜' 를 기준으로 삼고, 그 값을 run_key 로 기록한다.
    같은 as_of_end 로 재실행하면 모든 산출물이 같은 경로를 덮어써 멱등이다.
    """
    horizon = int(cfg.get_path("run.horizon_days", 14))
    holdout_days = int(cfg.get_path("run.holdout_days", 60))
    step = int(cfg.get_path("run.anchor_step_days", 7))
    rolling_months = int(cfg.get_path("run.rolling_months", 24))

    if as_of_end:
        end = as_of_end if isinstance(as_of_end, date) else date.fromisoformat(str(as_of_end))
        if label_ready_max and end > label_ready_max:
            raise ValueError(
                f"as_of_end={end} 는 라벨 확정 한계({label_ready_max})를 넘습니다. "
                f"horizon={horizon}일 만큼 라벨이 아직 관측되지 않았습니다."
            )
    else:
        if not label_ready_max:
            raise ValueError("as_of_end 또는 label_ready_max 중 하나는 필요합니다.")
        end = label_ready_max

    holdout_start = end - timedelta(days=holdout_days - 1)
    train_end = holdout_start - timedelta(days=horizon)  # 라벨창 겹침 방지 gap
    train_start = train_end - timedelta(days=int(rolling_months * 30.44))

    holdout = [holdout_start + timedelta(days=i) for i in range(holdout_days)]
    train, cur = [], train_end
    while cur >= train_start:
        train.append(cur)
        cur -= timedelta(days=step)
    train.sort()

    return {
        "run_key": end.strftime("%Y%m%d"),
        "as_of_end": end.isoformat(),
        "train_anchors": [d.isoformat() for d in train],
        "holdout_anchors": [d.isoformat() for d in holdout],
        "train_range": [train_start.isoformat(), train_end.isoformat()],
        "holdout_range": [holdout_start.isoformat(), end.isoformat()],
        "gap_days": horizon,
    }


def detect_label_ready_max(cfg) -> dict:
    """고장신고 최신일 - horizon = 라벨 확정 한계. 대여이력 최신일도 함께 본다.

    Spark 세션 없이 Iceberg 파티션 디렉터리(boto3)만 나열해서 구한다 (#148).
    fault/rent 둘 다 날짜 identity 파티션(reg_date_partition/rent_date_partition)
    이라 파티션 디렉터리 존재 여부가 곧 "그 날짜에 데이터가 있다"와 같다 — 파티션을
    지우는 잡이 없어 오탐이 없다 (common/partition_listing.py 참고).
    """
    from common.partition_listing import list_partitions

    horizon = int(cfg.get_path("run.horizon_days", 14))
    fault_days = list_partitions(cfg.get_path("sources.failure_report"), "reg_date_partition")
    rent_days = list_partitions(cfg.get_path("sources.rental_history"), "rent_date_partition")
    if not fault_days or not rent_days:
        raise ValueError("silver 원천이 비어 있습니다.")

    f_min, f_max = date.fromisoformat(fault_days[0]), date.fromisoformat(fault_days[-1])
    r_min, r_max = date.fromisoformat(rent_days[0]), date.fromisoformat(rent_days[-1])

    # 고장신고 공백 월 검사 — 라벨이 없는 구간에 앵커를 잡으면 학습이 망가진다
    have = {d[:7] for d in fault_days}
    cur, missing = date(f_min.year, f_min.month, 1), []
    while cur <= f_max:
        if cur.strftime("%Y-%m") not in have:
            missing.append(cur.strftime("%Y-%m"))
        cur = date(cur.year + (cur.month // 12), (cur.month % 12) + 1, 1)

    return {
        "fault_max": f_max.isoformat(),
        "fault_min": f_min.isoformat(),
        "rent_max": r_max.isoformat(),
        "rent_min": r_min.isoformat(),
        # as_of_end 상한 = min(rental 데이터 최신일, 라벨이 horizon일만큼 관찰된 최신일).
        # rental 은 horizon 을 뺄 이유가 없다 — 라벨(고장신고)만 앞으로 horizon일 관찰돼야 한다.
        # (구 버전은 min(f_max, r_max) - horizon 으로 계산해 rent_max 에도 불필요하게
        #  horizon 을 빼서 최대 horizon일만큼 앵커 구간을 손해봤다)
        "label_ready_max": min(r_max, f_max - timedelta(days=horizon)).isoformat(),
        "missing_fault_months": missing,
    }


# ── 샘플 적재 ─────────────────────────────────────────────────────────
def write_samples(spark: SparkSession, cfg, anchor_plan: dict) -> dict:
    """학습/홀드아웃 샘플과 메인지표 분모 테이블을 적재한다.

    파티션 단위 dynamic overwrite 라 같은 앵커로 재실행해도 중복되지 않는다.
    """
    from pipeline.train_risk_model.sql_engine import SqlEngine

    engine = SqlEngine.for_spark(spark)
    sample_path = cfg.get_path("paths.train_sample")
    pos_path = cfg.get_path("paths.label_pos_new")

    train_anchors = [date.fromisoformat(d) for d in anchor_plan["train_anchors"]]
    holdout_anchors = [date.fromisoformat(d) for d in anchor_plan["holdout_anchors"]]

    rent = ft.apply_trip_filters(engine, ft.read_rental(engine, cfg), cfg)
    fault = ft.read_fault(engine, cfg)
    rent.cache()

    stats = {}
    for anchor_type, anchors in (("train", train_anchors), ("holdout", holdout_anchors)):
        df = ft.build_samples(engine, cfg, anchors, anchor_type, rent=rent, fault=fault)
        (
            df.write.mode("overwrite")
            .partitionBy("snapshot_date")
            .parquet(f"{sample_path}/anchor_type={anchor_type}")
        )
        stats[f"{anchor_type}_anchors"] = len(anchors)

    # 메인지표 분모 (피처 테이블에 없는 자전거까지 포함)
    pos_new = ft.build_pos_new(engine, fault, ft.anchor_frame(engine, holdout_anchors), cfg)
    (
        pos_new.withColumnRenamed("as_of", "snapshot_date")
        .write.mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(pos_path)
    )

    rent.unpersist()
    stats.update({"sample_path": sample_path, "pos_new_path": pos_path})
    return stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--anchor-plan", required=True, help="resolve_anchors 결과 JSON 파일 경로")
    args = ap.parse_args()

    cfg = load_config(args.config)
    with open(args.anchor_plan, encoding="utf-8") as fh:
        plan = json.load(fh)

    spark = get_spark(cfg, "risk-model-build-samples")
    try:
        print(json.dumps(write_samples(spark, cfg, plan), ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
