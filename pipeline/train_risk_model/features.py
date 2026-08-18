"""피처 / 라벨 정의 — 학습 DAG과 추론 DAG이 **이 모듈만** 사용한다.

여기서 로직이 갈라지는 순간 train-serving skew 가 생긴다.
피처 정의를 바꾸면 FEATURE_VERSION 을 올린다.

성능 설계: 노트북은 앵커마다 대여이력을 필터+집계해서 앵커 N개면 N번 스캔했다.
여기서는 (bike_id, 날짜) 일 집계를 1회 만들고, 앵커 목록(작은 테이블)을 broadcast
join 해서 한 번에 모든 앵커의 피처를 만든다. 학습 앵커 간격이 7일이고 창이 14일이면
bike-day 한 건이 앵커 2개에만 걸리므로 fan-out 이 2배로 제한된다.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DateType, StructField, StructType

# 모델 입력 컬럼. 순서까지 아티팩트와 계약이다.
FEATURE_COLS = [
    "trips",
    "dist_km",
    "instant_ret",
    "fail_150d",
    "days_since_fail",
    "days_since_last_rent",
    "trend_ratio",
]
FEATURE_VERSION = "v1"

NEVER_FAILED_SENTINEL = 9999


# ── 원천 로드 ─────────────────────────────────────────────────────────
def _norm_bike_id(col):
    """대여이력과 고장신고의 자전거 번호 표기를 맞춘다 (공백/대소문자 혼재)."""
    return F.upper(F.regexp_replace(col, r"\s+", ""))


def read_silver(spark: SparkSession, table_ref: str, mode: str) -> DataFrame:
    """silver 읽기 어댑터.

    mode='iceberg' 일 때 table_ref 는 S3 경로가 아니라
    '<catalog>.<namespace>.<table>' 형태의 테이블 참조명이다
    (예: 'bike_catalog.silver.rental_history'). Spark 세션에 해당 카탈로그가
    미리 등록돼 있어야 한다 (spark.sql.catalog.<catalog> 설정).
    """
    if mode == "iceberg":
        if not spark.catalog.tableExists(table_ref):
            raise FileNotFoundError(
                f"Iceberg 테이블이 존재하지 않습니다: {table_ref} "
                "(ETL backfill 이 아직 안 됐을 가능성)"
            )
        return spark.table(table_ref)
    return spark.read.parquet(table_ref)


def _table_ref(cfg, key: str) -> str:
    """설정에 저장된 'silver.rental_history' 형태에 실행 시점 카탈로그명을 붙인다.

    카탈로그명을 yaml 에 하드코딩하지 않는 이유: ICEBERG_CATALOG_NAME 이 바뀌면
    (기본 bike_catalog) 자동으로 따라가야 하고, 두 곳에 같은 값이 사는 걸 피한다.
    """
    ref = cfg.get_path(key)
    if cfg.get_path("sources.mode", "iceberg") != "iceberg":
        return ref
    import config as team_config  # 최상위 config 패키지 (ingestion/staging 공통)

    catalog = team_config.SETTINGS.iceberg_catalog_name
    return f"{catalog}.{ref}"


def read_rental(spark: SparkSession, cfg) -> DataFrame:
    src = cfg["sources"]
    m = src["rental_columns"]
    df = read_silver(spark, _table_ref(cfg, "sources.rental_history"), src["mode"])

    out = (
        df.select(
            _norm_bike_id(F.col(m["bike_id"]).cast("string")).alias("bike_id"),
            F.col(m["rent_at"]).cast("timestamp").alias("rent_at"),
            F.col(m["return_at"]).cast("timestamp").alias("return_at"),
            F.col(m["dist_m"]).cast("double").alias("dist_m"),
            F.col(m["rent_station"]).cast("string").alias("rent_station"),
            F.col(m["return_station"]).cast("string").alias("return_station"),
            *(
                [F.col(m["dur_min"]).cast("double").alias("dur_min")]
                if m.get("dur_min")
                else []
            ),
        )
    )
    if not m.get("dur_min"):
        # silver.rental_history 에 이용시간 컬럼이 없으면 반납-대여로 파생한다.
        out = out.withColumn(
            "dur_min",
            (F.col("return_at").cast("long") - F.col("rent_at").cast("long")) / 60.0,
        )

    min_rent = cfg.get_path("run.min_rent_date")
    if min_rent:
        out = out.filter(F.col("rent_at") >= F.lit(min_rent).cast("timestamp"))
    return out.filter(F.col("bike_id").isNotNull() & F.col("rent_at").isNotNull())


def read_fault(spark: SparkSession, cfg) -> DataFrame:
    src = cfg["sources"]
    m = src["fault_columns"]
    df = read_silver(spark, _table_ref(cfg, "sources.failure_report"), src["mode"])
    return (
        df.select(
            _norm_bike_id(F.col(m["bike_id"]).cast("string")).alias("bike_id"),
            F.to_date(F.col(m["reported_at"]).cast("timestamp")).alias("reg_date"),
        )
        .filter(F.col("bike_id").isNotNull() & F.col("reg_date").isNotNull())
        .distinct()  # 같은 자전거·같은 날 복수 고장유형 행 → 이벤트 1건으로
    )


# ── 정제 ──────────────────────────────────────────────────────────────
def instant_return_cond(cfg):
    """즉시 반납: 빌린 대여소에 그대로 되돌려놓은 트립."""
    max_dist = cfg.get_path("cleaning.instant_return_max_dist_m", 10)
    same = F.col("rent_station").isNotNull() & (
        F.col("rent_station") == F.col("return_station")
    )
    return same & (F.coalesce(F.col("dist_m"), F.lit(99999.0)) <= F.lit(float(max_dist)))


def apply_trip_filters(df: DataFrame, cfg) -> DataFrame:
    if cfg.get_path("sources.assume_silver_clean", False):
        return df
    c = cfg["cleaning"]
    dur = F.coalesce(F.col("dur_min"), F.lit(0.0))
    speed = (F.col("dist_m") / 1000.0) / (F.col("dur_min") / 60.0)
    return (
        df.filter(~((F.col("dur_min") > 0) & (speed > F.lit(float(c["max_speed_kmh"])))))
        .filter(~(F.col("dist_m") > F.lit(float(c["max_dist_m"]))))
        .filter(~((dur == 0) & (F.col("dist_m") > F.lit(float(c["zero_dur_max_dist"])))))
    )


# ── 앵커 ──────────────────────────────────────────────────────────────
def anchor_frame(spark: SparkSession, anchors: list[date]) -> DataFrame:
    schema = StructType([StructField("as_of", DateType(), False)])
    rows = [(a if isinstance(a, date) else date.fromisoformat(str(a)),) for a in anchors]
    return spark.createDataFrame(rows, schema)


# ── 일 집계 ───────────────────────────────────────────────────────────
def build_daily_agg(rent: DataFrame, cfg) -> DataFrame:
    return (
        rent.withColumn("rent_date", F.to_date("rent_at"))
        .groupBy("bike_id", "rent_date")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum(F.coalesce("dist_m", F.lit(0.0))).alias("dist_m"),
            F.sum(F.coalesce("dur_min", F.lit(0.0))).alias("dur_min"),
            F.sum(F.when(instant_return_cond(cfg), 1).otherwise(0)).alias("instant_ret"),
        )
    )


# ── 피처 ──────────────────────────────────────────────────────────────
def build_usage_features(daily: DataFrame, anchors_df: DataFrame, window: int) -> DataFrame:
    """as_of 이전 window 일. as_of 당일은 포함하지 않는다."""
    half = window // 2
    joined = daily.join(
        F.broadcast(anchors_df),
        (F.col("rent_date") >= F.date_sub(F.col("as_of"), window))
        & (F.col("rent_date") < F.col("as_of")),
        "inner",
    )
    agg = joined.groupBy("as_of", "bike_id").agg(
        F.sum("trips").alias("trips"),
        (F.sum("dist_m") / 1000.0).alias("dist_km"),
        (F.sum("dur_min") / 60.0).alias("dur_h"),
        F.sum("instant_ret").alias("instant_ret"),
        F.sum(
            F.when(F.col("rent_date") < F.date_sub(F.col("as_of"), half), F.col("trips"))
            .otherwise(0)
        ).alias("trips_first_half"),
        F.sum(
            F.when(F.col("rent_date") >= F.date_sub(F.col("as_of"), half), F.col("trips"))
            .otherwise(0)
        ).alias("trips_second_half"),
        F.max("rent_date").alias("last_rent_date"),
    )
    return (
        agg.withColumn(
            "days_since_last_rent", F.datediff(F.col("as_of"), F.col("last_rent_date"))
        )
        # 트립 0건 방어를 위한 +1 라플라스 스무딩
        .withColumn(
            "trend_ratio",
            (F.col("trips_second_half") + 1) / (F.col("trips_first_half") + 1),
        )
        .drop("trips_first_half", "trips_second_half", "last_rent_date")
    )


def build_fault_features(fault: DataFrame, anchors_df: DataFrame, cfg) -> DataFrame:
    """as_of 이전 신고 이력만 사용 — 라벨 누수 없음."""
    fail_window = int(cfg.get_path("run.fail_window_days", 150))
    lookback = cfg.get_path("run.fault_lookback_days")  # null = 전체 이력

    cond = F.col("reg_date") < F.col("as_of")
    if lookback:
        cond = cond & (F.col("reg_date") >= F.date_sub(F.col("as_of"), int(lookback)))

    joined = fault.join(F.broadcast(anchors_df), cond, "inner")
    return joined.groupBy("as_of", "bike_id").agg(
        F.sum(
            F.when(F.col("reg_date") >= F.date_sub(F.col("as_of"), fail_window), 1).otherwise(0)
        ).alias("fail_150d"),
        F.max("reg_date").alias("last_fail_date"),
    )


# ── 라벨 ──────────────────────────────────────────────────────────────
def build_positives(fault: DataFrame, anchors_df: DataFrame, horizon: int) -> DataFrame:
    """as_of 이후 horizon 일 안에 신고가 들어온 자전거."""
    return (
        fault.join(
            F.broadcast(anchors_df),
            (F.col("reg_date") >= F.col("as_of"))
            & (F.col("reg_date") < F.date_add(F.col("as_of"), horizon)),
            "inner",
        )
        .select("as_of", "bike_id")
        .distinct()
    )


def build_excluded(fault: DataFrame, anchors_df: DataFrame, exclude_recent_days: int) -> DataFrame:
    """직전 N일에 이미 신고된 자전거 — 학습/평가/추론 모두에서 제외 대상.

    라벨 오염(재신고가 자명한 집단)과 train-serving skew 를 동시에 막는다.
    """
    if exclude_recent_days <= 0:
        return anchors_df.select("as_of").limit(0).withColumn("bike_id", F.lit(None).cast("string"))
    return (
        fault.join(
            F.broadcast(anchors_df),
            (F.col("reg_date") >= F.date_sub(F.col("as_of"), exclude_recent_days))
            & (F.col("reg_date") < F.col("as_of")),
            "inner",
        )
        .select("as_of", "bike_id")
        .distinct()
    )


def build_pos_new(fault: DataFrame, anchors_df: DataFrame, cfg) -> DataFrame:
    """메인지표의 분모: 직전 30일 신고 이력이 없는 '신규' 고장 자전거 전체.

    피처 테이블에 없는 자전거(창 내 대여 0건)까지 포함해야 노트북의 지표 정의와
    같아진다. 그래서 별도 테이블로 물리화한다.
    """
    horizon = int(cfg.get_path("run.horizon_days", 14))
    excl = int(cfg.get_path("run.exclude_recent_days", 30))
    pos = build_positives(fault, anchors_df, horizon)
    exc = build_excluded(fault, anchors_df, excl)
    return pos.join(exc, ["as_of", "bike_id"], "left_anti")


# ── 샘플 조립 ─────────────────────────────────────────────────────────
def bike_class_col(cfg):
    threshold = int(cfg.get_path("run.saessak_min_num", 80000))
    num = F.regexp_extract(F.col("bike_id"), r"(\d+)\s*$", 1)
    num = F.when(num == "", None).otherwise(num.cast("long"))
    return (
        F.when(num.isNull(), F.lit("unknown"))
        .when(num >= threshold, F.lit("saessak"))
        .otherwise(F.lit("normal"))
    )


def build_samples(
    spark: SparkSession,
    cfg,
    anchors: list[date],
    anchor_type: str,
    rent: DataFrame | None = None,
    fault: DataFrame | None = None,
    with_labels: bool = True,
) -> DataFrame:
    """앵커 목록에 대한 (피처 + 라벨) 샘플 테이블.

    후보군 = 창 내 대여 1건 이상인 자전거. 대여 0건 자전거를 넣으면
    '수거되어 정비 중' 인 자전거가 라벨 0 으로 들어와 모델이
    '미대여 = 안전' 을 학습하므로 제외한다 (README 참조).
    """
    window = int(cfg.get_path("run.window_days", 14))
    horizon = int(cfg.get_path("run.horizon_days", 14))
    excl_days = int(cfg.get_path("run.exclude_recent_days", 30))

    # rent 를 넘겨받은 경우 이미 정제된 것으로 본다 (필터 이중 적용 방지)
    if rent is None:
        rent = apply_trip_filters(read_rental(spark, cfg), cfg)
    if fault is None:
        fault = read_fault(spark, cfg)

    anchors_df = anchor_frame(spark, anchors)
    daily = build_daily_agg(rent, cfg)

    usage = build_usage_features(daily, anchors_df, window)
    faults = build_fault_features(fault, anchors_df, cfg)
    exc = build_excluded(fault, anchors_df, excl_days).withColumn("excluded", F.lit(True))

    out = usage.join(faults, ["as_of", "bike_id"], "left")
    if with_labels:
        pos = build_positives(fault, anchors_df, horizon).withColumn("label", F.lit(1))
        out = out.join(pos, ["as_of", "bike_id"], "left")
    else:
        out = out.withColumn("label", F.lit(None).cast("int"))

    out = (
        out.join(exc, ["as_of", "bike_id"], "left")
        .withColumn("fail_150d", F.coalesce("fail_150d", F.lit(0)))
        .withColumn(
            "days_since_fail",
            F.coalesce(
                F.datediff(F.col("as_of"), F.col("last_fail_date")),
                F.lit(NEVER_FAILED_SENTINEL),
            ),
        )
        .withColumn(
            "label",
            F.coalesce("label", F.lit(0)) if with_labels else F.col("label"),
        )
        .withColumn("excluded", F.coalesce("excluded", F.lit(False)))
        .withColumn("bike_class", bike_class_col(cfg))
        .withColumn("anchor_type", F.lit(anchor_type))
        .withColumn("feature_version", F.lit(FEATURE_VERSION))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumnRenamed("as_of", "snapshot_date")
        .drop("last_fail_date")
    )

    ordered = [
        "snapshot_date",
        "bike_id",
        *FEATURE_COLS,
        "dur_h",
        "label",
        "excluded",
        "bike_class",
        "anchor_type",
        "feature_version",
        "ingested_at",
    ]
    return out.select(*ordered)


# ── 추론 DAG 용 진입점 (학습과 동일 로직) ─────────────────────────────
def build_features_for_inference(spark: SparkSession, cfg, as_of: date) -> DataFrame:
    """gold.bike_features_daily 생성용. 라벨 없이 피처만."""
    df = build_samples(spark, cfg, [as_of], anchor_type="serve", with_labels=False)
    return df.drop("label")
