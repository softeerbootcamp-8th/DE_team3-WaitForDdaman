"""피처 / 라벨 정의 — 학습 DAG과 추론 DAG이 **이 모듈만** 사용한다.

여기서 로직이 갈라지는 순간 train-serving skew 가 생긴다.
피처 정의를 바꾸면 FEATURE_VERSION 을 올린다.

성능 설계: 노트북은 앵커마다 대여이력을 필터+집계해서 앵커 N개면 N번 스캔했다.
여기서는 (bike_id, 날짜) 일 집계를 1회 만들고, 앵커 목록(작은 테이블)을 broadcast
join 해서 한 번에 모든 앵커의 피처를 만든다. 학습 앵커 간격이 7일이고 창이 14일이면
bike-day 한 건이 앵커 2개에만 걸리므로 fan-out 이 2배로 제한된다.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DateType, StructField, StructType

from pipeline.train_risk_model.sql_dialect import (
    date_sub_days,
    days_between,
    epoch_seconds,
    escape_regex_literal,
    regexp_replace_global,
)
from pipeline.train_risk_model.sql_engine import SqlEngine

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
# #149: 실제 인프라가 Iceberg뿐이라(sources.mode 기본값도 "iceberg") parquet
# 폴백은 다루지 않는다 - read_silver()가 갖고 있던 그 분기는 애초에 실사용이 없었다.
def _table_ref(engine: SqlEngine, cfg, key: str) -> str:
    """설정에 저장된 'silver.rental_history' 형태의 참조를 엔진에 맞게 반환한다.

    Spark는 카탈로그.네임스페이스.테이블 형태가 필요하고(spark.sql.catalog.<catalog>
    에 미리 등록돼 있어야 함), pyiceberg(DuckDB 경로, SqlEngine.read_table 참고)는
    카탈로그 접두어 없이 네임스페이스.테이블만 받는다.
    """
    ref = cfg.get_path(key)
    if engine.dialect != "spark":
        return ref
    import config as team_config  # 최상위 config 패키지 (ingestion/staging 공통)

    catalog = team_config.SETTINGS.iceberg_catalog_name
    return f"{catalog}.{ref}"


def _rental_transform(engine: SqlEngine, cfg):
    """"rental_raw"(이미 register된 원본)를 표준 컬럼명으로 매핑한다.

    Iceberg를 안 쓰는 순수 SQL 변환이라 카탈로그 없이 단위 테스트 가능하다
    (read_rental()이 실제 읽기를 담당 - build_bike_location.py의 _delta()/
    _merge_baseline_delta() 분리와 동일한 이유).
    """
    m = cfg["sources"]["rental_columns"]
    bike_id_norm = regexp_replace_global(
        engine.dialect, f"CAST({m['bike_id']} AS STRING)", r"\s+", ""
    )

    if m.get("dur_min"):
        dur_min_expr = f"CAST({m['dur_min']} AS DOUBLE)"
    else:
        # silver.rental_history 에 이용시간 컬럼이 없으면 반납-대여(초)로 파생한다.
        ret_epoch = epoch_seconds(engine.dialect, "return_at")
        rent_epoch = epoch_seconds(engine.dialect, "rent_at")
        dur_min_expr = f"(CAST({ret_epoch} AS DOUBLE) - CAST({rent_epoch} AS DOUBLE)) / 60.0"

    min_rent = cfg.get_path("run.min_rent_date")
    min_rent_cond = f"AND rent_at >= TIMESTAMP '{min_rent}'" if min_rent else ""

    sql = f"""
        SELECT bike_id, rent_at, return_at, dist_m, rent_station, return_station,
               {dur_min_expr} AS dur_min
        FROM (
            SELECT UPPER({bike_id_norm}) AS bike_id,
                   CAST({m['rent_at']} AS TIMESTAMP) AS rent_at,
                   CAST({m['return_at']} AS TIMESTAMP) AS return_at,
                   CAST({m['dist_m']} AS DOUBLE) AS dist_m,
                   CAST({m['rent_station']} AS STRING) AS rent_station,
                   CAST({m['return_station']} AS STRING) AS return_station
            FROM rental_raw
        ) base
        WHERE bike_id IS NOT NULL AND rent_at IS NOT NULL {min_rent_cond}
    """
    return engine.sql(sql)


def read_rental(engine: SqlEngine, cfg, row_filter=None):
    table_ref = _table_ref(engine, cfg, "sources.rental_history")
    engine.read_table(table_ref, "rental_raw", row_filter=row_filter)
    return _rental_transform(engine, cfg)


def _fault_transform(engine: SqlEngine, cfg):
    """"fault_raw"(이미 register된 원본)를 표준 컬럼명으로 매핑한다 (_rental_transform과 동일한 이유로 분리)."""
    m = cfg["sources"]["fault_columns"]
    bike_id_norm = regexp_replace_global(
        engine.dialect, f"CAST({m['bike_id']} AS STRING)", r"\s+", ""
    )
    sql = f"""
        SELECT DISTINCT UPPER({bike_id_norm}) AS bike_id,
               CAST(CAST({m['reported_at']} AS TIMESTAMP) AS DATE) AS reg_date
        FROM fault_raw
        WHERE {m['bike_id']} IS NOT NULL AND {m['reported_at']} IS NOT NULL
    """
    # 같은 자전거·같은 날 복수 고장유형 행 → DISTINCT로 이벤트 1건으로
    return engine.sql(sql)


def read_fault(engine: SqlEngine, cfg):
    table_ref = _table_ref(engine, cfg, "sources.failure_report")
    engine.read_table(table_ref, "fault_raw")
    return _fault_transform(engine, cfg)


# ── 정제 ──────────────────────────────────────────────────────────────
def apply_trip_filters(engine: SqlEngine, df, cfg):
    if cfg.get_path("sources.assume_silver_clean", False):
        return df
    c = cfg["cleaning"]
    max_speed = float(c["max_speed_kmh"])
    max_dist = float(c["max_dist_m"])
    zero_dur_max_dist = float(c["zero_dur_max_dist"])
    engine.register("trips_raw", df)
    sql = f"""
        SELECT *
        FROM trips_raw
        WHERE NOT (dur_min > 0 AND (dist_m / 1000.0) / (dur_min / 60.0) > {max_speed})
          AND NOT (dist_m > {max_dist})
          AND NOT (COALESCE(dur_min, 0.0) = 0 AND dist_m > {zero_dur_max_dist})
    """
    return engine.sql(sql)


# ── 앵커 ──────────────────────────────────────────────────────────────
def anchor_frame(engine: SqlEngine, anchors: list[date]):
    values = [a if isinstance(a, date) else date.fromisoformat(str(a)) for a in anchors]
    if engine.dialect == "spark":
        schema = StructType([StructField("as_of", DateType(), False)])
        return engine.spark.createDataFrame([(v,) for v in values], schema)
    import pandas as pd

    engine.register("_anchor_seed", pd.DataFrame({"as_of": values}))
    return engine.sql("SELECT CAST(as_of AS DATE) AS as_of FROM _anchor_seed")


# ── 일 집계 ───────────────────────────────────────────────────────────
# #149: rent를 "rent" 이름으로 register한 뒤 이 SQL 그대로 Spark/DuckDB 양쪽에서 실행한다.
# 즉시반납 조건(CASE WHEN)은 예전 instant_return_cond(cfg)와 동일한 로직 - 표준 SQL이라
# 방언 분기가 필요 없다.
_DAILY_AGG_SQL = """
    SELECT bike_id, CAST(rent_at AS DATE) AS rent_date,
           COUNT(*) AS trips,
           SUM(COALESCE(dist_m, 0.0)) AS dist_m,
           SUM(COALESCE(dur_min, 0.0)) AS dur_min,
           CAST(SUM(CASE WHEN rent_station IS NOT NULL AND rent_station = return_station
                    AND COALESCE(dist_m, 99999.0) <= {max_dist}
               THEN 1 ELSE 0 END) AS BIGINT) AS instant_ret
    FROM rent
    GROUP BY bike_id, CAST(rent_at AS DATE)
"""


def build_daily_agg(engine: SqlEngine, rent, cfg):
    engine.register("rent", rent)
    max_dist = float(cfg.get_path("cleaning.instant_return_max_dist_m", 10))
    return engine.sql(_DAILY_AGG_SQL.format(max_dist=max_dist))


# ── 피처 ──────────────────────────────────────────────────────────────
def build_usage_features(engine: SqlEngine, daily, anchors_df, window: int):
    """as_of 이전 window 일. as_of 당일은 포함하지 않는다."""
    engine.register("daily", daily)
    engine.register("anchors", anchors_df)
    half = window // 2
    window_cut = date_sub_days(engine.dialect, "a.as_of", window)
    half_cut = date_sub_days(engine.dialect, "a.as_of", half)
    since_last_rent = days_between(engine.dialect, "last_rent_date", "as_of")
    sql = f"""
        WITH base AS (
            SELECT a.as_of AS as_of, d.bike_id AS bike_id,
                   CAST(SUM(d.trips) AS BIGINT) AS trips,
                   SUM(d.dist_m) / 1000.0 AS dist_km,
                   SUM(d.dur_min) / 60.0 AS dur_h,
                   CAST(SUM(d.instant_ret) AS BIGINT) AS instant_ret,
                   CAST(SUM(CASE WHEN d.rent_date < {half_cut} THEN d.trips ELSE 0 END) AS BIGINT) AS trips_first_half,
                   CAST(SUM(CASE WHEN d.rent_date >= {half_cut} THEN d.trips ELSE 0 END) AS BIGINT) AS trips_second_half,
                   MAX(d.rent_date) AS last_rent_date
            FROM daily d JOIN anchors a
              ON d.rent_date >= {window_cut} AND d.rent_date < a.as_of
            GROUP BY a.as_of, d.bike_id
        )
        SELECT as_of, bike_id, trips, dist_km, dur_h, instant_ret,
               {since_last_rent} AS days_since_last_rent,
               -- 트립 0건 방어를 위한 +1 라플라스 스무딩. 피연산자를 먼저 DOUBLE로
               -- 캐스트해야 한다 - 결과만 CAST하면 Spark SQL은 나눗셈 자체를 DECIMAL로
               -- (1.0 리터럴이 DECIMAL로 해석됨) 수행한 뒤 변환해서 DuckDB(네이티브 DOUBLE
               -- 나눗셈)와 마지막 비트가 갈린다 (#149에서 실측 발견).
               (CAST(trips_second_half AS DOUBLE) + 1.0) / (CAST(trips_first_half AS DOUBLE) + 1.0) AS trend_ratio
        FROM base
    """
    return engine.sql(sql)


def build_fault_features(engine: SqlEngine, fault, anchors_df, cfg, include_today: bool = False):
    """as_of 이전(또는 include_today=True 시 당일 포함) 신고 이력 사용."""
    engine.register("fault", fault)
    engine.register("anchors", anchors_df)
    fail_window = int(cfg.get_path("run.fail_window_days", 150))
    lookback = cfg.get_path("run.fault_lookback_days")  # null = 전체 이력

    lookback_cond = ""
    if lookback:
        lookback_cut = date_sub_days(engine.dialect, "a.as_of", int(lookback))
        lookback_cond = f"AND f.reg_date >= {lookback_cut}"
    fail_cut = date_sub_days(engine.dialect, "as_of", fail_window)

    cmp_op = "<=" if include_today else "<"
    sql = f"""
        WITH joined AS (
            SELECT a.as_of AS as_of, f.bike_id AS bike_id, f.reg_date AS reg_date
            FROM fault f JOIN anchors a
              ON f.reg_date {cmp_op} a.as_of {lookback_cond}
        )
        SELECT as_of, bike_id,
               CAST(SUM(CASE WHEN reg_date >= {fail_cut} THEN 1 ELSE 0 END) AS BIGINT) AS fail_150d,
               MAX(reg_date) AS last_fail_date
        FROM joined
        GROUP BY as_of, bike_id
    """
    return engine.sql(sql)


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


def build_excluded(engine: SqlEngine, fault, anchors_df, exclude_recent_days: int):
    """직전 N일에 이미 신고된 자전거 — 학습/평가/추론 모두에서 제외 대상.

    라벨 오염(재신고가 자명한 집단)과 train-serving skew 를 동시에 막는다.
    """
    if exclude_recent_days <= 0:
        return engine.sql("SELECT NULL AS as_of, NULL AS bike_id WHERE 1 = 0")
    engine.register("fault", fault)
    engine.register("anchors", anchors_df)
    lower = date_sub_days(engine.dialect, "a.as_of", exclude_recent_days)
    sql = f"""
        SELECT DISTINCT a.as_of AS as_of, f.bike_id AS bike_id
        FROM fault f JOIN anchors a
          ON f.reg_date >= {lower} AND f.reg_date < a.as_of
    """
    return engine.sql(sql)


def build_pos_new(engine: SqlEngine, fault: DataFrame, anchors_df: DataFrame, cfg) -> DataFrame:
    """메인지표의 분모: 직전 30일 신고 이력이 없는 '신규' 고장 자전거 전체.

    피처 테이블에 없는 자전거(창 내 대여 0건)까지 포함해야 노트북의 지표 정의와
    같아진다. 그래서 별도 테이블로 물리화한다. build_positives()와 마찬가지로
    학습(Spark) 전용 - engine은 항상 SqlEngine.for_spark()로 넘어와야 한다.
    """
    horizon = int(cfg.get_path("run.horizon_days", 14))
    excl = int(cfg.get_path("run.exclude_recent_days", 30))
    pos = build_positives(fault, anchors_df, horizon)
    exc = build_excluded(engine, fault, anchors_df, excl)
    return pos.join(exc, ["as_of", "bike_id"], "left_anti")


# ── 샘플 조립 ─────────────────────────────────────────────────────────
def _bike_class_sql(dialect: str, bike_id_expr: str, threshold: int) -> str:
    """bike_id 끝의 연속된 숫자로 새싹(threshold 이상)/일반/번호없음(unknown)을 가른다."""
    pattern = escape_regex_literal(dialect, r"(\d+)\s*$")
    num = f"regexp_extract({bike_id_expr}, '{pattern}', 1)"
    return (
        f"CASE WHEN {num} = '' THEN 'unknown' "
        f"WHEN CAST({num} AS BIGINT) >= {int(threshold)} THEN 'saessak' "
        f"ELSE 'normal' END"
    )


def build_samples(
    engine: SqlEngine,
    cfg,
    anchors: list[date],
    anchor_type: str,
    rent=None,
    fault=None,
    with_labels: bool = True,
    include_today_fault: bool = False,
):
    """앵커 목록에 대한 (피처 + 라벨) 샘플 테이블.

    후보군 = 창 내 대여 1건 이상인 자전거. 대여 0건 자전거를 넣으면
    '수거되어 정비 중' 인 자전거가 라벨 0 으로 들어와 모델이
    '미대여 = 안전' 을 학습하므로 제외한다 (README 참조).

    with_labels=True(학습)만 build_positives()를 쓴다 - 이 함수는 #149에서도
    Spark 전용으로 남겨뒀다(추론은 라벨 자체가 없어 애초에 안 씀, YAGNI).
    그래서 with_labels=True는 engine.dialect=="spark"일 때만 호출돼야 한다.
    """
    window = int(cfg.get_path("run.window_days", 14))
    horizon = int(cfg.get_path("run.horizon_days", 14))
    excl_days = int(cfg.get_path("run.exclude_recent_days", 30))

    # rent 를 넘겨받은 경우 이미 정제된 것으로 본다 (필터 이중 적용 방지)
    if rent is None:
        rent = apply_trip_filters(engine, read_rental(engine, cfg), cfg)
    if fault is None:
        fault = read_fault(engine, cfg)

    anchors_df = anchor_frame(engine, anchors)
    daily = build_daily_agg(engine, rent, cfg)

    usage = build_usage_features(engine, daily, anchors_df, window)
    faults = build_fault_features(engine, fault, anchors_df, cfg, include_today=include_today_fault)
    exc = build_excluded(engine, fault, anchors_df, excl_days)
    engine.register("usage", usage)
    engine.register("faults", faults)
    engine.register("exc", exc)

    label_expr = "CAST(NULL AS BIGINT) AS label"
    label_join = ""
    if with_labels:
        pos = build_positives(fault, anchors_df, horizon)
        engine.register("pos", pos)
        label_expr = "CASE WHEN pos.bike_id IS NOT NULL THEN 1 ELSE 0 END AS label"
        label_join = "LEFT JOIN pos ON pos.as_of = usage.as_of AND pos.bike_id = usage.bike_id"

    days_since_fail = days_between(engine.dialect, "faults.last_fail_date", "usage.as_of")
    bike_class = _bike_class_sql(engine.dialect, "usage.bike_id", int(cfg.get_path("run.saessak_min_num", 80000)))

    sql = f"""
        SELECT
            usage.as_of AS snapshot_date,
            usage.bike_id AS bike_id,
            usage.trips AS trips,
            usage.dist_km AS dist_km,
            usage.instant_ret AS instant_ret,
            COALESCE(faults.fail_150d, 0) AS fail_150d,
            COALESCE({days_since_fail}, {NEVER_FAILED_SENTINEL}) AS days_since_fail,
            usage.days_since_last_rent AS days_since_last_rent,
            usage.trend_ratio AS trend_ratio,
            usage.dur_h AS dur_h,
            {label_expr},
            CASE WHEN exc.bike_id IS NOT NULL THEN true ELSE false END AS excluded,
            {bike_class} AS bike_class,
            '{anchor_type}' AS anchor_type,
            '{FEATURE_VERSION}' AS feature_version,
            CURRENT_TIMESTAMP AS ingested_at
        FROM usage
        LEFT JOIN faults ON faults.as_of = usage.as_of AND faults.bike_id = usage.bike_id
        LEFT JOIN exc ON exc.as_of = usage.as_of AND exc.bike_id = usage.bike_id
        {label_join}
    """
    return engine.sql(sql)


_SAMPLE_COLUMNS_NO_LABEL = [
    "snapshot_date",
    "bike_id",
    *FEATURE_COLS,
    "dur_h",
    "excluded",
    "bike_class",
    "anchor_type",
    "feature_version",
    "ingested_at",
]


# ── 추론 DAG 용 진입점 (학습과 동일 로직) ─────────────────────────────
def build_features_for_inference(engine: SqlEngine, cfg, as_of: date):
    """gold.bike_features_daily 생성용. 라벨 없이 피처만."""
    result = build_samples(engine, cfg, [as_of], anchor_type="serve", with_labels=False)
    engine.register("features_for_inference", result)
    cols = ", ".join(_SAMPLE_COLUMNS_NO_LABEL)
    return engine.sql(f"SELECT {cols} FROM features_for_inference")
