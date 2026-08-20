"""
gold_risk_decision의 추론 입력을 만드는 feature 엔지니어링 job -
silver.rental_history + silver.failure_report -> gold.bike_features_daily (추론용)

피처 로직은 pipeline/train_risk_model/features.py의 build_samples()/read_rental()/
apply_trip_filters()를 그대로 재사용한다. train_risk_model README의 설계 원칙
("피처 로직은 features.py 하나를 학습·추론이 공유 - 갈라지면 train-serving skew")에
맞춰, features.py 자체는 수정하지 않는다.

기준일 이전 14일 rolling window라 dim_bike처럼 누적 처리하지 않는다 - 워터마크 없이
SNAPSHOT_DATE(기본값 오늘) 하루치를 매번 통째로 재계산해 OVERWRITE.

### 대여중단(SUSPEND) 당일 대여 기록 제외 (#85)
gold.fact_bike_decision에서 action == '대여중단'으로 결정된 자전거인데 같은 날
rental_history에 대여 기록이 남아있으면 모순된(이상치) 데이터다 - 오늘 피처(trips,
dist_km 등) 계산에 그대로 섞여 들어가면 안 된다. build_samples()가 이미 지원하는
`rent` 오버라이드 파라미터("필터 이중 적용 방지" 주석 참고)를 이용해, features.py는
그대로 두고 이 파일에서만 사전 필터링한 rent를 넘긴다 - 학습 파이프라인엔 영향 없음.

자전거 자체를 영구 제외하지 않는다(추론 대상에서 계속 빠지면 재평가 기회가 없어짐,
build_fact_bike_risk.py의 "한 번 대여중단되면 고정 구조로 만들면 안 된다" 원칙과 동일)
- 모순이 발생한 그 날짜의 rental 레코드만 걸러내고, 다른 날짜 이력은 정상 반영한다.
"""
import logging
import os
import sys
from datetime import date

os.environ.setdefault("SPARK_VERSION", "3.5")

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import types as T

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ
from pipeline.train_risk_model.features import apply_trip_filters, build_samples, read_rental
from pipeline.train_risk_model.settings import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = [
    "snapshot_date", "bike_id", "trips", "dist_km", "instant_ret",
    "fail_150d", "days_since_fail", "days_since_last_rent", "trend_ratio",
]

SUSPEND = "대여중단"

SUSPENDED_BIKE_DAYS_SCHEMA = T.StructType([
    T.StructField("bike_id", T.StringType()),
    T.StructField("snapshot_date", T.DateType()),
])


class GoldValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_features_daily"


def _fact_bike_decision_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_bike_decision"


def _ensure_bike_features_daily_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            snapshot_date DATE,
            bike_id STRING,
            trips INT,
            dist_km DOUBLE,
            instant_ret INT,
            fail_150d INT,
            days_since_fail INT,
            days_since_last_rent INT,
            trend_ratio DOUBLE
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def _validate_bike_features_daily(spark, df) -> None:
    check = Check(spark, CheckLevel.Error, "bike_features_daily_check")
    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            check.isComplete("bike_id")
            .hasUniqueness(["bike_id"], lambda fraction: fraction > 0.99)
            .isComplete("trips")
            .isComplete("trend_ratio")
        )
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show(truncate=False)

    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise GoldValidationError(f"bike_features_daily 품질 검증 실패: {failed_constraints}")


def _suspended_bike_days(spark, target_date: date, window_days: int) -> DataFrame:
    """[target_date - window_days, target_date) 구간에서 action == SUSPEND인 (bike_id, snapshot_date).

    fact_bike_decision은 이 DAG 뒤 단계(build_fact_bike_decision)가 만들기 때문에,
    최초 실행일엔 테이블 자체가 아직 없다 - 그 경우 자연히 제외 대상 0건으로 처리한다
    (build_fact_bike_risk.py의 _currently_collected_bike_ids와 동일한 콜드스타트 패턴).
    """
    if not spark.catalog.tableExists(_fact_bike_decision_table()):
        return spark.createDataFrame([], SUSPENDED_BIKE_DAYS_SCHEMA)
    return (
        spark.read.table(_fact_bike_decision_table())
        .filter(
            (F.col("snapshot_date") >= F.date_sub(F.lit(target_date), window_days))
            & (F.col("snapshot_date") < F.lit(target_date))
            & (F.col("action") == SUSPEND)
        )
        .select("bike_id", "snapshot_date")
    )


def _exclude_suspended_rental_days(rent_df: DataFrame, suspended_df: DataFrame) -> DataFrame:
    """SUSPEND 결정이 난 바로 그 날짜에 대여된 레코드(모순 데이터)를 제거한다.

    rent_df는 다른 날짜 이력을 그대로 유지한다 - 이 자전거 자체를 오늘 추론 대상에서
    빼는 게 아니라, 모순이 발생한 그 하루치 대여 레코드만 걸러내는 것이다.
    """
    rent_with_date = rent_df.withColumn("rent_date", F.to_date("rent_at"))
    suspended_by_rent_date = suspended_df.withColumnRenamed("snapshot_date", "rent_date")
    return rent_with_date.join(suspended_by_rent_date, ["bike_id", "rent_date"], "left_anti").drop("rent_date")


def _build_features(spark, cfg, target_date: date):
    window_days = int(cfg.get_path("run.window_days", 14))

    rent = apply_trip_filters(read_rental(spark, cfg), cfg)
    suspended = _suspended_bike_days(spark, target_date, window_days)
    filtered_rent = _exclude_suspended_rental_days(rent, suspended)

    excluded_count = rent.count() - filtered_rent.count()
    logger.info(
        "%s: SUSPEND 당일 대여 모순 레코드 %d건 제외 (SUSPEND bike-day %d건 대상)",
        target_date, excluded_count, suspended.count(),
    )

    df = build_samples(spark, cfg, [target_date], anchor_type="serve", rent=filtered_rent, with_labels=False)
    return df.drop("label").select(*OUTPUT_COLUMNS)


def _process_date(spark, cfg, target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")

    feat_df = _build_features(spark, cfg, target_date)
    row_count = feat_df.count()
    if row_count == 0:
        logger.info("%s: 최근 %d일 내 대여 이력 없음", date_str, int(cfg.get_path("run.window_days", 14)))
        return 0

    feat_df.writeTo(_gold_table()).overwritePartitions()

    written = spark.read.table(_gold_table()).filter(F.col("snapshot_date") == date_str)
    _validate_bike_features_daily(spark, written)  # 실패 시 GoldValidationError -> 배치 중단

    logger.info("%s: 자전거 %d대 feature 산출", date_str, row_count)
    return row_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    cfg = load_config()
    spark = build_spark_session_with_deequ("gold-build-bike-features-daily")
    try:
        _ensure_bike_features_daily_table(spark)

        snapshot_date_str = os.getenv("SNAPSHOT_DATE")
        target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

        try:
            _process_date(spark, cfg, target_date)
        except GoldValidationError as e:
            logger.error("%s 처리 실패, 배치 중단: %s", target_date, e)
            sys.exit(1)
    finally:
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()
