"""
gold_risk_decision 원안의 "4-a/4-b 필터 + 5. run_risk_scoring_model + 6. build_fact_bike_risk"
세 단계를 한 Spark job으로 구현 - gold.bike_features_daily -> 자전거별 risk_score/risk_grade
산출. 모델 로드/추론 자체는 run_risk_scoring_model.py에 있고 여기서는 그걸 불러 쓴다.

수거(정비중이라 미배치) 자전거만 제외하고, 대여중단 상태였던 자전거도 매일 다시
추론 대상에 포함시킨다 - 대여소 재고 상황이 바뀌면 어제 대여중단이었던 자전거가
오늘은 보류로 풀릴 수도 있어서, "한 번 대여중단되면 고정" 구조로 만들면 안 된다.
이 필터는 bikeman_action 최신 이벤트만 보므로 최초 실행일(cold start)에도 이력이
없으면 자연히 아무도 제외되지 않아 별도 분기가 필요 없다.

dim_bike처럼 날짜 범위를 누적 처리하지 않고 하루치를 통째로 재계산해 OVERWRITE하므로
워터마크가 없다.
"""
import logging
import os
import sys
from datetime import date

os.environ.setdefault("SPARK_VERSION", "3.5")

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ
from jobs.run_risk_scoring_model import load_model, score

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_SCHEMA = T.StructType(
    [
        T.StructField("snapshot_date", T.DateType()),
        T.StructField("bike_id", T.StringType()),
        T.StructField("risk_score", T.DoubleType()),
        T.StructField("risk_grade", T.StringType()),
        T.StructField("model_version", T.StringType()),
    ]
)


class GoldValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


def _bike_features_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_features_daily"


def _bikeman_action_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.bikeman_action"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_bike_risk"


def _ensure_fact_bike_risk_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            snapshot_date DATE,
            bike_id STRING,
            risk_score DOUBLE,
            risk_grade STRING,
            model_version STRING
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def _validate_fact_bike_risk(spark, df) -> None:
    """오늘자 파티션만 검증한다 (OVERWRITE 구조라 dim_bike처럼 테이블 전체를 볼 필요 없음)."""
    check = Check(spark, CheckLevel.Error, "fact_bike_risk_check")
    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            check.isComplete("bike_id")
            .isComplete("risk_score")
            .isComplete("risk_grade")
            .hasUniqueness(["bike_id"], lambda fraction: fraction > 0.99)
            .isContainedIn("risk_grade", ["Normal", "Warning", "Critical"])
            .satisfies("risk_score >= 0 AND risk_score <= 100", "risk_score_range")
        )
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show(truncate=False)

    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise GoldValidationError(f"fact_bike_risk 품질 검증 실패: {failed_constraints}")

    # Critical 컷오프를 상위 1%로 잡았으니, 실제 비율이 크게 벗어나면 모델/입력 데이터
    # 이상 신호다 (예: feature 스케일이 이상해서 다들 같은 leaf로 몰리는 경우).
    # 배치를 죽일 정도로 확신할 순 없어서 경고만 남긴다.
    total = df.count()
    critical_ratio = df.filter(F.col("risk_grade") == "Critical").count() / total if total else 0
    if not (0.0 <= critical_ratio <= 0.05):
        logger.warning("Critical 비율이 예상 범위(0~5%%) 밖: %.2f%%", critical_ratio * 100)


def _currently_collected_bike_ids(spark, as_of: date):
    """4-a/4-b (skip_filter_first_run / apply_lagged_filter) 구현.

    bikeman_action에서 bike_id별 최신 이벤트가 'COLLECT'인 자전거 = 아직 미배치(정비중).
    대여중단 상태는 여기서 제외되지 않는다 - 수거 이벤트가 없거나, 수거 뒤에 배치
    이벤트가 이미 찍혔으면 오늘도 추론 대상에 포함된다.

    cold start(최초 실행일)에도 이 함수 하나로 충분하다 - bikeman_action 이력이
    아직 없으면 자연히 아무도 안 걸러지므로, 원안의 4-a(필터 스킵)와 4-b(필터 적용)가
    실질적으로 같은 코드가 된다. 그래서 DAG에는 두 이름이 남아있지만 여기 함수는 하나뿐이다.
    """
    actions = spark.read.table(_bikeman_action_table()).filter(F.col("occurred_at") <= F.lit(str(as_of)))
    w = Window.partitionBy("bike_id").orderBy(F.col("occurred_at").desc())
    latest = (
        actions.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .filter(F.col("event_type") == "COLLECT")
        .select("bike_id")
    )
    return latest


def _process_date(spark, target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")

    features_df = spark.read.table(_bike_features_table()).filter(F.col("snapshot_date") == date_str)
    row_count = features_df.count()
    if row_count == 0:
        logger.info("%s: bike_features_daily에 처리할 데이터 없음", date_str)
        return 0

    # 4-a/4-b: 정비중(수거→미배치) 자전거 제외
    collected = _currently_collected_bike_ids(spark, target_date)
    eligible_df = features_df.join(collected, on="bike_id", how="left_anti")
    eligible_count = eligible_df.count()
    if eligible_count == 0:
        logger.info("%s: 정비중 제외 후 추론 대상 없음", date_str)
        return 0

    # 5: run_risk_scoring_model
    art = load_model()
    feat_pd = eligible_df.toPandas().set_index("bike_id")
    scored_pd = score(feat_pd, art).reset_index()
    scored_pd["snapshot_date"] = target_date
    scored_pd["risk_grade"] = scored_pd["risk_grade"].astype(str)
    scored_pd = scored_pd[["snapshot_date", "bike_id", "risk_score", "risk_grade", "model_version"]]

    # 6: build_fact_bike_risk
    out_df = spark.createDataFrame(scored_pd, schema=OUTPUT_SCHEMA)
    out_df.writeTo(_gold_table()).overwritePartitions()

    written = spark.read.table(_gold_table()).filter(F.col("snapshot_date") == date_str)
    _validate_fact_bike_risk(spark, written)  # 실패 시 GoldValidationError -> 배치 중단

    logger.info("%s: 자전거 %d대 risk_score 산출 (정비중 %d대 제외)", date_str, eligible_count, row_count - eligible_count)
    return eligible_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session_with_deequ("gold-build-fact-bike-risk")
    try:
        _ensure_fact_bike_risk_table(spark)

        snapshot_date_str = os.getenv("SNAPSHOT_DATE")
        target_date = date.fromisoformat(snapshot_date_str) if snapshot_date_str else date.today()

        try:
            _process_date(spark, target_date)
        except GoldValidationError as e:
            logger.error("%s 처리 실패, 배치 중단: %s", target_date, e)
            sys.exit(1)
    finally:
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()
