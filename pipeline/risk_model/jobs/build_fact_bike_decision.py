"""
gold_risk_decision 원안의 "7. build_fact_bike_decision" 단계 구현 -
gold.fact_bike_risk + gold.fact_station_inventory -> 대여중단 여부 결정.

action은 대여중단/보류 2가지뿐이다. capacity(정비 인력 기준)로 "대여중단" 중 오늘 몇 대를
실제로 수거할지 정하는 건 이 job 스코프 밖(mart 단계)이라 여기서는 만들지 않는다.

dim_bike처럼 날짜 범위를 누적 처리하는 잡이 아니라 하루치(target_date)를 통째로
재계산해서 OVERWRITE하는 구조라 워터마크가 없다 - 파티션을 다시 덮어써도 같은 입력이면
같은 결과가 나오므로 재실행이 그냥 멱등하게 처리된다.
"""
import logging
import os
import sys
from datetime import date

os.environ.setdefault("SPARK_VERSION", "3.5")

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SUSPEND = "대여중단"
HOLD = "보류"


class GoldValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


def _fact_bike_risk_table():
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_bike_risk"


def _bike_location_table():
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_location"


def _station_inventory_table():
    # NOTE: 대여소 재고 입력을 배치 추정(fact_station_inventory) vs 실시간(station_inventory_snapshot)
    # 중 뭘 쓸지 아직 팀 확정 전. 지금은 fact_station_inventory(bike_cnt/hold_num/target_bike_cnt)
    # 기준으로 짜뒀고, 확정되면 이 함수와 join 컬럼명만 바꾸면 된다.
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_station_inventory"


def _gold_table():
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.fact_bike_decision"


def _ensure_fact_bike_decision_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            snapshot_date DATE,
            bike_id STRING,
            action STRING
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )


def _validate_fact_bike_decision(spark, df, risk_df) -> None:
    """오늘자 파티션만 검증한다 (OVERWRITE 구조)."""
    check = Check(spark, CheckLevel.Error, "fact_bike_decision_check")
    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            check.isComplete("bike_id")
            .isComplete("action")
            .isContainedIn("action", [SUSPEND, HOLD])
        )
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show(truncate=False)

    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise GoldValidationError(f"fact_bike_decision 품질 검증 실패: {failed_constraints}")

    # 참조 무결성: 오늘자 fact_bike_decision의 모든 bike_id는 오늘자 fact_bike_risk에도 있어야 한다.
    orphan_count = df.join(risk_df.select("bike_id"), on="bike_id", how="left_anti").count()
    if orphan_count > 0:
        raise GoldValidationError(f"fact_bike_risk에 없는 bike_id {orphan_count}건 발견")


def _process_date(spark, target_date):
    date_str = target_date.strftime("%Y-%m-%d")

    risk_df = spark.read.table(_fact_bike_risk_table()).filter(F.col("snapshot_date") == date_str)
    row_count = risk_df.count()
    if row_count == 0:
        logger.info("%s: fact_bike_risk에 처리할 데이터 없음", date_str)
        return 0

    location_df = spark.read.table(_bike_location_table()).select("bike_id", "last_station_id")
    inventory_df = spark.read.table(_station_inventory_table()).select(
        "station_id", "bike_cnt", "target_bike_cnt"
    )

    # inventory_df left join: 재고 정보 없는 대여소(운영 중단 등)면 warning_available_cnt가
    # null로 남아 그 대여소의 Warning 자전거는 비교 조건이 거짓이 되어 자동으로 보류 처리된다
    # (에러로 죽지 않고 안전한 쪽으로 fallback).
    bikes = (
        risk_df.join(location_df, on="bike_id", how="inner")
        .withColumnRenamed("last_station_id", "station_id")
        .join(inventory_df, on="station_id", how="left")
    )

    # 대여소별 suspendable_bike_cnt(여유분), critical_cnt(즉시 대여중단 확정 수)
    # 설계 문서엔 "Critical부터 순서대로 잔여량 차감" 식 반복 처리로 설명돼있지만,
    # Critical은 예산 확인 없이 무조건 대여중단(=항상 critical_cnt만큼만 소진)이고
    # Warning은 고정 랭킹 컷이라, 반복문 없이 이 닫힌 식(suspendable - critical) +
    # 랭킹 비교 한 번으로 동일한 결과가 나온다 (문서 예시 두 개로 직접 검증함).
    station_agg = bikes.groupBy("station_id", "bike_cnt", "target_bike_cnt").agg(
        F.sum(F.when(F.col("risk_grade") == "Critical", 1).otherwise(0)).alias("critical_cnt")
    )
    station_agg = station_agg.withColumn(
        "suspendable_bike_cnt", F.greatest(F.lit(0), F.col("bike_cnt") - F.col("target_bike_cnt"))
    ).withColumn(
        "warning_available_cnt", F.greatest(F.lit(0), F.col("suspendable_bike_cnt") - F.col("critical_cnt"))
    )

    bikes = bikes.join(
        station_agg.select("station_id", "warning_available_cnt"), on="station_id", how="left"
    )

    w = Window.partitionBy("station_id").orderBy(F.col("risk_score").desc())
    bikes = bikes.withColumn("warning_rank", F.row_number().over(w))

    bikes = bikes.withColumn(
        "action",
        F.when(F.col("risk_grade") == "Critical", F.lit(SUSPEND))
        .when(
            (F.col("risk_grade") == "Warning") & (F.col("warning_rank") <= F.col("warning_available_cnt")),
            F.lit(SUSPEND),
        )
        .otherwise(F.lit(HOLD)),
    )

    out_df = bikes.select(
        F.lit(target_date).alias("snapshot_date"), "bike_id", "action"
    )
    out_df.writeTo(_gold_table()).overwritePartitions()

    written = spark.read.table(_gold_table()).filter(F.col("snapshot_date") == date_str)
    _validate_fact_bike_decision(spark, written, risk_df)  # 실패 시 GoldValidationError -> 배치 중단

    suspend_count = out_df.filter(F.col("action") == SUSPEND).count()
    logger.info("%s: 자전거 %d대 중 %d대 대여중단 결정", date_str, row_count, suspend_count)
    return row_count


def run():
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session_with_deequ("gold-build-fact-bike-decision")
    try:
        _ensure_fact_bike_decision_table(spark)

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
