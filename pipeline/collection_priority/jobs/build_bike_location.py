"""
Gold(TEMP) - 자전거별 현재 위치 (silver.rental_history -> gold.bike_location)

### 증분 처리 
매번 silver.rental_history 전체를 스캔+정렬하면 데이터가 쌓일수록 배치가
점점 느려진다. 대신 기존 gold.bike_location(직전 상태)을 baseline으로 읽고,
아직 반영 안 된 파티션만 스캔해서 델타를 구한 뒤 병합한다.

### 델타 구간을 "어제 하루"로 고정하지 않고 자기 자신의 snapshot_date로 추적하는 이유
처음엔 델타 구간을 무조건 `snapshot_date - 1`(어제) 하루로 고정했었다. 이러면
dag_gold_dim_fact가 하루라도 실행을 건너뛰면(예: catchup=False 상태에서 스케줄러
장애) 그 사이에 낀 rent_date_partition은 그 뒤로 "어제"가 아니게 되어 어떤 미래
실행에서도 다시는 스캔되지 않는다 - 해당 파티션에만 활동이 있던 자전거의 위치가
영영 갱신되지 않는 조용한 데이터 유실이 발생한다.

이를 막기 위해 gold.bike_location 자신의 snapshot_date 컬럼을 워터마크로 재사용한다.
이 테이블은 매 실행마다 전체가 동일한 snapshot_date로 덮어써지므로(TEMP 성격),
MAX(snapshot_date)가 곧 "그 값 - 1까지의 rent_date_partition은 이미 반영됨"을
뜻한다. 그래서 이번 실행의 델타 시작일 = 직전 MAX(snapshot_date)(비어있으면
None=하한 없음, 즉 전체 스캔), 끝일 = 이번 snapshot_date - 1이다. 평소(매일
정상 실행)엔 이 구간이 정확히 하루뿐이라 기존과 성능이 동일하고, 실행이
밀렸을 때만 자동으로 구간이 넓어져 누락을 스스로 복구한다(self-healing).
별도 워터마크 파일이 필요 없다.

오늘(아직 반영 안 된 파티션 기준 신규) 활동이 없는 자전거는 baseline 그대로 유지
(carry-forward)하고, 활동이 있는 자전거만 위치를 갱신한다. baseline/delta
둘 다 있는 경우 last_event_at(반납 시각)이 더 최신인 쪽을 채택해서, 같은 날
재실행되거나 날짜 순서가 어긋나는 백필 시나리오에서도 과거 데이터로 최신
상태를 덮어쓰지 않는다(idempotent).

### "현재 위치"의 정의
자전거별로 가장 최근 반납 건의 반납 대여소(return_station_id)를 현재 위치로
본다. silver.rental_history는 이미 반납 완료된 건만 들어오므로(return_dt가
비어있는 진행 중 대여 레코드는 존재하지 않음) "아직 반납 안 됨" 분기는 없다.

last_station_id가 null인 경우는 데이터 결측이 아니라, 대여소가 아닌 엉뚱한
곳에 반납된 경우다(예: 노상 방치). 그런 자전거는 어느 대여소에도 속하지
않는 게 맞으므로, gold.fact_station_inventory에서 재고 집계 대상에서
자연스럽게 빠지는 게 의도된 동작이다.

### last_event_at을 함께 저장하는 이유
gold.fact_station_inventory가 이 위치 정보와 silver.bikeman_action(수거/배치)
이벤트 중 어느 쪽이 더 최신인지 비교해서 최종 위치를 정해야 한다. 그 비교
기준 시각(반납 시각)을 여기서 같이 내려준다. 병합 시 baseline/delta 중
최신 쪽을 고르는 기준으로도 재사용된다.

### TEMP 성격 - 매번 전체 덮어쓰기
"현재 상태"만 의미가 있고 이력을 쌓지 않는다. 파티션 없이 매 실행마다 테이블
전체를 덮어쓴다(overwritePartitions는 파티션이 없는 테이블에서는 전체 교체와
동일하게 동작함). baseline을 자기 자신(gold.bike_location)에서 읽어와
병합 후 다시 자기 자신에 쓰는 구조라, cache() 없이 지연 평가만으로 두면
Spark가 같은 테이블을 읽고 쓰는 도중 참조하려다 꼬일 수 있어 병합 결과를
한 번 caching한 뒤 쓴다.

### 적재 전 PyDeequ 검증
build_dim_bike.py와 동일한 패턴 - bike_id는 병합 키이므로 병합 결과에서 항상
유일해야 하고(hasUniqueness), bike_id/snapshot_date는 결측이 있으면 안 된다.
실패하면 GoldValidationError로 배치를 즉시 중단한다(적재 전 검증이므로 gold
테이블에는 아무 영향 없음 - writeTo보다 먼저 검증함).

사용법:
    python -m jobs.build_bike_location
    SNAPSHOT_DATE=2026-08-17 python -m jobs.build_bike_location
"""
import logging
import os
import sys
from datetime import date, datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class GoldValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


def _silver_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.rental_history"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.bike_location"


def _ensure_gold_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            bike_id        STRING,
            last_station_id STRING,
            last_event_at  TIMESTAMP,
            snapshot_date  DATE
        )
        USING iceberg
        """
    )


def _baseline(spark):
    """직전 상태 전체 (테이블이 비어있으면 빈 DataFrame - cold start도 안전)."""
    return spark.read.table(_gold_table()).select(
        "bike_id",
        F.col("last_station_id").alias("base_station_id"),
        F.col("last_event_at").alias("base_event_at"),
    )


def _baseline_snapshot_date(spark) -> date | None:
    """gold.bike_location에 남아있는 MAX(snapshot_date) - 그 값보다 이전
    rent_date_partition은 이미 baseline에 반영되어 있다는 뜻이므로, 이번 실행의
    델타 시작일로 그대로 쓸 수 있다. 테이블이 비어있으면(cold start) None."""
    return spark.read.table(_gold_table()).agg(F.max("snapshot_date")).collect()[0][0]


def _delta(spark, start_date: date | None, end_date: date):
    """silver.rental_history에서 아직 baseline에 반영 안 된 구간
    [start_date, end_date]만 스캔해서 자전거별 가장 최근 반납 1건만 채택한다.
    start_date가 None이면(cold start) 하한 없이 end_date까지 전체를 스캔한다."""
    end_str = end_date.strftime("%Y-%m-%d")
    silver_df = spark.read.table(_silver_table())
    if start_date is not None:
        start_str = start_date.strftime("%Y-%m-%d")
        silver_df = silver_df.filter(F.col("rent_date_partition").between(start_str, end_str))
    else:
        silver_df = silver_df.filter(F.col("rent_date_partition") <= end_str)

    window = Window.partitionBy("bike_id").orderBy(F.col("return_dt").desc())
    return (
        silver_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            "bike_id",
            F.col("return_station_id").alias("delta_station_id"),
            F.col("return_dt").alias("delta_event_at"),
        )
    )


def _merge_baseline_delta(baseline_df, delta_df, snapshot_date: date):
    """baseline(직전 상태)과 delta(신규 반영분)를 병합한다 - Spark 세션/카탈로그
    없이 두 DataFrame만으로 동작하는 순수 로직이라 단위 테스트가 가능하다."""
    merged = baseline_df.join(delta_df, on="bike_id", how="outer")

    # baseline에 없던(신규) 자전거는 base_event_at이 null이라 무조건 delta 채택.
    # 둘 다 있으면 반납 시각이 더 최신인 쪽 채택 - 재실행/백필에도 안전.
    delta_is_newer = F.col("delta_event_at").isNotNull() & (
        F.col("base_event_at").isNull() | (F.col("delta_event_at") > F.col("base_event_at"))
    )

    return merged.select(
        "bike_id",
        F.when(delta_is_newer, F.col("delta_station_id")).otherwise(F.col("base_station_id")).alias(
            "last_station_id"
        ),
        F.when(delta_is_newer, F.col("delta_event_at")).otherwise(F.col("base_event_at")).alias("last_event_at"),
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
    )


def build_bike_location(spark, snapshot_date: date):
    baseline = _baseline(spark)
    delta_start = _baseline_snapshot_date(spark)
    delta_end = snapshot_date - timedelta(days=1)
    delta = _delta(spark, delta_start, delta_end)

    return _merge_baseline_delta(baseline, delta, snapshot_date)


def _validate_bike_location(spark, df) -> None:
    # 아직 반납 완료된 대여이력이 하나도 없는 환경(서비스 최초 구동 직후 등)에서는
    # 이 결과도 0행일 수 있다. PyDeequ의 hasUniqueness/isComplete는 빈 데이터셋을
    # "값이 전부 NULL이었다"로 취급해 실패 처리하므로(실측으로 확인), 빈 경우엔
    # 검증을 스킵한다 - 그렇지 않으면 문제 없는 배치가 데이터가 아직 없다는
    # 이유만으로 계속 막히게 된다.
    if df.isEmpty():
        logger.info("gold.bike_location 결과가 비어있음 - PyDeequ 검증 스킵")
        return

    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationResult, VerificationSuite

    check = Check(spark, CheckLevel.Error, "bike_location_check")
    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            check.hasUniqueness(["bike_id"], lambda fraction: fraction > 0.99)
            .isComplete("bike_id")
            .isComplete("snapshot_date")
        )
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show(truncate=False)

    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise GoldValidationError(f"gold.bike_location 품질 검증 실패: {failed_constraints}")


def run() -> None:
    snapshot_date_str = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date()

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session_with_deequ("gold-build-bike-location")
    try:
        _ensure_gold_table(spark)

        out_df = build_bike_location(spark, snapshot_date).cache()
        row_count = out_df.count()
        try:
            _validate_bike_location(spark, out_df)  # 실패 시 GoldValidationError -> 적재 없이 배치 중단
        except GoldValidationError as e:
            logger.error("%s: gold.bike_location 검증 실패, 적재 중단: %s", snapshot_date_str, e)
            sys.exit(1)

        out_df.writeTo(_gold_table()).overwritePartitions()
        out_df.unpersist()

        logger.info("%s: gold.bike_location %d행 갱신 완료 (증분 처리)", snapshot_date_str, row_count)
    finally:
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()
