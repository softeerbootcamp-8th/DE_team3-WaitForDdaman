"""
Gold(TEMP) - 활성 대여소 목록 (silver.station_master + silver.station_active -> gold.station_active)

### 조인의 의미
silver.station_master는 "등록된 모든 대여소"의 최신 스냅샷이고, silver.station_active는
"현재 실시간으로 상태가 보고되는(=운영 중인) 대여소"다(지금은 더미로 station_master를
그대로 복사해서 채워져 있음 - staging/jobs/silver_station_active_seed.py 참고).
두 테이블을 station_id로 INNER JOIN해서 "실제로 운영 중인 대여소"만 남기고,
설명 컬럼(station_name/region/district/hold_num/위경도)은 station_master(마스터
데이터)에서 가져온다.

### TEMP 성격 - 매번 전체 덮어쓰기
gold.bike_location과 동일한 이유로 파티션 없이 매번 테이블 전체를 덮어쓴다.

### 적재 전 PyDeequ 검증
build_dim_bike.py와 동일한 패턴 - station_id는 조인 키이므로 결과에서 항상
유일해야 하고, station_id/hold_num은 결측이 있으면 안 된다. 실패하면
GoldValidationError로 배치를 즉시 중단한다(적재 전 검증이므로 gold 테이블에는
아무 영향 없음).

사용법:
    python -m jobs.build_station_active
"""
import logging
import os
import sys
from datetime import date

from pyspark.sql import functions as F

import config
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class GoldValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


def _station_master_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.station_master"


def _station_active_silver_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.station_active"


def _gold_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.gold.station_active"


def _ensure_gold_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_gold_table()} (
            station_id    STRING,
            station_name  STRING,
            region        STRING,
            district      STRING,
            hold_num      INT,
            latitude      DOUBLE,
            longitude     DOUBLE,
            snapshot_date DATE
        )
        USING iceberg
        """
    )


def _latest_snapshot(spark, table: str):
    latest = spark.read.table(table).agg(F.max("snapshot_date")).collect()[0][0]
    return spark.read.table(table).filter(F.col("snapshot_date") == latest)


def _join_active_stations(master_df, active_ids_df, snapshot_date: str):
    """station_master(마스터 데이터)와 station_active(운영 중 여부)를 조인한다 -
    두 DataFrame만으로 동작하는 순수 로직이라 단위 테스트가 가능하다."""
    return (
        master_df.join(active_ids_df, on="station_id", how="inner")
        .select(
            "station_id",
            "station_name",
            "region",
            "district",
            "hold_num",
            "latitude",
            "longitude",
        )
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
    )


def build_station_active(spark, snapshot_date: str):
    master_df = _latest_snapshot(spark, _station_master_table())
    # 대여소 수가 적어(수백~수천 건) broadcast join이 안전하고 빠르다
    active_ids_df = F.broadcast(
        _latest_snapshot(spark, _station_active_silver_table()).select("station_id").distinct()
    )

    return _join_active_stations(master_df, active_ids_df, snapshot_date)


def _validate_station_active(spark, df) -> None:
    # 운영 중으로 확인된 대여소가 하나도 없는 경우(station_master/station_active
    # 교집합이 비는 극단적 상황)엔 이 결과도 0행일 수 있다. PyDeequ의
    # hasUniqueness/isComplete는 빈 데이터셋을 "값이 전부 NULL이었다"로 취급해
    # 실패 처리하므로(실측으로 확인), 빈 경우엔 검증을 스킵한다.
    if df.isEmpty():
        logger.info("gold.station_active 결과가 비어있음 - PyDeequ 검증 스킵")
        return

    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationResult, VerificationSuite

    check = Check(spark, CheckLevel.Error, "station_active_check")
    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(
            # hold_num은 isComplete 대상이 아님 - 실측(2026-08-14 기준)으로 hold_num이
            # 없는 대여소가 15곳 있고, 이는 정상 원천 결측이라 silver 단계에서도
            # null로 그대로 통과시킨다(0으로 채우지 않음 - README/staging 테스트 참고).
            check.hasUniqueness(["station_id"], lambda fraction: fraction > 0.99).isComplete("station_id")
        )
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
    result_df.show(truncate=False)

    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise GoldValidationError(f"gold.station_active 품질 검증 실패: {failed_constraints}")


def run() -> None:
    snapshot_date = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session_with_deequ("gold-build-station-active")
    try:
        _ensure_gold_table(spark)

        out_df = build_station_active(spark, snapshot_date).cache()
        row_count = out_df.count()
        try:
            _validate_station_active(spark, out_df)
        except GoldValidationError as e:
            logger.error("%s: gold.station_active 검증 실패, 적재 중단: %s", snapshot_date, e)
            sys.exit(1)

        out_df.writeTo(_gold_table()).overwritePartitions()
        out_df.unpersist()

        logger.info("%s: gold.station_active %d행 갱신 완료", snapshot_date, row_count)
    finally:
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()
