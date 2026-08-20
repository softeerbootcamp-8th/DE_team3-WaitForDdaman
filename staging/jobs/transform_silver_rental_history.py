import logging
import os
import sys
from datetime import date, timedelta

# pydeequ는 import되는 순간 SPARK_VERSION 환경변수를 읽어 어떤 Scala Deequ jar를 사용할지 결정함
# 그래서 import pydeequ 계열보다 반드시 먼저 환경변수를 설정해야 함
os.environ.setdefault("SPARK_VERSION", "3.5")

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import config
from common.s3_utils import ensure_bucket  # 워커/로컬 실행 시 대상 버킷이 없으면 만들어주는 안전장치
from common.spark_session import build_spark_session_with_deequ, stop_spark_session_with_deequ
from common.watermark import read_watermark, write_watermark # Bronze용으로 설계된 워터마크 모듈을 그대로 재사용하되 키만 다르게 줘서 Silver 전용 워터마크로 사용
from config.watermark_keys import SILVER_RENTAL_HISTORY  # build_dim_bike/set_watermark와 같은 값을 공유

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# S3(혹은 LocalStack)상의 실제 객체 키 경로
SILVER_WATERMARK_KEY = SILVER_RENTAL_HISTORY

# MAX_DAYS_PER_RUN 미지정 시 쓰는 기본 상한 - 평소 daily 운영(구간이 보통 하루)엔 영향 없음.
DEFAULT_MAX_DAYS_PER_RUN = 31

# --- table 이름 helper ---
def _bronze_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.rental_history"
# config.SETTINGS.iceberg_catalog_name : 로컬(Hadoop catalog)/AWS(Glue) 환경에 따라 다른 카탈로그 이름을 가리킴
# -> 환경별 분기가 필요 없어짐

def _silver_table() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.rental_history"


# --- 예외 정의 ---
class SilverValidationError(Exception):
    """PyDeequ 품질 검증 실패 - 이 예외는 배치를 즉시 중단시켜야 한다."""


# --- Silver 테이블 DDL ---
def _ensure_silver_table(spark) -> None:
    catalog = config.SETTINGS.iceberg_catalog_name
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_silver_table()} (
            bike_id STRING,
            rent_dt TIMESTAMP,
            return_dt TIMESTAMP,
            use_distance_m DOUBLE,
            rent_station_id STRING,
            return_station_id STRING,
            rent_date_partition STRING,
            source_file STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (rent_date_partition)
        """
    )
    # 여러 rent_date_partition에 걸친 구간을 한 번의 overwritePartitions()로 쓸 때,
    # 이 속성 없이는 Iceberg가 FanoutWriter를 써서 파티션마다 파일을 동시에 열어두다
    # OOM 나는 걸 initial_load_rental_history.py에서 실측으로 확인함 -> 여기도 동일 적용
    spark.sql(
        f"ALTER TABLE {_silver_table()} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )
# rent_date_partition는 Bronze와 동일한 파티션 컬럼을 사용 => overwritePartitions()가 날짜별로 정확히 매칭되기 위함


# --- 데이터 정제 ---
# 결측치 필터
def _drop_missing_keys(silver_df):
    return silver_df.filter(F.col("bike_id").isNotNull() & F.col("rent_dt").isNotNull())
# Column 객체끼리의 연산은 비트 연산자를 사용


# 중복 제거
def _dedup_bike_rent_dt(silver_df):
    # 같은 자전거가 같은 시각에 대여를 시작한 행들끼리 그룹핑
    window = Window.partitionBy("bike_id", "rent_dt").orderBy(
        F.col("return_dt").asc_nulls_last(), F.col("rent_station_id").asc()
    )
    return (
        silver_df.withColumn("_rn", F.row_number().over(window)) #그룹마다 1부터 순번 매김
        .filter(F.col("_rn") == 1) # 그룹당 첫 번째 행만 남김
        .drop("_rn")
    )


# --- rent_dt/return_dt 날짜 파싱 ---
# Bronze는 원본 문자열을 그대로 보존하므로 소스(API/CSV 초기 적재)마다 포맷이 다를 수 있음
# (rent_date_partition을 만들 때도 같은 이유로 포맷 편차에 대응함 - initial_load_rental_history.py 참고)
_KNOWN_DATETIME_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyyMMddHHmmss",
    "yyyy-MM-dd",
    "yyyyMMdd",
]


def _parse_datetime(col_name: str):
    # 알려진 포맷을 순서대로 시도해서 처음 매칭되는 값을 채택 (coalesce가 첫 not-null을 고름)
    parsed = F.lit(None).cast("timestamp")
    for fmt in _KNOWN_DATETIME_FORMATS:
        parsed = F.coalesce(parsed, F.to_timestamp(F.col(col_name), fmt))
    return parsed


# --- Bronze => Silver 변환 ---
def _cast_bronze_to_silver(bronze_df):
    return bronze_df.select(
        "bike_id",
        F.col("rent_dt").alias("_raw_rent_dt"),
        F.col("return_dt").alias("_raw_return_dt"),
        _parse_datetime("rent_dt").alias("rent_dt"),
        _parse_datetime("return_dt").alias("return_dt"),
        F.col("use_distance_m").cast("double").alias("use_distance_m"),
        "rent_station_id",
        "return_station_id",
        "rent_date_partition",
        "source_file",
        "ingested_at", # Bronze lineage 컬럼 그대로 승계
    )


# --- Silver 품질 검증 ---
def _validate_silver(spark, silver_df, range_label: str) -> None:
    check = Check(spark, CheckLevel.Error, f"silver_rental_history_{range_label}")
    
    result = (
        VerificationSuite(spark)
        .onData(silver_df)
        .addCheck(
            # _drop_missing_keys가 이미 걸렀어야 할 조건 재확인
            check.isComplete("bike_id") 
            .isComplete("rent_dt")
            
            # _dedup_bike_rent_dt가 이미 제거했어야 할 중복의 재확인
            .hasUniqueness(["bike_id", "rent_dt"], lambda fraction: fraction > 0.98)
            
            # 이동거리가 음수인 데이터 확인
            .isNonNegative("use_distance_m")
            
            # 대여일시 이후에 반납일시가 있어야 함
            .satisfies(
                "return_dt IS NULL OR return_dt >= rent_dt",
                "return_dt는 rent_dt 이후여야 함",
            )
        )
        .run()
    )
    result_df = VerificationResult.checkResultsAsDataFrame(spark, result) # 결과를 사람이 읽을 DataFrame으로 변환
    result_df.show(truncate=False) # Airflow task 로그에 검증 결과 테이블 출력

    # 실패한 제약 이름만 추려서 에러 메시지에 포함
    if result.status != "Success":
        failed_constraints = [r["constraint"] for r in result_df.collect() if r["constraint_status"] != "Success"]
        raise SilverValidationError(
            f"{range_label} Silver 품질 검증 실패: {failed_constraints}"
        )


# --- 날짜 범위 단위 처리 ---
def _process_range(spark, start_date: date, end_date: date) -> int:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    range_label = start_str if start_date == end_date else f"{start_str}~{end_str}"

    bronze_df = spark.read.table(_bronze_table()).filter(
        F.col("rent_date_partition").between(start_str, end_str)
    )

    # 캐스팅까지 마친 뒤 캐시 -> 아래 통계 집계와 이후 결측/중복 제거 단계가
    # Bronze를 두 번 다시 읽지 않고 이 캐시 하나를 같이 재사용함
    cast_df = _cast_bronze_to_silver(bronze_df).cache()

    # count 액션 한 번으로 전체 행수 + 파싱 실패 행수(원본 문자열은 있는데 알려진 포맷으로 하나도 안 파싱된 경우)를 같이 집계
    stats = cast_df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(
            F.when(
                (F.col("_raw_rent_dt").isNotNull() & (F.trim(F.col("_raw_rent_dt")) != "") & F.col("rent_dt").isNull())
                | (F.col("_raw_return_dt").isNotNull() & (F.trim(F.col("_raw_return_dt")) != "") & F.col("return_dt").isNull()),
                1,
            ).otherwise(0)
        ).alias("parse_failed"),
    ).first()
    row_count = stats["total"]
    parse_failed = stats["parse_failed"] or 0

    # 0행이면 Silver로 승격할 데이터가 없으므로 바로 종료
    if row_count == 0:
        cast_df.unpersist()
        logger.info("%s: Bronze에 처리할 데이터 없음", range_label)
        return 0

    if parse_failed > 0:
        cast_df.unpersist()
        raise SilverValidationError(
            f"{range_label}: rent_dt/return_dt 파싱 실패 {parse_failed}행 - 알려지지 않은 날짜 포맷일 가능성"
        )

    # 파싱 실패 감지용으로만 쓴 원본 컬럼은 여기서 드롭 (Silver 스키마엔 없음)
    trimmed_df = cast_df.drop("_raw_rent_dt", "_raw_return_dt")

    # 결측 제거 → 중복 제거를 순차 적용
    kept_df = _drop_missing_keys(trimmed_df)
    silver_df = _dedup_bike_rent_dt(kept_df).cache()
    silver_count = silver_df.count()
    if silver_count < row_count:
        logger.info(
            "%s: bike_id/rent_dt 결측 또는 중복으로 %d행 제외 (Bronze %d행 -> Silver %d행)",
            range_label, row_count - silver_count, row_count, silver_count,
        )
    _validate_silver(spark, silver_df, range_label)  # 실패 시 SilverValidationError -> 배치 중단

    # Iceberg의 dynamic partition overwrite API
    # silver_df에 실제로 존재하는 파티션만 덮어쓰므로, Bronze에 없는 날짜는 그대로 유지됨
    silver_df.writeTo(_silver_table()).overwritePartitions()
    silver_df.unpersist() # 캐시 해제
    cast_df.unpersist() # Bronze 캐스팅 결과 캐시도 해제

    logger.info("%s: %d행 Silver 승격 완료", range_label, silver_count)
    return silver_count # 로깅용 반환값


# --- main ---
def run() -> None:
    # 백필/daily_batch를 안 거치고 이 잡만 단독 실행하는 경우에도 안전하도록 버킷을 보장
    # (raw_bucket에는 워터마크 JSON이 있음 - Bronze는 두 버킷 다 보장하는데 여기선 빠져있었음)
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session_with_deequ("silver-transform-rental-history")
    try:
        _ensure_silver_table(spark)

        # 상한선: Bronze가 확정 커밋한 날짜 (rental_history 기본 워터마크 키)
        bronze_watermark = read_watermark()
        # 하한선: Silver 전용 워터마크
        silver_watermark = read_watermark(watermark_key=SILVER_WATERMARK_KEY)

        start_date = silver_watermark + timedelta(days=1)
        end_date = bronze_watermark

        # 미지정이면 DEFAULT_MAX_DAYS_PER_RUN을 대신 써서 항상 상한을 건다
        max_days = os.getenv("MAX_DAYS_PER_RUN") or str(DEFAULT_MAX_DAYS_PER_RUN)
        capped_end = start_date + timedelta(days=int(max_days) - 1)
        if capped_end < end_date:
            logger.info(
                "MAX_DAYS_PER_RUN=%s 적용 - 이번 실행은 %s ~ %s까지만 처리 (원래 끝: %s)",
                max_days, start_date, capped_end, end_date,
            )
            end_date = capped_end

        if start_date > end_date:
            logger.info(
                "처리할 신규 날짜 없음 (Silver 워터마크=%s, Bronze 워터마크=%s)",
                silver_watermark, bronze_watermark,
            )
            return

        try:
            # _process_range 성공 시에만 다음 줄 write_watermark 실행
            _process_range(spark, start_date, end_date)
            write_watermark(end_date, watermark_key=SILVER_WATERMARK_KEY)
        except SilverValidationError as e:
            logger.error("%s~%s 처리 실패, 배치 중단: %s", start_date, end_date, e)
            sys.exit(1)
    finally:
        # PyDeequ 콜백 서버를 안 내리면 sys.exit()/정상 종료 후에도 프로세스가 안 끝나는 문제를 해결하기 위함
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()
