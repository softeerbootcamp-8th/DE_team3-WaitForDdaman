"""
Silver - 대여소 마스터 정제

브론즈는 원본 보존 원칙에 따라 전부 STRING이고 값 정제를 하지 않는다.
실버는 세 가지만 한다.

    1. 타입 캐스팅   위경도 -> double, 거치대수 -> int, 스냅샷일 -> date
    2. region 파생   강남/강북. 어떤 원천에도 없는 값이라 자치구 상수 매핑으로 만든다
    3. 문자열 정규화 대여소명 공백/개행 정리

골드·프론트·백엔드에서 쓰지 않는 컬럼(station_no, station_id_name, address1/2)은
떨어뜨린다. 브론즈에 그대로 남아 있으므로 필요해지면 다시 꺼낼 수 있다.

SCD Type 2 이력화는 이번 범위가 아니다. 브론즈가 일일 스냅샷을 계속 보존하므로
실버가 이력을 만들지 않아도 정보 손실이 없고, 필요해지면 소급 생성할 수 있다.
판단 근거는 docs/superpowers/specs/2026-08-14-silver-station-master-design.md 참고.

사용법:
    python -m jobs.silver_station_master
    SNAPSHOT_DATE=2026-08-14 python -m jobs.silver_station_master   # 특정 날짜 재처리
"""
import logging
import os
import sys
from datetime import date

from pyspark.sql import functions as F

import config
from common.spark_session import build_spark_session
from common.watermark import write_watermark
from config.watermark_keys import SILVER_STATION_MASTER

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 한강 남/북 기준. 서울 자치구는 25개로 고정이다.
# 좌표 기반 판정(한강 폴리곤)은 경계 대여소를 오분류할 위험이 있어 쓰지 않는다.
REGION_BY_DISTRICT = {
    # 강남 11개구
    "강서구": "강남", "양천구": "강남", "구로구": "강남", "금천구": "강남",
    "영등포구": "강남", "동작구": "강남", "관악구": "강남", "서초구": "강남",
    "강남구": "강남", "송파구": "강남", "강동구": "강남",
    # 강북 14개구
    "종로구": "강북", "중구": "강북", "용산구": "강북", "성동구": "강북",
    "광진구": "강북", "동대문구": "강북", "중랑구": "강북", "성북구": "강북",
    "강북구": "강북", "도봉구": "강북", "노원구": "강북", "은평구": "강북",
    "서대문구": "강북", "마포구": "강북",
}

SILVER_COLUMNS = [
    "snapshot_date",
    "station_id",
    "station_name",
    "district",
    "region",
    "latitude",
    "longitude",
    "hold_num",
]


class UnknownDistrictError(Exception):
    """REGION_BY_DISTRICT에 없는 자치구 - 원천 이상이므로 즉시 실패시킨다."""


def _region_column():
    pairs = [F.lit(x) for kv in REGION_BY_DISTRICT.items() for x in kv]
    return F.create_map(*pairs)[F.col("district")]


def normalize(bronze_df):
    """
    브론즈 DataFrame을 실버 8컬럼으로 정제한다. 읽기/쓰기를 하지 않는다.

    자치구가 매핑에 없으면 UnknownDistrictError를 던진다. 조용히 null로 두면
    프론트의 지역 필터에서 그 대여소가 사라지는데 아무도 알아차리지 못한다.
    """
    df = bronze_df.select(
        F.col("snapshot_date").cast("date").alias("snapshot_date"),
        F.col("station_id"),
        # 연속 공백·개행을 공백 하나로 모은 뒤 앞뒤를 자른다
        F.trim(F.regexp_replace(F.col("station_name"), r"\s+", " ")).alias("station_name"),
        F.col("district"),
        _region_column().alias("region"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("hold_num").cast("int").alias("hold_num"),
    ).select(*SILVER_COLUMNS)

    unmapped = [
        r["district"]
        for r in df.filter(F.col("district").isNotNull() & F.col("region").isNull())
        .select("district")
        .distinct()
        .collect()
    ]
    if unmapped:
        raise UnknownDistrictError(
            f"REGION_BY_DISTRICT에 없는 자치구: {sorted(unmapped)} "
            "(원천 스키마 변경 또는 값 오류 점검 필요)"
        )

    return df


def _log_quality(df) -> None:
    """캐스팅 실패·이상값 건수를 남긴다. 조용히 넘기면 원천 변경을 놓친다."""
    stats = df.agg(
        F.count("*").alias("total"),
        F.sum(F.col("latitude").isNull().cast("int")).alias("lat_null"),
        F.sum(F.col("longitude").isNull().cast("int")).alias("lon_null"),
        F.sum(F.col("hold_num").isNull().cast("int")).alias("hold_null"),
        F.sum(((F.col("latitude") == 0) | (F.col("longitude") == 0)).cast("int")).alias("zero_coord"),
    ).collect()[0]

    logger.info("실버 %d행", stats["total"])
    for label, key in [
        ("위도 캐스팅 실패/결측", "lat_null"),
        ("경도 캐스팅 실패/결측", "lon_null"),
        ("거치대수 없음", "hold_null"),
        ("위경도가 0", "zero_coord"),
    ]:
        if stats[key]:
            logger.warning("%s: %d건", label, stats[key])


def _table_name(layer: str) -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.{layer}.station_master"


def _ensure_silver_table(spark) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name("silver")} (
            snapshot_date DATE   COMMENT '스냅샷 기준일, 파티션 키',
            station_id    STRING COMMENT '대여소 ID (ST-10), 골드 조인 키',
            station_name  STRING COMMENT '대여소명, 공백 정규화됨',
            district      STRING COMMENT '자치구',
            region        STRING COMMENT '강남 / 강북 (자치구에서 파생)',
            latitude      DOUBLE COMMENT '위도',
            longitude     DOUBLE COMMENT '경도',
            hold_num      INT    COMMENT '거치대 수 (null 가능)'
        )
        USING iceberg
        PARTITIONED BY (snapshot_date)
        """
    )
    spark.sql(
        f"ALTER TABLE {_table_name('silver')} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )


def _read_bronze(spark, snapshot_date: str | None):
    """
    API 파티션만 읽는다. 원천 조사 과정에서 적재한 반기 파일 파티션이
    브론즈에 남아 있고, 그 행들은 hold_num이 전부 null이다.
    """
    df = spark.table(_table_name("bronze")).filter(F.col("source_file").startswith("api:"))

    if snapshot_date:
        return df.filter(F.col("snapshot_date") == snapshot_date), snapshot_date

    latest = df.agg(F.max("snapshot_date")).collect()[0][0]
    if latest is None:
        raise ValueError("브론즈에 API 스냅샷이 없습니다. 일 배치를 먼저 실행하세요.")
    return df.filter(F.col("snapshot_date") == latest), latest


def run() -> None:
    spark = build_spark_session("silver-station-master")
    _ensure_silver_table(spark)

    bronze_df, snapshot_date = _read_bronze(spark, os.getenv("SNAPSHOT_DATE"))
    logger.info("브론즈 스냅샷 %s 처리 시작", snapshot_date)

    try:
        silver_df = normalize(bronze_df).cache()
        _log_quality(silver_df)
        silver_df.writeTo(_table_name("silver")).overwritePartitions()
        silver_df.unpersist()
    except UnknownDistrictError as e:
        logger.error("정제 실패: %s", e)
        sys.exit(1)

    # dag_gold_dim_fact가 이 워터마크로 "오늘 치 실버가 준비됐는지" 확인한다
    # (Asset 트리거 DagRun은 logical_date가 없어 ExternalTaskSensor를 못 씀).
    processed_date = snapshot_date if isinstance(snapshot_date, date) else date.fromisoformat(snapshot_date)
    write_watermark(processed_date, watermark_key=SILVER_STATION_MASTER)

    logger.info("%s: 실버 적재 완료", snapshot_date)


if __name__ == "__main__":
    run()
