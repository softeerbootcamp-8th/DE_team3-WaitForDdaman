"""
Silver - 실시간 대여정보 필터 테이블

이 원천(bikeList)을 수집하는 목적은 재고 수치(거치대 수/주차된 자전거 수/거치율)가
아니라 "오늘 실제로 운영 중인 대여소가 어디인지" 판별하는 것이다(bronze의
schema/station_active_schema.py 참고). 대여소명·위경도·자치구 등 서술 속성은
silver.station_master가 이미 갖고 있으므로, 여기서는 그 날 API 응답에 실제로
존재했던 station_id 집합만 남긴다.

운영/미운영 자체의 최종 판정(Gold의 build_station_active)은 이 테이블을 넘겨받는
담당 4가 한다. 담당 2는 그 판정 로직을 구현하지 않는다.

사용법:
    python -m jobs.silver_station_active
    SNAPSHOT_DATE=2026-08-14 python -m jobs.silver_station_active   # 특정 날짜 재처리
"""
import logging
import os
import sys

from pyspark.sql import functions as F

import config
from common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SILVER_COLUMNS = ["snapshot_date", "station_id"]


def normalize(bronze_df):
    """
    브론즈 DataFrame을 실버 2컬럼(snapshot_date, station_id)으로 정제한다.
    읽기/쓰기를 하지 않는다.

    station_id가 null인 행은 드롭한다. 같은 스냅샷 내 station_id 중복은
    dropDuplicates로 하나만 남긴다.
    """
    df = bronze_df.select(
        F.col("snapshot_date").cast("date").alias("snapshot_date"),
        F.col("station_id"),
    )

    not_null = df.filter(F.col("station_id").isNotNull())
    deduped = not_null.dropDuplicates(["station_id"])

    return deduped.select(*SILVER_COLUMNS)
