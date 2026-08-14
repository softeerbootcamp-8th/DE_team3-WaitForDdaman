"""
Bronze 일 배치 잡 - 따맨/bikeman 수거/배치 이벤트

전략: 증분 기준 = occurred_at(발생일), 다른 3개 원천의 daily_batch_*.py와 동일한
패턴(날짜 단위 워터마크, 날짜별 순차 처리, overwritePartitions로 멱등성 보장)을 따른다.
다만 조회 소스가 공공 API가 아니라 우리 자신의 Postgres(bikeman 스키마)라는 점만 다르다.

### 3일 lookback 재처리 (다른 3개 원천과의 유일한 구조적 차이)
bikeman은 "오프라인 작업 후 몰아서 제출"이 정상 케이스다(source_data 문서 참고) - 즉
occurred_at(발생)과 received_at(서버 수신) 사이에 수 시간~며칠 지연이 생길 수 있다.
그래서 "어제"만 처리하면, 이미 확정해서 넘어간 날짜에 늦게 도착한 이벤트를 영구히
놓친다. 이를 막기 위해 매 실행마다 처리 시작점을 LOOKBACK_DAYS만큼 앞당겨서
재계산한다. occurred_at 기준으로 그날 전체를 다시 조회해서 해당 파티션을 통째로
덮어쓰므로(overwritePartitions), 같은 날짜를 여러 번 재처리해도 안전하다(멱등).

### 워터마크
공통 유틸(common.watermark, 날짜 단위)을 그대로 재사용한다. 최초 실행 전에
`set_watermark.py`로 서비스 시작일 전날(2026-06-29)을 워터마크로 찍어둬야 한다
(안 찍으면 config의 기본 백필 시작일부터 처리를 시도해서 낭비가 생김 - 다른
데이터셋도 동일하게 요구되는 절차).

사용법:
    python -m jobs.daily_batch_bikeman_event
    MAX_DAYS_PER_RUN=1 python -m jobs.daily_batch_bikeman_event
"""
import json as _json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import config
from common.db_client import BikemanDbError, fetch_events_by_date
from common.s3_utils import ensure_bucket, put_json
from common.spark_session import build_spark_session
from common.watermark import read_watermark, write_watermark
from schema.bikeman_event_schema import (
    SchemaValidationError,
    build_select_exprs,
    validate_and_report,
    validate_event_types,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 다른 데이터셋과 워터마크 키가 겹치면 안 되므로 데이터셋별로 분리
WATERMARK_KEY = "_meta/watermark/bikeman_event.json"

# 지연 도착 대응 - source_data 문서에서 정한 재처리 윈도우
LOOKBACK_DAYS = 3

# bikeman 서비스 시작일(6/30) 이전은 데이터가 존재하지 않으므로, 워터마크가
# 비정상적으로 과거로 설정돼도 여기보다 앞으로는 재처리 범위를 확장하지 않는다
SERVICE_START_DATE = date(2026, 6, 30)


def _json_safe(value):
    """datetime/date 객체를 ISO 문자열로 변환 - common.s3_utils.put_json이
    default=str 없이 json.dumps를 호출해서, datetime을 그대로 넘기면
    TypeError('Object of type datetime is not JSON serializable')가 난다."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _json_safe_rows(rows: list[dict]) -> list[dict]:
    """원본 raw_rows는 건드리지 않고(뒤에서 Spark DataFrame 생성에 datetime 그대로
    필요), raw 백업용 JSON payload에만 쓸 문자열 변환 복사본을 만든다."""
    return [{k: _json_safe(v) for k, v in r.items()} for r in rows]


def _table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.bikeman_event"


def _quarantine_table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.bikeman_event_quarantine"


def _ensure_bronze_tables(spark) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_name()} (
            event_id      STRING,
            event_type    STRING,
            bike_id       STRING,
            station_id    STRING,
            worker_id     STRING,
            occurred_at   TIMESTAMP,
            received_at   TIMESTAMP,
            occurred_date_partition STRING,
            source_file   STRING,
            ingested_at   TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (occurred_date_partition)
        """
    )
    # 다른 Bronze 테이블과 동일한 이유 - Iceberg가 직접 분산/정렬해서 FanoutWriter의
    # 높은 메모리 사용(파티션별 파일 동시 오픈)을 피하게 한다.
    spark.sql(
        f"ALTER TABLE {_table_name()} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )

    # 미등록 event_type 등 스키마 위반 행을 원본 그대로 보존 (사람이 확인해야 하는 케이스)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_quarantine_table_name()} (
            raw_payload    STRING,
            reason         STRING,
            quarantined_at TIMESTAMP
        )
        USING iceberg
        """
    )


def _quarantine(spark, quarantine_rows: list[dict], reason: str) -> None:
    if not quarantine_rows:
        return

    rows = [
        {
            "raw_payload": _json.dumps(r, default=str, ensure_ascii=False),
            "reason": reason,
            "quarantined_at": datetime.now(timezone.utc),
        }
        for r in quarantine_rows
    ]
    df = spark.createDataFrame(rows)
    df.writeTo(_quarantine_table_name()).append()
    logger.warning("quarantine 적재: %d건 (사유: %s)", len(quarantine_rows), reason)


def _process_one_day(spark, target_date: date) -> int:
    date_str = target_date.strftime("%Y-%m-%d")
    raw_rows = fetch_events_by_date(target_date)

    ensure_bucket(config.SETTINGS.raw_bucket)
    put_json(
        config.SETTINGS.raw_bucket,
        f"raw/bikeman_event/db/occurred_date={date_str}/payload.json",
        {"occurred_date": date_str, "row_count": len(raw_rows), "rows": _json_safe_rows(raw_rows)},
    )

    if not raw_rows:
        logger.info("%s: 신규 데이터 없음", date_str)
        return 0

    actual_columns = list(raw_rows[0].keys())
    validate_and_report(actual_columns)  # 필수 컬럼 없으면 SchemaValidationError -> 상위에서 배치 중단

    valid_rows, quarantine_rows = validate_event_types(raw_rows)
    _quarantine(spark, quarantine_rows, reason="unregistered_event_type")

    if not valid_rows:
        logger.warning("%s: 전체가 quarantine 처리됨 (%d건)", date_str, len(quarantine_rows))
        return 0

    raw_df = spark.createDataFrame(valid_rows)
    select_exprs = build_select_exprs(actual_columns)
    mapped_df = raw_df.select(*select_exprs)

    bronze_df = (
        mapped_df.withColumn("occurred_date_partition", F.lit(date_str))
        .withColumn("source_file", F.lit(f"bikeman_db:{date_str}"))
        .withColumn("ingested_at", F.current_timestamp())
        .cache()
    )
    row_count = bronze_df.count()

    # 같은 날짜를 재처리(lookback)해도 그 파티션만 통째로 덮어써서 멱등성 보장
    bronze_df.writeTo(_table_name()).overwritePartitions()
    bronze_df.unpersist()

    logger.info("%s: %d행 적재 완료 (quarantine %d건)", date_str, row_count, len(quarantine_rows))
    return row_count


def run() -> None:
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session("bronze-daily-batch-bikeman-event")
    _ensure_bronze_tables(spark)

    last_processed = read_watermark(watermark_key=WATERMARK_KEY)
    start_date = last_processed + timedelta(days=1)
    end_date = date.today() - timedelta(days=1)

    max_days = os.getenv("MAX_DAYS_PER_RUN")
    if max_days:
        capped_end = start_date + timedelta(days=int(max_days) - 1)
        if capped_end < end_date:
            logger.info(
                "MAX_DAYS_PER_RUN=%s 적용 - 이번 실행은 %s ~ %s까지만 처리 (원래 끝: %s)",
                max_days, start_date, capped_end, end_date,
            )
            end_date = capped_end

    if start_date > end_date:
        logger.info("처리할 신규 날짜 없음 (워터마크=%s)", last_processed)
        return

    # 지연 도착 대응: 이미 "완료"로 확정된 날짜라도 LOOKBACK_DAYS만큼 다시 계산해서
    # 늦게 도착한 이벤트를 반영한다. SERVICE_START_DATE 이전으로는 확장하지 않음.
    reprocess_start = max(start_date - timedelta(days=LOOKBACK_DAYS), SERVICE_START_DATE)
    if reprocess_start < start_date:
        logger.info(
            "지연 도착 대응 - %s ~ %s 구간을 재계산 (기존 처리 완료 구간 포함)",
            reprocess_start, start_date - timedelta(days=1),
        )

    current = reprocess_start
    while current <= end_date:
        try:
            _process_one_day(spark, current)
            # 신규 구간(>= start_date)에 대해서만 워터마크를 전진시킨다.
            # lookback 구간(< start_date)은 재계산일 뿐 "새로 처리 완료"가 아니므로
            # 워터마크를 건드리지 않는다 (이미 그 지점까지는 워터마크가 찍혀 있음).
            if current >= start_date:
                write_watermark(current, watermark_key=WATERMARK_KEY)
        except (SchemaValidationError, BikemanDbError) as e:
            logger.error("%s 처리 실패, 배치 중단: %s", current, e)
            sys.exit(1)
        current += timedelta(days=1)


if __name__ == "__main__":
    run()