"""
Silver 변환 잡 - 따맨/bikeman 행동 이력 (bronze.bikeman_event -> silver.bike_man_action)

### 파티션 설계
bike_id를 파티션 키로 쓰지 않는다 - bike_id는 카디널리티가 높고(계속 증가) 계속
늘어나는 값이라 파티션으로 쓰면 파일이 지나치게 잘게 쪼개진다(small file problem).
다운스트림(위험도 스코어링 배치)도 "최근 N일 이벤트" 조회가 압도적으로 많아 날짜
기준이 맞다. Iceberg의 hidden partitioning(days(occurred_at))을 사용해서 별도
파티션 컬럼 없이 occurred_at 하나만으로 파티션 프루닝이 되게 한다.

### 3일 lookback을 Silver에서도 유지하는 이유
Bronze(daily_batch_bikeman_event.py)가 자체적으로 3일 lookback을 돌며 늦게
도착한 이벤트를 반영해서 해당 occurred_date_partition을 다시 덮어쓴다. Silver가
"어제"만 보면 Bronze가 나중에 갱신한 정정분을 놓친다. 그래서 Silver도 동일한
lookback 윈도우로 재계산한다 (Bronze의 정정이 Silver까지 전파되도록).

### 이상치 처리 (schema/bike_man_action_schema.py)
1차: null/enum/시간 정합성(occurred_at vs received_at, 서비스 시작일, 미래 시각)
     검사로 valid/quarantine 분리 + (bike_id, event_type, occurred_at) 중복 제거.
2차: PyDeequ로 valid 데이터셋 전체의 완전성/enum 준수율을 측정해 dq_check_result에
     기록. 이건 "행을 거르는" 게 아니라 "이 배치를 신뢰해도 되는가"를 판단하는
     최종 게이트 - 실패하면 Silver에 쓰지 않고 배치를 중단한다(안전하게 실패).

사용법:
    python -m jobs.silver_bike_man_action
    MAX_DAYS_PER_RUN=1 python -m jobs.silver_bike_man_action
"""
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

import config
from common.dq_utils import DEEQU_EXCLUDE, DEEQU_MAVEN_PACKAGE, ensure_dq_result_table, verify_bike_man_action
from common.s3_utils import ensure_bucket
from common.spark_session import build_spark_session, stop_spark_session_with_deequ
from common.watermark import read_watermark, write_watermark
from config.watermark_keys import SILVER_BIKE_MAN_ACTION
from schema.bikeman_action_schema import FINAL_COLUMNS, SERVICE_START_DATE, classify_rows, dedup_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Bronze와 워터마크 키가 겹치면 안 되므로 Silver 전용 키를 쓴다
WATERMARK_KEY = SILVER_BIKE_MAN_ACTION

# Bronze의 3일 lookback을 그대로 따라간다 (Bronze의 정정이 Silver까지 전파되도록)
LOOKBACK_DAYS = 3


def _bronze_table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.bikeman_event"


def _silver_table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.bike_man_action"


def _quarantine_table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.silver.bike_man_action_quarantine"


def _ensure_silver_tables(spark) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.silver")

    # hidden partitioning - occurred_at 자체는 그냥 TIMESTAMP 컬럼이고,
    # 파티션 값(날짜)은 Iceberg가 days(occurred_at)로 자동 계산한다.
    # 별도의 STRING 파티션 컬럼을 DataFrame에 만들 필요가 없다.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_silver_table_name()} (
            bike_id     STRING,
            event_type  STRING,
            occurred_at TIMESTAMP,
            station_id  STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(occurred_at))
        """
    )
    spark.sql(
        f"ALTER TABLE {_silver_table_name()} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )

    # bike_id로 자주 필터링할 걸 대비해 파티션 대신 정렬 순서로 지원
    # (파티션은 카디널리티 낮은 occurred_at, 조회는 sort order로 보완)
    spark.sql(f"ALTER TABLE {_silver_table_name()} WRITE ORDERED BY bike_id")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_quarantine_table_name()} (
            bike_id           STRING,
            event_type        STRING,
            occurred_at       TIMESTAMP,
            received_at       TIMESTAMP,
            quarantine_reason STRING,
            quarantined_at    TIMESTAMP
        )
        USING iceberg
        """
    )


def _read_bronze_day(spark, target_date: date) -> list[dict]:
    date_str = target_date.strftime("%Y-%m-%d")
    rows = (
        spark.table(_bronze_table_name())
        .filter(F.col("occurred_date_partition") == date_str)  # 파티션 프루닝
        .select("bike_id", "event_type", "occurred_at", "station_id", "received_at")
        .collect()
    )
    return [r.asDict() for r in rows]


def _write_quarantine(spark, quarantine_rows: list[dict]) -> None:
    if not quarantine_rows:
        return
    now = datetime.now(timezone.utc)
    rows = [{**r, "quarantined_at": now} for r in quarantine_rows]
    df = spark.createDataFrame(rows)
    df.writeTo(_quarantine_table_name()).append()
    logger.warning("Silver quarantine 적재: %d건", len(quarantine_rows))


def _process_one_day(spark, target_date: date) -> tuple[int, bool]:
    """반환: (Silver에 적재된 행 수, PyDeequ 검증 통과 여부)"""
    date_str = target_date.strftime("%Y-%m-%d")
    bronze_rows = _read_bronze_day(spark, target_date)

    if not bronze_rows:
        logger.info("%s: Bronze에 신규 데이터 없음", date_str)
        return 0, True

    # bronze의 occurred_at은 Spark TIMESTAMP -> naive datetime으로 넘어오므로,
    # classify_rows의 미래시각 비교(occurred_at > now)가 tz-aware/naive 비교 TypeError를
    # 내지 않도록 now도 naive UTC로 맞춘다.
    now = datetime.utcnow()
    valid_rows, quarantine_rows = classify_rows(bronze_rows, now)
    valid_rows = dedup_rows(valid_rows)

    _write_quarantine(spark, quarantine_rows)

    if not valid_rows:
        logger.warning("%s: 전체가 quarantine 처리됨 (%d건)", date_str, len(quarantine_rows))
        return 0, True

    # PyDeequ 검증용으로는 received_at까지 포함한 상태로 먼저 DataFrame을 만들고,
    # 최종 Silver select에서 FINAL_COLUMNS로 좁힌다.
    valid_df = spark.createDataFrame(valid_rows).withColumn("ingested_at", F.current_timestamp())

    passed = verify_bike_man_action(spark, valid_df, dataset="bike_man_action", occurred_date=date_str)
    if not passed:
        # 안전하게 실패: 이 배치는 Silver에 쓰지 않는다. Bronze는 이미 안전하게
        # 보존돼 있으므로 데이터 손실은 없고, 원인 파악 후 재실행하면 된다.
        logger.error("%s: PyDeequ 검증 실패로 Silver 적재 중단", date_str)
        return 0, False

    silver_df = valid_df.select(*FINAL_COLUMNS)
    row_count = silver_df.count()
    silver_df.writeTo(_silver_table_name()).overwritePartitions()

    logger.info(
        "%s: Silver %d행 적재 완료 (quarantine %d건)", date_str, row_count, len(quarantine_rows)
    )
    return row_count, True


def run() -> None:
    # 다른 잡들과 동일한 이유 - LocalStack은 버킷을 자동으로 만들어주지 않아서,
    # 이 호출이 없으면 warehouse_bucket 자체가 없어서 CREATE DATABASE 단계에서
    # NoSuchBucket으로 실패한다 (실측으로 확인됨).
    ensure_bucket(config.SETTINGS.raw_bucket)
    ensure_bucket(config.SETTINGS.warehouse_bucket)

    spark = build_spark_session(
        "silver-bike-man-action",
        extra_packages=[DEEQU_MAVEN_PACKAGE],
        extra_excludes=[DEEQU_EXCLUDE],
    )
    try:
        _ensure_silver_tables(spark)
        ensure_dq_result_table(spark)

        last_processed = read_watermark(watermark_key=WATERMARK_KEY)
        start_date = last_processed + timedelta(days=1)
        end_date = date.today() - timedelta(days=1)

        max_days = os.getenv("MAX_DAYS_PER_RUN")
        if max_days:
            capped_end = start_date + timedelta(days=int(max_days) - 1)
            if capped_end < end_date:
                end_date = capped_end

        if start_date > end_date:
            logger.info("처리할 신규 날짜 없음 (워터마크=%s)", last_processed)
            return

        reprocess_start = max(start_date - timedelta(days=LOOKBACK_DAYS), SERVICE_START_DATE)

        current = reprocess_start
        while current <= end_date:
            try:
                _, passed = _process_one_day(spark, current)
                if not passed:
                    logger.error("%s 처리 실패(PyDeequ), 배치 중단: 워터마크 유지", current)
                    sys.exit(1)
                if current >= start_date:
                    write_watermark(current, watermark_key=WATERMARK_KEY)
            except Exception as e:
                logger.error("%s 처리 중 예외, 배치 중단: %s", current, e)
                sys.exit(1)
            current += timedelta(days=1)
    finally:
        # verify_bike_man_action()가 PyDeequ VerificationSuite를 돌리면서 연 py4j
        # 콜백 서버(non-daemon 스레드)를 안 내리면 run()이 정상 반환돼도 프로세스가
        # 안 끝난다 - transform_silver_rental_history.py에서 실측된 것과 동일한 문제
        # (common/spark_session.py의 stop_spark_session_with_deequ 참고).
        stop_spark_session_with_deequ(spark)


if __name__ == "__main__":
    run()