"""선택된 Raw 관측본을 Bronze 대여이력 파티션으로 원자적으로 승격하는 잡.

이 파이프라인에서 Spark/Iceberg를 쓰는 유일한 단계다. selection.json에 적힌 payload만
읽어 한 DataFrame으로 합치고 `overwritePartitions()` 한 번으로 날짜 파티션을 교체한다.
Iceberg snapshot commit이 날짜 묶음의 원자 경계이므로, 일부 날짜만 먼저 반영되는 중간
상태가 생기지 않는다.

Bronze commit이 끝난 뒤에만 promotion.json(status=COMPLETE)을 쓴다. 이 마커가 없으면
하류(확정 워터마크, Asset)는 승격이 끝났다고 보지 않는다.
"""
import functools
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

import config
from common.api_client import strip_pagination_meta
from common.s3_utils import ensure_bucket, get_json, put_json
from common.spark_session import build_spark_session
from jobs.collect_rental_history_raw import parse_collection_cutoff, snapshot_keys
from jobs.rental_history_snapshot_policy import (
    DATASET,
    build_promotion_id,
    promotion_key,
    selection_key,
)
from schema.rental_history_schema import build_select_exprs, validate_and_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class PromotionError(Exception):
    """selection 계약이 깨졌거나 Bronze 승격을 안전하게 진행할 수 없는 상태."""


def bronze_table_name() -> str:
    return f"{config.SETTINGS.iceberg_catalog_name}.bronze.rental_history"


def ensure_bronze_table(spark) -> None:
    """
    initial_load_rental_history.py와 동일한 DDL - 초기 적재 없이 승격만 단독으로
    먼저 돌리는 경우(신규 환경, 최근 N일치만 받고 싶은 경우 등)에도 테이블이 없어서
    writeTo().overwritePartitions()가 TABLE_OR_VIEW_NOT_FOUND로 실패하지 않게 한다.
    이미 초기 적재로 테이블이 있어도 CREATE TABLE IF NOT EXISTS라 안전하다(no-op).
    """
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {config.SETTINGS.iceberg_catalog_name}.bronze"
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {bronze_table_name()} (
            bike_id STRING,
            rent_dt STRING,
            rent_station_no STRING,
            rent_station_name STRING,
            rent_hold STRING,
            return_dt STRING,
            return_station_no STRING,
            return_station_name STRING,
            return_hold STRING,
            use_min STRING,
            use_distance_m STRING,
            user_class_cd STRING,
            sex_cd STRING,
            birth_year STRING,
            rent_station_id STRING,
            return_station_id STRING,
            bike_se_cd STRING,
            rent_date_partition STRING,
            source_file STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (rent_date_partition)
        """
    )
    spark.sql(
        f"ALTER TABLE {bronze_table_name()} SET TBLPROPERTIES ('write.distribution-mode'='hash')"
    )


def build_bronze_dataframe(spark, raw_rows: list[dict], rent_date_partition: str, source_file: str):
    """API 원본 행을 Bronze 표준 컬럼 + lineage 컬럼으로 매핑한다.

    START_INDEX/END_INDEX/RNUM은 페이징 메타데이터일 뿐 실제 데이터 컬럼이 아니므로
    스키마 검증 전에 제거한다 (안 지우면 매 호출마다 "알 수 없는 컬럼" 경고가 계속 발생함).
    """
    rows = [strip_pagination_meta(row) for row in raw_rows]
    actual_columns = list(rows[0].keys())
    validate_and_report(actual_columns)  # 필수 컬럼 없으면 SchemaValidationError

    raw_df = spark.createDataFrame(rows)
    mapped_df = raw_df.select(*build_select_exprs(actual_columns))
    return (
        mapped_df.withColumn("rent_date_partition", F.lit(rent_date_partition))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingested_at", F.current_timestamp())
    )


def validate_selection_document(document, promotion_id: str) -> None:
    """Spark를 켜기 전에 selection 계약을 다시 검증한다."""
    if not isinstance(document, dict):
        raise PromotionError("selection.json을 읽을 수 없음")
    if document.get("dataset") != DATASET:
        raise PromotionError(f"dataset 불일치: {document.get('dataset')!r}")
    if document.get("promotion_id") != promotion_id:
        raise PromotionError(
            f"promotion_id 불일치: {document.get('promotion_id')!r} != {promotion_id!r}"
        )

    selected = document.get("selected_snapshots")
    if not selected:
        raise PromotionError("selected_snapshots가 비어 있어 승격할 파티션이 없음")

    for entry in selected:
        target_date = datetime.fromisoformat(entry["target_date"]).date()
        observed_at = datetime.fromisoformat(entry["observed_at"])
        expected_payload_key, _ = snapshot_keys(
            target_date, observed_at, entry["snapshot_type"]
        )
        if entry["payload_key"] != expected_payload_key:
            raise PromotionError(
                f"payload_key가 선택 메타데이터와 불일치: {entry['payload_key']!r}"
            )
        if not isinstance(entry.get("row_count"), int) or entry["row_count"] <= 0:
            raise PromotionError(
                f"{entry['target_date']} row_count가 유효하지 않음: {entry.get('row_count')!r}"
            )


def plan_promotion(document: dict) -> list[dict]:
    """날짜별 파티션 값과 lineage용 source_file(선택 payload key 전체)을 확정한다."""
    return [
        {
            "rent_date_partition": entry["target_date"],
            "source_file": entry["payload_key"],
            "row_count": entry["row_count"],
            "snapshot_type": entry["snapshot_type"],
            "observed_at": entry["observed_at"],
        }
        for entry in sorted(
            document["selected_snapshots"], key=lambda e: e["target_date"]
        )
    ]


def load_payloads(bucket: str, plan: list[dict]) -> list[dict]:
    """선택된 payload를 전부 먼저 읽는다. 하나라도 누락/변조면 Spark를 켜지 않고 실패한다."""
    loaded = []
    for item in plan:
        rows = get_json(bucket, item["source_file"])
        if rows is None:
            raise PromotionError(f"선택된 payload 객체가 없음: {item['source_file']}")
        if not isinstance(rows, list):
            raise PromotionError(f"payload 형식이 배열이 아님: {item['source_file']}")
        if len(rows) != item["row_count"]:
            raise PromotionError(
                f"{item['rent_date_partition']} payload row count 불일치: "
                f"{len(rows)} != {item['row_count']}"
            )
        loaded.append({**item, "rows": rows})
    return loaded


def promote(spark, payloads: list[dict]) -> dict[str, int]:
    """선택 bundle 전체를 한 DataFrame으로 만들어 파티션을 한 번에 교체한다."""
    started_at = time.monotonic()
    frames = [
        build_bronze_dataframe(
            spark, item["rows"], item["rent_date_partition"], item["source_file"]
        )
        for item in payloads
    ]
    bronze_df = functools.reduce(lambda left, right: left.unionByName(right), frames)
    bronze_df.writeTo(bronze_table_name()).overwritePartitions()

    partitions = [item["rent_date_partition"] for item in payloads]
    committed = (
        spark.table(bronze_table_name())
        .where(F.col("rent_date_partition").isin(partitions))
        .groupBy("rent_date_partition")
        .count()
        .collect()
    )
    counts = {row["rent_date_partition"]: row["count"] for row in committed}
    logger.info(
        "Bronze 파티션 교체 완료: partitions=%s input=%s committed=%s elapsed_seconds=%.3f",
        partitions,
        {item["rent_date_partition"]: item["row_count"] for item in payloads},
        counts,
        time.monotonic() - started_at,
    )
    return counts


def build_promotion_document(document: dict, partition_counts: dict[str, int]) -> dict:
    """Iceberg commit이 끝난 뒤에만 쓰는 Bronze commit marker를 만든다."""
    selected = sorted(document["selected_snapshots"], key=lambda e: e["target_date"])
    for entry in selected:
        target_date = entry["target_date"]
        actual = partition_counts.get(target_date)
        if actual != entry["row_count"]:
            raise PromotionError(
                f"{target_date} partition row count 불일치: {actual!r} != {entry['row_count']!r}"
            )

    required_confirmed = list(document.get("required_confirmed_dates") or [])
    promotion = dict(document)
    promotion.update(
        {
            "status": "COMPLETE",
            "promoted_partitions": [entry["target_date"] for entry in selected],
            "bronze_row_count_by_partition": {
                entry["target_date"]: partition_counts[entry["target_date"]]
                for entry in selected
            },
            # 당일 partial 파티션은 확정 후보가 아니므로 확정 대상 날짜의 마지막 날만 본다.
            "confirmed_through_candidate": required_confirmed[-1]
            if required_confirmed
            else None,
            "promotion_reasons": {
                entry["target_date"]: entry["fallback_reason"]
                for entry in selected
                if entry.get("fallback_reason")
            },
            "promoted_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return promotion


def run() -> dict:
    """selection.json을 읽어 Bronze로 승격하고 COMPLETE promotion marker를 남긴다."""
    cutoff_value = os.getenv("COLLECTION_CUTOFF_AT")
    if not cutoff_value:
        raise PromotionError("COLLECTION_CUTOFF_AT is required")

    cutoff = parse_collection_cutoff(cutoff_value)
    run_date = cutoff.date()
    promotion_id = build_promotion_id(cutoff)

    bucket = config.SETTINGS.raw_bucket
    ensure_bucket(bucket)
    document = get_json(bucket, selection_key(run_date, promotion_id))
    if not isinstance(document, dict):
        raise PromotionError(
            f"selection.json 없음: {selection_key(run_date, promotion_id)}"
        )

    if not document.get("selected_snapshots"):
        # 워터마크가 이미 최신이라 승격할 날짜가 없는 정상 no-op. 기존 일 배치와 같은
        # 의미이므로 실패시키지 않고 빈 commit marker만 남긴다.
        logger.info("승격할 파티션 없음 (selection이 비어 있음)")
        promotion = build_promotion_document(document, {})
    else:
        validate_selection_document(document, promotion_id)
        plan = plan_promotion(document)
        payloads = load_payloads(bucket, plan)

        ensure_bucket(config.SETTINGS.warehouse_bucket)
        spark = build_spark_session("bronze-promote-rental-history")
        ensure_bronze_table(spark)
        partition_counts = promote(spark, payloads)
        promotion = build_promotion_document(document, partition_counts)

    put_json(bucket, promotion_key(run_date, promotion_id), promotion)
    logger.info(
        "promotion marker 기록 완료: mode=%s partitions=%s key=%s",
        promotion["mode"],
        promotion["promoted_partitions"],
        promotion_key(run_date, promotion_id),
    )
    print(
        json.dumps(
            {
                "promotion_id": promotion_id,
                "mode": promotion["mode"],
                "status": promotion["status"],
                "promoted_partitions": promotion["promoted_partitions"],
            },
            ensure_ascii=False,
        )
    )
    return promotion


if __name__ == "__main__":
    try:
        run()
    except PromotionError as exc:
        logger.error("Bronze 승격 실패: %s", exc)
        sys.exit(1)
