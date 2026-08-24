"""선택된 Raw 관측본을 Bronze 대여이력 파티션으로 원자적으로 승격하는 잡.

selection.json에 적힌 payload만 읽어 하나의 PyArrow Table로 합치고, PyIceberg
`Table.overwrite(..., overwrite_filter=OR(EqualTo(...)))` 한 번으로 여러 날짜 파티션을
동시에 교체한다(#194). Iceberg snapshot commit이 날짜 묶음의 원자 경계이므로, 일부
날짜만 먼저 반영되는 중간 상태가 생기지 않는다.

Spark/JVM을 완전히 제거했다(#194) - 이 잡이 쓰는 유일한 Bronze 테이블은 initial_load_
rental_history.py(Spark, 파일 백필 전용)가 먼저 만들어 둔다고 가정하며, 테이블이 없으면
자동 생성하지 않고 초기 적재 선행이 필요하다는 PromotionError를 낸다.

Bronze commit이 끝난 뒤에만 promotion.json(status=COMPLETE)을 쓴다. 이 마커가 없으면
하류(확정 워터마크, Asset)는 승격이 끝났다고 보지 않는다.
"""
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.exceptions import NoSuchTableError

import config
from common.api_client import strip_pagination_meta
from common.iceberg_catalog import build_iceberg_catalog
from common.iceberg_io import build_partition_filter, overwrite_partitions
from common.s3_utils import ensure_bucket, get_json, put_json
from jobs.collect_rental_history_raw import parse_collection_cutoff, snapshot_keys
from jobs.rental_history_snapshot_policy import (
    DATASET,
    build_promotion_id,
    promotion_key,
    selection_key,
)
from schema.rental_history_schema import COLUMN_ALIAS_MAP, validate_and_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Bronze 테이블 컬럼 순서 - 기존 bronze.rental_history DDL(initial_load_rental_history.py)과
# 동일해야 한다. 값은 전부 문자열로 유지한다(Bronze 원칙 - 타입 캐스팅은 Silver 책임).
BRONZE_BUSINESS_COLUMNS = [
    "bike_id",
    "rent_dt",
    "rent_station_no",
    "rent_station_name",
    "rent_hold",
    "return_dt",
    "return_station_no",
    "return_station_name",
    "return_hold",
    "use_min",
    "use_distance_m",
    "user_class_cd",
    "sex_cd",
    "birth_year",
    "rent_station_id",
    "return_station_id",
    "bike_se_cd",
]

ARROW_SCHEMA = pa.schema(
    [pa.field(c, pa.string()) for c in BRONZE_BUSINESS_COLUMNS]
    + [
        pa.field("rent_date_partition", pa.string()),
        pa.field("source_file", pa.string()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)

PARTITION_COLUMN = "rent_date_partition"

# 표준 컬럼 -> 그 표준 컬럼에 매핑되는 모든 소스 별칭 목록 (역 인덱스).
# schema.rental_history_schema.build_select_exprs와 동일한 "컬럼 이름 존재 여부" 기준으로
# 소스 별칭을 고른다 (Spark Column 표현식이 아니라 PyArrow로 같은 의미를 구현).
_STANDARD_TO_ALIASES: dict[str, list[str]] = {}
for _src, _dst in COLUMN_ALIAS_MAP.items():
    _STANDARD_TO_ALIASES.setdefault(_dst, []).append(_src)


class PromotionError(Exception):
    """selection 계약이 깨졌거나 Bronze 승격을 안전하게 진행할 수 없는 상태."""


def bronze_table_name() -> str:
    return "bronze.rental_history"


def load_bronze_table(catalog=None):
    """
    Bronze 테이블을 로드한다. 테이블이 없으면 자동 생성하지 않고 initial_load_rental_history
    선행이 필요하다는 PromotionError를 낸다 - 이 잡은 Spark를 쓰지 않으므로 DDL(CREATE TABLE)을
    실행할 수단이 없고, 신규 환경에서 승격만 먼저 돌리는 경로를 조용히 허용하면 안 된다(#194).
    """
    cat = catalog or build_iceberg_catalog()
    try:
        return cat.load_table(bronze_table_name())
    except NoSuchTableError as exc:
        raise PromotionError(
            f"{bronze_table_name()} 테이블이 없음 - initial_load_rental_history를 먼저 "
            "실행해 Bronze 테이블을 만들어야 함 (이 잡은 테이블을 자동 생성하지 않음)"
        ) from exc


def build_bronze_arrow_table(raw_rows: list[dict], rent_date_partition: str, source_file: str) -> pa.Table:
    """API 원본 행을 Bronze 표준 컬럼 + lineage 컬럼으로 매핑한 PyArrow Table을 만든다.

    START_INDEX/END_INDEX/RNUM은 페이징 메타데이터일 뿐 실제 데이터 컬럼이 아니므로
    스키마 검증 전에 제거한다 (안 지우면 매 호출마다 "알 수 없는 컬럼" 경고가 계속 발생함).
    """
    rows = [strip_pagination_meta(row) for row in raw_rows]
    actual_columns = set(rows[0].keys())
    validate_and_report(list(actual_columns))  # 필수 컬럼 없으면 SchemaValidationError

    chosen_src = {
        dst: next((src for src in _STANDARD_TO_ALIASES[dst] if src in actual_columns), None)
        for dst in BRONZE_BUSINESS_COLUMNS
    }

    ingested_at = datetime.now(timezone.utc)
    cols: dict[str, list] = {dst: [] for dst in BRONZE_BUSINESS_COLUMNS}
    for row in rows:
        for dst in BRONZE_BUSINESS_COLUMNS:
            src = chosen_src[dst]
            value = row.get(src) if src is not None else None
            cols[dst].append(str(value) if value is not None else None)

    row_count = len(rows)
    cols["rent_date_partition"] = [rent_date_partition] * row_count
    cols["source_file"] = [source_file] * row_count
    cols["ingested_at"] = [ingested_at] * row_count

    return pa.table(cols, schema=ARROW_SCHEMA)


def validate_selection_document(document, promotion_id: str) -> None:
    """PyIceberg를 켜기 전에 selection 계약을 다시 검증한다."""
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
    """선택된 payload를 전부 먼저 읽는다. 하나라도 누락/변조면 Iceberg를 건드리지 않고 실패한다."""
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


def _committed_partition_counts(table, partitions: list[str]) -> dict[str, int]:
    """커밋된 snapshot을 다시 읽어 파티션별 실제 행 수를 센다 (부분 반영 여부 검증용)."""
    fresh_table = table.refresh()
    arrow = fresh_table.scan(
        row_filter=build_partition_filter(PARTITION_COLUMN, partitions),
        selected_fields=(PARTITION_COLUMN,),
    ).to_arrow()

    col = arrow.column(PARTITION_COLUMN)
    return {p: (pc.sum(pc.equal(col, p)).as_py() or 0) for p in partitions}


def promote(payloads: list[dict]) -> dict[str, int]:
    """선택 bundle 전체를 한 PyArrow Table로 만들어 파티션을 단일 snapshot commit으로 교체한다."""
    started_at = time.monotonic()
    table = load_bronze_table()

    arrow_tables = [
        build_bronze_arrow_table(
            item["rows"], item["rent_date_partition"], item["source_file"]
        )
        for item in payloads
    ]
    bronze_table = pa.concat_tables(arrow_tables)
    partitions = [item["rent_date_partition"] for item in payloads]

    overwrite_partitions(table, bronze_table, PARTITION_COLUMN, partitions)
    counts = _committed_partition_counts(table, partitions)

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
        partition_counts = promote(payloads)
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
