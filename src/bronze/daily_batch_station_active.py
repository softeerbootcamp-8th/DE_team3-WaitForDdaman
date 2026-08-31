"""
Bronze 일 배치 - 실시간 대여정보 (OA-15493 / bikeList)

이 원천을 수집하는 목적은 "지금 실제로 운영 중인 대여소가 어디인지" 판별하는 것이다
(2026-08-16 사용자 확인). 거치대 수/주차된 자전거 수/거치율 자체를 활용할 계획은 없다.

이 원천은 파일 백필이 없다. API 일일 전체 스냅샷 하나로만 적재한다.
station_master(tbCycleStationInfo)와 같은 "날짜 파라미터 없는 전체 스냅샷" 구조라
워터마크가 필요 없고, 파티션 키도 이벤트 발생일이 아니라 snapshot_date다.

⚠️ 과거를 소급 조회할 수 없다. 오늘 스냅샷을 놓치면 오늘 어떤 대여소가 실제로
   운영 중이었는지 영구히 알 수 없게 된다. 그래서 Lambda(fetch_station_active_raw)가
   매일 00:10 KST에 S3 raw 영역에 먼저 착지시키고, 이 잡은 S3 raw payload를 읽어
   Bronze Iceberg 테이블에 파티션 단위로 적재한다.

Spark를 완전히 제거하고 PyArrow + PyIceberg(SqlCatalog)로 경량화/고속화되었다 (Issue #142).

사용법:
    python -m jobs.daily_batch_station_active
    SNAPSHOT_DATE=2026-08-14 python -m jobs.daily_batch_station_active   # 재처리 전용
"""
import logging
import os
import sys
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import pyarrow as pa

import config
from common.api_client import (
    SeoulApiError,
    SeoulApiTransientError,
    fetch_station_active,
    strip_pagination_meta,
)
from common.iceberg_io import overwrite_partition
from common.s3_utils import ensure_bucket, get_json, put_json
from schemas.station_active_schema import (
    SchemaValidationError,
    collect_response_fields,
    validate_and_report,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ARROW_SCHEMA = pa.schema([
    pa.field("station_id", pa.string()),
    pa.field("station_name", pa.string()),
    pa.field("rack_tot_cnt", pa.string()),
    pa.field("parking_bike_tot_cnt", pa.string()),
    pa.field("shared", pa.string()),
    pa.field("latitude", pa.string()),
    pa.field("longitude", pa.string()),
    pa.field("snapshot_date", pa.string()),
    pa.field("source_file", pa.string()),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
])


def _table_name() -> str:
    return "bronze.station_active"


def _build_arrow_table(rows: List[Dict[str, Any]], snapshot_date: str) -> pa.Table:
    ingested_at = datetime.now(timezone.utc)
    source_file_val = f"api:{snapshot_date}"

    cols: Dict[str, list] = {
        "station_id": [],
        "station_name": [],
        "rack_tot_cnt": [],
        "parking_bike_tot_cnt": [],
        "shared": [],
        "latitude": [],
        "longitude": [],
        "snapshot_date": [],
        "source_file": [],
        "ingested_at": [],
    }

    for r in rows:
        cols["station_id"].append(str(r.get("stationId") or r.get("station_id") or "") or None)
        cols["station_name"].append(str(r.get("stationName") or r.get("station_name") or "") or None)
        cols["rack_tot_cnt"].append(str(r.get("rackTotCnt") or r.get("rack_tot_cnt") or "") or None)
        cols["parking_bike_tot_cnt"].append(str(r.get("parkingBikeTotCnt") or r.get("parking_bike_tot_cnt") or "") or None)
        cols["shared"].append(str(r.get("shared") or "") or None)
        cols["latitude"].append(str(r.get("stationLatitude") or r.get("latitude") or "") or None)
        cols["longitude"].append(str(r.get("stationLongitude") or r.get("longitude") or "") or None)
        cols["snapshot_date"].append(snapshot_date)
        cols["source_file"].append(source_file_val)
        cols["ingested_at"].append(ingested_at)

    return pa.table(cols, schema=ARROW_SCHEMA)


def _process_snapshot(snapshot_date: str) -> int:
    settings = config.SETTINGS
    s3_key = f"raw/station_active/api/snapshot_date={snapshot_date}/payload.json"

    if settings.raw_source == "s3":
        logger.info("%s: S3 raw payload 읽기 시도 (s3://%s/%s)", snapshot_date, settings.raw_bucket, s3_key)
        payload = get_json(settings.raw_bucket, s3_key)
        if payload is None:
            raise FileNotFoundError(
                f"S3 raw payload가 존재하지 않습니다: s3://{settings.raw_bucket}/{s3_key}. "
                f"fetch_station_active_raw Lambda가 실패했거나 아직 실행되지 않았습니다."
            )
        raw_rows = payload.get("rows", [])
    else:
        logger.info("%s: 공공 API 직접 호출 (RAW_SOURCE=api)", snapshot_date)
        raw_rows = list(fetch_station_active())

        ensure_bucket(settings.raw_bucket)
        put_json(
            settings.raw_bucket,
            s3_key,
            {"snapshot_date": snapshot_date, "row_count": len(raw_rows), "rows": raw_rows},
        )

    if not raw_rows:
        logger.warning("%s: raw 데이터가 비어있음 (0건) - 적재 생략", snapshot_date)
        return 0

    rows = [strip_pagination_meta(r) for r in raw_rows]
    actual_columns = collect_response_fields(rows)
    logger.info("필드 목록: %s", actual_columns)
    validate_and_report(actual_columns)

    arrow_table = _build_arrow_table(rows, snapshot_date)
    row_count = len(arrow_table)

    overwrite_partition(_table_name(), arrow_table, "snapshot_date", snapshot_date)
    logger.info("%s: %d행 PyIceberg 적재 완료", snapshot_date, row_count)
    return row_count


def run() -> None:
    settings = config.SETTINGS
    if settings.raw_source == "api" and settings.seoul_api_key in ("", "sample"):
        logger.error(
            "RAW_SOURCE=api 모드이지만 SEOUL_API_KEY가 비어있거나 'sample'(데모 키)입니다. "
            "data.seoul.go.kr에서 발급받은 실제 인증키로 ingestion/.env를 채우세요."
        )
        sys.exit(1)

    ensure_bucket(settings.raw_bucket)
    ensure_bucket(settings.warehouse_bucket)

    snapshot_date = os.getenv("SNAPSHOT_DATE") or date.today().strftime("%Y-%m-%d")

    try:
        _process_snapshot(snapshot_date)
    except (SchemaValidationError, SeoulApiError, SeoulApiTransientError, FileNotFoundError) as e:
        logger.error("%s 스냅샷 처리 실패: %s", snapshot_date, e)
        sys.exit(1)


if __name__ == "__main__":
    run()
