"""
Lambda Function: fetch_station_master_raw

서울시 공공자전거 대여소 정보(tbCycleStationInfo)를 수집하여 S3 raw 영역에 적재합니다.
과거 데이터 조회가 불가능한 API이므로 매일 00:10 KST에 EventBridge에 의해 트리거되어
당일 스냅샷을 안전하게 보존합니다.

착지 경로:
    s3://<RAW_BUCKET>/raw/station_master/api/snapshot_date=YYYY-MM-DD/payload.json
"""
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

KST = timezone(timedelta(hours=9))
DEFAULT_PAGE_SIZE = 1000
DEFAULT_BASE_URL = "http://openapi.seoul.go.kr:8088"
SUCCESS_CODE = "INFO-000"


def _extract_service_node(body: Dict[str, Any], service: str) -> Dict[str, Any]:
    """응답 JSON에서 서비스 노드 또는 RESULT/row를 포함한 노드를 탐색합니다."""
    if service in body:
        return body[service]
    if "RESULT" in body or "row" in body:
        return body
    for val in body.values():
        if isinstance(val, dict) and ("RESULT" in val or "row" in val):
            return val
    return body


def fetch_all_station_master(
    api_key: str,
    base_url: str = DEFAULT_BASE_URL,
    api_type: str = "json",
    service: str = "tbCycleStationInfo",
    page_size: int = DEFAULT_PAGE_SIZE,
) -> List[Dict[str, Any]]:
    """tbCycleStationInfo API를 페이징 순회하여 전체 대여소 정보를 반환합니다."""
    all_rows: List[Dict[str, Any]] = []
    start_idx = 1

    while True:
        end_idx = start_idx + page_size - 1
        url = f"{base_url.rstrip('/')}/{api_key}/{api_type}/{service}/{start_idx}/{end_idx}/"
        logger.info("Calling API: %s (idx: %d ~ %d)", service, start_idx, end_idx)

        req = urllib.request.Request(
            url,
            headers={"User-Agent": "WaitForDdaman-RawFetchLambda/1.0", "Accept": "application/json"},
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                status_code = resp.getcode()
                if status_code != 200:
                    raise RuntimeError(f"HTTP error {status_code} from {url}")
                raw_bytes = resp.read()
                data = json.loads(raw_bytes.decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"HTTPError {e.code} for URL {url}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"URLError {e.reason} for URL {url}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON response from {url}") from e

        node = _extract_service_node(data, service)
        code = node.get("RESULT", {}).get("CODE", "")
        msg = node.get("RESULT", {}).get("MESSAGE", "")

        if code and code != SUCCESS_CODE:
            raise RuntimeError(f"API Error {code}: {msg}")

        rows = node.get("row", [])
        if rows:
            all_rows.extend(rows)

        if not rows or len(rows) < page_size:
            break

        start_idx += page_size

    return all_rows


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda 핸들러 진입점."""
    event = event or {}
    snapshot_date = event.get("snapshot_date")
    if not snapshot_date:
        snapshot_date = datetime.now(KST).strftime("%Y-%m-%d")

    raw_bucket = os.environ.get("RAW_BUCKET", "waitforddaman-raw")
    api_key = os.environ.get("SEOUL_API_KEY")
    if not api_key:
        raise ValueError("SEOUL_API_KEY environment variable is required")

    base_url = os.environ.get("SEOUL_API_BASE_URL", DEFAULT_BASE_URL)

    logger.info("Starting raw fetch: service=tbCycleStationInfo, snapshot_date=%s, bucket=%s", snapshot_date, raw_bucket)
    rows = fetch_all_station_master(api_key=api_key, base_url=base_url)
    logger.info("Fetched %d rows for station_master", len(rows))

    payload = {
        "snapshot_date": snapshot_date,
        "row_count": len(rows),
        "rows": rows,
    }

    s3_key = f"raw/station_master/api/snapshot_date={snapshot_date}/payload.json"
    s3 = boto3.client("s3")
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(Bucket=raw_bucket, Key=s3_key, Body=body)
    logger.info("Successfully uploaded payload to s3://%s/%s (%d rows)", raw_bucket, s3_key, len(rows))

    return {
        "statusCode": 200,
        "snapshot_date": snapshot_date,
        "row_count": len(rows),
        "s3_key": s3_key,
    }
