"""
Iceberg S3 파티션 메타데이터 조회 모듈 (Spark 세션 불필요)

모든 테이블이 날짜 identity 파티션(예: snapshot_date=YYYY-MM-DD, rent_date_partition=YYYY-MM-DD)
구조를 가지므로, 실제 데이터 파일은 `{warehouse_path}/{namespace}/{table}/data/{partition_col}={value}/`
Hive 스타일 S3 디렉터리에 저장됩니다.

이 모듈은 Spark 세션을 기동하지 않고 boto3 S3 API(list_objects_v2)를 직접 호출하여
파티션 디렉터리 목록을 가볍고 빠르게 나열/조회합니다. 파이프라인에 파티션을 삭제하는
잡이 없으므로 S3 디렉터리 존재 여부가 곧 파티션 도착 여부와 일치합니다.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional
from urllib.parse import urlparse

from botocore.exceptions import ClientError

import config
from common.s3_utils import get_s3_client

logger = logging.getLogger(__name__)


def get_table_data_prefix(table_identifier: str) -> tuple[str, str]:
    """
    Iceberg 테이블의 S3 (bucket, data_prefix)를 계산합니다.
    예: 'silver.station_master' -> ('ttareungyi-warehouse', 'warehouse/silver/station_master/data/')
    """
    parts = table_identifier.split(".")
    if len(parts) == 1:
        namespace, table = "default", parts[0]
    elif len(parts) == 2:
        namespace, table = parts[0], parts[1]
    elif len(parts) == 3:
        # catalog.namespace.table
        namespace, table = parts[1], parts[2]
    else:
        raise ValueError(f"잘못된 테이블 식별자 형식입니다: {table_identifier}")

    parsed = urlparse(config.SETTINGS.iceberg_warehouse_path)
    root = parsed.path.lstrip("/")
    prefix = f"{root}/{namespace}/{table}/data/" if root else f"{namespace}/{table}/data/"
    bucket = parsed.netloc or config.SETTINGS.warehouse_bucket
    return bucket, prefix


def list_partitions(table_identifier: str, partition_col: str = "") -> list[str]:
    """
    해당 테이블의 모든 파티션 값 목록을 오름차순 정렬하여 반환합니다.
    partition_col이 지정된 경우 해당 컬럼의 파티션만 필터링합니다.
    """
    bucket, prefix = get_table_data_prefix(table_identifier)
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    partitions: list[str] = []
    prefix_filter = f"{partition_col}=" if partition_col else ""

    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
        for page in pages:
            for common_prefix in page.get("CommonPrefixes", []):
                dir_name = common_prefix["Prefix"][len(prefix):].rstrip("/")
                if prefix_filter and not dir_name.startswith(prefix_filter):
                    continue
                if "=" in dir_name:
                    val = dir_name.split("=", 1)[1]
                    partitions.append(val)
                elif not prefix_filter:
                    partitions.append(dir_name)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchBucket", "NoSuchKey", "404"):
            logger.debug("버킷 또는 프리픽스가 아직 존재하지 않음: s3://%s/%s", bucket, prefix)
            return []
        raise

    return sorted(list(set(partitions)))


def max_partition(table_identifier: str, partition_col: str = "") -> Optional[str]:
    """해당 테이블의 가장 최신(최대) 파티션 값을 반환합니다. 데이터가 없으면 None."""
    parts = list_partitions(table_identifier, partition_col)
    return parts[-1] if parts else None


def min_partition(table_identifier: str, partition_col: str = "") -> Optional[str]:
    """해당 테이블의 가장 과거(최소) 파티션 값을 반환합니다. 데이터가 없으면 None."""
    parts = list_partitions(table_identifier, partition_col)
    return parts[0] if parts else None


def partition_exists(table_identifier: str, value: str, partition_col: str = "") -> bool:
    """해당 테이블에 특정 파티션 값이 존재하는지 확인합니다."""
    bucket, prefix = get_table_data_prefix(table_identifier)
    s3 = get_s3_client()
    target_prefix = f"{prefix}{partition_col}={value}/" if partition_col else f"{prefix}{value}/"

    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=target_prefix, MaxKeys=1)
        return "Contents" in resp and len(resp["Contents"]) > 0
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchBucket", "NoSuchKey", "404"):
            return False
        raise
