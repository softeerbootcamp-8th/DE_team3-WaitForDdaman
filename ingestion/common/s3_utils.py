"""
S3 (LocalStack / AWS 공통) 유틸리티

boto3는 endpoint_url만 다르면 LocalStack과 실제 AWS S3를 동일한 코드로 다룰 수 있다.
로컬에서는 access key를 더미값으로 명시하고, AWS에서는 IAM Role을 쓰도록
자격증명을 아예 넘기지 않는다(boto3가 기본 자격증명 체인을 사용).
"""
import json
import logging
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

import config

logger = logging.getLogger(__name__)

_RETRY_CONFIG = Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"})


def get_s3_client():
    # NOTE: config.SETTINGS를 매 호출 시점에 속성 조회한다 (모듈 임포트 시점에 값을 캡처하지 않음).
    #       테스트에서 config.SETTINGS를 교체해 환경을 바꿔 검증할 수 있게 하기 위함.
    settings = config.SETTINGS
    if settings.env == "local":
        return boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            region_name=settings.s3_region,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=_RETRY_CONFIG,
        )
    # AWS: endpoint 미지정 -> 기본 AWS S3, 자격증명은 IAM Role/기본 체인 사용
    return boto3.client("s3", region_name=settings.s3_region, config=_RETRY_CONFIG)


def ensure_bucket(bucket: str) -> None:
    """버킷이 없으면 생성한다. (로컬 LocalStack 최초 구동 시 필요. AWS는 이미 존재한다고 가정)"""
    s3 = get_s3_client()
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket in existing:
        return
    create_kwargs = {"Bucket": bucket}
    if config.SETTINGS.s3_region != "us-east-1":
        create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": config.SETTINGS.s3_region}
    s3.create_bucket(**create_kwargs)
    logger.info("S3 버킷 생성: %s", bucket)


def upload_file(local_path: Path, bucket: str, key: str) -> None:
    s3 = get_s3_client()
    s3.upload_file(str(local_path), bucket, key)
    logger.info("업로드 완료: %s -> s3://%s/%s", local_path, bucket, key)


def put_json(bucket: str, key: str, payload: dict) -> None:
    s3 = get_s3_client()
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body)


def put_text(bucket: str, key: str, text: str) -> None:
    """CSV 등 임의의 텍스트를 UTF-8로 저장한다 (put_json의 비-JSON 버전)."""
    s3 = get_s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))


def get_json(bucket: str, key: str) -> Optional[dict]:
    s3 = get_s3_client()
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchKey", "404"):
            return None
        raise