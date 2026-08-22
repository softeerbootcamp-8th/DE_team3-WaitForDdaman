"""
S3 (LocalStack / AWS 공통) 유틸리티

boto3는 endpoint_url만 다르면 LocalStack과 실제 AWS S3를 동일한 코드로 다룰 수 있다.
로컬에서는 access key를 더미값으로 명시하고, AWS에서는 IAM Role을 쓰도록
자격증명을 아예 넘기지 않는다(boto3가 기본 자격증명 체인을 사용).
"""
import json
import logging
from pathlib import Path
from typing import Any, Optional

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
    """
    버킷이 없으면 생성한다. (로컬 LocalStack 최초 구동 시 필요. AWS는 이미 존재한다고 가정)

    목록 확인 후 생성하는 구조라 그 사이에 창이 있다. Bronze 일 배치의 원천별 태스크가
    병렬로 뜨면 여러 프로세스가 동시에 이 창을 통과해 같은 버킷을 만들려 하고, 진 쪽이
    BucketAlreadyOwnedByYou로 죽는다 (실측: LocalStack을 새로 띄운 직후 2ms 차이로 발생).
    "이미 내가 소유한 버킷이 있다"는 응답은 이 함수의 목적에서 보면 실패가 아니므로
    성공으로 처리한다.

    목록 확인 자체는 남겨둔다. 버킷이 이미 있는 평상시에 CreateBucket을 아예 호출하지
    않게 해서, 운영 IAM Role에 생성 권한이 없어도 동작하게 하기 위함이다.
    """
    s3 = get_s3_client()
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket in existing:
        return
    create_kwargs = {"Bucket": bucket}
    if config.SETTINGS.s3_region != "us-east-1":
        create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": config.SETTINGS.s3_region}

    try:
        s3.create_bucket(**create_kwargs)
    except ClientError as e:
        # BucketAlreadyExists(다른 계정이 그 전역 이름을 선점)는 일부러 그대로 올린다.
        # 그건 경쟁이 아니라 진짜 실패이고, 삼키면 뒤에서 AccessDenied로 더 헷갈리게 터진다.
        if e.response.get("Error", {}).get("Code") != "BucketAlreadyOwnedByYou":
            raise
        logger.info("S3 버킷이 이미 존재함 - 다른 잡이 먼저 생성: %s", bucket)
        return

    logger.info("S3 버킷 생성: %s", bucket)


def upload_file(local_path: Path, bucket: str, key: str) -> None:
    s3 = get_s3_client()
    s3.upload_file(str(local_path), bucket, key)
    logger.info("업로드 완료: %s -> s3://%s/%s", local_path, bucket, key)


def put_json(bucket: str, key: str, payload: Any) -> None:
    s3 = get_s3_client()
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body)


def put_text(bucket: str, key: str, text: str) -> None:
    """CSV 등 임의의 텍스트를 UTF-8로 저장한다 (put_json의 비-JSON 버전)."""
    s3 = get_s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))


def get_json(bucket: str, key: str) -> Optional[Any]:
    s3 = get_s3_client()
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchKey", "404"):
            return None
        raise


def list_keys(bucket: str, prefix: str) -> list[str]:
    """prefix 아래의 모든 객체 key를 페이지 누락 없이 정렬해 반환한다."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        item["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
    ]
    return sorted(keys)
