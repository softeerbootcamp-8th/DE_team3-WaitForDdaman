"""
S3 (LocalStack / AWS 공통) 유틸리티

boto3는 endpoint_url만 다르면 LocalStack과 실제 AWS S3를 동일한 코드로 다룰 수 있다.
로컬에서는 access key를 더미값으로 명시하고, AWS에서는 IAM Role을 쓰도록
자격증명을 아예 넘기지 않는다(boto3가 기본 자격증명 체인을 사용).
"""
import functools
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

import config

logger = logging.getLogger(__name__)

_RETRY_CONFIG = Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"})


@functools.lru_cache(maxsize=None)
def _build_s3_client(env: str, endpoint_url: Optional[str], region: str, access_key: Optional[str], secret_key: Optional[str]):
    if env == "local":
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=_RETRY_CONFIG,
        )
    # AWS: endpoint 미지정 -> 기본 AWS S3, 자격증명은 IAM Role/기본 체인 사용
    return boto3.client("s3", region_name=region, config=_RETRY_CONFIG)


def get_s3_client():
    # NOTE: config.SETTINGS를 매 호출 시점에 속성 조회한다 (모듈 임포트 시점에 값을 캡처하지 않음).
    #       테스트에서 config.SETTINGS를 교체해 환경을 바꿔 검증할 수 있게 하기 위함. 캐시 키가
    #       그 값들 자체라서, 설정이 바뀌면 새 클라이언트를 만들고 안 바뀌면 재사용한다.
    settings = config.SETTINGS
    # AWS 분기 흐름을 LocalStack으로 검증할 때만 켠다. 운영 APP_ENV=aws에서는
    # 기본적으로 이 값이 꺼져 있으므로 IAM Role을 사용하는 기존 동작을 유지한다.
    aws_local_simulation = os.getenv("AWS_LOCAL_SIMULATION", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if settings.env == "local" or aws_local_simulation:
        return _build_s3_client(
            "local",
            settings.s3_endpoint,
            settings.s3_region,
            settings.s3_access_key,
            settings.s3_secret_key,
        )
    return _build_s3_client(settings.env, None, settings.s3_region, None, None)


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


def _md5_hex(path: Path) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def head_object(bucket: str, key: str) -> Optional[dict]:
    """key가 없으면 None. head_object 응답(Metadata 포함)을 그대로 반환한다."""
    s3 = get_s3_client()
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey"):
            return None
        raise


def upload_file_if_changed(local_path: Path, bucket: str, key: str) -> bool:
    """
    같은 key에 내용이 동일한 파일이 이미 있으면 업로드를 스킵하고 재사용한다 (멱등).

    S3 ETag는 멀티파트 업로드 시 각 파트 MD5의 조합 해시라서 로컬 파일 MD5와 직접
    비교할 수 없다. 대신 업로드할 때 로컬 MD5를 커스텀 메타데이터로 함께 저장해두고,
    다음 실행에서는 HEAD로 그 메타데이터만 비교한다 - 내용이 같으면 재업로드 없이
    기존 객체를 그대로 재사용하고, 다르면(혹은 메타데이터가 없으면) 덮어쓴다.
    """
    local_md5 = _md5_hex(local_path)
    existing = head_object(bucket, key)
    if existing is not None and existing.get("Metadata", {}).get("content-md5") == local_md5:
        logger.info("동일한 파일이 이미 존재해 업로드 스킵: s3://%s/%s", bucket, key)
        return False

    s3 = get_s3_client()
    s3.upload_file(str(local_path), bucket, key, ExtraArgs={"Metadata": {"content-md5": local_md5}})
    logger.info("업로드 완료(멱등 갱신): %s -> s3://%s/%s", local_path, bucket, key)
    return True


def copy_object(bucket: str, source_key: str, dest_key: str) -> None:
    """서버사이드 CopyObject. 데이터가 S3 내부에서만 이동해 EC2로 내려받거나 다시 올리지 않는다."""
    s3 = get_s3_client()
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": source_key}, Key=dest_key)
    logger.info("서버사이드 복사 완료: s3://%s/%s -> s3://%s/%s", bucket, source_key, bucket, dest_key)


def reuse_or_upload_staging_file(local_path: Path, bucket: str, key: str, legacy_key: str) -> bool:
    """
    초기 적재 스테이징 업로드 - deterministic key(#247)에 아직 아무 것도 없으면, 로컬
    MD5 계산이나 재업로드보다 먼저 legacy key(한글/공백 원본 파일명을 그대로 쓰던 #218
    이전 방식)가 이미 S3에 있는지 확인한다. 있으면 서버사이드 CopyObject로 그 내용을
    그대로 재사용한다(#255) - #247에서 ASCII 안전 key로 넘어가면서, 예전에 이미 다
    올려둔 반기 파일(최대 700MB급)들이 새 key 기준으로는 "없는 파일"처럼 보여 처음부터
    다시 내려받고/해시하고/올리게 되는 낭비를 막는다.

    legacy 재사용이 없는 일반 경우(신규 key도 legacy key도 없거나, 신규 key가 이미
    있어 변경 여부를 확인해야 하는 경우)는 그대로 upload_file_if_changed의 기존 멱등
    로직(로컬 MD5로 변경 여부 판단)을 따른다.

    NOTE: legacy에서 복사된 객체는 content-md5 메타데이터가 없다 - 바로 다음 재시도에서
    upload_file_if_changed가 로컬 MD5를 다시 계산해 1회 더 덮어쓸 수 있다. 이 1회성
    재확인 비용은 원래 문제(재다운로드+재해시+재업로드가 매 실행마다 반복되던 것)에
    비하면 훨씬 작아 감내할 만하다.
    """
    if head_object(bucket, key) is None:
        legacy = head_object(bucket, legacy_key)
        if legacy is not None:
            copy_object(bucket, legacy_key, key)
            return True
    return upload_file_if_changed(local_path, bucket, key)


def to_spark_readable_path(local_path: Path, bucket: str, staging_prefix: str) -> str:
    """EMR driver의 임시 파일을 Spark executor가 읽을 수 있는 S3A 경로로 올린다."""
    if config.SETTINGS.env != "aws":
        return str(local_path)
    key = f"{staging_prefix}/{local_path.name}"
    upload_file(local_path, bucket, key)
    return f"s3a://{bucket}/{key}"


def download_file(bucket: str, key: str, local_path: Path) -> None:
    s3 = get_s3_client()
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(local_path))
    logger.info("다운로드 완료: s3://%s/%s -> %s", bucket, key, local_path)


def split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"S3 URI가 아닙니다: {uri}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"S3 URI 형식이 올바르지 않습니다: {uri}")
    return bucket, key


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
