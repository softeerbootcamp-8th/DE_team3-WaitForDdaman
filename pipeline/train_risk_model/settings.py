"""설정 로딩과 저장소 접근.

설정 파일 경로는 RISK_MODEL_CONFIG 환경변수로 바꿀 수 있다.
경로가 s3:// 로 시작하면 boto3, 아니면 로컬 파일시스템을 쓴다.
LocalStack 은 AWS_ENDPOINT_URL 환경변수만 있으면 그대로 붙는다.
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG = "/opt/airflow/pylib/config/risk_model.yaml"


class Config(dict):
    """점 표기로 읽는 얕은 래퍼. cfg.get_path('run.window_days')"""

    def get_path(self, dotted: str, default: Any = None) -> Any:
        cur: Any = self
        for part in dotted.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur


def load_config(path: str | None = None) -> Config:
    path = path or os.environ.get("RISK_MODEL_CONFIG", DEFAULT_CONFIG)
    with open(path, "r", encoding="utf-8") as fh:
        return Config(yaml.safe_load(fh))


# ── 저장소 ────────────────────────────────────────────────────────────
def is_s3(uri: str) -> bool:
    return str(uri).startswith("s3://")


def _s3():
    import boto3

    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None)


def _split_s3(uri: str) -> tuple[str, str]:
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    return bucket, key


def write_bytes(uri: str, data: bytes) -> str:
    if is_s3(uri):
        bucket, key = _split_s3(uri)
        _s3().put_object(Bucket=bucket, Key=key, Body=data)
    else:
        p = Path(uri)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return uri


def read_bytes(uri: str) -> bytes:
    if is_s3(uri):
        bucket, key = _split_s3(uri)
        buf = io.BytesIO()
        _s3().download_fileobj(bucket, key, buf)
        return buf.getvalue()
    return Path(uri).read_bytes()


def exists(uri: str) -> bool:
    if is_s3(uri):
        import botocore.exceptions

        bucket, key = _split_s3(uri)
        try:
            _s3().head_object(Bucket=bucket, Key=key)
            return True
        except botocore.exceptions.ClientError:
            return False
    return Path(uri).exists()


def write_json(uri: str, obj: Any) -> str:
    return write_bytes(uri, json.dumps(obj, ensure_ascii=False, indent=2, default=str).encode("utf-8"))


def read_json(uri: str) -> Any:
    return json.loads(read_bytes(uri).decode("utf-8"))


def join_uri(base: str, *parts: str) -> str:
    return "/".join([str(base).rstrip("/"), *[str(p).strip("/") for p in parts]])
