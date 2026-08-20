"""
서울 열린데이터광장 대용량 파일 다운로드 (초기 적재/백필용)

data.seoul.go.kr의 파일 목록 페이지는 자바스크립트로 렌더링되는 것처럼 보이지만,
실제로는 서버가 내려주는 HTML에 이미 (원본 파일명, seq) 쌍이 정적으로 박혀 있다
(예: `<span title="...대여이력...2601.csv" onclick="javascript:downloadFile('144');">`).
seq는 파일마다 부여된 내부 DB 일련번호라 파일명에서 계산할 수 없어 목록 페이지를
먼저 파싱해야 한다 (실측 확인 2026-08-19).

다운로드 자체는 GET이 아니라 frmFile 폼을 흉내낸 **POST**다:

    POST https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?&useCache=false
    body: infId=<데이터셋ID>&seqNo=&seq=<seq>&infSeq=1

로그인/세션/Referer 없이 동작하며, 성공 시 리다이렉트 없이 바로 200과
`Content-Disposition: attachment; filename="<원본파일명>"`(UTF-8 그대로), 정확한
`Content-Length`를 준다. `Range` 헤더는 서버가 무시하므로(항상 200 + 전체 바디)
이어받기는 불가능하다 - 실패하면 처음부터 다시 받는다.
"""
import logging
import os
import re
from pathlib import Path
from typing import Union

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

import config

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0"}
_LIST_ITEM_RE = re.compile(r'title="([^"]+)"\s+onclick="javascript:downloadFile\(\'(\d+)\'\);"')
_CHUNK_SIZE = 1024 * 1024


class FileDownloadError(Exception):
    """재시도해도 소용없는 실패 (응답이 파일이 아님, 목록을 찾을 수 없음 등)."""


class FileDownloadTransientError(Exception):
    """재시도 가능한 일시적 실패 (네트워크 오류, 5xx, 잘린 다운로드)."""


@retry(
    retry=retry_if_exception_type(FileDownloadTransientError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def _list_remote_files(dataset_id: str) -> dict:
    """목록 페이지 HTML에서 {파일명: seq}를 파싱한다."""
    url = f"{config.SETTINGS.seoul_data_list_base_url}/{dataset_id}/F/1/datasetView.do"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=30)
    except requests.RequestException as e:
        raise FileDownloadTransientError(str(e)) from e

    if resp.status_code == 429 or resp.status_code >= 500:
        raise FileDownloadTransientError(f"HTTP {resp.status_code}")
    resp.raise_for_status()

    matches = _LIST_ITEM_RE.findall(resp.text)
    if not matches:
        raise FileDownloadError(f"파일 목록을 찾을 수 없음 (데이터셋 페이지 구조 변경 의심): {url}")
    return dict(matches)


@retry(
    retry=retry_if_exception_type(FileDownloadTransientError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def _download_one(dataset_id: str, seq: str, filename: str, dest_dir: Path) -> None:
    url = f"{config.SETTINGS.seoul_data_file_base_url}?&useCache=false"
    payload = {"infId": dataset_id, "seqNo": "", "seq": seq, "infSeq": "1"}

    try:
        resp = requests.post(url, data=payload, headers=_HEADERS, stream=True, timeout=(10, 60))
    except requests.RequestException as e:
        raise FileDownloadTransientError(str(e)) from e

    if resp.status_code == 429 or resp.status_code >= 500:
        raise FileDownloadTransientError(f"HTTP {resp.status_code}")
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    content_disposition = resp.headers.get("Content-Disposition", "")
    if "attachment" not in content_disposition.lower() or "text/html" in content_type.lower():
        # 응답이 파일이 아니라 에러 페이지(인증 오류 등)인 경우 - 조용히 .csv로 저장되는 사고 방지
        raise FileDownloadError(
            f"{filename}: 파일이 아닌 응답 (Content-Type={content_type!r}, "
            f"Content-Disposition={content_disposition!r})"
        )

    expected_size = resp.headers.get("Content-Length")
    part_path = dest_dir / f".{filename}.part"
    if part_path.exists():
        part_path.unlink()

    written = 0
    with open(part_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
            if chunk:
                f.write(chunk)
                written += len(chunk)

    if expected_size is not None and written != int(expected_size):
        part_path.unlink(missing_ok=True)
        raise FileDownloadTransientError(
            f"{filename}: 다운로드 크기 불일치 (예상 {expected_size}bytes, 실제 {written}bytes) - 연결 끊김 의심"
        )

    os.replace(part_path, dest_dir / filename)
    logger.info("다운로드 완료: %s (%d bytes)", filename, written)


def ensure_backfill_files(dataset_id: str, dest_dir: Union[str, Path], file_pattern: str = "*") -> None:
    """
    dest_dir에 file_pattern에 맞는 파일이 없으면 열린데이터광장에서 받아 채워 넣는다.

    이미 로컬에 같은 파일명이 있으면 건드리지 않는다 - 기존에 수동으로 받아둔 파일이
    있는 환경에서는 그대로 재사용되고, 재다운로드가 일어나지 않는다.
    """
    import fnmatch

    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    remote_files = _list_remote_files(dataset_id)
    matched = {name: seq for name, seq in remote_files.items() if fnmatch.fnmatch(name, file_pattern)}

    for name, seq in sorted(matched.items()):
        if (dest_dir / name).exists():
            logger.info("이미 존재해 다운로드 스킵: %s", name)
            continue
        logger.info("다운로드 시작: %s (dataset=%s, seq=%s)", name, dataset_id, seq)
        _download_one(dataset_id, seq, name, dest_dir)
