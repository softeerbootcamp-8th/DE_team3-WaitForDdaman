from pathlib import Path
from unittest.mock import patch

import pytest
import requests

from common.file_downloader import (
    FileDownloadError,
    FileDownloadTransientError,
    ensure_backfill_files,
)

# 실제 data.seoul.go.kr 목록 페이지 구조 축약본 (2026-08-19 실측 확인된 패턴)
LIST_HTML = """
<html><body>
<span title="서울특별시 공공자전거 대여이력 정보_2601.csv" onclick="javascript:downloadFile('144');">서울특별시 공공자전거 대여이력 정보_2601.csv</span>
<span title="서울특별시 공공자전거 대여이력 정보_2015.zip" onclick="javascript:downloadFile('84');">서울특별시 공공자전거 대여이력 정보_2015.zip</span>
</body></html>
"""


class _FakeListResp:
    def __init__(self, status_code=200, text=LIST_HTML):
        self.status_code = status_code
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


class _FakeDownloadResp:
    def __init__(self, status_code=200, headers=None, body=b""):
        self.status_code = status_code
        self.headers = headers or {}
        self._body = body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def iter_content(self, chunk_size=1024):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]


def _ok_download_resp(filename: str, body: bytes) -> _FakeDownloadResp:
    return _FakeDownloadResp(
        200,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": "application/x-msdownload",
            "Content-Length": str(len(body)),
        },
        body=body,
    )


def test_downloads_only_files_matching_pattern_using_scraped_seq(tmp_path):
    posted = []

    def fake_post(url, data, **kwargs):
        posted.append(data)
        return _ok_download_resp("서울특별시 공공자전거 대여이력 정보_2601.csv", b"csv-body")

    with patch("requests.get", return_value=_FakeListResp()), patch("requests.post", side_effect=fake_post):
        ensure_backfill_files("OA-15182", tmp_path, file_pattern="*2601*")

    assert len(posted) == 1
    assert posted[0]["seq"] == "144"  # 목록 페이지에서 파싱한 seq를 그대로 사용
    assert (tmp_path / "서울특별시 공공자전거 대여이력 정보_2601.csv").read_bytes() == b"csv-body"
    assert not (tmp_path / "서울특별시 공공자전거 대여이력 정보_2015.zip").exists()


def test_skips_files_already_present_locally(tmp_path):
    existing = tmp_path / "서울특별시 공공자전거 대여이력 정보_2601.csv"
    existing.write_bytes(b"already-here")

    with patch("requests.get", return_value=_FakeListResp()), patch("requests.post") as mock_post:
        ensure_backfill_files("OA-15182", tmp_path, file_pattern="*2601*")

    mock_post.assert_not_called()
    assert existing.read_bytes() == b"already-here"  # 재다운로드로 덮어쓰지 않음


def test_html_error_page_response_raises_and_does_not_write_file(tmp_path):
    html_error = _FakeDownloadResp(
        200,
        headers={"Content-Type": "text/html; charset=utf-8"},
        body=b"<html>\xec\x9d\xb8\xec\xa6\x9d\xed\x82\xa4 \xec\x98\xa4\xeb\xa5\x98</html>",
    )

    with patch("requests.get", return_value=_FakeListResp()), patch("requests.post", return_value=html_error):
        with pytest.raises(FileDownloadError):
            ensure_backfill_files("OA-15182", tmp_path, file_pattern="*2601*")

    assert list(tmp_path.iterdir()) == []  # 에러 페이지가 파일로 조용히 저장되면 안 됨


def test_truncated_download_size_mismatch_raises_after_retries(tmp_path):
    call_count = {"n": 0}

    def fake_post(url, data, **kwargs):
        call_count["n"] += 1
        # Content-Length는 100바이트라고 주장하지만 실제 바디는 5바이트만 옴 (연결 끊김 재현)
        return _FakeDownloadResp(
            200,
            headers={
                "Content-Disposition": 'attachment; filename="x.csv"',
                "Content-Type": "application/x-msdownload",
                "Content-Length": "100",
            },
            body=b"short",
        )

    with patch("requests.get", return_value=_FakeListResp()), patch(
        "requests.post", side_effect=fake_post
    ), patch("tenacity.nap.time.sleep", return_value=None):
        with pytest.raises(FileDownloadTransientError):
            ensure_backfill_files("OA-15182", tmp_path, file_pattern="*2601*")

    assert call_count["n"] == 3  # tenacity stop_after_attempt(3)
    assert list(tmp_path.iterdir()) == []  # 잘린 임시 파일이 안 남아야 함


def test_transient_5xx_recovers_after_retry(tmp_path):
    call_count = {"n": 0}

    def fake_post(url, data, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 2:
            return _FakeDownloadResp(503)
        return _ok_download_resp("서울특별시 공공자전거 대여이력 정보_2601.csv", b"csv-body")

    with patch("requests.get", return_value=_FakeListResp()), patch(
        "requests.post", side_effect=fake_post
    ), patch("tenacity.nap.time.sleep", return_value=None):
        ensure_backfill_files("OA-15182", tmp_path, file_pattern="*2601*")

    assert call_count["n"] == 2
    assert (tmp_path / "서울특별시 공공자전거 대여이력 정보_2601.csv").read_bytes() == b"csv-body"
