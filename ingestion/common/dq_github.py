"""
DQ 이상 감지 시 GitHub 이슈 자동 생성/코멘트 (#217 2단계).

같은 원인(source_name+check_name+target_column)으로 매일 반복 실패해도 이슈가
계속 새로 생기면 안 되므로, fingerprint를 라벨로 박아두고 열린 이슈 중 동일
fingerprint가 있는지 먼저 검색한다. 있으면 코멘트만 추가하고, 없으면 새로 만든다.

이슈를 자동으로 닫는 로직은 절대 두지 않는다 - 원인이 실제로 해소됐는지는
사람이 판단해야 한다.
"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Callable, List, Optional, TypeVar

import requests

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"
FINGERPRINT_LABEL_PREFIX = "dq-fingerprint:"
AUTO_GENERATED_LABEL = "auto-generated"

T = TypeVar("T")


def compute_fingerprint(source_name: str, check_name: str, target_column: str) -> str:
    raw = f"{source_name}:{check_name}:{target_column}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class IssueResult:
    issue_number: int
    issue_url: str
    is_new: bool


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _with_retry(fn: Callable[[], T], attempts: int = 2) -> T:
    """1회 재시도 후에도 실패하면 예외를 그대로 올린다 - 호출부(잡)가 이 예외를
    잡아서 warning으로 격하시키고 파이프라인은 계속 진행한다(#217 안전장치)."""
    last_exc: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - 외부 API 호출 실패를 폭넓게 잡아 재시도
            last_exc = exc
            if attempt < attempts - 1:
                logger.warning("GitHub API 호출 실패, 재시도 %d/%d: %s", attempt + 1, attempts - 1, exc)
                time.sleep(1)
    raise last_exc  # type: ignore[misc]


def find_open_issue_by_fingerprint(repo: str, token: str, fingerprint: str) -> Optional[dict]:
    label = f"{FINGERPRINT_LABEL_PREFIX}{fingerprint}"
    query = f'repo:{repo} is:issue is:open label:"{label}"'

    def _search():
        resp = requests.get(
            f"{GITHUB_API_BASE}/search/issues",
            headers=_headers(token),
            params={"q": query},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    result = _with_retry(_search)
    items = result.get("items", [])
    return items[0] if items else None


def create_issue(repo: str, token: str, title: str, body: str, labels: List[str]) -> dict:
    def _create():
        resp = requests.post(
            f"{GITHUB_API_BASE}/repos/{repo}/issues",
            headers=_headers(token),
            json={"title": title, "body": body, "labels": labels},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    return _with_retry(_create)


def add_comment(repo: str, token: str, issue_number: int, body: str) -> dict:
    def _comment():
        resp = requests.post(
            f"{GITHUB_API_BASE}/repos/{repo}/issues/{issue_number}/comments",
            headers=_headers(token),
            json={"body": body},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    return _with_retry(_comment)


def report_issue(
    repo: str,
    token: str,
    source_name: str,
    check_name: str,
    target_column: str,
    severity: str,
    title: str,
    body_for_new_issue: str,
    body_for_comment: str,
) -> IssueResult:
    """열린 이슈 중 동일 fingerprint가 있으면 코멘트, 없으면 새 이슈를 만든다.

    라벨(auto-generated/severity:*/source:*/dq-fingerprint:*)은 GitHub의 이슈
    생성 API가 존재하지 않는 라벨을 자동으로 만들어주므로 사전에 리포에
    등록해둘 필요가 없다.
    """
    fingerprint = compute_fingerprint(source_name, check_name, target_column)
    existing = find_open_issue_by_fingerprint(repo, token, fingerprint)

    if existing is not None:
        add_comment(repo, token, existing["number"], body_for_comment)
        return IssueResult(issue_number=existing["number"], issue_url=existing["html_url"], is_new=False)

    labels = [
        AUTO_GENERATED_LABEL,
        f"severity:{severity}",
        f"source:{source_name}",
        f"{FINGERPRINT_LABEL_PREFIX}{fingerprint}",
    ]
    created = create_issue(repo, token, title, body_for_new_issue, labels)
    return IssueResult(issue_number=created["number"], issue_url=created["html_url"], is_new=True)
