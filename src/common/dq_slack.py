"""
DQ 이상 감지 Slack 알림 (#217 2단계) - GitHub 이슈 생성/코멘트 이후에 호출된다.

dag_common.py의 notify_slack_on_failure(태스크 실패 알림)와는 목적이 다른 별도
웹훅 알림이다 - 이건 "품질 이상"을 알리는 것이지 "태스크가 죽었다"를 알리는 게
아니라서, 굳이 하나로 합치지 않았다.
"""
from __future__ import annotations

import requests

_SEVERITY_EMOJI = {
    "critical": ":rotating_light:",
    "warning": ":warning:",
    "info": ":information_source:",
}


def build_message(
    source_name: str,
    check_name: str,
    severity: str,
    reasoning: str,
    issue_url: str | None,
    is_new: bool | None,
    error: str | None = None,
) -> str:
    emoji = _SEVERITY_EMOJI.get(severity, ":question:")

    if error:
        return (
            f":x: *[DQ] {source_name} - {check_name}* - GitHub 이슈 생성/코멘트 실패\n"
            f"{reasoning}\n"
            f"오류: {error} (수동 확인 필요)"
        )

    status = "신규 이슈" if is_new else "기존 이슈에 코멘트 추가"
    return (
        f"{emoji} *[DQ] {source_name} - {check_name}* ({status})\n"
        f"{reasoning}\n"
        f"{issue_url}"
    )


def send_dq_alert(webhook_url: str, message: str) -> None:
    resp = requests.post(webhook_url, json={"text": message}, timeout=10)
    resp.raise_for_status()
