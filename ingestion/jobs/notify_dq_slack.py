"""
DQ 이상 감지 Slack 알림 (#217 2단계)

report_dq_issue.py가 남긴 결과(신규 이슈든 기존 이슈 코멘트든, 심지어 GitHub API가
실패한 경우까지)를 읽어 Slack에 알린다 - 사람이 매번 인지하게 하는 게 목적이라
GitHub 이슈 생성이 실패해도(=error 필드가 있어도) "실패했다"는 메시지를 보낸다.

Slack 전송 자체가 실패해도(웹훅 만료 등) 이 태스크를 실패시키지 않는다 - 알림
실패가 배치를 막으면 안 된다는 정책은 report_dq_issue.py와 동일(#217).

사용법:
    EXECUTION_DATE=2026-08-24 DQ_SOURCE_NAME=rental_history python -m jobs.notify_dq_slack
"""
import logging
import os
import time

import config
from common.dq_slack import build_message, send_dq_alert
from common.s3_utils import get_json
from jobs.report_dq_issue import github_issues_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _send_with_retry(webhook_url: str, message: str, attempts: int = 2) -> bool:
    last_exc = None
    for attempt in range(attempts):
        try:
            send_dq_alert(webhook_url, message)
            return True
        except Exception as exc:  # noqa: BLE001 - 웹훅 호출 실패로 배치를 죽이지 않는다
            last_exc = exc
            if attempt < attempts - 1:
                logger.warning("Slack 전송 실패, 재시도 %d/%d: %s", attempt + 1, attempts - 1, exc)
                time.sleep(1)
    logger.warning("Slack 전송 실패 (재시도 후에도 실패) - %s", last_exc)
    return False


def run(source_name: str | None = None, execution_date_str: str | None = None) -> int:
    source_name = source_name or os.environ.get("DQ_SOURCE_NAME", "rental_history")
    execution_date_str = execution_date_str or os.environ["EXECUTION_DATE"]

    bucket = config.SETTINGS.raw_bucket
    issues = get_json(bucket, github_issues_key(source_name, execution_date_str))
    if not issues:
        logger.info("%s/%s: 알릴 이슈 없음 - Slack 알림 스킵", source_name, execution_date_str)
        return 0

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL 미설정 - Slack 알림 스킵 (품질 이슈 알림 실패는 warning 처리)")
        return 0

    sent = 0
    for item in issues:
        message = build_message(
            source_name=item["source_name"],
            check_name=item["check_name"],
            severity=item.get("severity") or "warning",
            reasoning=item.get("reasoning") or "",
            issue_url=item.get("issue_url"),
            is_new=item.get("is_new"),
            error=item.get("error"),
        )
        if _send_with_retry(webhook_url, message):
            sent += 1

    logger.info("%s/%s: Slack 알림 %d/%d건 전송", source_name, execution_date_str, sent, len(issues))
    return sent


if __name__ == "__main__":
    run()
